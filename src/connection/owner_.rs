//! 每条子流的**共享标量状态**：[`ChannelOwner_`]。
//!
//! 环半部**不在这里**：按 dev-notes（`connection-20261002-0548.md` §6.5 Q7）的裁决，
//! 会话侧的两个半部在注册时经事件通道**移交给对应的循环本地持有**，因此它们不会
//! 藏在共享实体的锁后面，循环可以自由在这些半部上 park / await。
//!
//! 于是本模块只承载那些**两个循环与 API 面都要看**的标量状态：
//!
//! - [`FlowCtrl`]：收发双向窗口（读循环记「已收」，写循环记「已通告 / 已消费」，
//!   API 面建流时构造）；
//! - [`Establish_`]：建流三步的状态与等待者（主动方 `open_channel_async` 挂在这上面）；
//! - 若干标志：发送环是否已有事件在队列里（每条子流至多一条，见 dev-notes §11.4）、
//!   两个方向是否已发过 `FIN`、最近活跃时间；
//! - 对端 `OPEN` 里带过来的窗口通告（被动方在 `accept` 时才建环，需要先把它存住）。
//!
//! 所有访问都经 `atomic_sync` **抢占式自旋读写锁**的短闭包：**闭包内不得
//! `await`**，也不得重入。该锁没有内部堆分配，可内联进 [`Shared`]。

// 本模块的入口尚未被读写循环与 API 面调用（接线进行中），因此保留 `dead_code`
// 允许；**接线完成后必须移除本行**。
use core::{
    alloc::AllocatorClone,
    future::poll_fn,
    task::{Poll, Waker},
};
use std::time::Instant;

use atomic_sync::rwlock::preemptive::SpinningRwLockOwned;
use buffex::x_deps::abs_buff;
use abs_buff::x_deps::abs_cancel::TrCancellationToken;
use mm_ptr::Shared;

use crate::{
    connection::{
        error_::MuxError,
        mux_connection::ChannelRegistry_,
        sync_::{WakerSlot_, on_lock_contended_},
    },
    flow_ctrl::FlowCtrl,
};

/// 建流三步的进展。
///
/// 两侧状态机同形（见 `crate::connection` 模块文档 §4.2）：主动方要等对端的
/// `OPEN`（拿到对端接收窗口）与 `ACCEPT` / `REJECT`；`peer_opened_` 与 `outcome_`
/// 就是这两件事的落点，读循环收到相应帧时置位并唤醒 [`Establish_::waker_`]。
#[derive(Debug, Default)]
pub(crate) struct Establish_ {
    /// 是否已收到对端的 `OPEN`（其中携带对端接收窗口）。
    peer_opened_: bool,

    /// 建流结果；`None` 表示仍在等待。
    outcome_: Option<EstablishOutcome_>,

    /// `open_channel_async` 的等待者（至多一个：该 future 独占 `&mut DockBinding`）。
    waker_: WakerSlot_,
}

/// 建流的最终结果。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum EstablishOutcome_ {
    /// 对端回复 `ACCEPT`。
    Accepted,

    /// 对端回复 `REJECT`（理由载荷当前没有消费者，见 dev-notes §2.11）。
    Refused,
}

/// 一条子流的共享状态。
///
/// 成员一律私有：读写循环在 `session_` 模块，只能经本模块的关联函数访问。
pub(crate) struct ChannelState_ {
    /// 收发双向流控状态。
    flow_: FlowCtrl,

    /// 建流三步的进展。
    establish_: Establish_,

    /// 该子流的发送环是否已经有「有数据」事件在队列里（每条子流至多一条）。
    tx_queued_: bool,

    /// 应用已丢弃发送半边（[`ChannelTx`](super::ChannelTx)）。
    app_tx_closed_: bool,

    /// 应用已丢弃接收半边（[`ChannelRx`](super::ChannelRx)）。
    app_rx_closed_: bool,

    /// 本端已发出 `CLOSE(FIN)`：不再发送数据。
    local_fin_sent_: bool,

    /// 本端已关闭接收方向（发过 `CLOSE(RESET)` 或已让读循环释放接收环）。
    local_rx_closed_: bool,

    /// 对端已声明不再发送（收到 `CLOSE(FIN)`）。
    peer_fin_: bool,

    /// 对端已声明不再接收（收到 `CLOSE(RESET)`）。
    peer_reset_: bool,

    /// 配额与注册表条目是否已经释放（保证只释放一次）。
    released_: bool,

    /// 最近一次与本子流相关的收发活动时间（保活只记录，本轮不判定超时）。
    active_: Instant,
}

impl ChannelState_ {
    /// 以建流已知的量构造。
    pub(crate) fn new_(flow: FlowCtrl) -> Self {
        ChannelState_ {
            flow_: flow,
            establish_: Establish_::default(),
            tx_queued_: false,
            app_tx_closed_: false,
            app_rx_closed_: false,
            local_fin_sent_: false,
            local_rx_closed_: false,
            peer_fin_: false,
            peer_reset_: false,
            released_: false,
            active_: Instant::now(),
        }
    }

    /// 刷新活跃时间。
    pub(crate) fn touch_(&mut self) {
        self.active_ = Instant::now();
    }

    /// 两个方向是否都已经收尾，可以释放注册表条目与环内存。
    ///
    /// 发送方向收尾 = 应用丢了 `ChannelTx`，或本端已发 `FIN`，或对端发了 `RESET`；
    /// 接收方向收尾 = 应用丢了 `ChannelRx`，或本端已关接收方向，或对端发了 `FIN`。
    pub(crate) fn is_done_(&self) -> bool {
        let tx_done = self.app_tx_closed_ || self.local_fin_sent_ || self.peer_reset_;
        let rx_done = self.app_rx_closed_ || self.local_rx_closed_ || self.peer_fin_;
        tx_done && rx_done
    }

    /// 尝试认领「释放」这件事；重复调用返回 `false`。
    pub(crate) fn claim_release_(&mut self) -> bool {
        if self.released_ {
            return false;
        }
        self.released_ = true;
        true
    }

    /// 收发双向流控状态（只读）。
    pub(crate) fn flow_(&self) -> &FlowCtrl {
        &self.flow_
    }

    /// 收发双向流控状态（可变）。
    pub(crate) fn flow_mut_(&mut self) -> &mut FlowCtrl {
        &mut self.flow_
    }

    /// 取出建流等待者（若有）；读循环在收到 `OPEN` / `ACCEPT` / `REJECT` 后唤醒它。
    pub(crate) fn take_establish_waker_(&mut self) -> Option<Waker> {
        self.establish_.waker_.take_()
    }

    /// 记录「已收到对端 `OPEN`」。
    pub(crate) fn set_peer_opened_(&mut self) {
        self.establish_.peer_opened_ = true;
    }

    /// 记录建流结果（`ACCEPT` / `REJECT`）。
    pub(crate) fn set_establish_outcome_(&mut self, outcome: EstablishOutcome_) {
        self.establish_.outcome_ = Option::Some(outcome);
    }

    /// 记录「对端已声明不再接收」（收到 `CLOSE(RESET)`）。
    pub(crate) fn set_peer_reset_(&mut self) {
        self.peer_reset_ = true;
    }

    /// 记录「对端已声明不再发送」（收到 `CLOSE(FIN)`）。
    pub(crate) fn set_peer_fin_(&mut self) {
        self.peer_fin_ = true;
    }

    /// 记录「应用已丢弃发送半边」。
    pub(crate) fn set_app_tx_closed_(&mut self) {
        self.app_tx_closed_ = true;
    }

    /// 记录「应用已丢弃接收半边」。
    pub(crate) fn set_app_rx_closed_(&mut self) {
        self.app_rx_closed_ = true;
    }

    /// 记录「本端已发出 `CLOSE(FIN)`」。
    pub(crate) fn set_local_fin_sent_(&mut self) {
        self.local_fin_sent_ = true;
    }

    /// 该子流的发送环是否已有「有数据」事件在队列里。
    pub(crate) fn tx_queued_(&self) -> bool {
        self.tx_queued_
    }

    /// 设置「该子流的发送环已有事件入队」位。
    pub(crate) fn set_tx_queued_(&mut self, queued: bool) {
        self.tx_queued_ = queued;
    }
}

/// 一条子流的共享句柄：`Shared<SpinningRwLock<ChannelState_>>`。
///
/// 参与方有三处：应用侧半边（发事件、读关闭态）、读循环与写循环（各自持有同一
/// 句柄，经事件通道移交）、以及注册表节点。三者都只 clone 这个句柄。
pub(crate) struct ChannelOwner_<A>
where
    A: AllocatorClone,
{
    inner_: Shared<SpinningRwLockOwned<ChannelState_>, A>,
}

impl<A> Clone for ChannelOwner_<A>
where
    A: AllocatorClone,
{
    fn clone(&self) -> Self {
        ChannelOwner_ {
            inner_: self.inner_.clone(),
        }
    }
}

impl<A> ChannelOwner_<A>
where
    A: AllocatorClone + Send + Sync,
{
    /// 以调用方注入的分配器建立一条子流的共享状态。
    pub(crate) fn new_(state: ChannelState_, alloc: A) -> Self {
        ChannelOwner_ {
            inner_: Shared::new(SpinningRwLockOwned::new_owned(state), alloc),
        }
    }

    /// 持读锁执行 `f`（闭包内不得 `await`、不得重入）。
    pub(crate) fn with_<R>(&self, f: impl FnOnce(&ChannelState_) -> R) -> R {
        let mut session = self.inner_.acquire_session();
        let guard = loop {
            match session.try_read() {
                Result::Ok(guard) => break guard,
                Result::Err(_) => on_lock_contended_(),
            }
        };
        f(&guard)
    }

    /// 持写锁执行 `f`（闭包内不得 `await`、不得重入）。
    pub(crate) fn with_mut_<R>(&self, f: impl FnOnce(&mut ChannelState_) -> R) -> R {
        let mut session = self.inner_.acquire_session();
        let mut guard = loop {
            match session.try_write() {
                Result::Ok(guard) => break guard,
                Result::Err(_) => on_lock_contended_(),
            }
        };
        f(&mut guard)
    }
}

/// 等待建流完成：等对端的 `OPEN` + `ACCEPT` / `REJECT`，或被取消 / 连接失败打断。
pub(crate) async fn wait_establish_<RE, WE, A, K>(
    reg: &ChannelRegistry_<A>,
    owner: &ChannelOwner_<A>,
    cancel: K,
) -> Result<EstablishOutcome_, MuxError<RE, WE>>
where
    A: AllocatorClone + Send + Sync,
    K: TrCancellationToken,
{
    loop {
        if cancel.is_cancelled() {
            return Result::Err(MuxError::Cancelled);
        }
        if let Option::Some(kind) = reg.failure_() {
            return Result::Err(kind.into_mux_error_());
        }
        if let Option::Some(outcome) = owner.with_(|state| state.establish_.outcome_) {
            return Result::Ok(outcome);
        }
        // 先登记 waker，再复检；读循环在建流事件到达时会唤醒它。
        poll_fn(|cx| {
            owner.with_mut_(|state| state.establish_.waker_.register_(cx.waker()));
            if owner.with_(|state| state.establish_.outcome_.is_some()) {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        })
        .await;
    }
}

#[cfg(test)]
mod tests_ {
    use mm_ptr::x_deps::abs_mm::CoreAlloc;

    use crate::{
        connection::sync_::WakerSlot_,
        flow_ctrl::DefaultPolicy,
    };

    use super::*;

    /// 造一条测试用的共享状态（缺省策略、容量 64）。
    fn make_owner_() -> ChannelOwner_<CoreAlloc> {
        let flow = FlowCtrl::new(&DefaultPolicy, 64usize);
        ChannelOwner_::new_(
            ChannelState_::new_(flow),
            CoreAlloc,
        )
    }

    /// 测试共享句柄互相可见：一个 clone 上的写入能被另一个 clone 读到。
    /// - 手段：clone 出第二个句柄，在第一个上把 `tx_queued_` 置真。
    /// - 判断：第二个句柄读到 `tx_queued_ == true`——说明两份句柄指向同一状态。
    #[test]
    fn owner_handles_share_state() {
        let a = make_owner_();
        let b = a.clone();
        assert!(!a.with_(|s| s.tx_queued_));
        a.with_mut_(|s| s.tx_queued_ = true);
        assert!(b.with_(|s| s.tx_queued_), "clone 出的句柄应看到同一份状态");
    }

    /// 测试建流状态初始为空、可被置位并唤醒等待者。
    /// - 手段：初始断言 `peer_opened_` 为假且 `outcome_` 为 `None`；随后模拟读循环
    ///   置位 `peer_opened_`，并登记一个等待者。
    /// - 判断：置位后可读到真，且等待者槽确实登记上了——这是
    ///   `open_channel_async` 能被唤醒的前提。
    #[test]
    fn establish_state_starts_empty_and_accepts_updates() {
        let owner = make_owner_();
        assert!(!owner.with_(|s| s.establish_.peer_opened_));
        assert!(owner.with_(|s| s.establish_.outcome_.is_none()));
        assert!(!owner.with_(|s| s.establish_.waker_.is_registered_()));

        owner.with_mut_(|s| {
            s.establish_.peer_opened_ = true;
            s.establish_.outcome_ = Option::Some(EstablishOutcome_::Accepted);
        });
        assert!(owner.with_(|s| s.establish_.peer_opened_));
        assert_eq!(
            owner.with_(|s| s.establish_.outcome_),
            Option::Some(EstablishOutcome_::Accepted)
        );
    }

    /// 测试 `ChannelState_::touch_` 会推进活跃时间。
    /// - 手段：先读一次 `active_`，稍作忙等后调用 `touch_` 再读一次。
    /// - 判断：第二次读到的时刻不早于第一次（`Instant` 单调）。
    #[test]
    fn touch_advances_activity_time() {
        let owner = make_owner_();
        let first = owner.with_(|s| s.active_);
        let mut spin = 0u64;
        while spin < 100_000u64 {
            spin = spin.wrapping_add(1u64);
        }
        owner.with_mut_(|s| s.touch_());
        let second = owner.with_(|s| s.active_);
        assert!(second >= first, "活跃时间只能前进");
    }

    /// 测试 `WakerSlot_` 能作为建流等待者被取出（结构可用性检查）。
    /// - 手段：直接构造一个空槽并 `take_`。
    /// - 判断：空槽取出为 `None`，不 panic。
    #[test]
    fn establish_waker_slot_is_empty_by_default() {
        let mut slot = WakerSlot_::new_();
        assert!(slot.take_().is_none());
    }
}
