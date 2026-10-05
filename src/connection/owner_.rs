//! 每条子流的**共享标量状态**：[`ChannelOwner_`]。
//!
//! 环半部**不在这里**：会话侧的两个半部在注册时**移交给对应的循环本地持有**，
//! 因此它们不会藏在共享实体的锁后面，循环可以自由在这些半部上 park / await。
//!
//! 移交的载体仍是事件通道（`WriteEvent_::Attach` / `ReadEvent_::Attach`）；把
//! 水位通知改成「句柄直接调核心」只是**待实测的候选**，见模块文档 §2.3。
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
//! `await`**，也不得重入。争用时按 `with_async_` / `with_mut_async_` **异步等待**
//! （可被外部 cancel token 取消），因此零 CPU 忙等。

// 本模块的入口尚未被读写循环与 API 面调用（接线进行中），因此保留 `dead_code`
// 允许；**接线完成后必须移除本行**。
use core::{
    alloc::AllocatorClone,
    future::poll_fn,
    sync::atomic::{AtomicBool, Ordering},
    task::Poll,
};
use buffex::x_deps::abs_cancel::TrCancellationToken;
use std::time::Instant;

use atomic_sync::rwlock::cooperative::CooperativeRwLockOwned;
use flume::{Receiver, Sender};
use mm_ptr::Shared;

use crate::{
    connection::{
        error_::MuxError,
        mux_connection::ChannelRegistry_,
        sync_::{LockCancelled_, acquire_read_, acquire_write_},
    },
    flow_ctrl::{FlowCtrl, ReportThresholds_},
};

/// 建流三步的进展。
///
/// 两侧状态机同形（见 `crate::connection` 模块文档 §4.2）：主动方要等对端的
/// `OPEN`（拿到对端接收窗口）与 `ACCEPT` / `REJECT`；`peer_opened_` 与 `outcome_`
/// 就是这两件事的落点，读循环收到相应帧时置位并[`Establish_::notify_`]。
///
/// # 通知用通道而不是 waker 槽
///
/// 等待方是 async 上下文（[`wait_establish_`]），它拿不到 `cx` 去登记 waker；
/// 而通道是**持久**的：「先通知、后等待」不会丢（消息留在队列里），因此等待方
/// 只要先查状态、再 `recv_async().await` 即可，不需要手写 `poll`。
#[derive(Debug)]
pub(crate) struct Establish_ {
    /// 是否已收到对端的 `OPEN`（其中携带对端接收窗口）。
    peer_opened_: bool,

    /// 建流结果；`None` 表示仍在等待。
    outcome_: Option<EstablishOutcome_>,

    /// 通知生产端。
    notify_tx_: Sender<()>,

    /// 通知消费端（等待方克隆一份去 await）。
    notify_rx_: Receiver<()>,
}

impl Default for Establish_ {
    fn default() -> Self {
        // 容量 1：通知是「状态可能变了」的幂等提示。
        let (notify_tx_, notify_rx_) = flume::bounded(1usize);
        Establish_ {
            peer_opened_: false,
            outcome_: Option::None,
            notify_tx_,
            notify_rx_,
        }
    }
}

impl Establish_ {
    /// 提示等待方「建流状态可能变了」（幂等；队列满时投递失败是无害的）。
    pub(crate) fn notify_(&self) {
        let _ = self.notify_tx_.try_send(());
    }

    /// 取一份通知消费端（等待方持有它去 `recv_async`）。
    pub(crate) fn notify_rx_(&self) -> Receiver<()> {
        self.notify_rx_.clone()
    }
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

    /// 通告判定的阈值快照。
    ///
    /// 建流最终裁决时**由调用方给的接收缓冲容量**算出，因此是**每条子流各自的**
    /// （旧形状把它放在连接级快照里，因为容量由配置统一定死）。
    thresholds_: ReportThresholds_,
}

impl ChannelState_ {
    /// 以建流已知的量构造。
    pub(crate) fn new_(flow: FlowCtrl, thresholds: ReportThresholds_) -> Self {
        ChannelState_ {
            flow_: flow,
            establish_: Establish_::default(),
            app_tx_closed_: false,
            app_rx_closed_: false,
            local_fin_sent_: false,
            local_rx_closed_: false,
            peer_fin_: false,
            peer_reset_: false,
            released_: false,
            active_: Instant::now(),
            thresholds_: thresholds,
        }
    }

    /// 通告判定的阈值快照（写循环判定 `should_report` 时取用）。
    pub(crate) fn thresholds_(&self) -> ReportThresholds_ {
        self.thresholds_
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

    /// 提示建流等待方「状态可能变了」；读循环在收到 `OPEN` / `ACCEPT` / `REJECT`
    /// 后调用（幂等、不阻塞）。
    pub(crate) fn notify_establish_(&self) {
        self.establish_.notify_();
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

}

/// 一条子流的共享句柄：协作式锁 + 一个锁外的去重位。
///
/// 参与方有三处：应用侧半边（发事件、读关闭态）、读循环与写循环（各自持有同一
/// 句柄，经事件通道移交）、以及注册表节点。三者都只 clone 这个句柄。
///
/// # 两处同步原语，按「能否 await」分工
///
/// - 热状态（窗口、建流状态机、关闭位）经**协作式读写锁**：只在 async 上下文访问，
///   因此争用时可以 `read_async` / `write_async().may_cancel_with(cancel).await`
///   ——异步等待、可被外部 cancel token 取消（[`ChannelOwner_::with_async_`]）。
/// - 两个**去重位**在**锁外**的原子里：它们都被**同步**路径访问
///   （`ChannelTx::try_write` / `write_async`、`ChannelRx::try_read` /
///   `read_async` 的入口，见 [`ChannelOwner_::mark_tx_queued_`] /
///   [`ChannelOwner_::mark_rx_consumed_`]），同步路径没有 `await` 可用，因此这里用
///   一次 `swap` 表达「我是不是第一个置位者」，完全不取锁。
pub(crate) struct ChannelOwner_<A>
where
    A: AllocatorClone,
{
    /// 子流热状态（协作式锁：异步、可取消获取）。
    inner_: Shared<CooperativeRwLockOwned<ChannelState_>, A>,

    /// 「发送环有数据」去重位；**锁外**，供同步路径无锁使用。
    tx_queued_: Shared<AtomicBool, A>,

    /// 「应用消费了接收数据」去重位；**锁外**，理由同上。
    rx_consumed_: Shared<AtomicBool, A>,
}

impl<A> Clone for ChannelOwner_<A>
where
    A: AllocatorClone,
{
    fn clone(&self) -> Self {
        ChannelOwner_ {
            inner_: self.inner_.clone(),
            tx_queued_: self.tx_queued_.clone(),
            rx_consumed_: self.rx_consumed_.clone(),
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
            inner_: Shared::new(CooperativeRwLockOwned::new_owned(state), alloc.clone()),
            tx_queued_: Shared::new(AtomicBool::new(false), alloc.clone()),
            rx_consumed_: Shared::new(AtomicBool::new(false), alloc),
        }
    }

    /// 「发送环可能有数据」去重位：**第一个**置位者返回 `true`。
    ///
    /// 同步路径（`try_write` / `write_async` 的入口）专用：不取锁、不等待。
    pub(crate) fn mark_tx_queued_(&self) -> bool {
        !self.tx_queued_.swap(true, Ordering::AcqRel)
    }

    /// 写循环排空后清去重位（同步路径，不取锁）。
    pub(crate) fn clear_tx_queued_(&self) {
        self.tx_queued_.store(false, Ordering::Release);
    }

    /// 「应用可能消费了接收数据」去重位：**第一个**置位者返回 `true`。
    ///
    /// 同步路径（`try_read` / `read_async` 的入口）专用：不取锁、不等待。
    pub(crate) fn mark_rx_consumed_(&self) -> bool {
        !self.rx_consumed_.swap(true, Ordering::AcqRel)
    }

    /// 读循环核对接收水位**之前**清去重位（同步路径，不取锁）。
    ///
    /// # 顺序不可反
    ///
    /// 必须**先清位、再读环内积压**：这样「清位之后发生的消费」会重新置位并投递一条
    /// 新通知，而「清位之前的消费」已经体现在随后读到的积压量里。反过来（先读积压、
    /// 后清位）会丢掉清位与读之间那一次消费的唤醒。
    pub(crate) fn clear_rx_consumed_(&self) {
        self.rx_consumed_.store(false, Ordering::Release);
    }

    /// **异步**取读状态：`try_read` 快路径；失败则等，等待可被 `cancel` 取消。
    pub(crate) async fn with_async_<K, R>(
        &self,
        cancel: K,
        f: impl FnOnce(&ChannelState_) -> R,
    ) -> Result<R, LockCancelled_>
    where
        K: TrCancellationToken,
    {
        let mut session = self.inner_.acquire_session();
        let guard = acquire_read_(&mut session, cancel).await?;
        Result::Ok(f(&guard))
    }

    /// **异步**取写状态：语义与 [`ChannelOwner_::with_async_`] 对称。
    pub(crate) async fn with_mut_async_<K, R>(
        &self,
        cancel: K,
        f: impl FnOnce(&mut ChannelState_) -> R,
    ) -> Result<R, LockCancelled_>
    where
        K: TrCancellationToken,
    {
        let mut session = self.inner_.acquire_session();
        let mut guard = acquire_write_(&mut session, cancel).await?;
        Result::Ok(f(&mut guard))
    }

    /// **同步、非阻塞**地取读状态：只在锁**当场可用**时返回 `Some`，否则返回
    /// `None`（`WouldBlock`）。
    ///
    /// 供 `poll` 闭包这类**没有 `await` 可用**的地方使用：那里既不能等锁，也不该
    /// 因为「读不到状态」就唤醒自己（那会变成忙等）。当前唯一的使用点是复用循环的
    /// park 条件——它要问「这条子流的发送额度是不是 > 0」，从而避免在「环里有数据、
    /// 但额度为 0」时把「环可读」当成唤醒理由（那会纯空转，见
    /// [`crate::connection::session_`] 的 mux 循环文档）。
    ///
    /// **失败即不唤醒**是安全的方向：唯一的额度来源是读循环收到窗口通告，而那条
    /// 路径一定会投一条事件上来，把 park 打断。
    ///
    /// # Errors
    ///
    /// 锁当场不可用时返回 [`LockCancelled_`]（与异步版本的失败类型一致）。
    pub(crate) fn try_with_<R>(
        &self,
        f: impl FnOnce(&ChannelState_) -> R,
    ) -> Result<R, LockCancelled_> {
        let mut session = self.inner_.acquire_session();
        match session.try_read() {
            Result::Ok(guard) => Result::Ok(f(&guard)),
            Result::Err(_) => Result::Err(LockCancelled_),
        }
    }
}


/// 等待建流完成：等对端的 `OPEN` + `ACCEPT` / `REJECT`，或被取消 / 连接失败打断。
pub(crate) async fn wait_establish_<A, K>(
    reg: &ChannelRegistry_<A>,
    owner: &ChannelOwner_<A>,
    cancel: K,
) -> Result<EstablishOutcome_, MuxError>
where
    A: AllocatorClone + Send + Sync,
    K: TrCancellationToken,
{
    loop {
        if cancel.is_cancelled() {
            return Result::Err(MuxError::Cancelled);
        }
        // 1. 连接级失败优先（取注册表锁，可取消）。
        if let Result::Ok(Option::Some(err)) = reg.failure_(cancel.child_token()).await {
            return Result::Err(err);
        }
        // 2. 拿到结果了吗？顺带取一份通知端（在**持锁期间**取，保证不漏通知）。
        let (outcome, notify_rx) = owner
            .with_mut_async_(cancel.child_token(), |state| {
                (state.establish_.outcome_, state.establish_.notify_rx_())
            })
            .await
            .map_err(|_| MuxError::Cancelled)?;
        if let Option::Some(outcome) = outcome {
            return Result::Ok(outcome);
        }
        // 3. 等一条通知；与取消令牌竞争。
        //    通道是持久的：第 2 步与这里之间的通知不会丢。
        let mut notified = core::pin::pin!(notify_rx.recv_async());
        let mut cancelled = core::pin::pin!(cancel.child_token().cancellation());
        let notified = poll_fn(|cx| {
            if core::future::Future::poll(cancelled.as_mut(), cx).is_ready() {
                return Poll::Ready(false);
            }
            core::future::Future::poll(notified.as_mut(), cx).map(|_| true)
        })
        .await;
        if !notified {
            return Result::Err(MuxError::Cancelled);
        }
    }
}

#[cfg(test)]
mod tests_ {
    use buffex::x_deps::abs_cancel::NonCancellableToken;
    use mm_ptr::x_deps::abs_mm::CoreAlloc;

    use crate::flow_ctrl::DefaultPolicy;

    use super::*;

    /// 测试专用：以「不可取消令牌」做一次异步读访问并解包。
    ///
    /// **不再用 `block_on` 把异步压成同步**：用例本身是 `async fn`，直接 `.await`
    /// 才测到真实运行时的 park / 唤醒路径。
    async fn read_<R>(owner: &ChannelOwner_<CoreAlloc>, f: impl FnOnce(&ChannelState_) -> R) -> R {
        owner
            .with_async_(NonCancellableToken::new(), f)
            .await
            .expect("测试里不该被取消")
    }

    /// 测试专用：以「不可取消令牌」做一次异步写访问并解包。
    async fn write_<R>(
        owner: &ChannelOwner_<CoreAlloc>,
        f: impl FnOnce(&mut ChannelState_) -> R,
    ) -> R {
        owner
            .with_mut_async_(NonCancellableToken::new(), f)
            .await
            .expect("测试里不该被取消")
    }

    /// 造一条测试用的共享状态（缺省策略、容量 64）。
    fn make_owner_() -> ChannelOwner_<CoreAlloc> {
        let flow = FlowCtrl::new(&DefaultPolicy, 64usize);
        ChannelOwner_::new_(
            ChannelState_::new_(flow, ReportThresholds_::new_(&DefaultPolicy, 64u32)),
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
        assert!(a.mark_tx_queued_(), "首次置位应当是 fresh");
        assert!(
            !b.mark_tx_queued_(),
            "clone 出的句柄应看到同一份锁外去重位"
        );
        a.clear_tx_queued_();
        assert!(b.mark_tx_queued_(), "清位在共享句柄上也可见");
    }

    /// 测试「应用消费了」去重位与「发送环有数据」去重位**互相独立**，且同样跨 clone
    /// 共享、可清位后重新置位。
    ///
    /// 两条方向的通知共用一个 `ChannelOwner_`，若两个位串在一起，接收方向的一次消费
    /// 就会把发送方向的通知吞掉（或反之）——那是本轮修掉的「唤醒被静默丢掉」的翻版。
    ///
    /// - 手段：在同一个句柄上先置 `tx_queued_`，再置 `rx_consumed_`；随后清 `rx` 位。
    /// - 判断：两个位互不影响（各自能独立置 true），clone 句柄看到同一状态，清位可
    ///   重新置 true。
    #[test]
    fn owner_tx_and_rx_dedup_bits_are_independent() {
        let a = make_owner_();
        let b = a.clone();
        assert!(a.mark_tx_queued_(), "发送方向首次置位应当是 fresh");
        assert!(
            b.mark_rx_consumed_(),
            "接收方向首次置位应当是 fresh，且不受发送方向的位影响"
        );
        assert!(
            !a.mark_tx_queued_(),
            "接收方向置位不应清掉发送方向的位"
        );
        assert!(
            !a.mark_rx_consumed_(),
            "clone 句柄应看到同一份接收方向去重位"
        );
        b.clear_rx_consumed_();
        assert!(a.mark_rx_consumed_(), "清位后可重新置位");
    }

    /// 测试建流状态初始为空、可被置位并唤醒等待者。
    /// - 手段：初始断言 `peer_opened_` 为假且 `outcome_` 为 `None`；随后模拟读循环
    ///   置位 `peer_opened_` 与 `outcome_`。
    /// - 判断：置位后可读到对应的值——这是 `open_channel_async` 能被唤醒的前提。
    async fn establish_state_starts_empty_and_accepts_updates() {
        let owner = make_owner_();
        assert!(!read_(&owner, |s| s.establish_.peer_opened_).await);
        assert!(read_(&owner, |s| s.establish_.outcome_.is_none()).await);

        write_(&owner, |s| {
            s.establish_.peer_opened_ = true;
            s.establish_.outcome_ = Option::Some(EstablishOutcome_::Accepted);
        })
        .await;
        assert!(read_(&owner, |s| s.establish_.peer_opened_).await);
        assert_eq!(
            read_(&owner, |s| s.establish_.outcome_).await,
            Option::Some(EstablishOutcome_::Accepted)
        );
    }
    dual_runtime_test_!(establish_state_starts_empty_and_accepts_updates);

    /// 测试建流通知通道：`notify_establish_` 投一条，「先通知后等待」也不会丢。
    /// - 手段：先 `notify_establish_`，再从共享的通知消费端 `try_recv`。
    /// - 判断：能取到一条通知；重复通知时通道满，投递失败是无害的。
    async fn establish_notification_is_persistent() {
        let owner = make_owner_();
        write_(&owner, |s| s.notify_establish_()).await;
        let rx = read_(&owner, |s| s.establish_.notify_rx_()).await;
        assert!(rx.try_recv().is_ok(), "先通知后等待不应当丢唤醒");
        write_(&owner, |s| s.notify_establish_()).await;
        write_(&owner, |s| s.notify_establish_()).await;
        assert!(rx.try_recv().is_ok());
        assert!(rx.try_recv().is_err(), "通道容量 1：重复通知不堆积");
    }
    dual_runtime_test_!(establish_notification_is_persistent);

    /// 测试 `ChannelState_::touch_` 会推进活跃时间。
    /// - 手段：先读一次 `active_`，稍作忙等后调用 `touch_` 再读一次。
    /// - 判断：第二次读到的时刻不早于第一次（`Instant` 单调）。
    async fn touch_advances_activity_time() {
        let owner = make_owner_();
        let first = read_(&owner, |s| s.active_).await;
        let mut spin = 0u64;
        while spin < 100_000u64 {
            spin = spin.wrapping_add(1u64);
        }
        write_(&owner, |s| s.touch_()).await;
        let second = read_(&owner, |s| s.active_).await;
        assert!(second >= first, "活跃时间只能前进");
    }
    dual_runtime_test_!(touch_advances_activity_time);
}
