//! 连接的共享状态层：读写循环与 API 面之间的唯一共享点。
//!
//! 本模块承载三样东西：
//!
//! - [`CancelToken_`]：**可主动触发**的取消令牌；
//! - [`FailKind_`]：连接级失败的「载荷无关」投影，连同失败标志；
//! - [`WakerSlot_`]：单等待者唤醒槽。
//!
//! 注册表（dock / 子流索引与配额）已移到 `mux_connection::registry_`。
//!
//! # 共享单元：直接用 `atomic_sync` 的读写锁
//!
//! 本模块不再手搓「内部可变性单元」。需要共享可变状态的两处都直接用
//! `atomic_sync` 的锁，按各自需求选型：
//!
//! - `mux_connection::registry_` 的注册表用**协作式读写锁**
//!   （`rwlock::cooperative::CooperativeRwLock`）：临界区同样是短闭包，但它保留
//!   了 `read_async` / `write_async`，便于后续把需要跨 `await` 持锁的路径迁进来；
//! - [`CancelToken_`] 的内部状态用**抢占式自旋读写锁**
//!   （`rwlock::preemptive::SpinningRwLock`）：`TrCancellationToken` 要求
//!   `Send + Sync`，而该锁零内部堆分配、可内联进 `Shared`，符合本 crate
//!   「不隐式分配」的纪律；每条子流的 `ChannelOwner_` 也在 `owner_` 里用它。
//!
//! 两者都只取同步快路径（`try_read` / `try_write`），因此**闭包内不得 `await`、
//! 不得重入**：连接与全部句柄都是单线程对象，取不到锁只可能是重入，因此直接
//! panic（见 [`on_lock_contended_`]）。
//!
//! # 唤醒
//!
//! [`WakerSlot_`] 是**单等待者**唤醒槽。本 crate 的每个等待点都天然至多有一个
//! 等待者（`income_async` / `accept_async` / `open_channel_async` / telegraph 收发
//! 都取 `&mut self`），因此一个槽就够；唤醒时先把 waker **取出**，等释放锁之后
//! 再 `wake()`，避免唤醒路径重入本层造成自锁。
//!
//! # 分配
//!
//! 根对象在连接建立时分配一次，dock / 子流索引节点在登记时**按需**用调用方注入
//! 的分配器分配、在释放时归还；本 crate 不隐式分配、不隐藏内存预算。


// 本模块的入口尚未被读写循环与 API 面调用（第 7、8 步接线），因此这里保留
// `dead_code` 允许；**接线完成后必须移除本行**。
use core::{
    alloc::AllocatorClone,
    future::Future,
    pin::Pin,
    task::{Context, Poll, Waker},
};

use abs_cancel::TrCancellationToken;
use atomic_sync::rwlock::preemptive::SpinningRwLockOwned;
use buffex::x_deps::{abs_cancel, atomic_sync};
use mm_ptr::Shared;

use buffex::x_deps::abs_buff::{TrBuffTryRead, TrBuffTryWrite};

use crate::{
    connection::MuxError,
    flow_ctrl::FlowCtrlError,
};


/// 同步取锁失败时的统一处理。
///
/// 单线程配置下「取不到锁」必然是**临界区重入**这一代码 bug（没有别的执行者），
/// 因此直接 panic——与旧 `RefCell` 单元一致，便于尽早暴露；多线程配置下则可能
/// 只是另一线程短暂持锁，自旋等待即可。
pub(crate) fn on_lock_contended_() -> ! {
    panic!("共享状态的临界区不可重入");
}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// 可触发的取消令牌
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

/// 可主动触发的取消令牌。
///
/// `abs_cancel` 只提供 [`CancelledToken`](abs_cancel::CancelledToken)（恒已取消）
/// 与 [`NonCancellableToken`](abs_cancel::NonCancellableToken)（永不取消）两个
/// 常量令牌，缺「可触发」的那一个，因此本 crate 补上它。
///
/// 内部状态必须 `Send + Sync`（[`TrCancellationToken`] 的 supertrait 要求），
/// 因此用 `atomic_sync` 的**抢占式自旋读写锁**（零内部堆分配，可内联进
/// [`Shared`]），而不是手搓单元。
///
/// # 单等待者
///
/// 取消信号只需要被**每个循环**观察到一次，且每个循环一次只 `await` 一处，因此
/// 内部只保留**一个**等待者槽：[`TrCancellationToken::cancellation`] 的 future
/// 每次 poll 都会（重新）登记自己，触发时取出并唤醒。若同一个令牌同时有两个
/// `cancellation()` 在 poll，后登记者会覆盖前一个——本 crate 的用法不会这样。
pub(crate) struct CancelToken_<A>
where
    A: AllocatorClone,
{
    inner_: Shared<SpinningRwLockOwned<CancelInner_>, A>,
}

/// 取消令牌的内部状态。
struct CancelInner_ {
    /// 是否已收到取消信号。
    cancelled_: bool,

    /// 登记中的等待者（至多一个）。
    waker_: Option<Waker>,
}

impl<A> Clone for CancelToken_<A>
where
    A: AllocatorClone,
{
    fn clone(&self) -> Self {
        CancelToken_ {
            inner_: self.inner_.clone(),
        }
    }
}

impl<A> CancelToken_<A>
where
    A: AllocatorClone + Send + Sync,
{
    /// 用调用方注入的分配器建立令牌（未取消）。
    pub(crate) fn new_(alloc: A) -> Self {
        CancelToken_ {
            inner_: Shared::new(
                SpinningRwLockOwned::new_owned(CancelInner_ {
                    cancelled_: false,
                    waker_: Option::None,
                }),
                alloc,
            ),
        }
    }

    /// 触发取消：置位并唤醒登记中的等待者（若有）。
    ///
    /// 重复触发是幂等的。唤醒在**释放锁之后**进行，避免重入本层。
    pub(crate) fn cancel_(&self) {
        let waker = self.with_mut_(|inner| {
            inner.cancelled_ = true;
            inner.waker_.take()
        });
        if let Option::Some(waker) = waker {
            waker.wake();
        }
    }

    /// 持读锁执行 `f`（临界区不得 `await`、不得重入）。
    fn with_<R>(&self, f: impl FnOnce(&CancelInner_) -> R) -> R {
        let mut session = self.inner_.acquire_session();
        let guard = match session.try_read() {
            Result::Ok(guard) => guard,
            Result::Err(_) => on_lock_contended_(),
        };
        f(&guard)
    }

    /// 持写锁执行 `f`（临界区不得 `await`、不得重入）。
    fn with_mut_<R>(&self, f: impl FnOnce(&mut CancelInner_) -> R) -> R {
        let mut session = self.inner_.acquire_session();
        let mut guard = match session.try_write() {
            Result::Ok(guard) => guard,
            Result::Err(_) => on_lock_contended_(),
        };
        f(&mut guard)
    }
}

impl<A> TrCancellationToken for CancelToken_<A>
where
    A: AllocatorClone + Send + Sync,
{
    type Cancellation = CancelWait_<A>;

    type ChildToken = Self;

    fn is_cancelled(&self) -> bool {
        self.with_(|inner| inner.cancelled_)
    }

    fn can_be_cancelled(&self) -> bool {
        true
    }

    fn child_token(&self) -> Self::ChildToken {
        self.clone()
    }

    fn cancellation(self) -> Self::Cancellation {
        CancelWait_ { token_: self }
    }
}

/// [`CancelToken_::cancellation`] 产出的 future。
pub(crate) struct CancelWait_<A>
where
    A: AllocatorClone,
{
    token_: CancelToken_<A>,
}

impl<A> Future for CancelWait_<A>
where
    A: AllocatorClone + Send + Sync,
{
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        // 本类型只含一个共享句柄，天然 `Unpin`。
        let this = self.get_mut();
        // 先登记再检查：若反过来，取消信号可能落在「检查完、还没登记」之间而丢失。
        let cancelled = this.token_.with_mut_(|inner| {
            if inner.cancelled_ {
                return true;
            }
            let stale = match inner.waker_.as_ref() {
                Option::Some(old) => !old.will_wake(cx.waker()),
                Option::None => true,
            };
            if stale {
                inner.waker_ = Option::Some(cx.waker().clone());
            }
            false
        });
        if cancelled {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    }
}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// 连接级失败
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

/// 连接级失败的**载荷无关**投影。
///
/// 循环返回的是 [`MuxError<R, W>`]，但底层错误值（两个载荷）无法存进共享
/// 状态（它们只在循环那一侧存在），因此共享状态里只保留「失败的原因种类」，
/// 由 API 面再映射回 [`MuxError`]。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FailKind_ {
    /// 底层传输失败（读或写）。原载荷丢失，但**方向**保留。
    Transport { write: bool },

    /// 对端主动关闭连接 / 子流。
    PeerClosed,

    /// 子流 / 连接已关闭。
    Closed,

    /// 子流空闲超时（保活无应答）。
    IdleTimeout,

    /// dock 是保留取值（`wildcard` / `unspecified`），不能作为子流 dock。
    ReservedDock,

    /// 帧结构非法。
    MalformedFrame,

    /// 未知或保留的字段 / 取值。
    UnsupportedField,

    /// 帧超过协商的 `max_packet_size`。
    FrameTooLarge,







    /// 流控失败。
    FlowCtrl(FlowCtrlError),

    /// 操作被取消。
    Cancelled,
}

impl FailKind_ {
    /// 从循环侧的 [`MuxError`] 取出可共享的那部分。
    ///
    /// `Rx` / `Tx` 携带的底层错误值无法保存，投影为 [`FailKind_::Transport`]；
    /// **方向**（读 / 写）保留，因此「网络错误中断」与「对端主动关闭」在 API 面
    /// 是两种不同的错误。
    pub(crate) fn of_<R, W>(err: &MuxError<R, W>) -> Self
    where
        R: TrBuffTryRead<u8>,
        W: TrBuffTryWrite<u8>,
    {
        match err {
            MuxError::Rx(_) => FailKind_::Transport { write: false },
            MuxError::Tx(_) => FailKind_::Transport { write: true },
            MuxError::Transport { write } => FailKind_::Transport { write: *write },
            MuxError::Cancelled => FailKind_::Cancelled,
            MuxError::PeerClosed => FailKind_::PeerClosed,
            MuxError::Closed => FailKind_::Closed,
            MuxError::IdleTimeout => FailKind_::IdleTimeout,
            MuxError::ReservedDock => FailKind_::ReservedDock,
            MuxError::MalformedFrame => FailKind_::MalformedFrame,
            MuxError::UnsupportedField => FailKind_::UnsupportedField,
            MuxError::FrameTooLarge => FailKind_::FrameTooLarge,
            // 「拒绝接受调用方给的内存」是本地判定，不属于连接级失败状态。
            MuxError::FlowCtrl(err) => FailKind_::FlowCtrl(*err),
        }
    }

    /// 映射回 API 面使用的 [`MuxError`]。`FailKind_` 是 `Copy`，按值取。
    pub(crate) fn into_mux_error_<R, W>(self) -> MuxError<R, W>
    where
        R: TrBuffTryRead<u8>,
        W: TrBuffTryWrite<u8>,
    {
        match self {
            FailKind_::Transport { write } => MuxError::Transport { write },
            FailKind_::PeerClosed => MuxError::PeerClosed,
            FailKind_::Closed => MuxError::Closed,
            FailKind_::IdleTimeout => MuxError::IdleTimeout,
            FailKind_::ReservedDock => MuxError::ReservedDock,
            FailKind_::MalformedFrame => MuxError::MalformedFrame,
            FailKind_::UnsupportedField => MuxError::UnsupportedField,
            FailKind_::FrameTooLarge => MuxError::FrameTooLarge,
            FailKind_::FlowCtrl(err) => MuxError::FlowCtrl(err),
            FailKind_::Cancelled => MuxError::Cancelled,
        }
    }
}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// 唤醒槽
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

/// 单等待者唤醒槽。
///
/// 用法：等待者在**持有外层锁**时 [`WakerSlot_::register_`]，唤醒方用
/// [`WakerSlot_::take_`] 取出 waker，随后**释放外层锁**再 `wake()`。
pub(crate) struct WakerSlot_ {
    waker_: Option<Waker>,
}

impl core::fmt::Debug for WakerSlot_ {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("WakerSlot_")
            .field("registered", &self.waker_.is_some())
            .finish()
    }
}

impl Default for WakerSlot_ {
    fn default() -> Self {
        WakerSlot_::new_()
    }
}

impl WakerSlot_ {
    /// 空槽。
    pub(crate) const fn new_() -> Self {
        WakerSlot_ {
            waker_: Option::None,
        }
    }

    /// 登记（或更新）等待者；同一个 waker 重复登记不会 clone。
    pub(crate) fn register_(&mut self, waker: &Waker) {
        let stale = match self.waker_.as_ref() {
            Option::Some(old) => !old.will_wake(waker),
            Option::None => true,
        };
        if stale {
            self.waker_ = Option::Some(waker.clone());
        }
    }

    /// 取出等待者（唤醒的责任交给调用方）。
    pub(crate) fn take_(&mut self) -> Option<Waker> {
        self.waker_.take()
    }

    /// 当前是否有等待者登记。
    // 仅供单元测试断言「登记成功 / 取出后清空」；生产路径只经 `register_` /
    // `take_` 使用本槽，不需要查询态。
    #[allow(dead_code)]
    pub(crate) fn is_registered_(&self) -> bool {
        self.waker_.is_some()
    }
}

#[cfg(test)]
mod tests_ {
    use std::{
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        task::Wake,
    };

    use mm_ptr::x_deps::abs_mm::CoreAlloc;


use crate::{
        connection::{MuxError, NoHalfway_, mux_connection::ChannelRegistry_},
        handshake::opts::BasicOpts,
    };

    use super::*;

    /// 计数唤醒器：统计 `wake` 被调用次数，用来断言唤醒确实发生。
    struct CountingWake_ {
        count_: AtomicUsize,
    }

    impl Wake for CountingWake_ {
        fn wake(self: Arc<Self>) {
            self.count_.fetch_add(1usize, Ordering::SeqCst);
        }

        fn wake_by_ref(self: &Arc<Self>) {
            self.count_.fetch_add(1usize, Ordering::SeqCst);
        }
    }

    /// 造一个带计数的 `Waker`。
    /// - 手段：把 [`CountingWake_`] 包进 `Arc` 再转成 `Waker`。
    /// - 判断：返回的 `Waker` 每次被 `wake` 都会让计数加一。
    fn counting_waker_() -> (Waker, Arc<CountingWake_>) {
        let probe = Arc::new(CountingWake_ {
            count_: AtomicUsize::new(0),
        });
        let waker = Waker::from(probe.clone());
        (waker, probe)
    }

    /// 测试取消令牌能唤醒已经挂起的等待者，且唤醒后 future 立即就绪。
    /// - 手段：先 poll 一次 `cancellation()`（此时应 `Pending` 并登记 waker），
    ///   再触发 `cancel_()`，最后再 poll 一次。
    /// - 判断：第一次返回 `Pending` 且尚未唤醒；触发取消后计数加一（说明登记的
    ///   waker 被唤醒）；再次 poll 返回 `Ready`。
    #[test]
    fn cancel_token_wakes_parked_waiter() {
        let token = CancelToken_::new_(CoreAlloc);
        let (waker, probe) = counting_waker_();
        let mut context = Context::from_waker(&waker);

        let mut wait = core::pin::pin!(token.clone().cancellation());
        assert_eq!(wait.as_mut().poll(&mut context), Poll::Pending);
        assert_eq!(probe.count_.load(Ordering::SeqCst), 0usize);

        token.cancel_();
        assert_eq!(
            probe.count_.load(Ordering::SeqCst),
            1usize,
            "触发取消应当唤醒登记中的等待者"
        );
        assert_eq!(wait.as_mut().poll(&mut context), Poll::Ready(()));

        // 幂等：再次触发不会重复唤醒（槽已被取空）。
        token.cancel_();
        assert_eq!(probe.count_.load(Ordering::SeqCst), 1usize);
    }
    /// 测试「先取消、后等待」时 future 立刻就绪。
    /// - 手段：先 `cancel_()`，再 poll 一个新建的 `cancellation()`。
    /// - 判断：`TrCancellationToken::is_cancelled` 为真，且首次 poll 即返回 `Ready`（`can_be_cancelled`
    ///   也为真，说明它不是常量令牌）。
    #[test]
    fn cancel_token_already_cancelled_is_ready_at_once() {
        let token = CancelToken_::new_(CoreAlloc);
        assert!(!TrCancellationToken::is_cancelled(&token));
        assert!(TrCancellationToken::can_be_cancelled(&token));

        token.cancel_();
        assert!(TrCancellationToken::is_cancelled(&token));

        let (waker, probe) = counting_waker_();
        let mut context = Context::from_waker(&waker);
        let mut wait = core::pin::pin!(token.cancellation());
        assert_eq!(wait.as_mut().poll(&mut context), Poll::Ready(()));
        assert_eq!(probe.count_.load(Ordering::SeqCst), 0usize, "已取消无需唤醒");
    }
    /// 测试连接级失败只保留首个原因，并取消两个循环的令牌。
    /// - 手段：建注册表后先后用 `PeerClosed` 与 `ChanLimit` 标记失败。
    /// - 判断：`failure_` 始终是第一次的 `PeerClosed`；两个循环令牌在第一次标记后
    ///   就都已取消。
    #[test]
    fn failure_keeps_first_cause_and_cancels_loops() {
        let registry = ChannelRegistry_::new_(BasicOpts::default(), CoreAlloc);
        assert!(!registry.is_failed_());
        let read_loop = registry.loop_token_(0usize);
        let write_loop = registry.loop_token_(1usize);
        assert!(!TrCancellationToken::is_cancelled(&read_loop) && !TrCancellationToken::is_cancelled(&write_loop));

        registry.mark_failed_(&MuxError::<NoHalfway_, NoHalfway_>::PeerClosed);
        assert!(registry.is_failed_());
        assert_eq!(registry.failure_(), Option::Some(FailKind_::PeerClosed));
        assert!(TrCancellationToken::is_cancelled(&read_loop) && TrCancellationToken::is_cancelled(&write_loop));

        registry.mark_failed_(&MuxError::<NoHalfway_, NoHalfway_>::MalformedFrame);
        assert_eq!(
            registry.failure_(),
            Option::Some(FailKind_::PeerClosed),
            "首个失败原因应当保留"
        );
    }
    /// 测试底层读写错误被投影为「传输失败」，并保留方向、与「对端主动关闭」区分。
    /// - 手段：对 `Rx(())` / `Tx(())` 取 `FailKind_` 再映射回 `MuxError`；另取
    ///   `PeerClosed` 作对照。
    /// - 判断：读错误映射为 `Transport { write: false }`、写错误映射为
    ///   `Transport { write: true }`；两者都不等于 `PeerClosed`。
    #[test]
    fn transport_failure_keeps_direction_and_differs_from_peer_close() {
        let read = FailKind_::of_(&MuxError::<NoHalfway_, NoHalfway_>::Transport { write: false });
        let write = FailKind_::of_(&MuxError::<NoHalfway_, NoHalfway_>::Transport { write: true });
        assert_eq!(read, FailKind_::Transport { write: false });
        assert_eq!(write, FailKind_::Transport { write: true });

        let mapped_read: MuxError<NoHalfway_, NoHalfway_> = read.into_mux_error_();
        let mapped_write: MuxError<NoHalfway_, NoHalfway_> = write.into_mux_error_();
        assert!(matches!(mapped_read, MuxError::Transport { write: false }));
        assert!(matches!(mapped_write, MuxError::Transport { write: true }));

        let peer = FailKind_::of_(&MuxError::<NoHalfway_, NoHalfway_>::PeerClosed);
        assert_eq!(peer, FailKind_::PeerClosed);
        assert_ne!(peer, read, "对端主动关闭与传输中断必须是不同的失败原因");
    }
    /// 测试唤醒槽只保留一个等待者，取出后即清空。
    /// - 手段：空槽先 `take_`；登记一个 waker 后检查状态；再 `take_` 两次。
    /// - 判断：空槽取出为 `None`；登记后 `is_registered_` 为真；第一次 `take_`
    ///   拿到 waker，第二次为 `None`（已被取空）。
    #[test]
    fn waker_slot_takes_and_clears() {
        let mut slot = WakerSlot_::new_();
        assert!(!slot.is_registered_());
        assert!(slot.take_().is_none());

        let (waker, _probe) = counting_waker_();
        slot.register_(&waker);
        assert!(slot.is_registered_());
        assert!(slot.take_().is_some());
        assert!(!slot.is_registered_());
        assert!(slot.take_().is_none());
    }
}
