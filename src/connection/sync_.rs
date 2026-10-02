//! 连接的共享状态层：读写循环与 API 面之间的唯一共享点。
//!
//! 本模块承载四样东西：
//!
//! - [`SyncCell_`]：内部可变性，按 feature 分叉（缺省 `RefCell`、开启
//!   `multi-thread` 后原子自旋锁）；
//! - [`CancelToken_`]：**可主动触发**的取消令牌；
//! - [`FailKind_`]：连接级失败的「载荷无关」投影，连同失败标志；
//! - [`ChannelRegistry_`]：dock / 子流索引与配额（`DockInUse` / `DockChanLimit` /
//!   `ChanLimit`），以及每个 dock 上的唤醒槽。
//!
//! # 为什么单线程配置用 `RefCell` 是健全的
//!
//! `mm_ptr::Shared<T, A>` 的 `Send` / `Sync` 是**有条件**的：两者都要求
//! `T: Send + Sync`。因此 `Shared<RefCell<_>, A>` 自身就是 `!Send + !Sync`，
//! 与 [`crate::connection`] §6「缺省配置连接为 `!Send`」完全一致；开启
//! `multi-thread` 后 [`SyncCell_`] 换成原子自旋锁，共享状态才可跨线程。
//!
//! 反过来说：**必须 `Send + Sync` 的类型不能用 [`SyncCell_`]**——[`CancelToken_`]
//! 要实现 [`TrCancellationToken`]（该 trait 自身要求 `Send + Sync`），因此它用
//! [`SyncAtomicCell_`]（两种配置下都是原子锁）。
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
#![allow(dead_code)]

#[cfg(not(feature = "multi-thread"))]
use core::cell::RefCell;
use core::{
    alloc::AllocatorClone,
    future::Future,
    pin::Pin,
    task::{Context, Poll, Waker},
};

use abs_cancel::{NonCancellableToken, TrCancellationToken};
use atomic_sync::mutex::preemptive::SpinningMutex;
use buffex::x_deps::{abs_cancel, atomic_sync};
use mm_ptr::{Owned, Shared};

use crate::{
    connection::{Dock, MuxError},
    flow_ctrl::FlowCtrlError,
    handshake::opts::BasicOpts,
};

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// 共享单元
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

/// 缺省（单线程）配置下的内部可变性。
#[cfg(not(feature = "multi-thread"))]
type CellImpl_<T> = RefCell<T>;

/// `multi-thread` 配置下的内部可变性：原子自旋锁（`atomic_sync`，`buffex` 同款）。
#[cfg(feature = "multi-thread")]
type CellImpl_<T> = SpinningMutex<T>;

/// 共享单元：把「内部可变性」收敛到一处，两种线程模型只在这里分叉。
///
/// 只暴露「持锁执行一段闭包」两个方法，闭包内**不得**再次进入同一单元（`RefCell`
/// 会 panic、自旋锁会自锁），也**不得** `await`。
pub(crate) struct SyncCell_<T> {
    inner_: CellImpl_<T>,
}

/// 恒原子的共享单元：给**必须** `Send + Sync` 的状态用（例如取消令牌）。
///
/// 与 [`SyncCell_`] 的区别只在缺省配置：那里是 `RefCell`，这里是原子自旋锁。
pub(crate) struct SyncAtomicCell_<T> {
    inner_: SpinningMutex<T>,
}

#[cfg(not(feature = "multi-thread"))]
impl<T> SyncCell_<T> {
    /// 就地构造。
    pub(crate) const fn new_(value: T) -> Self {
        SyncCell_ {
            inner_: RefCell::new(value),
        }
    }

    /// 持共享借用执行 `f`。
    pub(crate) fn with_<R>(&self, f: impl FnOnce(&T) -> R) -> R {
        f(&self.inner_.borrow())
    }

    /// 持可变借用执行 `f`。
    pub(crate) fn with_mut_<R>(&self, f: impl FnOnce(&mut T) -> R) -> R {
        f(&mut self.inner_.borrow_mut())
    }
}

#[cfg(feature = "multi-thread")]
impl<T> SyncCell_<T> {
    /// 就地构造。
    pub(crate) const fn new_(value: T) -> Self {
        SyncCell_ {
            inner_: SpinningMutex::new(value, core::sync::atomic::AtomicUsize::new(0)),
        }
    }

    /// 持共享借用执行 `f`。
    pub(crate) fn with_<R>(&self, f: impl FnOnce(&T) -> R) -> R {
        self.with_mut_(|value| f(value))
    }

    /// 持可变借用执行 `f`。
    pub(crate) fn with_mut_<R>(&self, f: impl FnOnce(&mut T) -> R) -> R {
        with_spin_lock_(&self.inner_, f)
    }
}

impl<T> SyncAtomicCell_<T> {
    /// 就地构造。
    pub(crate) const fn new_(value: T) -> Self {
        SyncAtomicCell_ {
            inner_: SpinningMutex::new(value, core::sync::atomic::AtomicUsize::new(0)),
        }
    }

    /// 持共享借用执行 `f`。
    pub(crate) fn with_<R>(&self, f: impl FnOnce(&T) -> R) -> R {
        self.with_mut_(|value| f(value))
    }

    /// 持可变借用执行 `f`。
    pub(crate) fn with_mut_<R>(&self, f: impl FnOnce(&mut T) -> R) -> R {
        with_spin_lock_(&self.inner_, f)
    }
}

/// 取得自旋锁并执行 `f`。
///
/// 只可能因「等待期间令牌被取消」而失败，而这里传的是
/// [`NonCancellableToken`]，因此失败分支不可达；用 `unreachable!` 而不是
/// `expect`，是为了让「不可达」这件事在代码里显式可见。
fn with_spin_lock_<T, R>(lock: &SpinningMutex<T>, f: impl FnOnce(&mut T) -> R) -> R {
    let mut session = lock.lock_session();
    match session
        .lock()
        .may_break_with(NonCancellableToken::new())
    {
        Result::Ok(mut guard) => f(&mut guard),
        // 见函数文档：不可取消令牌下自旋锁不会获取失败。
        Result::Err(_) => unreachable!("不可取消令牌下自旋锁不会获取失败"),
    }
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
/// 因此用 [`SyncAtomicCell_`] 而不是按 feature 分叉的 [`SyncCell_`]。
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
    inner_: Shared<SyncAtomicCell_<CancelInner_>, A>,
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
                SyncAtomicCell_::new_(CancelInner_ {
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
        let waker = self.inner_.with_mut_(|inner| {
            inner.cancelled_ = true;
            inner.waker_.take()
        });
        if let Option::Some(waker) = waker {
            waker.wake();
        }
    }

    /// 是否已收到取消信号。
    pub(crate) fn is_cancelled_(&self) -> bool {
        self.inner_.with_(|inner| inner.cancelled_)
    }
}

impl<A> TrCancellationToken for CancelToken_<A>
where
    A: AllocatorClone + Send + Sync,
{
    type Cancellation = CancelWait_<A>;

    type ChildToken = Self;

    fn is_cancelled(&self) -> bool {
        self.inner_.with_(|inner| inner.cancelled_)
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
        let cancelled = this.token_.inner_.with_mut_(|inner| {
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
/// 循环返回的是 [`MuxError<RE, WE>`]，但底层错误值（`RE` / `WE`）无法存进共享
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

    /// dock 已被 channel 或 telegraph 占用。
    DockInUse,

    /// 该 dock 上的活动子流数已达上限。
    DockChanLimit,

    /// 连接上的活动子流数已达上限。
    ChanLimit,

    /// 对端拒绝建立子流。
    Refused,

    /// 同一条子流上出现重复请求。
    Duplicate,

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
    pub(crate) fn of_<RE, WE>(err: &MuxError<RE, WE>) -> Self {
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
            MuxError::DockInUse => FailKind_::DockInUse,
            MuxError::DockChanLimit => FailKind_::DockChanLimit,
            MuxError::ChanLimit => FailKind_::ChanLimit,
            MuxError::Refused => FailKind_::Refused,
            MuxError::Duplicate => FailKind_::Duplicate,
            MuxError::FlowCtrl(err) => FailKind_::FlowCtrl(*err),
        }
    }

    /// 映射回 API 面使用的 [`MuxError`]。`FailKind_` 是 `Copy`，按值取。
    pub(crate) fn into_mux_error_<RE, WE>(self) -> MuxError<RE, WE> {
        match self {
            FailKind_::Transport { write } => MuxError::Transport { write },
            FailKind_::PeerClosed => MuxError::PeerClosed,
            FailKind_::Closed => MuxError::Closed,
            FailKind_::IdleTimeout => MuxError::IdleTimeout,
            FailKind_::ReservedDock => MuxError::ReservedDock,
            FailKind_::MalformedFrame => MuxError::MalformedFrame,
            FailKind_::UnsupportedField => MuxError::UnsupportedField,
            FailKind_::FrameTooLarge => MuxError::FrameTooLarge,
            FailKind_::DockInUse => MuxError::DockInUse,
            FailKind_::DockChanLimit => MuxError::DockChanLimit,
            FailKind_::ChanLimit => MuxError::ChanLimit,
            FailKind_::Refused => MuxError::Refused,
            FailKind_::Duplicate => MuxError::Duplicate,
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
    pub(crate) fn is_registered_(&self) -> bool {
        self.waker_.is_some()
    }
}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// dock / 子流索引
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

/// dock 的占用用途：channel 与 telegraph **不得共用**同一个 local_dock。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DockUse_ {
    /// 已作为 channel 的 local_dock（监听或发起）。
    Channel,

    /// 已作为 telegraph 端点。
    Telegraph,
}

/// 一条活动子流在索引里的条目。
///
/// 只保存身份（`remote_dock_`）；子流自身的共享状态（窗口、环半部句柄等）在第 7
/// 步随读写循环一并挂上。
struct ChannelNode_<A>
where
    A: AllocatorClone,
{
    /// 对端 dock；与本节点所属 dock 节点合起来就是子流身份。
    remote_dock_: Dock,

    /// 同一条链上的下一个子流。
    next_: Option<Owned<ChannelNode_<A>, A>>,
}

impl<A> DockNode_<A>
where
    A: AllocatorClone,
{
    /// 是否「空」：无子流、无显式占用、无等待者。
    fn is_empty_(&self) -> bool {
        self.chan_count_ == 0 && self.use_.is_none() && !self.inbound_waker_.is_registered_()
    }
}

/// 一个 local_dock 的索引条目。
struct DockNode_<A>
where
    A: AllocatorClone,
{
    /// 本节点对应的 local_dock。
    dock_: Dock,

    /// 显式登记的用途（telegraph）；`None` 表示尚未被显式占用。
    use_: Option<DockUse_>,

    /// 本 dock 上当前在册的子流数（受 `max_dock_chan_count` 约束）。
    chan_count_: usize,

    /// 本 dock 上的子流链头。
    head_: Option<Owned<ChannelNode_<A>, A>>,

    /// 下一个 dock 节点。
    next_: Option<Owned<DockNode_<A>, A>>,

    /// 「本 dock 上有入向事件」的等待者（listener 的 `income_async`）。
    inbound_waker_: WakerSlot_,
}

/// 共享注册表的内部状态。
struct RegistryInner_<A>
where
    A: AllocatorClone,
{
    /// 协商结果：本层只用到其中的两个配额。
    opts_: BasicOpts,

    /// 分配器（每个节点按需分配时克隆使用）。
    alloc_: A,

    /// dock 索引链头。
    docks_: Option<Owned<DockNode_<A>, A>>,

    /// 整条连接上当前在册的子流数（受 `max_channel_count` 约束）。
    total_: usize,

    /// 连接级失败（**首个**原因保留，之后的失败不再覆盖）。
    fail_: Option<FailKind_>,

    /// 两个循环各自的取消令牌（见 [`ChannelRegistry_::cancel_loops_`]）。
    loops_: [CancelToken_<A>; 2],
}

/// 读写循环与 API 面共享的 dock / 子流索引。
///
/// 所有方法都取 `&self`：内部可变性由 [`SyncCell_`] 提供。临界区都很短（链表
/// 增删与计数），且**不跨 `await`**。
pub(crate) struct ChannelRegistry_<A>
where
    A: AllocatorClone,
{
    inner_: Shared<SyncCell_<RegistryInner_<A>>, A>,
}

impl<A> ChannelRegistry_<A>
where
    A: AllocatorClone + Send + Sync,
{
    /// 建立注册表：分配根对象，并为两个循环建好取消令牌。
    ///
    /// # Panics
    ///
    /// 分配根对象失败时 panic（与标准库容器在 OOM 时的行为一致）。调用方应按
    /// `max_channel_count` 量级准备分配器。
    pub(crate) fn new_(opts: BasicOpts, alloc: A) -> Self {
        let loops = [
            CancelToken_::new_(alloc.clone()),
            CancelToken_::new_(alloc.clone()),
        ];
        ChannelRegistry_ {
            inner_: Shared::new(
                SyncCell_::new_(RegistryInner_ {
                    opts_: opts,
                    alloc_: alloc.clone(),
                    docks_: Option::None,
                    total_: 0usize,
                    fail_: Option::None,
                    loops_: loops,
                }),
                alloc,
            ),
        }
    }

    /// 协商出的「整条连接最多同时在册子流数」。
    pub(crate) fn max_channel_count_(&self) -> usize {
        self.inner_.with_(|inner| inner.opts_.max_channel_count)
    }

    /// 协商出的「单个 dock 最多同时在册子流数」。
    pub(crate) fn max_dock_chan_count_(&self) -> usize {
        self.inner_.with_(|inner| inner.opts_.max_dock_chan_count)
    }

    /// 当前在册子流总数。
    pub(crate) fn total_channels_(&self) -> usize {
        self.inner_.with_(|inner| inner.total_)
    }

    /// 为 `(local_dock, remote_dock)` 登记一条子流。
    ///
    /// # Errors
    ///
    /// - `local_dock` 已被 telegraph 占用 → [`MuxError::DockInUse`]；
    /// - 该 dock 上的在册子流数已达 `max_dock_chan_count` → [`MuxError::DockChanLimit`]；
    /// - 连接上的在册子流数已达 `max_channel_count` → [`MuxError::ChanLimit`]。
    pub(crate) fn reserve_channel_(
        &self,
        local_dock: Dock,
        remote_dock: Dock,
    ) -> Result<(), MuxError<(), ()>> {
        self.inner_.with_mut_(|inner| {
            if inner.total_ >= inner.opts_.max_channel_count {
                return Result::Err(MuxError::ChanLimit);
            }
            // 先把限额与分配器取出来：下面要可变借用 dock 节点。
            let max_dock = inner.opts_.max_dock_chan_count;
            let alloc = inner.alloc_.clone();

            let dock = inner.dock_mut_(local_dock);
            if let Option::Some(DockUse_::Telegraph) = dock.use_ {
                return Result::Err(MuxError::DockInUse);
            }
            if dock.chan_count_ >= max_dock {
                return Result::Err(MuxError::DockChanLimit);
            }
            dock.use_ = Option::Some(DockUse_::Channel);
            dock.chan_count_ += 1;
            dock.head_ = Option::Some(Owned::new(
                ChannelNode_ {
                    remote_dock_: remote_dock,
                    next_: dock.head_.take(),
                },
                alloc,
            ));
            inner.total_ += 1;
            Result::Ok(())
        })
    }

    /// 拆掉一条子流并释放配额；不存在时是空操作。
    ///
    /// 若该 dock 随之「无子流、无显式占用、无等待者」，节点会被释放。
    pub(crate) fn release_channel_(&self, local_dock: Dock, remote_dock: Dock) {
        self.inner_.with_mut_(|inner| {
            let removed = match inner.find_dock_mut_(local_dock) {
                Option::None => false,
                Option::Some(dock) => {
                    let removed = remove_channel_(&mut dock.head_, remote_dock);
                    if removed {
                        dock.chan_count_ = dock.chan_count_.saturating_sub(1);
                    }
                    if dock.chan_count_ == 0 {
                        dock.use_ = Option::None;
                    }
                    removed
                }
            };
            if removed {
                inner.total_ = inner.total_.saturating_sub(1);
            }
            inner.prune_dock_(local_dock);
        })
    }

    /// 在 `local_dock` 上显式登记一个用途（telegraph 端点 / channel 监听）。
    ///
    /// # Errors
    ///
    /// 该 dock 已被**另一种**用途占用 → [`MuxError::DockInUse`]；重复登记同一种
    /// 用途是幂等的。
    pub(crate) fn reserve_dock_(
        &self,
        local_dock: Dock,
        use_: DockUse_,
    ) -> Result<(), MuxError<(), ()>> {
        self.inner_.with_mut_(|inner| {
            let dock = inner.dock_mut_(local_dock);
            match dock.use_ {
                Option::None => {
                    dock.use_ = Option::Some(use_);
                    Result::Ok(())
                }
                Option::Some(existing) if existing == use_ => Result::Ok(()),
                Option::Some(_) => Result::Err(MuxError::DockInUse),
            }
        })
    }

    /// 解除 [`ChannelRegistry_::reserve_dock_`] 的登记；不存在时是空操作。
    pub(crate) fn release_dock_(&self, local_dock: Dock) {
        self.inner_.with_mut_(|inner| {
            let Some(dock) = inner.find_dock_mut_(local_dock) else {
                return;
            };
            if dock.chan_count_ == 0 {
                dock.use_ = Option::None;
            }
            inner.prune_dock_(local_dock);
        })
    }

    /// 登记 `local_dock` 上的入向等待者（listener 的 `income_async`）。
    pub(crate) fn register_inbound_waker_(&self, local_dock: Dock, waker: &Waker) {
        self.inner_.with_mut_(|inner| {
            inner.dock_mut_(local_dock).inbound_waker_.register_(waker);
        })
    }

    /// 唤醒 `local_dock` 上的入向等待者（若有）；唤醒在释放锁之后进行。
    pub(crate) fn notify_inbound_(&self, local_dock: Dock) {
        let waker = self.inner_.with_mut_(|inner| {
            inner.dock_mut_(local_dock).inbound_waker_.take_()
        });
        if let Option::Some(waker) = waker {
            waker.wake();
        }
    }

    /// 记下连接级失败（**首个**原因生效），并唤醒两个循环的取消令牌。
    pub(crate) fn mark_failed_<RE, WE>(&self, err: &MuxError<RE, WE>) {
        let kind = FailKind_::of_(err);
        self.inner_.with_mut_(|inner| {
            if inner.fail_.is_none() {
                inner.fail_ = Option::Some(kind);
            }
        });
        self.cancel_loops_();
    }

    /// 连接级失败的原因（若有）。
    pub(crate) fn failure_(&self) -> Option<FailKind_> {
        self.inner_.with_(|inner| inner.fail_)
    }

    /// 连接是否已经失败。
    pub(crate) fn is_failed_(&self) -> bool {
        self.inner_.with_(|inner| inner.fail_.is_some())
    }

    /// 取第 `idx` 个循环的取消令牌（`0` = 读循环，`1` = 写循环）。
    pub(crate) fn loop_token_(&self, idx: usize) -> CancelToken_<A> {
        self.inner_.with_(|inner| inner.loops_[idx].clone())
    }

    /// 触发两个循环的取消令牌（连接关闭或失败时调用）。
    pub(crate) fn cancel_loops_(&self) {
        let tokens = self.inner_.with_(|inner| inner.loops_.clone());
        for token in tokens.iter() {
            token.cancel_();
        }
    }
}

impl<A> RegistryInner_<A>
where
    A: AllocatorClone + Send + Sync,
{
    /// 找到 `dock` 对应的节点；不存在时返回 `None`。
    fn find_dock_mut_(&mut self, dock: Dock) -> Option<&mut DockNode_<A>> {
        let mut cursor = self.docks_.as_mut();
        while let Option::Some(node) = cursor {
            if node.dock_ == dock {
                return Option::Some(node);
            }
            cursor = node.next_.as_mut();
        }
        Option::None
    }

    /// 找到（找不到就按需分配并插到链头）`dock` 对应的节点。
    fn dock_mut_(&mut self, dock: Dock) -> &mut DockNode_<A> {
        if self.find_dock_mut_(dock).is_some() {
            // 上一行已确认存在；这里再走一次，把借用缩到本次调用。
            match self.find_dock_mut_(dock) {
                Option::Some(node) => return node,
                Option::None => unreachable!("上一步已确认节点存在"),
            }
        }
        let alloc = self.alloc_.clone();
        self.docks_ = Option::Some(Owned::new(
            DockNode_ {
                dock_: dock,
                use_: Option::None,
                chan_count_: 0usize,
                head_: Option::None,
                next_: self.docks_.take(),
                inbound_waker_: WakerSlot_::new_(),
            },
            alloc,
        ));
        match self.docks_.as_mut() {
            Option::Some(node) => node,
            // 刚插入，必然存在。
            Option::None => unreachable!("dock 节点刚被插入"),
        }
    }

    /// 释放「空」的 dock 节点：无子流、无显式占用、无等待者。
    ///
    /// 单链表摘除需要同时看到「前驱」与「后继」，因此头节点单独处理，避免在一次
    /// 遍历里同时持有两个可变借用。
    fn prune_dock_(&mut self, dock: Dock) {
        let remove_head = match self.docks_.as_ref() {
            Option::Some(node) => node.dock_ == dock && node.is_empty_(),
            Option::None => false,
        };
        if remove_head {
            let next = self.docks_.as_mut().and_then(|node| node.next_.take());
            self.docks_ = next;
            return;
        }

        let mut cursor = self.docks_.as_mut();
        while let Option::Some(node) = cursor {
            let remove_next = match node.next_.as_ref() {
                Option::Some(next) => next.dock_ == dock && next.is_empty_(),
                Option::None => false,
            };
            if remove_next {
                let next_next = node.next_.as_mut().and_then(|next| next.next_.take());
                node.next_ = next_next;
                return;
            }
            cursor = node.next_.as_mut();
        }
    }
}

fn remove_channel_<A>(
    head: &mut Option<Owned<ChannelNode_<A>, A>>,
    remote_dock: Dock,
) -> bool
where
    A: AllocatorClone,
{
    let remove_head = match head.as_ref() {
        Option::Some(node) => node.remote_dock_ == remote_dock,
        Option::None => false,
    };
    if remove_head {
        let next = head.as_mut().and_then(|node| node.next_.take());
        *head = next;
        return true;
    }

    let mut cursor = head.as_mut();
    while let Option::Some(node) = cursor {
        let remove_next = match node.next_.as_ref() {
            Option::Some(next) => next.remote_dock_ == remote_dock,
            Option::None => false,
        };
        if remove_next {
            let next_next = node.next_.as_mut().and_then(|next| next.next_.take());
            node.next_ = next_next;
            return true;
        }
        cursor = node.next_.as_mut();
    }
    false
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

    /// 测试共享单元在两种配置下都能读写同一份状态。
    /// - 手段：分别对 [`SyncCell_`] 与 [`SyncAtomicCell_`] 做一次 `with_mut_` 自增，
    ///   再用 `with_` 读回。
    /// - 判断：读回值等于写入值——说明内部可变性确实生效且两种单元语义一致。
    #[test]
    fn cells_share_state_between_handles() {
        let cell = SyncCell_::new_(0usize);
        cell.with_mut_(|value| *value += 1usize);
        assert_eq!(cell.with_(|value| *value), 1usize);

        let atomic = SyncAtomicCell_::new_(0usize);
        atomic.with_mut_(|value| *value += 2usize);
        assert_eq!(atomic.with_(|value| *value), 2usize);
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
    /// - 判断：`is_cancelled_` 为真，且首次 poll 即返回 `Ready`（`can_be_cancelled`
    ///   也为真，说明它不是常量令牌）。
    #[test]
    fn cancel_token_already_cancelled_is_ready_at_once() {
        let token = CancelToken_::new_(CoreAlloc);
        assert!(!token.is_cancelled_());
        assert!(TrCancellationToken::can_be_cancelled(&token));

        token.cancel_();
        assert!(token.is_cancelled_());

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
        assert!(!read_loop.is_cancelled_() && !write_loop.is_cancelled_());

        registry.mark_failed_(&MuxError::<(), ()>::PeerClosed);
        assert!(registry.is_failed_());
        assert_eq!(registry.failure_(), Option::Some(FailKind_::PeerClosed));
        assert!(read_loop.is_cancelled_() && write_loop.is_cancelled_());

        registry.mark_failed_(&MuxError::<(), ()>::ChanLimit);
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
        let read = FailKind_::of_(&MuxError::<(), ()>::Rx(()));
        let write = FailKind_::of_(&MuxError::<(), ()>::Tx(()));
        assert_eq!(read, FailKind_::Transport { write: false });
        assert_eq!(write, FailKind_::Transport { write: true });

        let mapped_read: MuxError<(), ()> = read.into_mux_error_();
        let mapped_write: MuxError<(), ()> = write.into_mux_error_();
        assert!(matches!(mapped_read, MuxError::Transport { write: false }));
        assert!(matches!(mapped_write, MuxError::Transport { write: true }));

        let peer = FailKind_::of_(&MuxError::<(), ()>::PeerClosed);
        assert_eq!(peer, FailKind_::PeerClosed);
        assert_ne!(peer, read, "对端主动关闭与传输中断必须是不同的失败原因");
    }

    /// 测试 dock 级与连接级配额各自生效、释放后可重新登记。
    /// - 手段：协商值收紧为「整条连接 2 条、单 dock 1 条」；依次登记
    ///   `(1,9)`、`(1,10)`、`(2,9)`、`(3,9)`，再释放 `(1,9)` 后重新登记。
    /// - 判断：同一 dock 上的第二条报 `DockChanLimit`；连接上的第三条报
    ///   `ChanLimit`；释放后总数回落且同一 dock 可再登记。
    #[test]
    fn reserve_enforces_dock_and_connection_limits() {
        let opts = BasicOpts {
            max_channel_count: 2usize,
            max_dock_chan_count: 1usize,
            ..BasicOpts::default()
        };
        let registry = ChannelRegistry_::new_(opts, CoreAlloc);
        assert_eq!(registry.max_channel_count_(), 2usize);
        assert_eq!(registry.max_dock_chan_count_(), 1usize);

        assert!(registry.reserve_channel_(Dock::new(1u32), Dock::new(9u32)).is_ok());
        assert_eq!(registry.total_channels_(), 1usize);

        let err = registry
            .reserve_channel_(Dock::new(1u32), Dock::new(10u32))
            .unwrap_err();
        assert!(matches!(err, MuxError::DockChanLimit));
        assert!(registry.reserve_channel_(Dock::new(2u32), Dock::new(9u32)).is_ok());
        let err = registry
            .reserve_channel_(Dock::new(3u32), Dock::new(9u32))
            .unwrap_err();
        assert!(matches!(err, MuxError::ChanLimit));

        registry.release_channel_(Dock::new(1u32), Dock::new(9u32));
        assert_eq!(registry.total_channels_(), 1usize);
        assert!(
            registry.reserve_channel_(Dock::new(1u32), Dock::new(11u32)).is_ok(),
            "释放配额后同一 dock 应当可以重新登记"
        );
        assert_eq!(registry.total_channels_(), 2usize);
    }

    /// 测试 channel 与 telegraph 不得共用同一个 local_dock（双向都拒绝）。
    /// - 手段：先在一个 dock 上登记 telegraph 再登记 channel；换一个 dock 反向
    ///   操作。
    /// - 判断：两次都报 `MuxError::DockInUse`；同一种用途重复登记是幂等的。
    #[test]
    fn channel_and_telegraph_are_mutually_exclusive_on_a_dock() {
        let registry = ChannelRegistry_::new_(BasicOpts::default(), CoreAlloc);

        assert!(
            registry
                .reserve_dock_(Dock::new(5u32), DockUse_::Telegraph)
                .is_ok()
        );
        assert!(
            registry
                .reserve_dock_(Dock::new(5u32), DockUse_::Telegraph)
                .is_ok(),
            "重复登记同一种用途应当幂等"
        );
        let err = registry
            .reserve_channel_(Dock::new(5u32), Dock::new(9u32))
            .unwrap_err();
        assert!(matches!(err, MuxError::DockInUse));

        assert!(registry.reserve_channel_(Dock::new(6u32), Dock::new(9u32)).is_ok());
        let err = registry
            .reserve_dock_(Dock::new(6u32), DockUse_::Telegraph)
            .unwrap_err();
        assert!(matches!(err, MuxError::DockInUse));
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

    /// 测试入向等待者只被唤醒一次，且唤醒发生在取出之后。
    /// - 手段：在 dock 4 上登记等待者，调用两次 `notify_inbound_`。
    /// - 判断：第一次唤醒计数为 1；第二次仍是 1（槽已空，没有可唤醒的等待者）。
    #[test]
    fn notify_inbound_wakes_registered_listener_once() {
        let registry = ChannelRegistry_::new_(BasicOpts::default(), CoreAlloc);
        let (waker, probe) = counting_waker_();

        registry.register_inbound_waker_(Dock::new(4u32), &waker);
        registry.notify_inbound_(Dock::new(4u32));
        assert_eq!(probe.count_.load(Ordering::SeqCst), 1usize);

        registry.notify_inbound_(Dock::new(4u32));
        assert_eq!(
            probe.count_.load(Ordering::SeqCst),
            1usize,
            "没有登记等待者时不应再唤醒"
        );
    }
}
