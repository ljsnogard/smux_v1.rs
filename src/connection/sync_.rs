//! 连接的共享状态层：读写循环与 API 面之间的唯一共享点。
//!
//! 本模块承载三样东西：
//!
//! - [`CancelToken_`]：**可主动触发**的取消令牌（无锁）；
//! - [`NotifySlot_`]：**零分配**的单等待者持久通知槽（建流等待者用）；
//! - [`acquire_read_`] / [`acquire_write_`]：协作式锁的**可取消异步获取**。
//!
//! 注册表（dock / 子流索引与配额）已移到 `mux_connection::registry_`。
//!
//! # 共享单元：`acquire_session` + 可取消的异步获取
//!
//! 需要共享可变状态的地方用 `atomic_sync` 的**协作式读写锁**：现在只剩
//! `mux_connection::registry_` 的**身份表**（dock / 子流索引与配额）一处——它是
//! 冷路径（登记 / 释放 / 入向待决），且临界区不跨 `await`。
//!
//! 每条子流的状态**不在**这里：`owner_::ChannelState_` 是一个原子字 + 原子窗口，
//! 由读写循环与 API 面经共享句柄**无锁**访问（见该模块文档）。
//!
//! 取锁一律 `acquire_session()` 之后走 [`acquire_read_`] / [`acquire_write_`]：
//! `try_read` / `try_write` 快路径；失败则 `read_async` / `write_async()` 配
//! `may_cancel_with(cancel)` **异步等待**——让出执行权、可被外部 cancel token 取消，
//! 既不自旋也不阻塞（[`LockCancelled_`]）。
//!
//! 因此**闭包内不得 `await`、不得重入**：临界区在拿到守卫之后同步跑完。
//!
//! # 没有 `await` 可用的地方
//!
//! `Drop` 与同步 trait 方法里没有 `await`，因此那两处不取任何锁：
//! `Drop` 只投 [`SessionEvent_`](crate::connection::signal_::SessionEvent_) 消息，
//! 并顺手在**原子状态字**上记下「应用丢了这一边」；同步路径需要的共享可变量
//! （两个去重位）也都在那个字里。
//!
//! # 取消令牌是无锁的
//!
//! [`CancelToken_`] 的入口全是同步的（`is_cancelled` 可从任意线程调用、
//! `cancellation()` 的等待在 `poll` 中登记），套锁解决不了等待问题：它用
//! `AtomicBool` + **持久通知通道**实现，取消状态是原子读，唤醒是 `recv_async`。
//!
//! # 唤醒：按**实例数**选落点
//!
//! 「某件事发生了」有两种落点，选择依据是这条通知**每多少实例一个**：
//!
//! - **持久通知通道**（`flume` 容量 1）用于 [`CancelToken_`] 与 listener 的入向等待：
//!   它们每**连接**（或每 dock）一个，通道那一次全局分配可以接受；通道持久意味着
//!   「先通知、后等待」不会丢唤醒，而且等待方在 async 上下文里只要「先查状态、再
//!   `recv_async().await`」即可——不需要 `cx`，没有手写的 `poll`；
//! - [`NotifySlot_`]（一个原子位 + 一个 waker 槽，**零堆分配**）用于**建流等待者**：
//!   它是**每条子流**一个的，通道那一次全局分配正是待整改项
//!   （`dev-notes/audit-heap-alloc-20261004-1122.md` §3.1 #7）。它要手写 `poll`，因此
//!   「登记 → 复检」的协议与「至多一个等待者」的前提都写在 [`NotifySlot_`] 的文档里，
//!   并有用例钉住。
//!
//! 早先这里删过一版 waker 槽（`WakerSlot_`），理由是「改用通道就不必手写 `poll`」；
//! 本轮因为**每条子流一次的全局分配**把它按上述协议重新引入——推翻的是当时那个
//! 选择，不是当时对丢唤醒的警惕。
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
    sync::atomic::{AtomicBool, Ordering},
    task::{Context, Poll, Waker},
};

use abs_cancel::{TrCancellationToken, TrMayCancel};
use buffex::x_deps::{abs_cancel, atomic_sync};
use flume::{Receiver, Sender};
use mm_ptr::Shared;

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// 异步取锁：acquire_session + 可取消的异步获取
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

// 协作式锁的会话与守卫类型（把 `CooperativeRwLockOwned<T>` 的默认参数展开，
// 便于在调用点直接声明 `let mut session = reg.lock_session_();`）。
use core::sync::atomic::AtomicUsize;
use atomic_sync::{
    mutex::preemptive::SpinningMutexOwned,
    rwlock::cooperative::{CooperativeAcqSession, ReaderGuard, WriterGuard},
    x_deps::{abs_sync, atomex::StrictOrderings},
};
use abs_sync::may_break::TrMayBreak;

/// 协作式锁的取锁会话（`CooperativeRwLockOwned<T>` 的默认参数展开）。
pub(crate) type CoopSession_<'a, T> =
    CooperativeAcqSession<'a, T, usize, AtomicUsize, StrictOrderings>;

/// 协作式锁的读守卫。
pub(crate) type CoopReadGuard_<'a, 'g, T> =
    ReaderGuard<'a, 'g, T, usize, AtomicUsize, StrictOrderings>;

/// 协作式锁的写守卫。
pub(crate) type CoopWriteGuard_<'a, 'g, T> =
    WriterGuard<'a, 'g, T, usize, AtomicUsize, StrictOrderings>;

/// 取锁等待期间被取消（[`acquire_read_`] / [`acquire_write_`] 的失败）。
///
/// 它不是数据竞争、也不是重入：只是调用方在**等锁**时把 cancel token 触发了。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct LockCancelled_;

/// 异步取**读**许可：`try_read` 快路径；失败则等——等待本身可被 `cancel` 取消。
///
/// 调用点自己建会话（`lock.acquire_session()`）并把它借给本函数，守卫因此挂在
/// 调用方的会话上，可以正常逃逸回调用点使用。
pub(crate) async fn acquire_read_<'a, 'g, T, K>(
    session: &'g mut CoopSession_<'a, T>,
    cancel: K,
) -> Result<CoopReadGuard_<'a, 'g, T>, LockCancelled_>
where
    K: TrCancellationToken,
{
    if let Result::Ok(guard) = session.try_read() {
        return Result::Ok(guard);
    }
    match session.read_async().may_cancel_with(cancel).await {
        Result::Ok(guard) => Result::Ok(guard),
        // 唯一的失败来源就是取消（非阻塞获取在 future 里已重试）。
        Result::Err(_) => Result::Err(LockCancelled_),
    }
}

/// 异步取**写**许可：语义与 [`acquire_read_`] 对称。
pub(crate) async fn acquire_write_<'a, 'g, T, K>(
    session: &'g mut CoopSession_<'a, T>,
    cancel: K,
) -> Result<CoopWriteGuard_<'a, 'g, T>, LockCancelled_>
where
    K: TrCancellationToken,
{
    if let Result::Ok(guard) = session.try_write() {
        return Result::Ok(guard);
    }
    match session.write_async().may_cancel_with(cancel).await {
        Result::Ok(guard) => Result::Ok(guard),
        Result::Err(_) => Result::Err(LockCancelled_),
    }
}



//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// 可触发的取消令牌
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

/// 可主动触发的取消令牌（**无锁**）。
///
/// `abs_cancel` 只提供 [`CancelledToken`](abs_cancel::CancelledToken)（恒已取消）
/// 与 [`NonCancellableToken`](abs_cancel::NonCancellableToken)（永不取消）两个
/// 常量令牌，缺「可触发」的那一个，因此本 crate 补上它。
///
/// # 为什么是无锁的
///
/// 令牌的入口全是**同步**的：[`TrCancellationToken::is_cancelled`] 可以从任意线程
/// 调用，[`TrCancellationToken::cancellation`] 的 future 在 `poll` 里登记等待——
/// 同步入口里没有 `await`，套一把「可异步获取」的锁也解决不了等待问题。因此这里
/// 用「原子标志 + 通知通道」：
///
/// - 取消状态是一个 `AtomicBool`，`is_cancelled()` 就是一次原子读（循环热路径不取锁）；
/// - 唤醒走一条 `flume` 通知通道：`cancel_` 置位后投一条，等待方 `recv_async().await`。
///
/// 通道是**持久**的，因此「先取消、后登记」不会丢唤醒：等待 future 每次 poll 先查
/// 原子标志，已取消就直接就绪。
///
/// # 单等待者
///
/// 通知只投一条，因此假定**同一个令牌在同一时刻至多一个等待者**（本 crate 的用法：
/// 每个循环一个令牌，一次只 `await` 一处）。这与旧实现的单 waker 槽是同一条契约。
pub(crate) struct CancelToken_<A>
where
    A: AllocatorClone,
{
    /// 取消标志（走原子，读侧无锁）。
    cancelled_: Shared<AtomicBool, A>,

    /// 通知生产端；`cancel_` 投一条。
    notify_tx_: Sender<()>,

    /// 通知消费端；`cancellation()` 的 future 收一条。
    notify_rx_: Receiver<()>,
}

impl<A> Clone for CancelToken_<A>
where
    A: AllocatorClone,
{
    fn clone(&self) -> Self {
        CancelToken_ {
            cancelled_: self.cancelled_.clone(),
            notify_tx_: self.notify_tx_.clone(),
            notify_rx_: self.notify_rx_.clone(),
        }
    }
}

impl<A> CancelToken_<A>
where
    A: AllocatorClone + Send + Sync,
{
    /// 用调用方注入的分配器建立令牌（未取消）。
    pub(crate) fn new_(alloc: A) -> Self {
        // 容量 1：通知是幂等的「已取消」信号，重复投递没有意义。
        let (notify_tx_, notify_rx_) = flume::bounded(1usize);
        CancelToken_ {
            cancelled_: Shared::new(AtomicBool::new(false), alloc),
            notify_tx_,
            notify_rx_,
        }
    }

    /// 触发取消：置位并投一条通知（幂等）。
    ///
    /// 只有**第一次**置位者投通知，因此重复取消不会在通道里堆积消息。
    pub(crate) fn cancel_(&self) {
        if !self.cancelled_.swap(true, Ordering::AcqRel) {
            // 通道已满（已有一条待取通知）时投递失败是无害的：等待方会先查原子标志。
            let _ = self.notify_tx_.try_send(());
        }
    }
}

impl<A> TrCancellationToken for CancelToken_<A>
where
    A: AllocatorClone + Send + Sync,
{
    type Cancellation = impl Future<Output = ()>;

    type ChildToken = Self;

    fn is_cancelled(&self) -> bool {
        self.cancelled_.load(Ordering::Acquire)
    }

    fn can_be_cancelled(&self) -> bool {
        true
    }

    fn child_token(&self) -> Self::ChildToken {
        self.clone()
    }

    /// 等待取消的 future。
    ///
    /// 它是 async 块（而不是手写 `poll`），因为 `recv_async()` 的 future 要借用
    /// 消费端并**跨 poll 存活**——手写 `poll` 里临时建、出 `poll` 就丢，会在
    /// 「登记后、消息到达前」丢掉唤醒。
    fn cancellation(self) -> Self::Cancellation {
        async move {
            if self.is_cancelled() {
                return;
            }
            // 通道是持久的：上面的原子检查与这里的登记之间发生的取消，通知已经排在
            // 队列里，因此不会丢。
            let _ = self.notify_rx_.recv_async().await;
        }
    }
}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// 零分配的单等待者通知槽
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

/// **零分配**的单等待者持久通知槽。
///
/// # 为什么不直接用 `flume` 通道
///
/// 通道的「先通知、后等待不丢」由队列的**持久性**提供，代价是每条实例一次**全局**
/// 堆分配（见 `dev-notes/audit-heap-alloc-20261004-1122.md` §3.1 #7）。建流等待者是
/// **每条子流**一个、且**至多一个**，因此这里用「一个原子位 + 一个 waker 槽」表达
/// 同一语义：零堆分配；唯一一把自旋锁只保护那一个 `Option<Waker>`，且只在
/// 登记 / 取走这两处同步短临界区里被持有。
///
/// # 协议：登记 → 复检（两端都不可省）
///
/// - **通知方**（[`NotifySlot_::notify_`]）：先置位 `pending_`，再取走 waker；取锁
///   释放**之后**才 `wake()`——`wake` 可能同步重入 `poll` 并再次抢这把锁，持锁调用
///   在单线程执行器上就是自死锁；
/// - **等待方**（[`NotifySlot_::poll_wait_`]）：先消费 `pending_`（有则立即就绪）；
///   没有则登记 waker；**登记之后再复检一次** `pending_`。少了这次复检，
///   「先查后登记」之间发生的通知会永久丢失；
/// - `pending_` 是**持久**的：只有等待方消费它，因此「先通知、后登记」也不会丢
///   （这正是容量 1 通道在同一场景下的行为）。
///
/// # 至多一个等待者
///
/// 槽只保存**一个** waker：第二个等待者登记会覆盖第一个，第一个此后不再被唤醒。
/// 这是调用方必须保证的前提，与 [`CancelToken_`] 的「单等待者」是同一条契约。
/// 当前唯一的用法是建流等待（每条子流至多一个等待者：`ChannelHandle` 是 `!Clone`，
/// `accept_async` 取 `&mut self`）。
pub(crate) struct NotifySlot_ {
    /// 「状态可能变了」的持久位；只有等待方消费它。
    pending_: AtomicBool,

    /// 等待者槽：`Option<Waker>`，用零分配自旋锁保护。
    waker_: SpinningMutexOwned<Option<Waker>>,
}

impl core::fmt::Debug for NotifySlot_ {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("NotifySlot_")
            .field("pending", &self.pending_.load(Ordering::Acquire))
            .finish()
    }
}

impl Default for NotifySlot_ {
    fn default() -> Self {
        Self::new_()
    }
}

impl NotifySlot_ {
    /// 空槽（未通知、无等待者）。
    pub(crate) fn new_() -> Self {
        NotifySlot_ {
            pending_: AtomicBool::new(false),
            waker_: SpinningMutexOwned::new_owned(Option::None),
        }
    }

    /// 在槽锁下访问 waker（不可取消的自旋等待）。
    ///
    /// `wait_or` 的失败分支只可能来自**可取消**的等待；这里用的是不可取消令牌，
    /// 因此该分支不可达（与 `flow_ctrl::SendWindow::with_inner_` 同款）。
    fn with_waker_<R>(&self, f: impl FnOnce(&mut Option<Waker>) -> R) -> R {
        let mut session = self.waker_.lock_session();
        let mut guard = session
            .lock()
            .wait_or(|| unreachable!("不可取消的自旋锁不会以取消告终"));
        f(&mut guard)
    }

    /// 报告「状态可能变了」：置位并唤醒等待者（若有）。幂等、不阻塞。
    pub(crate) fn notify_(&self) {
        self.pending_.store(true, Ordering::Release);
        // 取 waker 与 `wake` 分开：不得在持锁时 `wake`（理由见类型文档）。
        let waker = self.with_waker_(|slot| slot.take());
        if let Option::Some(waker) = waker {
            waker.wake();
        }
    }

    /// 等下一次通知：消费持久位，或在槽里登记 waker 之后返回 [`Poll::Pending`]。
    pub(crate) fn poll_wait_(&self, cx: &mut Context<'_>) -> Poll<()> {
        if self.take_pending_() {
            return Poll::Ready(());
        }
        // 登记（同一个 waker 重复登记不 clone）。
        self.with_waker_(|slot| {
            let stale = match slot.as_ref() {
                Option::Some(old) => !old.will_wake(cx.waker()),
                Option::None => true,
            };
            if stale {
                *slot = Option::Some(cx.waker().clone());
            }
        });
        // 复检：登记与上一次检查之间发生的通知不能丢。
        if self.take_pending_() {
            return Poll::Ready(());
        }
        Poll::Pending
    }

    /// 消费「状态可能变了」那一位；返回它此前是否为真。
    fn take_pending_(&self) -> bool {
        self.pending_.swap(false, Ordering::AcqRel)
    }

    /// 当前是否有等待者登记（诊断 / 测试用）。
    #[cfg(test)]
    pub(crate) fn is_registered_(&self) -> bool {
        self.with_waker_(|slot| slot.is_some())
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

    use atomic_sync::rwlock::cooperative::CooperativeRwLockOwned;
    use mm_ptr::x_deps::abs_mm::CoreAlloc;

    use super::*;

    /// 形态探针：守卫必须能**逃逸出** `acquire_*`（它借用调用方的会话），
    /// 这样调用点才能「自建会话 → 取守卫 → 在守卫上直接操作」。
    /// - 手段：建一把 `u32` 协作式锁，自建会话后用 `acquire_read_` 取读守卫。
    /// - 判断：守卫可读回锁内的初值（编译通过本身就是本探针的主要目的）。
    async fn acquire_guard_escapes_session() {
        let lock = CooperativeRwLockOwned::<u32>::new_owned(7u32);
        let mut session = lock.acquire_session();
        let guard = acquire_read_(&mut session, abs_cancel::NonCancellableToken::new())
            .await
            .expect("非取消路径不应失败");
        assert_eq!(*guard, 7u32);
    }
    dual_runtime_test_!(acquire_guard_escapes_session);

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
    ///   waker 被唤醒）；再次 poll 返回 `Ready`。这里的「手工 poll」是**故意**的：
    ///   本用例验的正是 waker 记账本身，而不是某个运行时的调度行为。
    async fn cancel_token_wakes_parked_waiter() {
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
    dual_runtime_test_!(cancel_token_wakes_parked_waiter);

    /// 测试「先取消、后等待」时 future 立刻就绪。
    /// - 手段：先 `cancel_()`，再 poll 一个新建的 `cancellation()`。
    /// - 判断：`TrCancellationToken::is_cancelled` 为真，且首次 poll 即返回 `Ready`（`can_be_cancelled`
    ///   也为真，说明它不是常量令牌）。
    async fn cancel_token_already_cancelled_is_ready_at_once() {
        let token = CancelToken_::new_(CoreAlloc);
        assert!(!TrCancellationToken::is_cancelled(&token));
        assert!(TrCancellationToken::can_be_cancelled(&token));

        token.cancel_();
        assert!(TrCancellationToken::is_cancelled(&token));

        let (waker, probe) = counting_waker_();
        let mut context = Context::from_waker(&waker);
        let mut wait = core::pin::pin!(token.cancellation());
        assert_eq!(wait.as_mut().poll(&mut context), Poll::Ready(()));
        assert_eq!(
            probe.count_.load(Ordering::SeqCst),
            0usize,
            "已取消无需唤醒"
        );
    }
    dual_runtime_test_!(cancel_token_already_cancelled_is_ready_at_once);

    /// 测试通知槽在**没有等待者**时也持久保存通知（等价于容量 1 通道的行为）。
    /// - 手段：先 `notify_`，再用计数 waker poll 一次；随后连续 `notify_` 两次，再 poll。
    /// - 判断：两次 poll 都立刻 `Ready`（重复通知合并成一次、不堆积）；因为期间从未
    ///   有等待者登记，计数 waker 一次也没有被唤醒。
    async fn notify_slot_is_persistent_without_waiter() {
        let slot = NotifySlot_::new_();
        let (waker, probe) = counting_waker_();
        let mut context = Context::from_waker(&waker);

        slot.notify_();
        assert_eq!(
            slot.poll_wait_(&mut context),
            Poll::Ready(()),
            "先通知后等待不应当丢"
        );
        slot.notify_();
        slot.notify_();
        assert_eq!(
            slot.poll_wait_(&mut context),
            Poll::Ready(()),
            "重复通知合并成一次，仍然就绪"
        );
        assert_eq!(
            slot.poll_wait_(&mut context),
            Poll::Pending,
            "通知已被消费，不应再就绪"
        );
        assert_eq!(
            probe.count_.load(Ordering::SeqCst),
            0usize,
            "没有等待者登记时不应当发生唤醒"
        );
    }
    dual_runtime_test_!(notify_slot_is_persistent_without_waiter);

    /// 测试通知槽唤醒**已经登记**的等待者，且取走 waker 后不再重复唤醒。
    /// - 手段：poll 一次（应当 `Pending` 并登记）→ `notify_` → 再 `notify_` → poll。
    /// - 判断：第一次通知恰好唤醒一次；第二次通知没有等待者可唤醒（唤醒计数仍为 1）；
    ///   随后 poll 返回 `Ready`。
    async fn notify_slot_wakes_registered_waiter_once() {
        let slot = NotifySlot_::new_();
        let (waker, probe) = counting_waker_();
        let mut context = Context::from_waker(&waker);

        assert_eq!(slot.poll_wait_(&mut context), Poll::Pending);
        assert!(slot.is_registered_(), "挂起之后应当留下等待者");

        slot.notify_();
        assert_eq!(
            probe.count_.load(Ordering::SeqCst),
            1usize,
            "通知应当唤醒登记中的等待者"
        );
        slot.notify_();
        assert_eq!(
            probe.count_.load(Ordering::SeqCst),
            1usize,
            "waker 已被取走，重复通知不再唤醒"
        );
        assert_eq!(slot.poll_wait_(&mut context), Poll::Ready(()));
    }
    dual_runtime_test_!(notify_slot_wakes_registered_waiter_once);

    /// 测试通知槽在**高频置位 + 等待者反复进出**下不丢唤醒（audit §5.6 要求的压测）。
    /// - 手段：单线程循环 4096 轮，每轮「poll（应当 `Pending`，登记 waker）→ `notify_`
    ///   → poll（应当 `Ready`）」；结束后在无通知时再 poll 一次。
    /// - 判断：每一轮都必须以 `Ready` 收尾（任何一轮丢唤醒都会失败）；每轮通知都应当
    ///   唤醒登记的等待者（计数恰为轮数）；末尾无通知时必须挂起而不是忙就绪。
    async fn notify_slot_survives_high_frequency_races() {
        const K_ROUNDS: usize = 4096usize;

        let slot = NotifySlot_::new_();
        let (waker, probe) = counting_waker_();
        let mut context = Context::from_waker(&waker);

        for round in 0..K_ROUNDS {
            assert_eq!(
                slot.poll_wait_(&mut context),
                Poll::Pending,
                "第 {round} 轮应当先挂起"
            );
            slot.notify_();
            assert_eq!(
                slot.poll_wait_(&mut context),
                Poll::Ready(()),
                "第 {round} 轮的通知不得丢"
            );
        }
        assert_eq!(
            probe.count_.load(Ordering::SeqCst),
            K_ROUNDS,
            "每一轮通知都应当唤醒恰好一次"
        );
        assert_eq!(
            slot.poll_wait_(&mut context),
            Poll::Pending,
            "没有新通知时应当挂起"
        );
    }
    dual_runtime_test_!(notify_slot_survives_high_frequency_races);
}
