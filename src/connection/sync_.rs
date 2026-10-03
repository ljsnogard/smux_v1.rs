//! 连接的共享状态层：读写循环与 API 面之间的唯一共享点。
//!
//! 本模块承载两样东西：
//!
//! - [`CancelToken_`]：**可主动触发**的取消令牌（无锁）；
//! - [`acquire_read_`] / [`acquire_write_`]：协作式锁的**可取消异步获取**。
//!
//! 注册表（dock / 子流索引与配额）已移到 `mux_connection::registry_`。
//!
//! # 共享单元：`acquire_session` + 可取消的异步获取
//!
//! 需要共享可变状态的地方都用 `atomic_sync` 的**协作式读写锁**：
//!
//! - `mux_connection::registry_` 的身份表与取消令牌 [`CancelToken_`]；
//! - `owner_::ChannelOwner_` 的每条子流热状态。
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
//! `Drop` 与同步 trait 方法里没有 `await`，因此那两处的共享状态**不取锁**：
//! `Drop` 只投 [`SessionEvent_`](crate::connection::signal_::SessionEvent_) 消息；
//! 同步路径唯一需要共享的可变量（「发送环有数据」去重位）是锁外的原子。
//!
//! # 取消令牌是无锁的
//!
//! [`CancelToken_`] 的入口全是同步的（`is_cancelled` 可从任意线程调用、
//! `cancellation()` 的等待在 `poll` 中登记），套锁解决不了等待问题：它用
//! `AtomicBool` + **持久通知通道**实现，取消状态是原子读，唤醒是 `recv_async`。
//!
//! # 唤醒
//!
//! 「某件事发生了」一律走**持久通知通道**（`flume` 容量 1）：取消令牌、建流等待者
//! 与 listener 的入向等待都是它。通道持久意味着「先通知、后等待」不会丢唤醒，
//! 而且等待方在 async 上下文里只要「先查状态、再 `recv_async().await`」即可——
//! 不需要 `cx`、因此不再有手写的 `poll` 与 waker 槽（旧 `WakerSlot_` 已删除）。
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
    rwlock::cooperative::{CooperativeAcqSession, ReaderGuard, WriterGuard},
    x_deps::atomex::StrictOrderings,
};

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

#[cfg(test)]
mod tests_ {
    use core::task::{Context, Poll, Waker};
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
    #[test]
    fn acquire_guard_escapes_session() {
        let lock = CooperativeRwLockOwned::<u32>::new_owned(7u32);
        let mut session = lock.acquire_session();
        let guard = futures::executor::block_on(acquire_read_(
            &mut session,
            abs_cancel::NonCancellableToken::new(),
        ))
        .expect("非取消路径不应失败");
        assert_eq!(*guard, 7u32);
    }

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
}
