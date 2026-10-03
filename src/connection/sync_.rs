//! 连接的共享状态层：读写循环与 API 面之间的唯一共享点。
//!
//! 本模块承载三样东西：
//!
//! - [`CancelToken_`]：**可主动触发**的取消令牌；
//! - [`WakerSlot_`]：单等待者唤醒槽。
//!
//! 注册表（dock / 子流索引与配额）已移到 `mux_connection::registry_`。
//!
//! # 共享单元：`atomic_sync` 的协作式读写锁 + 阻塞等待
//!
//! 本模块不再手搓「内部可变性单元」。需要共享可变状态的地方直接用 `atomic_sync`
//! 的锁：
//!
//! - `mux_connection::registry_` 的注册表与 [`CancelToken_`] 都用**协作式读写锁**
//!   （`rwlock::cooperative::CooperativeRwLock`）。它保留 `read_async` /
//!   `write_async`，因此争用时可以**让出 CPU**：许可释放时锁内部唤醒我们注册的
//!   waker，而那个 waker 就是 [`std::thread::unpark`]（见 [`TrBlockingAcquire_`]）。
//! - 每条子流的 `ChannelOwner_` 仍在 `owner_` 里用**抢占式自旋读写锁**：它按子流
//!   分配，用协作式锁会为每条子流多一次全局 `Arc` 分配。它走
//!   [`TrBackoffAcquire_`]——争用时**睡眠重试**，因此同样零忙等、不 panic；该处的
//!   跨线程访问本身属于第 2 期要消除的对象（outlook §5.5）。
//!
//! 取锁一律只走同步快路径（`try_read` / `try_write`），失败才进阻塞慢路径；
//! 因此**闭包内不得 `await`、不得重入**——重入会让本线程永久 park（见
//! [`TrBlockingAcquire_`] 的 `# Panics`）。
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
    time::Duration,
};
use std::{
    sync::Arc,
    task::Wake,
    thread::{self, Thread},
};

use abs_cancel::TrCancellationToken;
use atomic_sync::rwlock::{
    cooperative::CooperativeRwLockOwned,
    preemptive::SpinningRwLockOwned,
};
use buffex::x_deps::{abs_cancel, atomic_sync};
use mm_ptr::Shared;



//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// 阻塞式取锁：争用时 park 本线程，零 CPU 忙等
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

/// 退避式重试的睡眠步长。
///
/// 受定时器粒度约束（Linux 默认 timer slack 约 50µs），因此这是**争用时的延迟
/// 下限**；只在自旋锁的争用路径上使用，正常快路径不付这个代价。
const K_BACKOFF: Duration = Duration::from_micros(50);

/// **抢占式（自旋）锁的退避式获取**。
///
/// 自旋锁内部没有等待队列，无法被「许可释放」直接唤醒，因此只能轮询；这里让每次
/// 失败重试之间 `park_timeout` **睡眠**而不是自旋，所以同样零 CPU 忙等。
///
/// # 与 [`TrBlockingAcquire_`] 的分工
///
/// 协作式锁能注册 waker，于是许可一释放就把线程叫醒（无额外延迟）；自旋锁没有
/// 这个能力，只能靠定时重试，因此争用时延迟受定时器粒度约束。当前只有
/// `owner_::ChannelOwner_` 用本 trait——它按**子流**分配，换协作式锁会为每条子流
/// 多一次全局 `Arc` 分配；而它按第 2 期计划会被移出跨线程域，届时整个退避路径
/// 连同这把自旋锁一起消失。
///
/// # Panics
///
/// 同一线程重入会**永久睡眠重试**。`owner_` 的临界区不调用任何取锁方法（与注册表
/// 同样检查过），因此这是代码纪律而非运行期条件。
pub(crate) trait TrBackoffAcquire_<T> {
    /// 取读许可（必要时睡眠重试）后执行 `f`（`f` 内不得 `await`、不得重入）。
    fn with_read_backoff_<R>(&self, f: impl FnOnce(&T) -> R) -> R;

    /// 取写许可（必要时睡眠重试）后执行 `f`（`f` 内不得 `await`、不得重入）。
    fn with_write_backoff_<R>(&self, f: impl FnOnce(&mut T) -> R) -> R;
}

impl<T> TrBackoffAcquire_<T> for SpinningRwLockOwned<T> {
    fn with_read_backoff_<R>(&self, f: impl FnOnce(&T) -> R) -> R {
        let mut session = self.acquire_session();
        loop {
            match session.try_read() {
                Result::Ok(guard) => return f(&guard),
                Result::Err(_) => thread::park_timeout(K_BACKOFF),
            }
        }
    }

    fn with_write_backoff_<R>(&self, f: impl FnOnce(&mut T) -> R) -> R {
        let mut session = self.acquire_session();
        loop {
            match session.try_write() {
                Result::Ok(mut guard) => return f(&mut guard),
                Result::Err(_) => thread::park_timeout(K_BACKOFF),
            }
        }
    }
}

/// 「唤醒 = unpark 指定线程」的 waker：把协作式锁的 waker 唤醒接到线程 park 上。
struct UnparkWake_ {
    /// 等待取锁的那条线程。
    thread_: Thread,
}

impl Wake for UnparkWake_ {
    fn wake(self: Arc<Self>) {
        self.thread_.unpark();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.thread_.unpark();
    }
}

/// 协作式读写锁的**同步阻塞**获取。
///
/// 快路径仍是 `try_read` / `try_write`（无争用时零等待、零调度）；争用时把
/// `read_async` / `write_async` 的唤醒接到 [`std::thread::park`]：许可释放时锁内部
/// 调用我们注册的 waker → `unpark` → `park` 返回 → 重新 poll。因此 CPU 不空转，
/// 也不会像自旋那样在核间制造 cache line 乒乓。
///
/// # 为什么不是 `.await`
///
/// 取锁点分布在 async 上下文、`Future::poll` 与 `Drop` 三类位置，后两类无法
/// `await`。用 park 版同步获取把它们统一到**同一条争用策略**上，避免同一把锁出现
/// 两套等待语义（异步等待与同步等待混用时极易写出活锁）。若将来某条纯 async 路径
/// 需要「不占用运行时线程」，再为它单独引入 `*_async` 获取即可，语义互补。
///
/// # Panics
///
/// 同一线程重入同一把锁会**永久 park**：协作式锁的等待队列不会把许可发给尚未释放
/// 的持有者。本 crate 的临界区从不重入（`registry_` 的闭包内不调用任何取锁方法），
/// 因此这是一条**代码纪律**；若将来出现重入，应在这里补持有者线程检测。
pub(crate) trait TrBlockingAcquire_<T> {
    /// 同步取读许可后执行 `f`（`f` 内不得 `await`、不得重入）。
    fn with_read_blocking_<R>(&self, f: impl FnOnce(&T) -> R) -> R;

    /// 同步取写许可后执行 `f`（`f` 内不得 `await`、不得重入）。
    fn with_write_blocking_<R>(&self, f: impl FnOnce(&mut T) -> R) -> R;
}

impl<T> TrBlockingAcquire_<T> for CooperativeRwLockOwned<T> {
    fn with_read_blocking_<R>(&self, f: impl FnOnce(&T) -> R) -> R {
        let mut session = self.acquire_session();
        if let Result::Ok(guard) = session.try_read() {
            return f(&guard);
        }
        // 慢路径：**让出 CPU**——登记「unpark 本线程」的 waker 后 park，等许可释放。
        let current = thread::current();
        let waker = Waker::from(Arc::new(UnparkWake_ { thread_: current }));
        let mut cx = Context::from_waker(&waker);
        let mut fut = core::pin::pin!(core::future::IntoFuture::into_future(
            session.read_async(),
        ));
        loop {
            match fut.as_mut().poll(&mut cx) {
                Poll::Ready(Result::Ok(guard)) => return f(&guard),
                // 不可取消的获取 future 只会被取消令牌置为 `Cancelled`，而这里没有令牌。
                Poll::Ready(Result::Err(err)) => {
                    unreachable!("不可取消的读获取 future 不应返回错误：{err:?}")
                }
                // `park` 允许虚假唤醒，回到循环重新 poll 即可；许可释放时的 `unpark`
                // 会留下令牌，因此「poll 返回 Pending」与「park」之间不会丢唤醒。
                Poll::Pending => thread::park(),
            }
        }
    }

    fn with_write_blocking_<R>(&self, f: impl FnOnce(&mut T) -> R) -> R {
        let mut session = self.acquire_session();
        if let Result::Ok(mut guard) = session.try_write() {
            return f(&mut guard);
        }
        let current = thread::current();
        let waker = Waker::from(Arc::new(UnparkWake_ { thread_: current }));
        let mut cx = Context::from_waker(&waker);
        let mut fut = core::pin::pin!(core::future::IntoFuture::into_future(
            session.write_async(),
        ));
        loop {
            match fut.as_mut().poll(&mut cx) {
                Poll::Ready(Result::Ok(mut guard)) => return f(&mut guard),
                // 同上：不可取消的获取 future 不会返回错误。
                Poll::Ready(Result::Err(err)) => {
                    unreachable!("不可取消的写获取 future 不应返回错误：{err:?}")
                }
                Poll::Pending => thread::park(),
            }
        }
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
/// 因此用 `atomic_sync` 的**协作式读写锁**（可内联进 [`Shared`]，争用时经
/// [`TrBlockingAcquire_`] park 等待，零 CPU 忙等）。协作式锁内部会多一次
/// 「每条连接两个令牌」的全局 `Arc` 分配，与注册表同一处已记录的例外。
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
    inner_: Shared<CooperativeRwLockOwned<CancelInner_>, A>,
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
                CooperativeRwLockOwned::new_owned(CancelInner_ {
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
        self.inner_.with_read_blocking_(f)
    }

    /// 持写锁执行 `f`（临界区不得 `await`、不得重入）。
    fn with_mut_<R>(&self, f: impl FnOnce(&mut CancelInner_) -> R) -> R {
        self.inner_.with_write_blocking_(f)
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
