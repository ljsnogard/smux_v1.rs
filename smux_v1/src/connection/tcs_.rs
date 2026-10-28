//! 请求完成源（TaskCompletionSource）：一次控制面请求的异步答复。
//!
//! `main_loop` 处理请求时**不能**直接在调用方的 future 上完成动作（它不在调用方的
//! 栈上），因此请求只携带一个 [`Tcs_`]：调用方 `.await` 它，`main_loop` 处理完后
//! `set_` 结果。这与 C# 的 `TaskCompletionSource` / 一次性 oneshot 同形。
//!
//! # 状态机
//!
//! ```text
//! Pending ──set(v)──► Done(v) ──poll_wait_──► Taken
//!    │
//!    └──abandon_()──► Abandoned ──set(v)──► 丢弃 v（返回 false）
//! ```
//!
//! - `Done` 只在**第一次** `set_` 时成立；重复 `set_` 丢弃新值并返回 `false`。
//! - `Taken` 是「值已经被等待方取走」：之后 `set_` / `poll_wait_` 都不再交出值。
//! - `Abandoned` 由等待 future 的 `Drop` 置上（取消 / 放弃）：`main_loop` 仍会完成
//!   协议动作，只是 `set_` 发现没人要就丢弃结果——**不能**因为没人等就跳过回帧。
//!
//! # 分配
//!
//! `Tcs_` 本体（`TcsInner_`）放在**连接级临时对象域**（`sw_scope::Scope`）里，用
//! `Retain<TcsInner_<T>>` 当句柄。一次请求一次放入，生命周期远短于子流；域随连接
//! 关闭显式清盘。理由与边界见 `dev-notes/sw-scope-20261028-2130.md` §4.1。
//!
//! 每次访问都先 `try_sharing()` 借出视图：视图本身不存储，避免把借用生命周期塞进
//! 请求结构；代价是每次访问一次状态 CAS（请求路径不是每帧热路径）。
//!
//! # 为什么 `state_` 与 `waker_` 用两把锁
//!
//! 两者各自是独立的短临界区：`set_` 先在状态锁内提交 `Done`，再到 waker 锁里取走
//! 等待者；`poll_wait_` 先看状态、再登记 waker、最后**复检**状态。复检保证
//! 「检查到 `Pending` 之后、登记之前」发生的 `set_` 不会丢唤醒：那一侧取走的是
//! `None`（或旧 waker），而复检会看到 `Done` 并立刻返回 `Ready`。

use core::{
    fmt,
    task::{Context, Poll, Waker},
};

use abs_sync::may_break::TrMayBreak;
use atomic_sync::mutex::preemptive::SpinningMutexOwned;
use sw_scope::{Retain, Scope, ScopeError};

use crate::x_deps::{abs_sync, atomic_sync, sw_scope};

/// 一次请求的完成源。
///
/// 句柄可克隆：调用方持一份等待，`main_loop` 持一份 `set_`。克隆只增加 `Retain`
/// 的弱计数，不复制结果。
pub(crate) struct Tcs_<T> {
    /// 完成源本体（域内对象）。
    inner_: Retain<TcsInner_<T>>,
}

impl<T> Clone for Tcs_<T> {
    fn clone(&self) -> Self {
        Tcs_ {
            inner_: self.inner_.clone(),
        }
    }
}

impl<T> fmt::Debug for Tcs_<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Tcs_")
            .field("state", &self.state_name_())
            .finish()
    }
}

/// 共享的完成源内容。
struct TcsInner_<T> {
    /// 结果状态。
    state_: SpinningMutexOwned<TcsState_<T>>,

    /// 唯一等待者的 waker（每个请求只有一个 API future 在等）。
    waker_: SpinningMutexOwned<Option<Waker>>,
}

/// 完成源的状态。
enum TcsState_<T> {
    /// 还没有结果。
    Pending,

    /// 结果已就绪、尚未被取走。
    Done(T),

    /// 结果已经被等待方取走。
    Taken,

    /// 等待方已经放弃；此后的 `set_` 丢弃结果。
    Abandoned,
}

impl<T> Tcs_<T> {
    /// 在连接级临时对象域里建一个尚未完成的完成源。
    ///
    /// # Errors
    ///
    /// 域已关闭（连接正在收尾）或池分配失败时返回 [`ScopeError`]。
    pub(crate) async fn new_(scope: &Scope) -> Result<Self, ScopeError> {
        let inner_ = scope
            .put_async(TcsInner_ {
                state_: SpinningMutexOwned::new_owned(TcsState_::Pending),
                waker_: SpinningMutexOwned::new_owned(Option::None),
            })
            .await?;
        Result::Ok(Tcs_ { inner_ })
    }

    /// 提交结果；返回 `false` 表示等待方已放弃 / 值已被取走 / 域已清盘，结果被丢弃。
    ///
    /// 本方法不唤醒、不 `await`；唤醒在锁外做。重复调用是安全的（第二次起返回
    /// `false`），因此 `main_loop` 不必额外记录“是否已答复”。
    pub(crate) fn set_(&self, value: T) -> bool {
        let Option::Some(inner) = self.inner_.try_sharing() else {
            // 域已清盘：没有等待者能收到结果，按“已放弃”丢弃。
            return false;
        };
        let accepted = {
            let mut session = inner.state_.lock_session();
            let mut guard = session
                .lock()
                .wait_or(|| unreachable!("不可取消的自旋锁不会以取消告终"));
            if matches!(&*guard, TcsState_::Pending) {
                *guard = TcsState_::Done(value);
                true
            } else {
                // `value` 随函数结束被丢弃（已放弃 / 已取走 / 重复完成）。
                false
            }
        };
        if accepted {
            Self::wake_waiter_(&inner);
        }
        accepted
    }

    /// 等结果：`Ready(Some(v))` 取到值，`Ready(None)` 表示已被放弃。
    ///
    /// 登记 waker 之后**复检**状态，因此不会丢唤醒（见模块文档）。
    pub(crate) fn poll_wait_(&self, cx: &mut Context<'_>) -> Poll<Option<T>> {
        let Option::Some(inner) = self.inner_.try_sharing() else {
            // 域已清盘：完成源已失效，等价于等待方被取消。
            return Poll::Ready(Option::None);
        };
        if let Option::Some(value) = Self::take_done_(&inner) {
            return Poll::Ready(Option::Some(value));
        }
        if Self::is_finished_(&inner) {
            return Poll::Ready(Option::None);
        }
        Self::register_waker_(&inner, cx);
        if let Option::Some(value) = Self::take_done_(&inner) {
            return Poll::Ready(Option::Some(value));
        }
        if Self::is_finished_(&inner) {
            return Poll::Ready(Option::None);
        }
        Poll::Pending
    }

    /// 等待方放弃：把 `Pending` 置为 `Abandoned`，并清掉已登记的 waker。
    ///
    /// 已 `Done` 的结果保持原样（随最后一个句柄一起释放）；这样“先取到值、再丢弃
    /// future”不会把值提前丢掉。域已清盘时无事可做。
    pub(crate) fn abandon_(&self) {
        let Option::Some(inner) = self.inner_.try_sharing() else {
            return;
        };
        let changed = {
            let mut session = inner.state_.lock_session();
            let mut guard = session
                .lock()
                .wait_or(|| unreachable!("不可取消的自旋锁不会以取消告终"));
            if matches!(&*guard, TcsState_::Pending) {
                *guard = TcsState_::Abandoned;
                true
            } else {
                false
            }
        };
        if changed {
            Self::clear_waker_(&inner);
        }
    }

    /// 是否已经没有可交付的结果（已被取走或已被放弃）。
    fn is_finished_(inner: &TcsInner_<T>) -> bool {
        let mut session = inner.state_.lock_session();
        let guard = session
            .lock()
            .wait_or(|| unreachable!("不可取消的自旋锁不会以取消告终"));
        matches!(&*guard, TcsState_::Taken | TcsState_::Abandoned)
    }

    /// 等待方是否已经放弃。
    pub(crate) fn is_abandoned_(&self) -> bool {
        let Option::Some(inner) = self.inner_.try_sharing() else {
            return true;
        };
        let mut session = inner.state_.lock_session();
        let guard = session
            .lock()
            .wait_or(|| unreachable!("不可取消的自旋锁不会以取消告终"));
        matches!(&*guard, TcsState_::Abandoned)
    }

    /// 取走已就绪的结果（`Done -> Taken`）。
    fn take_done_(inner: &TcsInner_<T>) -> Option<T> {
        let mut session = inner.state_.lock_session();
        let mut guard = session
            .lock()
            .wait_or(|| unreachable!("不可取消的自旋锁不会以取消告终"));
        if matches!(&*guard, TcsState_::Done(_)) {
            match core::mem::replace(&mut *guard, TcsState_::Taken) {
                TcsState_::Done(value) => Option::Some(value),
                _ => unreachable!("刚确认过状态是 Done"),
            }
        } else {
            Option::None
        }
    }

    /// 登记等待者（同一个 waker 不重复 clone）。
    fn register_waker_(inner: &TcsInner_<T>, cx: &mut Context<'_>) {
        let mut session = inner.waker_.lock_session();
        let mut guard = session
            .lock()
            .wait_or(|| unreachable!("不可取消的自旋锁不会以取消告终"));
        let stale = match guard.as_ref() {
            Option::Some(old) => !old.will_wake(cx.waker()),
            Option::None => true,
        };
        if stale {
            *guard = Option::Some(cx.waker().clone());
        }
    }

    /// 取走并唤醒等待者（锁外 `wake`）。
    fn wake_waiter_(inner: &TcsInner_<T>) {
        let waker = {
            let mut session = inner.waker_.lock_session();
            let mut guard = session
                .lock()
                .wait_or(|| unreachable!("不可取消的自旋锁不会以取消告终"));
            guard.take()
        };
        if let Option::Some(waker) = waker {
            waker.wake();
        }
    }

    /// 丢弃已登记的等待者（放弃路径不需要再唤醒它）。
    fn clear_waker_(inner: &TcsInner_<T>) {
        let mut session = inner.waker_.lock_session();
        let mut guard = session
            .lock()
            .wait_or(|| unreachable!("不可取消的自旋锁不会以取消告终"));
        *guard = Option::None;
    }

    /// 状态名（仅用于 `Debug`；不要求 `T: Debug`）。
    fn state_name_(&self) -> &'static str {
        let Option::Some(inner) = self.inner_.try_sharing() else {
            return "closed";
        };
        let mut session = inner.state_.lock_session();
        let guard = session
            .lock()
            .wait_or(|| unreachable!("不可取消的自旋锁不会以取消告终"));
        match &*guard {
            TcsState_::Pending => "pending",
            TcsState_::Done(_) => "done",
            TcsState_::Taken => "taken",
            TcsState_::Abandoned => "abandoned",
        }
    }
}

/// 等待守卫：等待 future 被丢弃时自动 [`Tcs_::abandon_`]。
///
/// API future 在提交请求后把等待句柄包进本守卫：调用方取消 / 丢弃 future 时，
/// `main_loop` 仍会完成协议动作，但 `set_` 会发现没人要而丢弃结果——不会把结果
/// 泄漏到已放弃的等待方，也不会让 `main_loop` 卡在答复上。
pub(crate) struct TcsAbandonGuard_<'a, T> {
    /// 被守卫的等待句柄。
    tcs_: &'a Tcs_<T>,
}

impl<'a, T> TcsAbandonGuard_<'a, T> {
    /// 守卫一个等待句柄。
    pub(crate) fn new_(tcs_: &'a Tcs_<T>) -> Self {
        TcsAbandonGuard_ { tcs_ }
    }
}

impl<T> Drop for TcsAbandonGuard_<'_, T> {
    fn drop(&mut self) {
        self.tcs_.abandon_();
    }
}

#[cfg(test)]
mod tests_ {
    use std::{
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        task::{Context, Wake},
    };

    use futures::executor::block_on;
    use sw_scope::Scope;

    use super::*;

    /// 计数唤醒器：统计 `wake` 次数，用于断言确实被唤醒。
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

    /// 造一个计数 waker 与它的计数句柄。
    fn counting_waker_() -> (Waker, Arc<CountingWake_>) {
        let probe = Arc::new(CountingWake_ {
            count_: AtomicUsize::new(0usize),
        });
        (Waker::from(probe.clone()), probe)
    }

    /// 在测试域里建一个完成源。
    fn new_tcs_<T>(scope: &Scope) -> Tcs_<T> {
        block_on(Tcs_::<T>::new_(scope)).expect("测试域应当可用")
    }

    /// 测试 `set_` 之后 `poll_wait_` 取到值，且第二次 poll 不再交出。
    /// - 手段：建 `Tcs_<u32>`，`set_(7)` 后用固定 waker 的 `Context` poll 两次。
    /// - 判断：第一次返回 `Ready(Some(7))`；第二次返回 `Ready(None)`（已 `Taken`）。
    ///
    /// Test goal: a set value is observed once, then the source reports "taken".
    /// Method: build a `Tcs_<u32>`, `set_(7)`, then poll twice with a fixed waker.
    /// Assertion: first poll is `Ready(Some(7))`; second is `Ready(None)`.
    #[test]
    fn tcs_set_then_poll_takes_value_once_() {
        let scope = Scope::new();
        let tcs = new_tcs_::<u32>(&scope);
        let (waker, _probe) = counting_waker_();
        let mut cx = Context::from_waker(&waker);

        assert!(tcs.set_(7u32));
        assert!(matches!(
            tcs.poll_wait_(&mut cx),
            Poll::Ready(Option::Some(7u32))
        ));
        assert!(matches!(tcs.poll_wait_(&mut cx), Poll::Ready(Option::None)));
    }

    /// 测试等待者在 `Pending` 时登记，`set_` 之后被唤醒并能取到值。
    /// - 手段：新建 `Tcs_`，先 poll 一次（应为 `Pending` 且登记 waker），再 `set_(9)`，
    ///   最后重新 poll。
    /// - 判断：首次 `Pending`；`set_` 使唤醒计数增加；末次 `Ready(Some(9))`。
    ///
    /// Test goal: a pending waiter is woken by `set_` and then receives the value.
    /// Method: poll once (expect `Pending`, registering the waker), `set_(9)`, poll again.
    /// Assertion: `Pending` first; wake count increases; `Ready(Some(9))` last.
    #[test]
    fn tcs_set_wakes_pending_waiter_() {
        let scope = Scope::new();
        let tcs = new_tcs_::<u32>(&scope);
        let (waker, probe) = counting_waker_();
        let mut cx = Context::from_waker(&waker);

        assert!(tcs.poll_wait_(&mut cx).is_pending());
        assert!(tcs.set_(9u32));
        assert_eq!(probe.count_.load(Ordering::SeqCst), 1usize);
        assert!(matches!(
            tcs.poll_wait_(&mut cx),
            Poll::Ready(Option::Some(9u32))
        ));
    }

    /// 测试放弃之后 `set_` 丢弃结果、返回 `false`，且值被 drop。
    /// - 手段：`Tcs_<DropProbe_>`，先 `abandon_()` 再 `set_(probe)`。
    /// - 判断：`set_` 返回 `false`；`is_abandoned_()` 为真；probe 的 drop 计数为 1。
    ///
    /// Test goal: after abandonment a set value is dropped and reported as rejected.
    /// Method: `Tcs_<DropProbe_>`, call `abandon_()` then `set_(probe)`.
    /// Assertion: `set_` returns false; `is_abandoned_` is true; drop count is 1.
    #[test]
    fn tcs_abandon_drops_late_value_() {
        struct DropProbe_ {
            count_: Arc<AtomicUsize>,
        }
        impl Drop for DropProbe_ {
            fn drop(&mut self) {
                self.count_.fetch_add(1usize, Ordering::SeqCst);
            }
        }

        let count = Arc::new(AtomicUsize::new(0usize));
        let scope = Scope::new();
        let tcs = new_tcs_::<DropProbe_>(&scope);
        tcs.abandon_();
        assert!(!tcs.set_(DropProbe_ {
            count_: count.clone(),
        }));
        assert!(tcs.is_abandoned_());
        assert_eq!(count.load(Ordering::SeqCst), 1usize);
    }

    /// 测试克隆句柄共享同一个完成源：一份 `set_`，另一份取到值。
    /// - 手段：`clone()` 出两份，在副本上 `set_(11)`，在原句柄上 poll。
    /// - 判断：poll 返回 `Ready(Some(11))`；重复 `set_` 返回 `false`。
    ///
    /// Test goal: cloned handles share one source.
    /// Method: clone the handle, `set_(11)` through the clone, poll the original.
    /// Assertion: `Ready(Some(11))`; a second `set_` returns false.
    #[test]
    fn tcs_clone_shares_state_() {
        let scope = Scope::new();
        let tcs = new_tcs_::<u32>(&scope);
        let other = tcs.clone();
        let (waker, _probe) = counting_waker_();
        let mut cx = Context::from_waker(&waker);

        assert!(other.set_(11u32));
        assert!(!other.set_(12u32));
        assert!(matches!(
            tcs.poll_wait_(&mut cx),
            Poll::Ready(Option::Some(11u32))
        ));
    }

    /// 测试等待守卫在 `Drop` 时放弃完成源。
    /// - 手段：`TcsAbandonGuard_::new_(&tcs)` 放进一个块，块结束时守卫析构。
    /// - 判断：`is_abandoned_()` 为真；随后 `set_(1)` 返回 `false`。
    ///
    /// Test goal: the wait guard abandons the source when dropped.
    /// Method: create `TcsAbandonGuard_::new_(&tcs)` inside a block so it drops at
    /// the end of the block.
    /// Assertion: `is_abandoned_()` is true; a later `set_(1)` returns false.
    #[test]
    fn tcs_abandon_guard_abandons_on_drop_() {
        let scope = Scope::new();
        let tcs = new_tcs_::<u32>(&scope);
        {
            let _guard = TcsAbandonGuard_::new_(&tcs);
        }
        assert!(tcs.is_abandoned_());
        assert!(!tcs.set_(1u32));
    }

    /// 测试 `set_` 之后再 `abandon_` 不会丢掉已就绪的值。
    /// - 手段：`set_(13)` 后 `abandon_()`，再 poll。
    /// - 判断：`is_abandoned_()` 为假；poll 返回 `Ready(Some(13))`。
    ///
    /// Test goal: abandoning after completion keeps the ready value.
    /// Method: `set_(13)`, then `abandon_()`, then poll.
    /// Assertion: not abandoned; `Ready(Some(13))`.
    #[test]
    fn tcs_abandon_after_set_keeps_value_() {
        let scope = Scope::new();
        let tcs = new_tcs_::<u32>(&scope);
        let (waker, _probe) = counting_waker_();
        let mut cx = Context::from_waker(&waker);

        assert!(tcs.set_(13u32));
        tcs.abandon_();
        assert!(!tcs.is_abandoned_());
        assert!(matches!(
            tcs.poll_wait_(&mut cx),
            Poll::Ready(Option::Some(13u32))
        ));
    }

    /// 测试域关闭后完成源失效：`set_` 返回 `false`，`poll_wait_` 返回 `Ready(None)`。
    /// - 手段：`Scope::new()` 建完成源，`close()` 域，再访问完成源。
    /// - 判断：`set_` 为假；`poll_wait_` 为 `Ready(None)`；`is_abandoned_` 为真。
    ///
    /// Test goal: a closed domain makes the source fail closed.
    /// Method: create the source, `close()` the domain, then touch the source.
    /// Assertion: `set_` is false; `poll_wait_` is `Ready(None)`; `is_abandoned_` is true.
    #[test]
    fn tcs_closed_scope_reports_cancelled_() {
        let scope = Scope::new();
        let tcs = new_tcs_::<u32>(&scope);
        let (waker, _probe) = counting_waker_();
        let mut cx = Context::from_waker(&waker);

        scope.close();
        assert!(!tcs.set_(1u32));
        assert!(matches!(tcs.poll_wait_(&mut cx), Poll::Ready(Option::None)));
        assert!(tcs.is_abandoned_());
    }
}
