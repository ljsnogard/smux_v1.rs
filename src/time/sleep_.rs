//! [`sleep`] / [`sleep_until`] 与它们背后的等待者。
//!
//! 等待者 `Sleep_` 是**私有**类型：对外只承诺「一个 `await` 到点的 future」。这样
//! 将来若要加一条「不占 `BTreeMap` 节点」的单等待者快路径（compio 的
//! `TimerFuture` 就是每条一个节点），改动不构成公开面变更。

use core::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

use embedded_timers::clock::Clock;

use super::wheel_::{Timer, TimerKey_};

/// 等到 `deadline` 那一刻。
///
/// `deadline` 已过则**立刻**完成，且**不在轮盘上占位**（因此不会把驱动方拴在一个
/// 早已到期的期限上）。轮盘不会自己醒来，就绪的前提是有人驱动它——见 [`Timer`]。
///
/// # Panics
///
/// 本函数自身不 panic；panic 只会来自调用方构造 `deadline` 时的时刻加法。
///
/// # Examples
///
/// 与所有 [`crate::time`] 的等待者一样，`sleep_until` 就绪的前提是**有人驱动轮盘**
/// （见 [`Timer`]），因此下面只示意登记动作本身：
///
/// ```no_run
/// use embedded_timers::{clock::Clock, instant::Instant64};
/// use smux_v1::time::{Timer, sleep_until};
///
/// struct ZeroClock;
///
/// impl Clock for ZeroClock {
///     type Instant = Instant64<1_000_000>;
///     fn now(&self) -> Self::Instant {
///         Instant64::new(0)
///     }
/// }
///
/// async fn wait(timer: &Timer<ZeroClock>) {
///     sleep_until(timer, timer.now()).await;
/// }
/// ```
pub async fn sleep_until<C: Clock>(timer: &Timer<C>, deadline: C::Instant) {
    // 期限已过 ⇒ 没有等待者可言，直接完成（compio 的 `TimerFuture::try_new` 同形）。
    if let Some(sleep) = Sleep_::try_new_(timer, deadline) {
        sleep.await;
    }
}

/// 等 `duration` 那么久。
///
/// 等价于 `sleep_until(timer, timer.now() + duration)`；注意「现在」是**首次轮询**
/// 那一刻取的，不是调用本函数那一刻。
///
/// # Panics
///
/// `now + duration` 不可表示（溢出）时 panic——与 `std::time::Instant` 的 `+` 一致。
///
/// # Examples
///
/// ```no_run
/// # use core::time::Duration;
/// use embedded_timers::{clock::Clock, instant::Instant64};
/// use smux_v1::time::{Timer, sleep};
///
/// struct ZeroClock;
///
/// impl Clock for ZeroClock {
///     type Instant = Instant64<1_000_000>;
///     fn now(&self) -> Self::Instant {
///         Instant64::new(0)
///     }
/// }
///
/// async fn wait(timer: &Timer<ZeroClock>) {
///     sleep(timer, Duration::from_millis(100)).await;
/// }
/// ```
pub async fn sleep<C: Clock>(timer: &Timer<C>, duration: Duration) {
    sleep_until(timer, timer.now() + duration).await
}

/// 轮盘上的一个一次性等待者。
///
/// `key_` 为 `None` 表示「不必再登记」（期限已过，或已经就绪）。丢弃即撤销登记，
/// 因此**取消就是丢弃**——与 compio 的 `TimerFuture` 相同。
pub(crate) struct Sleep_<C: Clock> {
    key_: Option<TimerKey_<C::Instant>>,
    timer_: Timer<C>,
}

impl<C: Clock> Sleep_<C> {
    /// 登记一个新的等待者；`deadline` 已过则返回 `None`（调用方应当立刻完成）。
    pub(crate) fn try_new_(timer: &Timer<C>, deadline: C::Instant) -> Option<Self> {
        let key_ = timer.insert_(deadline)?;
        Some(Self {
            key_: Some(key_),
            timer_: timer.clone(),
        })
    }
}

impl<C: Clock> Future for Sleep_<C> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        // `Sleep_` 的每个字段都 `Unpin`（`Instant: Unpin`，`Timer<C>` 只含 `Rc`），
        // 因此 `get_mut` 成立，不需要 `unsafe` 的 `Pin::new_unchecked`。
        let this = self.get_mut();
        let Some(key) = this.key_ else {
            return Poll::Ready(());
        };
        let polled = this.timer_.poll_timer_(cx, &key);
        if polled.is_ready() {
            // 就绪即注销键：此后 `Drop` 不必再去轮盘上找一次。
            this.key_ = None;
        }
        polled
    }
}

impl<C: Clock> Drop for Sleep_<C> {
    fn drop(&mut self) {
        if let Some(key) = self.key_.take() {
            // 尚未就绪就被丢弃：把登记撤掉。否则轮盘上会留下一个再也不会被轮询的
            // waker 槽，`min_timeout` 也会一直把驱动方拴在这个死期限上。
            self.timer_.cancel_(&key);
        }
    }
}
