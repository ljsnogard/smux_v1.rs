//! [`timeout`] / [`timeout_at`] 与它们的失败类型 [`Elapsed`]。

use core::{
    future::{Future, poll_fn},
    pin::pin,
    task::Poll,
    time::Duration,
};

use embedded_timers::{
    clock::Clock,
    instant::Instant,
};

use super::{
    sleep_::sleep_until,
    wheel_::Timer,
};

/// [`timeout`] / [`timeout_at`] 的失败：期限已到，而内层 future 还没完成。
///
/// 字段是私有的：本类型只能由 [`timeout`] / [`timeout_at`] 产生，调用方**无法伪造**
/// 「期限已到」再把它经 `?` 塞进自己的错误类型里，从而不会出现「我没超时却报超时」的
/// 假信号。
///
/// # Examples
///
/// 正因为不可构造，例子里只能 `match` 出它：
///
/// ```no_run
/// use core::time::Duration;
///
/// use embedded_timers::{clock::Clock, instant::Instant64};
/// use smux_v1::time::{Timer, timeout};
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
/// async fn demo(timer: &Timer<ZeroClock>) {
///     let outcome = timeout(timer, Duration::from_millis(100), core::future::pending::<()>()).await;
///     match outcome {
///         Ok(()) => {}
///         Err(elapsed) => assert_eq!(elapsed.to_string(), "期限已到"),
///     }
/// }
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("期限已到")]
pub struct Elapsed(());

/// 要求 `future` 在 `duration` 之内完成。
///
/// 内层先完成则原样返回它的输出；期限先到则返回 [`Elapsed`]，并**丢弃**内层 future
/// （取消就是丢弃，与 [`crate::time`] 的其余等待者一致）。
///
/// # 平局
///
/// 同一轮轮询里内层与期限**都**就绪时，**内层赢**：`poll` 先问内层。这与 compio 用
/// `select!`（按声明顺序轮询）的行为一致。
///
/// # Panics
///
/// `now + duration` 不可表示（溢出）时 panic——与 `std::time::Instant` 的 `+` 一致。
///
/// # Examples
///
/// ```no_run
/// use core::time::Duration;
///
/// use embedded_timers::{clock::Clock, instant::Instant64};
/// use smux_v1::time::{Timer, timeout};
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
/// async fn demo(timer: &Timer<ZeroClock>) -> Result<u8, smux_v1::time::Elapsed> {
///     timeout(timer, Duration::from_millis(100), async { 7u8 }).await
/// }
/// ```
pub async fn timeout<C, F>(timer: &Timer<C>, duration: Duration, future: F) -> Result<F::Output, Elapsed>
where
    C: Clock,
    F: Future,
{
    let deadline = timer.now() + duration;
    let mut sleep = pin!(sleep_until(timer, deadline));
    let mut future = pin!(future);
    // 手写 select 而不是用 `futures::select!`：本仓的正式依赖里没有 `futures`
    // （它只在 dev-dependencies 里），而这里要的只是「两个 future 竞争、内层优先」，
    // 与 `connection::session_` 里 `race_cancel_` 的手写 `poll_fn` 同一种写法。
    poll_fn(|cx| {
        if let Poll::Ready(output) = future.as_mut().poll(cx) {
            return Poll::Ready(Ok(output));
        }
        if sleep.as_mut().poll(cx).is_ready() {
            return Poll::Ready(Err(Elapsed(())));
        }
        Poll::Pending
    })
    .await
}

/// 要求 `future` 在 `deadline` 之前完成。
///
/// `deadline` 已过时，除非内层在**第一次轮询**里就完成，否则立刻返回 [`Elapsed`]。
///
/// # Panics
///
/// 与 [`timeout`] 相同：`now + (deadline - now)` 不可表示时 panic。
///
/// # Examples
///
/// ```no_run
/// # use core::time::Duration;
/// # use embedded_timers::{clock::Clock, instant::Instant64};
/// # use smux_v1::time::{Timer, timeout_at};
/// # struct ZeroClock;
/// # impl Clock for ZeroClock {
/// #     type Instant = Instant64<1_000_000>;
/// #     fn now(&self) -> Self::Instant { Instant64::new(0) }
/// # }
/// async fn demo(timer: &Timer<ZeroClock>) {
///     let deadline = timer.now() + Duration::from_millis(100);
///     let _ = timeout_at(timer, deadline, async { 7u8 }).await;
/// }
/// ```
pub async fn timeout_at<C, F>(
    timer: &Timer<C>,
    deadline: C::Instant,
    future: F,
) -> Result<F::Output, Elapsed>
where
    C: Clock,
    F: Future,
{
    // 与 compio 同形：先把「还剩多久」量出来，再交给 `timeout` 去竞争。
    // `saturating_duration_since` 让「期限已过」退化为 0，于是 `timeout` 里的
    // `sleep_until(now)` 立刻完成——正是「期限已过即立刻超时」。
    timeout(timer, deadline.saturating_duration_since(timer.now()), future).await
}
