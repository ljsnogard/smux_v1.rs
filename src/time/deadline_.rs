//! 绝对期限能力：在 [`TrTime`] 之上，用注入的 [`Clock`] 补出**绝对期限**形式。
//!
//! 这里定义的是 trait + 对所有 `T: TrTime` 的 blanket 实现，因此调用点长成
//! **运行时类型的关联函数**（与 `abs_art` 家族既有的
//! `Runtime::block_on(..)` / `Runtime::delay(..)` / `scope.spawn_local(..)` 同形）：
//!
//! ```text
//! D::sleep_until(&clock, deadline).await
//! D::timeout_at(&clock, deadline, future).await
//! ```
//!
//! 其中 `D` 是最终二进制选中的后端（`abs_art_tokio::Runtime<{ FULL }>` 等）。

use core::future::Future;

use abs_art::TrTime;
use embedded_timers::{
    clock::Clock,
    instant::Instant,
};

/// 「带时钟的计时能力」：把后端的相对计时（[`TrTime`]）与本仓注入的时钟绑成
/// **绝对期限**的关联函数。
///
/// # 为什么是 trait + blanket 实现，而不是自由函数
///
/// 两个方法各需要两样东西：后端的计时能力（挂在运行时的 `Runtime<CAPS>` 上）与
/// 调用方注入的时钟（`&C`）。若做成自由函数，调用点就会是
/// `sleep_until::<D, C>(&clock, deadline)`——**运行时类型退化成一个 turbofish
/// 参数**，与 `abs_art` 家族「能力挂在运行时类型上」的形状不一致，读起来也看不出
/// 它用的是哪个运行时。
///
/// blanket 实现让任何 `T: TrTime` 自动获得本 trait，因此业务代码只要写
/// `D::sleep_until(..)` 即可，不需要额外给后端类型加实现。
///
/// # 与 [`TrTime`] 的分工
///
/// [`TrTime`] 只认**相对时长**（`Duration`），因此它不必知道任何 `Instant` 类型
/// （理由见 `abs_art::time` 模块文档）；本 trait 负责用时钟把绝对期限**折算**成
/// 相对时长，再交给 [`TrDelay::delay`](abs_art::TrDelay::delay) / [`TrTime::timeout`]。
///
/// 这样「后端」与「时刻来源」各自独立：后端由最终二进制选中，而时刻来源是**注入**的，
/// 因此测试可以给假时钟做确定性验收。
pub trait TrDeadline: TrTime {
    /// 等到 `deadline` 那一刻；`deadline` 已过则**立刻**完成。
    ///
    /// 返回类型就是后端的 [`TrDelay::Delay`](abs_art::TrDelay::Delay)——**具体类型**
    /// （不是 `impl Future`），因此调用方能命名它、也能对它写自动 trait 约束。
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use core::time::Duration;
    ///
    /// use abs_art::FULL;
    /// use abs_art_tokio::Runtime;
    /// use embedded_timers::{clock::Clock, instant::Instant64};
    /// use smux_v1::time::TrDeadline;
    ///
    /// /// 演示用的微秒时钟。
    /// struct ZeroClock;
    ///
    /// impl Clock for ZeroClock {
    ///     type Instant = Instant64<1_000_000>;
    ///     fn now(&self) -> Self::Instant {
    ///         Instant64::new(0)
    ///     }
    /// }
    ///
    /// # async fn demo() {
    /// let clock = ZeroClock;
    /// let deadline = clock.now() + Duration::from_millis(100);
    /// Runtime::<{ FULL }>::sleep_until(&clock, deadline).await;
    /// # }
    /// ```
    fn sleep_until<C>(clock: &C, deadline: C::Instant) -> Self::Delay
    where
        C: Clock,
        Self: Sized,
    {
        // 折算成相对时长即完事：饱和减法让「期限已过」退化为「睡 0」，立刻返回。
        Self::delay(deadline.saturating_duration_since(clock.now()))
    }

    /// 要求 `future` 在 `deadline` 之前完成。
    ///
    /// 内层先完成则原样返回其输出；期限先到则返回 [`Elapsed`](abs_art::Elapsed)
    /// 并丢弃内层 future。
    /// 判定细节（平局时内层赢、不早于期限结束）由 [`TrTime::timeout`] 的契约给出，
    /// 本方法只负责折算绝对期限。
    ///
    /// # Errors
    ///
    /// 期限先到时返回 [`Elapsed`](abs_art::Elapsed)。
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use core::time::Duration;
    ///
    /// use abs_art::FULL;
    /// use abs_art_tokio::Runtime;
    /// use embedded_timers::{clock::Clock, instant::Instant64};
    /// use smux_v1::time::{Elapsed, TrDeadline};
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
    /// # async fn demo() -> Result<u8, Elapsed> {
    /// let clock = ZeroClock;
    /// let deadline = clock.now() + Duration::from_millis(100);
    /// Runtime::<{ FULL }>::timeout_at(&clock, deadline, async { 7u8 }).await
    /// # }
    /// ```
    fn timeout_at<C, F>(
        clock: &C,
        deadline: C::Instant,
        future: F,
    ) -> abs_art::Timeout<Self, F>
    where
        C: Clock,
        F: Future,
        Self: Sized,
    {
        // 返回类型是 `TrTime::Timeout` 这个**具体类型**（不是 `impl Future`），
        // 因此调用方可以命名它、对它写自动 trait 约束。
        Self::timeout(deadline.saturating_duration_since(clock.now()), future)
    }
}

/// 任何具备计时能力的类型都自动获得绝对期限形式。
impl<T: TrTime> TrDeadline for T {}
