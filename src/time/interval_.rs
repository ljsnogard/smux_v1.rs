//! [`Interval`] 与它的两个构造函数。

use core::time::Duration;

use embedded_timers::clock::Clock;

use super::{
    sleep_::sleep_until,
    wheel_::Timer,
};

/// [`interval`] / [`interval_at`] 的返回值：按固定周期产生时刻的**序列**。
///
/// 对外只有 [`Interval::tick`] 一个入口。本类型**不**实现 `futures_core::Stream`：
/// 那会把 `futures-core` 拉进正式依赖，而连接层只需要 `tick().await`。
///
/// # 为什么不是「循环里 `sleep`」
///
/// `Interval` 记的是**相位**（相对起点 `start_` 的整周期数），不是「上一觉睡了多久」。
/// 若两次 `tick` 之间应用自己干了 1 秒，下一次 `tick` 只会等剩下的那部分；而「循环里
/// `sleep(period)`」会把这一秒也算进去，周期被拖长（compio 文档里的同一段话）。
pub struct Interval<C: Clock> {
    first_ticked_: bool,
    start_: C::Instant,
    period_: Duration,
    timer_: Timer<C>,
}

impl<C: Clock> Interval<C> {
    /// 由 [`interval`] / [`interval_at`] 构造（周期为 0 的检查在那边做）。
    pub(crate) fn new_(timer: &Timer<C>, start: C::Instant, period: Duration) -> Self {
        Self {
            first_ticked_: false,
            start_: start,
            period_: period,
            timer_: timer.clone(),
        }
    }

    /// 等到周期里的下一个时刻，并返回那个时刻。
    ///
    /// 第一次调用在 `start_` 完成（已过则立刻完成）；此后每次都**相位对齐**到
    /// `start_ + k * period`（`k` 为整数），而不是「上一次 tick 之后再过 `period`」。
    ///
    /// 与所有等待者一样，就绪的前提是**有人驱动轮盘**——见 [`Timer`]。
    ///
    /// # Panics
    ///
    /// `now + period` 不可表示（溢出）时 panic——与 `std::time::Instant` 的 `+` 一致。
    /// `period` 为零在构造时就已经被 [`interval_at`] 拒绝。
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use core::time::Duration;
    ///
    /// use embedded_timers::{clock::Clock, instant::Instant64};
    /// use smux_v1::time::{Timer, interval};
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
    /// async fn ticks(timer: &Timer<ZeroClock>) {
    ///     let mut interval = interval(timer, Duration::from_millis(10));
    ///     interval.tick().await; // 立即完成
    ///     interval.tick().await; // 10 ms 之后
    /// }
    /// ```
    pub async fn tick(&mut self) -> C::Instant {
        if !self.first_ticked_ {
            sleep_until(&self.timer_, self.start_).await;
            self.first_ticked_ = true;
            return self.start_;
        }
        let now = self.timer_.now();
        // 相位对齐：把「距 `start_` 的整周期余数」从下一个周期里扣掉。
        //
        // `now - start_` 用 `Sub<Self>`（饱和到 0），因此时钟若因故回退也不会 panic；
        // 余数天然小于 `period`。上面 `interval_at` 已挡住 `period == 0`，此处不会除零。
        let since_start = (now - self.start_).as_nanos() % self.period_.as_nanos();
        let next = now + self.period_ - Duration::from_nanos(since_start as u64);
        sleep_until(&self.timer_, next).await;
        next
    }
}

/// 创建从**现在**开始、每隔 `period` 产生一个时刻的 [`Interval`]。
///
/// 第一次 [`Interval::tick`] **立即**完成（从现在这一刻算起）。
///
/// # Panics
///
/// `period` 为零时 panic。
///
/// # Examples
///
/// ```
/// use core::time::Duration;
///
/// use embedded_timers::{clock::Clock, instant::Instant64};
/// use smux_v1::time::{Timer, interval};
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
/// let timer = Timer::new(ZeroClock);
/// let _interval = interval(&timer, Duration::from_millis(10));
/// // 只是构造：第一个 tick 要等驱动方推时钟。
/// assert!(timer.is_empty());
/// ```
pub fn interval<C: Clock>(timer: &Timer<C>, period: Duration) -> Interval<C> {
    interval_at(timer, timer.now(), period)
}

/// 创建从 `start` 开始、每隔 `period` 产生一个时刻的 [`Interval`]。
///
/// 第一次 [`Interval::tick`] 在 `start` 完成（`start` 已过则立即完成）。
///
/// # Panics
///
/// `period` 为零时 panic。
///
/// # Examples
///
/// ```
/// # use core::time::Duration;
/// # use embedded_timers::{clock::Clock, instant::Instant64};
/// # use smux_v1::time::{Timer, interval_at};
/// # struct ZeroClock;
/// # impl Clock for ZeroClock {
/// #     type Instant = Instant64<1_000_000>;
/// #     fn now(&self) -> Self::Instant { Instant64::new(0) }
/// # }
/// let timer = Timer::new(ZeroClock);
/// // 50 ms 之后第一次，此后每 10 ms 一次。
/// let start = Instant64::new(50_000);
/// let _interval = interval_at(&timer, start, Duration::from_millis(10));
/// assert!(timer.is_empty());
/// ```
pub fn interval_at<C: Clock>(timer: &Timer<C>, start: C::Instant, period: Duration) -> Interval<C> {
    // 零周期会让 `tick` 里的 `% period.as_nanos()` 除零 panic，而「相隔零的无穷序列」
    // 语义上也没有意义：与其在深层 panic，不如在构造点就说清楚（compio 同形）。
    assert!(period > Duration::ZERO, "`period` must be non-zero.");
    Interval::new_(timer, start, period)
}
