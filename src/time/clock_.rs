//! 连接级时钟：把**运行时值**与**连接建立时刻**（epoch）绑成「连接内毫秒」。
//!
//! # 为什么需要 epoch
//!
//! 协议里的时间量都是**相对连接**的：每子流「最后活动」、拆流宽限期到期、空闲超时
//! 期限。它们只要求单调，不要求任何绝对起点，因此一律记成
//! **自连接建立起算的毫秒数**（`u64`）。这样：
//!
//! - 每子流只存一个 `AtomicU64`，不存任何 `Instant` 值
//!   （`ChannelState_` 因此不必对时刻类型泛型）；
//! - 索引（拆流宽限期的到期集）可以按 `u64` 排序，比较与排序都退化成整数运算；
//! - 绝对时刻只在**本模块**出现一次（`epoch_`），其余各处只见毫秒。
//!
//! # 时刻与计时器**同源**：本模块不再有「注入的时钟」
//!
//! 时刻来源就是**运行时值** `R`（[`TrClock`] 的实现，例如
//! `abs_art_tokio::Runtime<{ FULL }>`）。这不是风格偏好，而是结构约束：
//! `abs_art::TrTime` 的超 trait 关系是 `TrTime: TrDelay + TrClock`，也就是说
//! **「能等」与「现在几点」由同一个值回答**。
//!
//! 曾经本模块持有一个**注入式**时钟（`embedded_timers::Clock`，与后端的计时器
//! 彼此独立）。那是「睡在虚拟时钟上、读在墙上时钟上」的错配结构：tokio 的
//! `start_paused` 虚拟时间下，空闲超时永远不会触发。改成从运行时值读时刻之后，
//! 这条错配在**类型层面**就不成立了——要假时钟，就换一个假的运行时值
//! （`abs_art_mock_clock::ManualTime`），时刻与 `delay` 必然一致。
//!
//! # 为什么是 `Clone`
//!
//! 时钟值要被**多个**持有者共享：核心（API 路径要读「现在」）与两个内侧循环
//! （`is_wait_close_` / `release_channel_` 需要「现在」）。运行时值本身
//! （`abs_art_tokio::Runtime` 等）是可克隆的把手，克隆即共享同一个计时器与时刻，
//! 因此这里沿用同一形状。
//!
//! # 为什么这里**没有** `deadline_()`
//!
//! 上一版还有一个 `deadline_(ms)`，把「连接内毫秒」还原成后端的绝对时刻，供
//! `TrDeadline::sleep_until` 使用。计时循环改用 `R: TrDelay` 之后，它只需要一个
//! **相对时长**（`delay(期限 − 现在)`），绝对时刻不再需要还原。顺带绕开一个坑：
//! [`TrClock::Instant`] 的结构约束只有 `Add<Duration>` 与 `Sub<Self>`，**没有**
//! `checked_add`（那是 `embedded_timers::Instant` 才有的），因此刻意不在这里
//! 做「epoch + 毫秒」的加法。

use core::time::Duration;

use abs_art::TrClock;

/// 连接级时钟：运行时值 + 建连时刻（epoch）。
///
/// 成员私有，只能经 [`ConnClock_::new_`] 建立；`Clone` 出的每一份共享**同一个**
/// epoch（epoch 是一个值，克隆即复制），因此各处算出的毫秒互相可比。
pub(crate) struct ConnClock_<R>
where
    R: TrClock,
{
    /// 时刻来源：与连接的计时器**同源**的那个运行时值。
    rt_: R,

    /// 连接建立起的那一刻：一切「连接内毫秒」的零点。
    epoch_: R::Instant,
}

impl<R> Clone for ConnClock_<R>
where
    R: TrClock + Clone,
{
    fn clone(&self) -> Self {
        ConnClock_ {
            rt_: self.rt_.clone(),
            epoch_: self.epoch_,
        }
    }
}

impl<R> ConnClock_<R>
where
    R: TrClock,
{
    /// 以「此刻」为 epoch 建立连接级时钟。
    pub(crate) fn new_(rt: R) -> Self {
        let epoch_ = rt.now();
        ConnClock_ { rt_: rt, epoch_ }
    }

    /// 运行时值本身（计时循环经它等「下一个期限」）。
    pub(crate) fn rt_(&self) -> &R {
        &self.rt_
    }

    /// 建连时刻（epoch）本身。
    ///
    /// 核心只存这个**纯数据**（而不是整个运行时值），因为核心必须无条件
    /// `Send + Sync`，而 compio 的运行时值是 `!Send + !Sync`——见
    /// `mux_connection::core_` 的类型文档。
    pub(crate) fn epoch_(&self) -> R::Instant {
        self.epoch_
    }

    /// 自连接建立起算的毫秒数（单调不减）。
    pub(crate) fn now_millis_(&self) -> u64 {
        // `TrClock::Instant: Sub<Self, Output = Duration>` 是「还有多久 / 过去多久」
        // 的唯一原语，因此这里不需要任何 `elapsed()` 之类的额外约定。
        millis_of_(self.rt_.now() - self.epoch_)
    }
}

/// [`Duration`] → 毫秒；超出 `u64` 表示范围时饱和。
///
/// 饱和而不是截断：一个「比 `u64::MAX` 毫秒还久」的时长在语义上就是「永不」，
/// 截断成一个小数字会让超时提前触发。
pub(crate) fn millis_of_(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}
