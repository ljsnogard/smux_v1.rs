//! 连接级时钟：把注入的 [`Clock`] 与**连接建立时刻**（epoch）绑成「连接内毫秒」。
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
//! - 绝对时刻只在**本模块**出现一次（`deadline_`），供后端等待用。
//!
//! # 为什么是 `Clone`
//!
//! 时钟值要被**多个**持有者共享：核心（API 路径要读「现在」）、两个内侧循环
//! （`is_wait_close_` / `release_channel_` 需要「现在」）与计时循环（自己算期限）。
//! 注入的时钟因此要求可克隆——系统时钟是零大小类型，测试用的假时钟同样可以做成
//! 零大小类型，克隆没有任何实际代价。
//!
//! # 与 [`TrDeadline`](crate::time::TrDeadline) 的关系
//!
//! `TrDeadline` 要求调用方**同时**给出后端类型与 `Clock`；本模块把「同一连接共享
//! 同一个 epoch」这一点固化下来，于是连接内部各处读到的「现在」互相可比。

use core::time::Duration;

use embedded_timers::{clock::Clock, instant::Instant};

/// 系统时钟：[`std::time::Instant`] 作为时刻类型的缺省实现。
///
/// 它是 [`DefaultConnCfg`](crate::connection::DefaultConnCfg) 的缺省时钟，也是
/// 「不想自己提供时钟」的调用方可以直接用的那一个。零大小，克隆无代价。
///
/// # Examples
///
/// ```
/// use smux_v1::time::{Clock, SystemClock};
///
/// let clock = SystemClock;
/// let start = clock.now();
/// let after = clock.now();
/// assert!(after.duration_since(start) < core::time::Duration::from_secs(1u64));
/// ```
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SystemClock;

impl Clock for SystemClock {
    type Instant = std::time::Instant;

    fn now(&self) -> Self::Instant {
        std::time::Instant::now()
    }
}

/// 连接级时钟：注入的时钟值 + 建连时刻（epoch）。
///
/// 成员私有，只能经 [`ConnClock_::new_`] 建立；`Clone` 出的每一份共享**同一个**
/// epoch（epoch 是一个值，克隆即复制），因此各处算出的毫秒互相可比。
pub(crate) struct ConnClock_<K>
where
    K: Clock,
{
    /// 调用方注入的时钟。
    clock_: K,

    /// 连接建立起的那一刻：一切「连接内毫秒」的零点。
    epoch_: K::Instant,
}

impl<K> Clone for ConnClock_<K>
where
    K: Clock + Clone,
{
    fn clone(&self) -> Self {
        ConnClock_ {
            clock_: self.clock_.clone(),
            epoch_: self.epoch_,
        }
    }
}

impl<K> ConnClock_<K>
where
    K: Clock,
{
    /// 以「此刻」为 epoch 建立连接级时钟。
    pub(crate) fn new_(clock: K) -> Self {
        let epoch_ = clock.now();
        ConnClock_ {
            clock_: clock,
            epoch_,
        }
    }

    /// 注入的时钟本身（后端等待要用它把期限折算成相对时长）。
    pub(crate) fn clock_(&self) -> &K {
        &self.clock_
    }

    /// 自连接建立起算的毫秒数（单调不减）。
    pub(crate) fn now_millis_(&self) -> u64 {
        millis_of_(self.clock_.elapsed(self.epoch_))
    }

    /// 把「连接内毫秒」还原成一个绝对期限。
    ///
    /// 溢出（只可能出现在规格极小的时刻类型上、且已经接近其表示上限）时退化为
    /// 「此刻」：等待立刻返回，调用方下一轮重新计算，不会永久挂起。
    pub(crate) fn deadline_(&self, millis: u64) -> K::Instant {
        self.epoch_
            .checked_add(Duration::from_millis(millis))
            .unwrap_or_else(|| self.clock_.now())
    }
}

/// [`Duration`] → 毫秒；超出 `u64` 表示范围时饱和。
///
/// 饱和而不是截断：一个「比 `u64::MAX` 毫秒还久」的时长在语义上就是「永不」，
/// 截断成一个小数字会让超时提前触发。
pub(crate) fn millis_of_(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}
