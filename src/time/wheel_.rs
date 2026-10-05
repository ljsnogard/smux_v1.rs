//! 轮盘：[`Timer`] 及其内部的期限索引。
//!
//! 本模块是 compio `runtime::time::TimerRuntime` 的**去运行时化**移植：compio 把
//! 轮盘挂在全局 `Runtime` 上、由运行时的事件循环驱动；本仓没有、也不该有一个全局
//! 运行时（连接可能跑在 tokio 或 compio 上），因此轮盘改成**调用方持有的值**
//! （[`Timer`]），由连接自己的 tick 循环驱动。
//!
//! # 驱动协议
//!
//! 轮盘**自己不会醒来**，也不会自己去等；它只维护「谁想在什么时刻被唤醒」。驱动方
//! （连接的 tick 循环）按下面的次序循环：
//!
//! 1. [`Timer::min_timeout`] 取最近期限还剩多久（`None` = 轮盘为空）；
//! 2. 用运行时提供的异步等待源等那么久，**并与取消令牌竞争**；
//! 3. [`Timer::wake`] 唤醒所有已到期等待者，回到第 1 步。
//!
//! 第 2 步的等待源不在本模块内：`abs_art::TrDelay::delay` 是关联函数、返回不透明的
//! `impl Future`（不可存储、不可重挂），具体形状仍待裁决（见
//! `dev-notes/keepalive-20261005-0901.md` §5.2）。本模块只要求「能等一个 `Duration`」。
//!
//! **登记不会唤醒驱动方**：新登记一个更早的期限时，驱动方可能正等在一个更晚的期限
//! 上（甚至该期限已被它算过期）。compio 靠「事件循环每轮都重算 `min_timeout`」把
//! 这一点掩盖在调度粒度里；本仓的落点（复用既有两条事件通道，还是新增一个连接级
//! 通知槽）同样待裁决（同文 §5.5）。因此本模块**只提供** [`Timer::min_timeout`] 与
//! [`Timer::len`] 供驱动方判断，不预判那个形状。

use alloc::{
    collections::BTreeMap,
    rc::Rc,
};
use core::{
    cell::RefCell,
    fmt,
    mem,
    task::{Context, Poll, Waker},
    time::Duration,
};

use embedded_timers::{
    clock::Clock,
    instant::Instant,
};

/// 轮盘上的一个键：**期限 + 生成序号**。
///
/// 生成序号不是装饰：`BTreeMap` 以键为唯一性判据，两个**期限完全相同**的等待者若
/// 共用同一个键，后登记的那个会把先登记者的 waker 槽顶掉，先登记者就再也醒不过来。
/// 序号让每一次登记都得到互不相等的键；而同一个等待者的一生里键保持不变。
///
/// 字段顺序即比较顺序：先比期限、再比序号——[`TimerWheel_::take_expired_`] 的
/// `split_off` 正是靠这个顺序一次性切出「全部已到期者」。
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct TimerKey_<I> {
    deadline_: I,
    generation_: u64,
}

/// 期限索引：`(期限, 序号) → waker 槽`。
///
/// 槽为 `None` 表示「已登记、但还没被轮询过」——登记（`insert_`）与首次轮询
/// （`poll_timer_`）之间必然有一个没有 waker 的窗口。
#[derive(Debug)]
pub(crate) struct TimerWheel_<I> {
    generation_: u64,
    wheel_: BTreeMap<TimerKey_<I>, Option<Waker>>,
}

impl<I: Instant> TimerWheel_<I> {
    /// 空的轮盘。
    pub(crate) fn new_() -> Self {
        Self {
            generation_: 0,
            wheel_: BTreeMap::new(),
        }
    }

    /// 轮盘上的登记数。
    pub(crate) fn len_(&self) -> usize {
        self.wheel_.len()
    }

    /// 该键是否**已不在**轮盘上。
    ///
    /// 「已不在」即就绪：等待者被唤醒的唯一途径是 [`Self::take_expired_`] 把它摘掉，
    /// 因此轮询只需问这一个问题，不需要额外的状态位。
    pub(crate) fn is_completed_(&self, key: &TimerKey_<I>) -> bool {
        !self.wheel_.contains_key(key)
    }

    /// 登记一个期限；`deadline <= now` 时**不登记**并返回 `None`（已经到期，调用方
    /// 应当立刻完成，不必在轮盘上占位）。
    ///
    /// `now` 由调用方传入而不是自己取：轮盘只需要**时刻类型**（`I`）就能工作，
    /// 不必牵连整个 [`Clock`]。
    pub(crate) fn insert_(&mut self, now: I, deadline: I) -> Option<TimerKey_<I>> {
        if deadline <= now {
            return None;
        }
        let key = TimerKey_ {
            deadline_: deadline,
            generation_: self.generation_,
        };
        self.wheel_.insert(key, None);
        // 这里用回绕而非 panic：序号只是「同一期限内的区分位」，要撞键需要 2^64 次
        // 登记、**且**那个同期限的等待者还挂在轮盘上。真撞上也只是「先登记者的 waker
        // 被顶掉」，不产生任何不安全行为，因此不值得为它在库代码里 panic
        // （AGENTS §6：库代码不用 unwrap / expect）。
        self.generation_ = self.generation_.wrapping_add(1);
        Some(key)
    }

    /// 登记 / 更新某个键的 waker。
    pub(crate) fn update_waker_(&mut self, key: &TimerKey_<I>, waker: &Waker) {
        // 已被唤醒（键已不在轮盘上）就什么都不做：waker 无处可挂，也不该挂。
        let Some(slot) = self.wheel_.get_mut(key) else {
            return;
        };
        // 同一个 waker 不重复克隆：每次轮询都克隆一份 waker 是白花的分支。
        if let Some(existing) = slot.as_ref()
            && existing.will_wake(waker)
        {
            return;
        }
        *slot = Some(waker.clone());
    }

    /// 撤销一个登记（丢弃尚未就绪的等待者时用）。
    pub(crate) fn cancel_(&mut self, key: &TimerKey_<I>) {
        self.wheel_.remove(key);
    }

    /// 最近期限距 `now` 还有多久。
    ///
    /// 已到期但尚未被 [`Self::take_expired_`] 摘除时饱和为 [`Duration::ZERO`]：
    /// 驱动方据此知道「立刻再 `wake` 一轮」，而不是拿到一个负时长。
    pub(crate) fn min_timeout_(&self, now: I) -> Option<Duration> {
        self.wheel_
            .first_key_value()
            .map(|(key, _)| key.deadline_.saturating_duration_since(now))
    }

    /// 把全部**已到期**的登记**切下来并交还**（而不是就地唤醒）。
    ///
    /// 返回值的意义在于「借用已经结束」：调用方拿到这个 `BTreeMap` 时轮盘已经不再
    /// 被借用，于是可以**在借用之外**逐个 `waker.wake()`。这一点比 compio 原版更严
    /// ——原版在 `borrow_mut()` 内直接 `wake`，而 `wake` 可能**同步重入** `poll` 并
    /// 再次去借轮盘，那就是一次 `RefCell` 重入 panic。同时这也与本仓既有的
    /// 「锁内只置位 / 取 waker，锁外 `wake`」纪律（`dev-notes/keepalive…` §6.3）一致。
    ///
    /// 切下来的节点随返回值一起移动，唤醒完即整体释放，因此**不额外分配**。
    ///
    /// # 到期判据
    ///
    /// 用 `<=`（`deadline == now` 即到期）：时钟单调，`now` 一旦越过某个期限就不会
    /// 回头，因此可以安全地「切下来就不再放回」；而 `<=` 让测试里的假时钟能正好停
    /// 在期限上验收。
    pub(crate) fn take_expired_(&mut self, now: I) -> BTreeMap<TimerKey_<I>, Option<Waker>> {
        if self.wheel_.is_empty() {
            // 空轮盘直接给一个空 map：`BTreeMap::new()` 不分配。
            return BTreeMap::new();
        }
        // `split_off` 返回「`>= 切点`」的部分、自身留下「`< 切点`」的部分。切点取
        // `(now, u64::MAX)` ⇒ 留下来的正是 `deadline < now`，或
        // （`deadline == now` 且序号 `< u64::MAX`）——即全部已到期者。
        let pending = self.wheel_.split_off(&TimerKey_ {
            deadline_: now,
            generation_: u64::MAX,
        });
        mem::replace(&mut self.wheel_, pending)
    }

    /// 轮询一个等待者：已不在轮盘上即就绪，否则登记 waker 后挂起。
    pub(crate) fn poll_timer_(&mut self, cx: &mut Context<'_>, key: &TimerKey_<I>) -> Poll<()> {
        if self.is_completed_(key) {
            Poll::Ready(())
        } else {
            self.update_waker_(key, cx.waker());
            Poll::Pending
        }
    }
}

/// **每个连接一个**的计时器轮盘句柄。
///
/// 它是 [`Rc`] + [`RefCell`] 的共享句柄：`Clone` 只加引用计数，因此「驱动的 tick
/// 循环」与「登记等待者的那些循环」可以各持一份，且**不需要**调用方规定谁先构造。
/// 三个用法上的约定：
///
/// - **不跨线程**：[`Rc`] / [`RefCell`] 使本类型 `!Send`（与 compio 的 `TimerFuture`
///   一致）。连接的四个循环本就经 `TrLocalScope::spawn_local` 投递在同一个本地作用
///   域上，因此这不是限制。
/// - **唤醒在借用之外发生**：见 [`Timer::wake`]——`waker.wake()` 不会在持有
///   [`RefCell`] 借用时被调用，因而不怕 waker 同步重入 `poll`。
/// - **登记不会唤醒驱动方**：见本模块文档。驱动方在把轮盘等空之后，必须 park 在
///   「有人登记了新期限」这条通知上；该通知的落点尚待裁决，本模块不预判。
///
/// # Examples
///
/// ```
/// use embedded_timers::{clock::Clock, instant::Instant64};
/// use smux_v1::time::Timer;
///
/// /// 演示用的时钟：`now` 恒为 0。
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
/// // 没有等待者时轮盘是空的：驱动方应当 park 在「有新登记」的通知上，而不是忙等。
/// assert!(timer.is_empty());
/// assert_eq!(timer.min_timeout(), None);
/// ```
pub struct Timer<C: Clock> {
    wheel_: Rc<RefCell<TimerWheel_<C::Instant>>>,
    clock_: Rc<C>,
}

// 手写而不是 `derive(Clone)`：`derive` 会给 `C` 加上 `C: Clone` 约束，而本类型
// 克隆的是**两个 `Rc`**，与 `C` 本身能不能克隆毫无关系。
impl<C: Clock> Clone for Timer<C> {
    fn clone(&self) -> Self {
        Self {
            wheel_: Rc::clone(&self.wheel_),
            clock_: Rc::clone(&self.clock_),
        }
    }
}

// 手写 `Debug` 的理由同上（不给 `C` 加 `Debug` 约束），并且用 `try_borrow` 而不是
// `borrow`：`Debug` 可能在与 `wake` / `poll` 相邻的日志里被调用，为了打印而 panic
// 是最坏的选择。
impl<C: Clock> fmt::Debug for Timer<C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let pending = self.wheel_.try_borrow().map(|wheel| wheel.len_()).ok();
        f.debug_struct("Timer")
            .field("pending", &pending)
            .finish_non_exhaustive()
    }
}

impl<C: Clock> Timer<C> {
    /// 以一个时钟值新建轮盘。
    ///
    /// # Examples
    ///
    /// ```
    /// use embedded_timers::{clock::Clock, instant::Instant64};
    /// use smux_v1::time::Timer;
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
    /// assert_eq!(timer.now(), Instant64::new(0));
    /// ```
    pub fn new(clock: C) -> Self {
        Self {
            wheel_: Rc::new(RefCell::new(TimerWheel_::new_())),
            clock_: Rc::new(clock),
        }
    }

    /// 当前时刻。
    ///
    /// # Examples
    ///
    /// 见 [`Timer::new`]。
    pub fn now(&self) -> C::Instant {
        self.clock_.now()
    }

    /// 最近期限距「现在」还有多久；轮盘为空时返回 `None`。
    ///
    /// 返回 [`Duration::ZERO`] 表示「已经有到期的等待者，立刻 `wake` 一轮」。
    ///
    /// # Examples
    ///
    /// ```
    /// # use embedded_timers::{clock::Clock, instant::Instant64};
    /// # use smux_v1::time::Timer;
    /// # struct ZeroClock;
    /// # impl Clock for ZeroClock {
    /// #     type Instant = Instant64<1_000_000>;
    /// #     fn now(&self) -> Self::Instant { Instant64::new(0) }
    /// # }
    /// let timer = Timer::new(ZeroClock);
    /// assert_eq!(timer.min_timeout(), None);
    /// ```
    pub fn min_timeout(&self) -> Option<Duration> {
        let now = self.now();
        self.wheel_.borrow().min_timeout_(now)
    }

    /// 唤醒所有已到期的等待者，并把它们从轮盘上摘除。
    ///
    /// # 借用与唤醒
    ///
    /// 「切出到期者」与「调用它们的 waker」被**刻意分成两步**：前者在
    /// [`RefCell`] 借用之内完成并返回一个不再借用轮盘的局部 `BTreeMap`，后者在借用
    /// 之外进行。这样即便某个 waker 同步重入 `poll` 并再次借轮盘，也不会撞上
    /// `RefCell` 的重入 panic，同时不产生额外分配。
    ///
    /// # Examples
    ///
    /// ```
    /// # use embedded_timers::{clock::Clock, instant::Instant64};
    /// # use smux_v1::time::Timer;
    /// # struct ZeroClock;
    /// # impl Clock for ZeroClock {
    /// #     type Instant = Instant64<1_000_000>;
    /// #     fn now(&self) -> Self::Instant { Instant64::new(0) }
    /// # }
    /// let timer = Timer::new(ZeroClock);
    /// // 空轮盘上 wake 是空操作（也不会为唤醒去分配）。
    /// timer.wake();
    /// assert!(timer.is_empty());
    /// ```
    pub fn wake(&self) {
        let now = self.now();
        let expired = self.wheel_.borrow_mut().take_expired_(now);
        for (_, slot) in expired {
            if let Some(waker) = slot {
                waker.wake();
            }
        }
    }

    /// 轮盘上没有等待者时返回 `true`。
    ///
    /// 驱动方据此选择 park 落点：空轮盘时「等到最近期限」没有意义，应当 park 在
    /// 「有人登记了新期限」的通知上（落点待裁决，见模块文档）。
    ///
    /// # Examples
    ///
    /// 见 [`Timer::new`]。
    pub fn is_empty(&self) -> bool {
        self.wheel_.borrow().len_() == 0
    }

    /// 轮盘上的等待者数量。
    ///
    /// # Examples
    ///
    /// 见 [`Timer::new`]。
    pub fn len(&self) -> usize {
        self.wheel_.borrow().len_()
    }

    /// 登记一个期限（[`crate::time::sleep_until`] 一侧用）。
    pub(crate) fn insert_(&self, deadline: C::Instant) -> Option<TimerKey_<C::Instant>> {
        let now = self.now();
        self.wheel_.borrow_mut().insert_(now, deadline)
    }

    /// 撤销一个登记（等待者被丢弃时用）。
    pub(crate) fn cancel_(&self, key: &TimerKey_<C::Instant>) {
        self.wheel_.borrow_mut().cancel_(key);
    }

    /// 轮询一个等待者（已到期即就绪）。
    pub(crate) fn poll_timer_(&self, cx: &mut Context<'_>, key: &TimerKey_<C::Instant>) -> Poll<()> {
        self.wheel_.borrow_mut().poll_timer_(cx, key)
    }
}
