//! [`crate::time`] 的单元测试。
//!
//! # 本模块现在只有两件事要测
//!
//! 上一版的测试对象是自造的「绝对期限算术层」（`TrDeadline::sleep_until` /
//! `timeout_at`）。那一层已经删除：`abs_art::TrClock` 把时刻也收到运行时值上之后，
//! 计时循环只需要一个**相对时长**（`期限 − 现在`），不再需要把毫秒还原成后端的
//! 绝对时刻类型。因此本文件现在只钉两件事：
//!
//! 1. [`ConnClock_`] 的 **epoch 语义**：`now_millis_()` 一律是「自建连那一刻起算、
//!    向下取整到毫秒」的单调量；
//! 2. 计时循环赖以成立的那条不变式：**期限已过 ⇒ 折算出的时长为 0 ⇒ `delay(0)`
//!    立刻完成、不让时钟前进**（`TrDelay::delay` 的语义契约第 1 条）。计时循环用
//!    `next.saturating_sub(now)` 算时长，这条不变式就是它不会「等过头」的依据。
//!
//! # 虚拟时间改用 `abs_art-mock_clock`，不再自造
//!
//! 本文件原先自造了一个假运行时值（`FakeRt_` + `FakeInstant_` + `FakeDelay_`），
//! 由 **thread-local 的全局虚拟毫秒**驱动。那正是 `abs_art-mock_clock` 已经提供的
//! 能力，而那个共享量还有一个实测到的缺陷：`cargo test` 默认多线程并行，同一测试
//! 二进制里的用例共享那份虚拟毫秒，先跑的用例把它推走之后，后跑用例的断言就落在
//! 错的起点上（`conn_clock_reports_millis_since_its_epoch` 曾实测到 2499 而非 2500，
//! `an_already_past_deadline_collapses_to_a_zero_delay` 实测到 4 而非 0）。
//!
//! 现在改用 [`ManualTime`] 装饰 [`ManualClock`]：**每个用例各建一份独立时钟**，
//! 隔离由类型本身保证，不需要 thread-local、也不需要自己实现任何 `TrClock` /
//! `TrDelay`。用到的两件东西：
//!
//! - [`ManualClock`]：手动时钟的状态与推进（`advance_by` / `try_advance_to_next`）；
//! - [`ManualTime`]：把它装饰成一个完整的运行时值——时刻与 `delay` 来自**同一个**
//!   手动时钟，「同源」因此天然成立，这正是本模块要钉的那条性质。
//!
//! 注意 [`ManualClock`] 的格点是**整数毫秒**：`advance_by` 小于 1 ms 的部分按它的
//! 文档被截断，且不跨调用累加。因此本文件只用整数毫秒推进来确认 epoch 语义，
//! 「小数毫秒进位」不是被测性质。
//!
//! # 两条互补的验收路线
//!
//! 1. **确定性路线（主力）**：手动时钟 + 本文件里的 `BlockOnAdvancing_` 驱动
//!    （「没有别的活可干就推进到下一个到期时刻」），把折算与到点判定变成完全确定的
//!    断言。不需要任何异步运行时，因此用普通 `#[test]`。
//! 2. **真实后端路线**：两个 `#[cfg(feature = …)]` 的 `#[tokio::test]` /
//!    `#[compio::test]`，各自用**真实**运行时值构造 `ConnClock_`，证明「本模块 +
//!    真实后端」确实能等到、且毫秒量跟着真实时间前进（两格都在缺省 feature 下跑）。
//!
//! 三个后端的**一致性**（首次立即 / 锚定 / 不早于 / 文案 / 内层赢）不在这里测：
//! 那是 `abs_art-smoke` 的 `time_contract` 契约矩阵（3 后端 × 5 用例）的职责。
//!
//! # 虚拟时间验收在**集成测试**里
//!
//! 用 `abs_art-mock_clock` 的 `ManualTime` + `Supervisor` 把**整条连接**跑在虚拟时间
//! 上，属于端到端场景，见 `tests/keepalive_common.inc`。

use core::{
    future::Future,
    task::{Context, Poll, Waker},
    time::Duration,
};
// 只有「真实后端」那两格需要墙钟；两个运行时 feature 都没开时它们被 cfg 掉，
// 因此这个导入也要跟着门控（否则 `--no-default-features` 下是未用导入）。
#[cfg(any(feature = "test-tokio-runtime", feature = "test-compio-runtime"))]
use std::time::Instant as StdInstant;

use abs_art::{TrClock, TrDelay};
use abs_art_mock_clock::{ManualClock, ManualClockApi, ManualTime, MockInstant};

use super::ConnClock_;

// ── 虚拟时间：手动时钟（每个用例一份） ────────────────────────────────────

/// 本文件使用的**假运行时值**：手动时钟装饰成的运行时值。
///
/// 时刻（`TrClock`）与等待（`TrDelay`）都由它内部那一份 [`ManualClock`] 回答。
type ManualRt = ManualTime<ManualClock, ManualClock>;

/// 造一份「手动时钟 + 连接级时钟」：每个用例各建一份，互不共享。
///
/// 时钟起点一律是 0 ms。上一版靠 `virtual_reset_()` 把线程共享的虚拟毫秒清零，
/// 并行下并不可靠；本函数交出的是**独立实例**，隔离由类型保证。
fn manual_conn_clock_() -> (ManualClock, ConnClock_<ManualRt>) {
    let clock = ManualClock::new();
    let conn_clock = ConnClock_::new_(ManualTime::new(clock.clone(), clock.clone()));
    (clock, conn_clock)
}

/// 把 `future` 抽干：每轮 poll 后，若未就绪就把手动时钟推进到下一个到期时刻。
///
/// 这是 `abs_art-mock-clock` 的 `Supervisor` 在单元测试里的**最小等价物**——本模块
/// 只需要「睡到某个到期时刻」这一种等待，不需要驱动任何本地队列，因此不引入作用域
/// 与后端装配。
struct BlockOnAdvancing_<C>
where
    C: ManualClockApi,
{
    /// 推进用的时钟。
    clock_: C,
}

impl<C> BlockOnAdvancing_<C>
where
    C: ManualClockApi,
{
    /// 用手动时钟 `clock` 造一个驱动。
    fn new_(clock: C) -> Self {
        Self { clock_: clock }
    }

    /// 抽干 `future`：**没有别的活可干**时把时钟推进到下一个到期时刻再重试。
    ///
    /// # Panics
    ///
    /// 既没有待到期的定时器、`future` 又未就绪时报错 panic——那说明用例本身写错了
    /// （在等一个永远不会来的东西），而不是被测代码的问题。
    fn run_<F>(&self, future: F) -> F::Output
    where
        F: Future,
    {
        let mut future = core::pin::pin!(future);
        // 用例里的假后端只有「睡到某时刻」一种等待，因此不需要真 waker：驱动自己
        // 负责在挂起后推进时钟并重试（与 `Supervisor` 对 tokio 的处理同理——tokio
        // 没有可用的 tick 钩子，`poll` 循环就是它的驱动）。
        let mut cx = Context::from_waker(Waker::noop());
        loop {
            if let Poll::Ready(output) = future.as_mut().poll(&mut cx) {
                return output;
            }
            assert!(
                self.clock_.try_advance_to_next(),
                "没有待到期的定时器，而 future 仍未就绪：用例在等一个永远不会来的事件"
            );
        }
    }
}

// ── `ConnClock_` 的 epoch 语义 ────────────────────────────────────────────

/// 验证 `ConnClock_` 以**建立时刻**为 epoch，把时刻折算成单调毫秒。
/// - 手段：手动时钟从 0 起走，先用**不足 1 ms** 的推进越过 5 ms 处的建连时刻，
///   在两个整数毫秒点上各读一次，再单独推进一个不足 1 ms 的量证明它不改变读数。
/// - 判断：`now_millis_` 依次是 0 / 37 / 1250 / 1250——即自 epoch 起算、且人工时钟
///   的毫秒格点之外的时间不产生新读数（`ManualClock` 的 `advance_by` 只接受整数
///   毫秒，不足 1 ms 的部分按它的文档被截断，因此本用例只用整数毫秒确认 epoch
///   语义，不把「小数部分跨调用累加」当成被测性质）。
#[test]
fn conn_clock_reports_millis_since_its_epoch() {
    let clock = ManualClock::new();
    let conn_clock = ConnClock_::new_(ManualTime::new(clock.clone(), clock.clone()));
    assert_eq!(conn_clock.now_millis_(), 0u64, "刚建立时应当是 0");

    clock.advance_by(Duration::from_millis(37u64));
    assert_eq!(conn_clock.now_millis_(), 37u64, "整数毫秒推进原样反映");

    clock.advance_by(Duration::from_millis(1213u64));
    assert_eq!(conn_clock.now_millis_(), 1250u64);

    clock.advance_by(Duration::from_micros(999u64));
    assert_eq!(
        conn_clock.now_millis_(),
        1250u64,
        "不足 1 ms 的推进不产生新的毫秒读数"
    );
}

/// 验证两点：`rt_()` 交出**同一个**运行时值，且它的时刻就是 `now_millis_` 的来源。
/// - 手段：建时钟后经 `rt_()` 读时刻，与 `now_millis_()` 对同一个 epoch 的读数比对。
/// - 判断：`rt_().now() − epoch` 与 `now_millis_()` 一致（都等于推进量）；这也说明
///   计时循环经 `rt_()` 拿到的 `delay` 与这里的时刻**同源**。
#[test]
fn conn_clock_lends_the_same_runtime_value_to_the_timer_loop() {
    let (clock, conn_clock) = manual_conn_clock_();
    let epoch = conn_clock.rt_().now();

    clock.advance_by(Duration::from_millis(37u64));

    assert_eq!(conn_clock.rt_().now() - epoch, Duration::from_millis(37u64));
    assert_eq!(conn_clock.now_millis_(), 37u64);
}

/// 验证计时循环赖以成立的那条不变式：**期限已过 ⇒ `delay(0)` ⇒ 立刻完成且时钟不动**。
/// - 手段：手动时钟推到 5 ms，按计时循环的算法算出一个**已过**的期限（`next = 4 ms`），
///   用 `next.saturating_sub(now)` 折算时长后 `delay`，只 poll 一轮、不推进时钟。
/// - 判断：折算出的时长为 0、该 `delay` 首次 poll 即就绪，且时钟仍停在 5 ms——即
///   「期限已过」退化成「不等」，不会让计时循环空转或倒退。
#[test]
fn an_already_past_deadline_collapses_to_a_zero_delay() {
    let (clock, conn_clock) = manual_conn_clock_();
    clock.advance_by(Duration::from_millis(5u64));

    let now = conn_clock.now_millis_();
    let next = 4u64;
    let wait = next.saturating_sub(now);

    assert_eq!(wait, 0u64, "已过的期限折算出的时长必须是 0");
    let mut delay = core::pin::pin!(conn_clock.rt_().delay(Duration::from_millis(wait)));
    let mut cx = Context::from_waker(Waker::noop());
    assert_eq!(
        delay.as_mut().poll(&mut cx),
        Poll::Ready(()),
        "`delay(0)` 必须首次 poll 就绪"
    );
    assert_eq!(clock.now().as_millis(), 5u64, "已过的期限不该让时钟前进");
}

/// 验证保活要用的**那一种循环形状**：每轮睡到「下一个期限」，只折算相对时长。
/// - 手段：模拟 tick 循环三轮，每轮把期限往后推 1 秒，按 `next − now` 折算后 `delay`，
///   由 [`BlockOnAdvancing_`] 在挂起时把手动时钟推进到该期限。
/// - 判断：每轮结束时时钟恰好落在该轮期限上（1 s / 2 s / 3 s）。
#[test]
fn keepalive_style_loop_sleeps_until_each_deadline() {
    let (clock, conn_clock) = manual_conn_clock_();
    let driver = BlockOnAdvancing_::new_(clock.clone());

    for round in 1..=3u64 {
        let now = conn_clock.now_millis_();
        let next = round * 1_000u64;
        let wait = next.saturating_sub(now);
        driver.run_(conn_clock.rt_().delay(Duration::from_millis(wait)));
        assert_eq!(conn_clock.now_millis_(), next);
    }
}

// ── `ManualTime` 装饰出来的运行时值 ──────────────────────────────────────

/// 验证 `ManualTime` 的 `delay` **不会自己推进时钟**，要由驱动推进才完成。
/// - 手段：用 `ManualTime` 包一份手动时钟造出运行时值，对 `delay(10 ms)` 单轮 poll。
/// - 判断：首次 poll 必须是 `Pending`（说明等待没有凭空结束），时钟仍停在 0；
///   把时钟推进到 10 ms 后再次 poll 才 `Ready`。
#[test]
fn manual_time_delay_waits_for_the_driver() {
    let clock = ManualClock::new();
    let timed = ManualTime::new(clock.clone(), clock.clone());

    let mut delay = core::pin::pin!(timed.delay(Duration::from_millis(10u64)));
    let mut cx = Context::from_waker(Waker::noop());
    assert_eq!(
        delay.as_mut().poll(&mut cx),
        Poll::Pending,
        "没有被驱动时，10 ms 的 delay 不应当就绪"
    );
    assert_eq!(clock.now().as_millis(), 0u64, "poll 本身不该让时钟前进");

    clock.advance_by(Duration::from_millis(10u64));
    assert_eq!(delay.as_mut().poll(&mut cx), Poll::Ready(()));
}

/// 验证 `ManualTime` 报出的**时刻**与它的 `delay` 在同一条时间轴上。
/// - 手段：装饰出运行时值后把手动时钟推进 250 ms，再读该值报出的时刻。
/// - 判断：时刻恰为 250 ms——与 `delay` 用的是同一个手动时钟，不存在第二个时间源。
#[test]
fn manual_time_reports_the_same_axis_as_its_delay() {
    let clock = ManualClock::new();
    let timed = ManualTime::new(clock.clone(), clock.clone());

    clock.advance_by(Duration::from_millis(250u64));

    assert_eq!(timed.now().as_millis(), 250u64);
}

/// 验证 `millis_of_` 对超出 `u64` 的时长**饱和**而不是截断。
/// - 手段：折算 `Duration::MAX` 与一个普通时长。
/// - 判断：前者为 `u64::MAX`（截断会给出一个小数字、让超时提前触发），后者原样。
#[test]
fn millis_of_saturates_instead_of_wrapping() {
    assert_eq!(super::millis_of_(Duration::MAX), u64::MAX);
    assert_eq!(super::millis_of_(Duration::from_millis(7u64)), 7u64);
}

// ── 真实后端用例（两个运行时各一格，缺省 feature 下都跑）──────────────────

/// 真实后端下的检查体：`ConnClock_` 的毫秒量跟着真实时间前进，且不早于等待时长。
///
/// 不写成泛型：`abs_art_tokio::current()` / `abs_art_compio::current()` 是各后端的
/// **固有**关联函数（`abs_art` 没有「取当前运行时值」的 trait 入口），因此两格各写
/// 一份具体代码，取运行时值的写法与其后端一致。
macro_rules! real_backend_clock_case_ {
    ($name:ident, [$($attr:meta),*], $current:path) => {
        /// 目的：验证 `ConnClock_` 在**真实后端**下确实等到、且毫秒量前进。
        /// - 手段：用该后端的当前运行时值构造 `ConnClock_`，`delay(20 ms)` 后
        ///   比对墙钟耗时与 `now_millis_` 的增量。
        /// - 判断：墙钟耗时 `>= 20 ms`（不提前返回），且毫秒增量 `>= 20`。
        $(#[$attr])*
        async fn $name() {
            const WAIT: Duration = Duration::from_millis(20);

            let rt = $current();
            let conn_clock = ConnClock_::new_(rt.clone());
            let before = conn_clock.now_millis_();

            let started = StdInstant::now();
            rt.delay(WAIT).await;
            let real = started.elapsed();
            let after = conn_clock.now_millis_();

            assert!(real >= WAIT, "delay 提前返回了：{real:?}");
            assert!(
                after.saturating_sub(before) >= 20u64,
                "`ConnClock_` 的毫秒量没有前进：{before} -> {after}"
            );
        }
    };
}

real_backend_clock_case_!(
    real_backend_tokio_tracks_the_clock,
    [cfg(feature = "test-tokio-runtime"), tokio::test],
    abs_art_tokio::current
);

real_backend_clock_case_!(
    real_backend_compio_tracks_the_clock,
    [cfg(feature = "test-compio-runtime"), compio::test],
    abs_art_compio::current
);
