//! [`crate::time`] 的单元测试。
//!
//! # 两条互补的验收路线
//!
//! 1. **确定性路线（主力）**：注入假时钟 + 假 [`TrTime`]，把「期限折算」与「到点判定」
//!    变成完全确定的断言——差一微秒不醒、正好到点醒、内层赢时不走秒表、超时即丢弃
//!    内层。这一路不需要任何运行时，因此用普通 `#[test]`。
//! 2. **真实后端路线**：两个 `#[cfg(feature = …)]` 的 `#[tokio::test]` /
//!    `#[compio::test]`，各自用**真实**时钟与真实后端跑一遍
//!    [`TrDeadline::sleep_until`](super::TrDeadline::sleep_until)，证明
//!    「本模块 + 真实后端」确实能等、且不提前返回（两格都在缺省 feature 下跑）。
//!
//! 三个后端的**一致性**（首次立即 / 锚定 / 不早于 / 文案 / 内层赢）不在这里测：
//! 那是 `abs_art-smoke` 的 `time_contract` 契约矩阵（3 后端 × 5 用例）的职责。

use core::{
    cell::Cell,
    future::{Future, pending, ready},
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Instant as StdInstant,
};

use abs_art::{
    Elapsed, FULL, TrDelay, TrInterval, TrTime,
};
use embedded_timers::{
    clock::Clock,
    instant::Instant64,
};

use super::TrDeadline;

// ── 虚拟时间：假时钟 + 假 TrTime ──────────────────────────────────────────

thread_local! {
    /// 虚拟时钟（微秒）。
    ///
    /// 用 thread-local 而不是 `static`：测试默认多线程并行，每个用例各在自己的
    /// 线程上跑，thread-local 天然把它们各自的虚拟时钟隔开。
    static VIRTUAL_MICROS: Cell<u64> = const { Cell::new(0) };
}

/// 读虚拟时钟（微秒）。
fn virtual_micros_() -> u64 {
    VIRTUAL_MICROS.with(Cell::get)
}

/// 把虚拟时钟重置到 0。
fn virtual_reset_() {
    VIRTUAL_MICROS.with(|c| c.set(0));
}

/// 把虚拟时钟向前推 `duration`（向下取整到微秒）。
fn virtual_advance_(duration: Duration) {
    VIRTUAL_MICROS.with(|c| c.set(c.get() + duration.as_micros() as u64));
}

/// 假的**时钟**：读 thread-local 虚拟时钟。零大小，`now` 是纯读。
#[derive(Debug, Clone, Copy, Default)]
struct FakeClock_;

impl Clock for FakeClock_ {
    /// 微秒刻度：`Instant64<1_000_000>` 的每一 tick 恰好 1 µs。
    type Instant = Instant64<1_000_000>;

    fn now(&self) -> Self::Instant {
        Instant64::new(virtual_micros_())
    }
}

/// 假的**计时能力**：`sleep(d)` 把虚拟时钟推进 `d` 后**立刻**就绪。
///
/// 这就是「等待层可注入」的兑现——`TrTime` 是 `Duration`-only 的，因此一个
/// 假后端只要推进自己的时钟就够了，不必模拟任何调度。
struct FakeTime_;

impl TrDelay for FakeTime_ {
    type Delay = FakeDelay_;

    fn delay(duration: Duration) -> Self::Delay {
        FakeDelay_ {
            duration_: duration,
            done_: false,
        }
    }
}

/// 虚拟时间的睡眠 future：**第一次被 `poll` 时**推进虚拟时钟，随即就绪（只推一次）。
///
/// 刻意**不**在 `delay(..)` 被调用时推进：真实后端那一刻只是把期限记下来，到点才
/// 醒。若在构造时就推进，「内层先赢」的用例会因为 `Timeout` 构造即起计时器而看到
/// 时钟前进（本文件那条用例正是这么发现的）。
struct FakeDelay_ {
    duration_: Duration,
    done_: bool,
}

impl Future for FakeDelay_ {
    type Output = ();

    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<()> {
        let this = self.get_mut();
        if !this.done_ {
            this.done_ = true;
            virtual_advance_(this.duration_);
        }
        Poll::Ready(())
    }
}

impl TrTime for FakeTime_ {
    type Interval = FakeInterval_;

    fn interval(period: Duration) -> Self::Interval {
        assert!(period > Duration::ZERO, "`period` must be non-zero.");
        FakeInterval_(period)
    }
}

/// 虚拟时间的周期源：每次 `tick` 推进一个周期。
struct FakeInterval_(Duration);

impl TrInterval for FakeInterval_ {
    /// 假后端的 tick future：同样是具体的 `Ready`。
    type Tick<'a> = core::future::Ready<()>;

    fn tick(&mut self) -> Self::Tick<'_> {
        virtual_advance_(self.0);
        core::future::ready(())
    }
}

// ── 测试用的小工具 ────────────────────────────────────────────────────────

/// 抽干一个 future：假后端的所有等待都立刻就绪，轮询即可推进。
///
/// 轮数有上限：真出现「假后端居然挂起」的回归时应当**立刻失败**而不是死循环。
fn block_on_<F: Future>(future: F) -> F::Output {
    const MAX_ROUNDS: usize = 64;
    let mut future = core::pin::pin!(future);
    let mut cx = Context::from_waker(core::task::Waker::noop());
    for _ in 0..MAX_ROUNDS {
        if let Poll::Ready(output) = future.as_mut().poll(&mut cx) {
            return output;
        }
    }
    panic!("假后端不该挂起：{MAX_ROUNDS} 轮仍未就绪");
}

/// 永不就绪、被丢弃时置位的 future：用来观察「超时即丢弃内层」。
struct NeverReady_(Arc<AtomicBool>);

impl Future for NeverReady_ {
    type Output = ();

    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<()> {
        Poll::Pending
    }
}

impl Drop for NeverReady_ {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}

// ── 确定性用例 ────────────────────────────────────────────────────────────

/// 验证 `sleep_until` 把时钟正好推进到期限。
/// - 手段：虚拟时钟从 0 起，等一个 10 ms 的绝对期限。
/// - 判断：虚拟时钟恰好停在 10 ms（不早不晚）。
#[test]
fn sleep_until_advances_the_clock_to_the_deadline() {
    virtual_reset_();
    let clock = FakeClock_;
    let deadline = Instant64::<1_000_000>::new(10_000);

    block_on_(FakeTime_::sleep_until(&clock, deadline));

    assert_eq!(virtual_micros_(), 10_000);
}

/// 验证期限已过的 `sleep_until` 立刻完成且**不让时钟前进**。
/// - 手段：虚拟时钟推到 5 ms，再等一个 4 ms 的（过去）期限。
/// - 判断：虚拟时钟仍停在 5 ms——折算出的时长是 0，后端不该被要求走秒表。
#[test]
fn sleep_until_in_the_past_does_not_advance_the_clock() {
    virtual_reset_();
    virtual_advance_(Duration::from_millis(5));
    let clock = FakeClock_;
    let past = clock.now() - Duration::from_millis(1);

    block_on_(FakeTime_::sleep_until(&clock, past));

    assert_eq!(virtual_micros_(), 5_000, "已过的期限不该让时钟前进");
}

/// 验证 `timeout_at` 在内层挂起时**正好在期限上**返回 `Elapsed`。
/// - 手段：虚拟时钟从 0 起，期限 3 ms，内层永不就绪。
/// - 判断：结果为 `Err`、文案「期限已到」、虚拟时钟恰好停在 3 ms。
#[test]
fn timeout_at_elapses_exactly_at_the_deadline() {
    virtual_reset_();
    let clock = FakeClock_;
    let deadline = Instant64::<1_000_000>::new(3_000);

    let outcome = block_on_(FakeTime_::timeout_at(
        &clock,
        deadline,
        pending::<u8>(),
    ));
    let elapsed = outcome.expect_err("期限已到应当是错误");

    assert_eq!(elapsed.to_string(), "期限已到");
    assert_eq!(virtual_micros_(), 3_000, "应当正好停在期限上");
}

/// 验证期限已过的 `timeout_at` 立刻超时、不再等。
/// - 手段：虚拟时钟推到 5 ms，再给一个 4 ms 的（过去）期限与永不就绪的内层。
/// - 判断：结果为 `Err`，且虚拟时钟仍停在 5 ms。
#[test]
fn timeout_at_with_a_past_deadline_elapses_without_waiting() {
    virtual_reset_();
    virtual_advance_(Duration::from_millis(5));
    let clock = FakeClock_;
    let past = clock.now() - Duration::from_millis(1);

    let outcome = block_on_(FakeTime_::timeout_at(&clock, past, pending::<u8>()));
    assert!(outcome.is_err(), "期限已过应当立刻超时");
    assert_eq!(virtual_micros_(), 5_000, "不该再等");
}

/// 验证内层先完成时 `timeout_at` 原样返回输出，且**秒表没有走**。
/// - 手段：期限 10 ms，内层用立刻就绪的 `ready(7)`。
/// - 判断：结果为 `Ok(7)`，虚拟时钟仍为 0（`sleep` 从未被轮询）。
#[test]
fn timeout_at_returns_the_output_when_the_inner_future_wins() {
    virtual_reset_();
    let clock = FakeClock_;
    let deadline = clock.now() + Duration::from_millis(10);

    let outcome = block_on_(FakeTime_::timeout_at(&clock, deadline, ready(7u8)));

    assert_eq!(outcome, Ok(7u8));
    assert_eq!(virtual_micros_(), 0, "内层先赢 ⇒ 秒表不该走");
}

/// 验证期限先到时**内层 future 被丢弃**（取消就是丢弃）。
/// - 手段：内层用「永不就绪、被丢弃时置位」的 future，期限 1 ms。
/// - 判断：结果为 `Err`，且丢弃标志被置位。
#[test]
fn timeout_at_drops_the_inner_future_when_the_deadline_wins() {
    virtual_reset_();
    let clock = FakeClock_;
    let dropped = Arc::new(AtomicBool::new(false));
    let deadline = Instant64::<1_000_000>::new(1_000);

    let outcome = block_on_(FakeTime_::timeout_at(
        &clock,
        deadline,
        NeverReady_(Arc::clone(&dropped)),
    ));

    assert!(outcome.is_err(), "内层永不就绪 ⇒ 期限先到");
    assert!(dropped.load(Ordering::SeqCst), "超时应当丢弃内层 future");
}

/// 验证转出的 `Elapsed` 就是 `abs_art` 的那个错误类型，且文案稳定。
/// - 手段：编译期断言 `Elapsed: core::error::Error`，并取一个真实产生的错误。
/// - 判断：`source()` 为 `None`，`Display` 为「期限已到」——三个后端共用同一文案，
///   因此这里的判定与 `abs_art-smoke` 的契约矩阵是同一个来源。
#[test]
fn elapsed_is_abs_arts_error_type_with_a_stable_message() {
    fn assert_error_<E: core::error::Error>() {}
    assert_error_::<Elapsed>();

    virtual_reset_();
    let clock = FakeClock_;
    let outcome = block_on_(FakeTime_::timeout_at(
        &clock,
        Instant64::<1_000_000>::new(1_000),
        pending::<u8>(),
    ));
    let elapsed = outcome.expect_err("期限已到应当是错误");

    assert_eq!(elapsed.to_string(), "期限已到");
    assert!(core::error::Error::source(&elapsed).is_none());
}

/// 验证保活要用的**那一种循环形状**：每轮睡到「下一个绝对期限」，没有轮盘。
/// - 手段：模拟 tick 循环三轮，每轮把期限往后推 1 秒并 `sleep_until`。
/// - 判断：每轮结束时虚拟时钟恰好落在该轮期限上（1 s / 2 s / 3 s）。
#[test]
fn keepalive_style_loop_sleeps_until_each_deadline() {
    virtual_reset_();
    let clock = FakeClock_;

    for round in 1..=3u64 {
        let deadline = Instant64::<1_000_000>::new(round * 1_000_000);
        block_on_(FakeTime_::sleep_until(&clock, deadline));
        assert_eq!(virtual_micros_(), round * 1_000_000);
    }
}

// ── 真实后端用例（两个运行时各一格，缺省 feature 下都跑）──────────────────

/// 以构造时刻为 epoch 的**真实**时钟（纳秒刻度）。
#[derive(Debug)]
struct RealClock_ {
    base_: StdInstant,
}

impl Clock for RealClock_ {
    /// 纳秒刻度：`Instant64<1_000_000_000>` 的每一 tick 恰好 1 ns。
    type Instant = Instant64<1_000_000_000>;

    fn now(&self) -> Self::Instant {
        Instant64::new(self.base_.elapsed().as_nanos() as u64)
    }
}

/// 真实后端下的端到端检查体：两个运行时各用它跑一遍。
async fn real_sleep_until_waits_at_least_<D: TrTime>() {
    const WAIT: Duration = Duration::from_millis(20);
    let clock = RealClock_ {
        base_: StdInstant::now(),
    };
    let started = StdInstant::now();
    D::sleep_until(&clock, clock.now() + WAIT).await;
    assert!(
        started.elapsed() >= WAIT,
        "sleep_until 提前返回了：{:?}",
        started.elapsed()
    );
}

/// 目的：验证 `smux_v1::time::sleep_until` 在 **tokio 真实后端**上能真正等到。
/// - 手段：`#[tokio::test]` 提供 tokio 运行时（含 time 驱动），后端取
///   `abs_art_tokio::Runtime<{ FULL }>`。
/// - 判断：实际耗时 `>= 20 ms`（不提前返回）。
#[cfg(feature = "test-tokio-runtime")]
#[tokio::test]
async fn real_backend_tokio_waits_until_the_deadline() {
    real_sleep_until_waits_at_least_::<abs_art_tokio::Runtime<{ FULL }>>().await;
}

/// 目的：验证 `smux_v1::time::sleep_until` 在 **compio 真实后端**上能真正等到。
/// - 手段：`#[compio::test]` 提供 compio 运行时，后端取
///   `abs_art_compio::Runtime<{ FULL }>`。
/// - 判断：实际耗时 `>= 20 ms`（不提前返回）。
#[cfg(feature = "test-compio-runtime")]
#[compio::test]
async fn real_backend_compio_waits_until_the_deadline() {
    real_sleep_until_waits_at_least_::<abs_art_compio::Runtime<{ FULL }>>().await;
}
