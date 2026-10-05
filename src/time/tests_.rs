//! [`crate::time`] 的单元测试。
//!
//! # 为什么这里的用例用手写轮询，而不是 `dual_runtime_test_!`
//!
//! 本模块**故意不依赖任何运行时**：轮盘不会自己醒来，唤醒源由调用方（连接的 tick
//! 循环）注入，而那个形状尚待裁决（`dev-notes/keepalive-20261005-0901.md` §5.2）。
//! 换言之，本模块里**没有「运行时相关」的行为可测**——搬来真实运行时反而会把「被测
//! 的确定性逻辑」与「运行时的调度粒度」混在一起，让判定变成看门狗掐时间。
//!
//! 因此这里的做法是**假时钟 + 虚拟时间驱动**（[`drive_`]）：时钟只在我们推进它时才
//! 走，驱动方每轮把假时钟直接推到轮盘上最近的期限并 `wake`。于是「差 1 µs 不醒 /
//! 正好到点醒一次 / 同期限两个等待者互不覆盖」这类判定都是**确定性**的。
//!
//! 真实运行时下的端到端用例（真实 tick 循环 + 真实时钟 + 两个运行时各一遍）留在
//! §5.2 裁决之后，按本仓既有做法落到 `tests/` 下按运行时各跑一遍。

use core::{
    future::{Future, pending, ready},
    pin::Pin,
    task::{Context, Poll},
};
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    },
    task::{Wake, Waker},
    time::Duration,
};

use embedded_timers::{
    clock::Clock,
    instant::Instant64,
};

use super::{
    Elapsed, Timer, interval, interval_at, sleep, sleep_until, timeout, timeout_at,
};

/// 假时钟：**微秒**计数，只在用例推进它时才走。
///
/// 内部用 `Arc<AtomicU64>` 而不是裸 `u64`，是为了让用例自己留一个把手推进时间：
/// [`Timer::new`] 会拿走时钟值，而推进时间的人往往是用例主体。
#[derive(Debug, Clone, Default)]
struct FakeClock_ {
    ticks_: Arc<AtomicU64>,
}

impl FakeClock_ {
    /// 把假时钟往前推 `duration`（向下取整到微秒）。
    fn advance_(&self, duration: Duration) {
        self.ticks_
            .fetch_add(duration.as_micros() as u64, Ordering::Relaxed);
    }
}

impl Clock for FakeClock_ {
    /// 微秒刻度：`Instant64<1_000_000>` 的每一 tick 恰好 1 µs，因此 `Duration`
    /// 只要落在微秒格点上就**无舍入**——判定才能钉到「差 1 µs」。
    type Instant = Instant64<1_000_000>;

    fn now(&self) -> Self::Instant {
        Instant64::new(self.ticks_.load(Ordering::Relaxed))
    }
}

/// 只记「被唤醒几次」的 waker。
///
/// 用它而不是 `Waker::noop()`：本模块的唤醒纪律是「到期才醒、且只醒一次」，只判
/// 「醒没醒」会把重复唤醒与提前唤醒一起漏掉。
struct CountWake_(Arc<AtomicUsize>);

impl Wake for CountWake_ {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

/// 造一个挂在 `counter` 上的记数 waker。
fn counting_waker_(counter: &Arc<AtomicUsize>) -> Waker {
    Waker::from(Arc::new(CountWake_(Arc::clone(counter))))
}

/// 用假时钟把 `future` 推到就绪：每轮先轮询；挂起就把假时钟**直接推到**轮盘上最近
/// 的期限并 `wake` 一轮，然后重试。
///
/// # Panics
///
/// future 挂起而轮盘为空时 panic：那说明用例构造了「没有任何人能唤醒它」的死锁，
/// 与其静默挂死，不如立刻失败。
fn drive_<F: Future>(clock: &FakeClock_, timer: &Timer<FakeClock_>, future: F) -> F::Output {
    let woken = Arc::new(AtomicUsize::new(0));
    let waker = counting_waker_(&woken);
    let mut cx = Context::from_waker(&waker);
    let mut future = core::pin::pin!(future);
    loop {
        if let Poll::Ready(output) = future.as_mut().poll(&mut cx) {
            return output;
        }
        let Some(next) = timer.min_timeout() else {
            panic!("future 挂起，但轮盘上没有任何期限可以唤醒它：用例构造了死锁");
        };
        clock.advance_(next);
        timer.wake();
    }
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

/// 第 N 次被轮询时才就绪的 future：用来在**不推进时钟**的前提下让内层赢。
struct ReadyAfter_(Arc<AtomicUsize>, usize);

impl Future for ReadyAfter_ {
    type Output = &'static str;

    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let polls = this.0.fetch_add(1, Ordering::SeqCst) + 1;
        if polls >= this.1 {
            Poll::Ready("内层完成")
        } else {
            Poll::Pending
        }
    }
}

/// 验证 `sleep` 只在期限**恰好**到达时唤醒一次，差一个微秒都不醒。
/// - 手段：假时钟 + 记数 waker；先轮询一次登记，再把时钟推到「期限差 1 µs」并
///   `wake` 一轮，最后走到期限再 `wake` 一轮。
/// - 判断：第一次轮询为 `Pending` 且轮盘上有 1 个登记；差 1 µs 时仍 `Pending`、
///   唤醒计数仍为 0、`min_timeout` 恰为 1 µs；走满 10 ms 后唤醒计数为 1，再轮询
///   即 `Ready`，轮盘清空。
#[test]
fn sleep_wakes_once_exactly_at_the_deadline() {
    let clock = FakeClock_::default();
    let timer = Timer::new(clock.clone());
    let woken = Arc::new(AtomicUsize::new(0));
    let waker = counting_waker_(&woken);
    let mut cx = Context::from_waker(&waker);

    let mut going = core::pin::pin!(sleep(&timer, Duration::from_millis(10)));
    assert!(going.as_mut().poll(&mut cx).is_pending());
    assert_eq!(timer.len(), 1usize, "首次轮询应当已登记");
    assert_eq!(woken.load(Ordering::SeqCst), 0usize);

    // 差 1 µs：还没到，轮盘不该唤醒任何人，也不该把登记摘掉。
    clock.advance_(Duration::from_micros(9_999));
    timer.wake();
    assert!(going.as_mut().poll(&mut cx).is_pending());
    assert_eq!(woken.load(Ordering::SeqCst), 0usize, "提前唤醒是错的");
    assert_eq!(timer.min_timeout(), Some(Duration::from_micros(1)));
    assert_eq!(timer.len(), 1usize);

    // 正好到点：`wake` 摘除登记并唤醒**一次**。
    clock.advance_(Duration::from_micros(1));
    timer.wake();
    assert_eq!(woken.load(Ordering::SeqCst), 1usize, "到期应当唤醒恰好一次");
    assert!(going.as_mut().poll(&mut cx).is_ready());
    assert_eq!(timer.len(), 0usize);
}

/// 验证期限已过的登记**不在轮盘上占位**、且立刻完成。
/// - 手段：把假时钟推到 5 ms，再 `sleep_until` 一个 4 ms 的（过去）期限，轮询一次。
/// - 判断：第一次轮询即 `Ready`；轮盘长度为 0；没有任何唤醒。
#[test]
fn sleep_until_in_the_past_completes_without_registering() {
    let clock = FakeClock_::default();
    let timer = Timer::new(clock.clone());
    clock.advance_(Duration::from_millis(5));
    let woken = Arc::new(AtomicUsize::new(0));
    let waker = counting_waker_(&woken);
    let mut cx = Context::from_waker(&waker);

    let past = clock.now() - Duration::from_millis(1);
    let mut immediate = core::pin::pin!(sleep_until(&timer, past));
    assert_eq!(immediate.as_mut().poll(&mut cx), Poll::Ready(()));
    assert_eq!(timer.len(), 0usize, "已过的期限不该占位");
    assert_eq!(woken.load(Ordering::SeqCst), 0usize);
}

/// 验证「尚未就绪就丢弃」会把登记从轮盘上撤掉。
/// - 手段：登记一个 10 ms 的 `sleep` 并轮询一次（此时已登记），随后离开作用域丢弃。
/// - 判断：轮盘长度从 1 回到 0，`min_timeout` 回到 `None`——否则驱动方会被一个
///   再也没人等的死期限一直拴住。
#[test]
fn dropping_a_pending_sleep_withdraws_its_registration() {
    let clock = FakeClock_::default();
    let timer = Timer::new(clock.clone());
    let woken = Arc::new(AtomicUsize::new(0));
    let waker = counting_waker_(&woken);
    let mut cx = Context::from_waker(&waker);

    {
        let mut going = core::pin::pin!(sleep(&timer, Duration::from_millis(10)));
        assert!(going.as_mut().poll(&mut cx).is_pending());
        assert_eq!(timer.len(), 1usize);
    }
    assert_eq!(timer.len(), 0usize, "丢弃应当撤销登记");
    assert_eq!(timer.min_timeout(), None);
}

/// 验证 `min_timeout` 跟着**最早**的期限走，而不是登记顺序；且先到期的先醒。
/// - 手段：先登记 10 ms、再登记 3 ms（登记顺序与期限顺序相反）。
/// - 判断：两次登记后 `min_timeout` 依次为 10 ms 与 3 ms；把时钟推 3 ms 后
///   `wake`，只应让 3 ms 那个就绪，10 ms 那个仍 `Pending`。
#[test]
fn min_timeout_follows_the_earliest_deadline() {
    let clock = FakeClock_::default();
    let timer = Timer::new(clock.clone());
    let woken = Arc::new(AtomicUsize::new(0));
    let waker = counting_waker_(&woken);
    let mut cx = Context::from_waker(&waker);

    let mut late = core::pin::pin!(sleep(&timer, Duration::from_millis(10)));
    assert!(late.as_mut().poll(&mut cx).is_pending());
    assert_eq!(timer.min_timeout(), Some(Duration::from_millis(10)));

    let mut early = core::pin::pin!(sleep(&timer, Duration::from_millis(3)));
    assert!(early.as_mut().poll(&mut cx).is_pending());
    assert_eq!(
        timer.min_timeout(),
        Some(Duration::from_millis(3)),
        "更早的期限应当接管 min_timeout"
    );

    clock.advance_(Duration::from_millis(3));
    timer.wake();
    assert!(early.as_mut().poll(&mut cx).is_ready(), "先到期的应当先醒");
    assert!(late.as_mut().poll(&mut cx).is_pending(), "后到期的还不该醒");
    assert_eq!(timer.len(), 1usize);
}

/// 验证**期限完全相同**的两个等待者互不覆盖（键里的生成序号就是为它存在的）。
/// - 手段：两个 5 ms 的 `sleep` 各自轮询一次登记，再推到 5 ms 并 `wake`。
/// - 判断：登记后轮盘长度为 2（若键只有期限，后一个会顶掉前一个、长度只会是 1）；
///   `wake` 后两个都 `Ready`、唤醒计数为 2、轮盘清空。
#[test]
fn two_registrations_at_the_same_deadline_do_not_collide() {
    let clock = FakeClock_::default();
    let timer = Timer::new(clock.clone());
    let woken = Arc::new(AtomicUsize::new(0));
    let waker = counting_waker_(&woken);
    let mut cx = Context::from_waker(&waker);

    let mut first = core::pin::pin!(sleep(&timer, Duration::from_millis(5)));
    let mut second = core::pin::pin!(sleep(&timer, Duration::from_millis(5)));
    assert!(first.as_mut().poll(&mut cx).is_pending());
    assert!(second.as_mut().poll(&mut cx).is_pending());
    assert_eq!(timer.len(), 2usize, "同期限的两个登记必须各占一个键");

    clock.advance_(Duration::from_millis(5));
    timer.wake();
    assert_eq!(woken.load(Ordering::SeqCst), 2usize, "两个等待者都该醒");
    assert!(first.as_mut().poll(&mut cx).is_ready());
    assert!(second.as_mut().poll(&mut cx).is_ready());
    assert_eq!(timer.len(), 0usize);
}

/// 验证 `interval` 的第一次 tick 立即完成，其后每个周期一个时刻。
/// - 手段：假时钟 + 虚拟驱动，连续 tick 四次并记下每次返回的时刻与当时的假时钟。
/// - 判断：返回时刻依次为 0 / 10 / 20 / 30 ms，且每次都等于当时的 `clock.now()`
///   （即「tick 返回的就是现在」），相位始终对齐到起点。
#[test]
fn interval_ticks_immediately_then_every_period() {
    let clock = FakeClock_::default();
    let timer = Timer::new(clock.clone());
    let mut ticking = interval(&timer, Duration::from_millis(10));

    for expected_ms in [0u64, 10, 20, 30] {
        let ticked = drive_(&clock, &timer, ticking.tick());
        assert_eq!(ticked, Instant64::<1_000_000>::new(expected_ms * 1_000));
        assert_eq!(ticked, clock.now(), "tick 返回的应当是「就是现在」");
    }
    assert_eq!(timer.len(), 0usize, "tick 就绪后不该留下登记");
}

/// 验证 `interval_at` 从给定的 `start` 起算，而不是从「现在」起算。
/// - 手段：`start` 取 50 ms、周期 10 ms，连续 tick 三次。
/// - 判断：返回时刻依次为 50 / 60 / 70 ms，每次都等于当时的 `clock.now()`。
#[test]
fn interval_at_waits_for_the_given_start() {
    let clock = FakeClock_::default();
    let timer = Timer::new(clock.clone());
    let start = Instant64::<1_000_000>::new(50_000);
    let mut ticking = interval_at(&timer, start, Duration::from_millis(10));

    for expected_ms in [50u64, 60, 70] {
        let ticked = drive_(&clock, &timer, ticking.tick());
        assert_eq!(ticked, Instant64::<1_000_000>::new(expected_ms * 1_000));
        assert_eq!(ticked, clock.now());
    }
}

/// 验证零周期在**构造点**就被拒绝，而不是留到 `tick` 里除零。
/// - 手段：`#[should_panic]` 捕获 `interval` 的断言。
/// - 判断：panic 文案含 "must be non-zero"。
#[test]
#[should_panic(expected = "must be non-zero")]
fn interval_rejects_a_zero_period() {
    let timer = Timer::new(FakeClock_::default());
    let _ = interval(&timer, Duration::ZERO);
}

/// 验证 `timeout` 在内层**立刻**就绪时原样返回输出，且不占用轮盘。
/// - 手段：内层用 `core::future::ready(7)`，期限 10 ms。
/// - 判断：结果为 `Ok(7)`；轮盘长度为 0（内层先被轮询，`sleep` 一侧从未登记）。
#[test]
fn timeout_returns_immediately_when_the_inner_future_is_ready() {
    let clock = FakeClock_::default();
    let timer = Timer::new(clock.clone());
    let outcome = drive_(
        &clock,
        &timer,
        timeout(&timer, Duration::from_millis(10), ready(7u8)),
    );
    assert_eq!(outcome, Ok(7u8));
    assert_eq!(timer.len(), 0usize);
}

/// 验证 `timeout` 在内层**稍后**就绪时返回输出，并撤销已经登记的期限。
/// - 手段：内层用「第 2 次轮询才就绪」的 future；手动轮询两次，**不推进**假时钟。
/// - 判断：第一次为 `Pending` 且轮盘上已有 1 个登记；第二次为 `Ok(..)`，且轮盘被
///   清空——证明 `sleep` 一侧被丢弃时把登记撤掉了。
#[test]
fn timeout_releases_the_registration_when_the_inner_future_wins() {
    let clock = FakeClock_::default();
    let timer = Timer::new(clock.clone());
    let woken = Arc::new(AtomicUsize::new(0));
    let waker = counting_waker_(&woken);
    let mut cx = Context::from_waker(&waker);

    let polls = Arc::new(AtomicUsize::new(0));
    let mut racing = core::pin::pin!(timeout(
        &timer,
        Duration::from_millis(10),
        ReadyAfter_(Arc::clone(&polls), 2),
    ));

    assert!(racing.as_mut().poll(&mut cx).is_pending(), "内层首次应当挂起");
    assert_eq!(timer.len(), 1usize, "内层挂起时 sleep 一侧应当已登记");
    assert_eq!(
        racing.as_mut().poll(&mut cx),
        Poll::Ready(Ok("内层完成")),
        "内层第二次应当赢"
    );
    assert_eq!(timer.len(), 0usize, "内层先完成 ⇒ sleep 被丢弃 ⇒ 登记撤销");
}

/// 验证期限先到时**内层 future 被丢弃**（取消就是丢弃），且轮盘不留残余。
/// - 手段：内层用「永不就绪、被丢弃时置位」的 future，`drive_` 把假时钟推到期限。
/// - 判断：结果为 `Err`；丢弃标志被置位；轮盘清空。
#[test]
fn timeout_drops_the_inner_future_when_the_deadline_wins() {
    let clock = FakeClock_::default();
    let timer = Timer::new(clock.clone());
    let dropped = Arc::new(AtomicBool::new(false));
    let outcome = drive_(
        &clock,
        &timer,
        timeout(
            &timer,
            Duration::from_millis(10),
            NeverReady_(Arc::clone(&dropped)),
        ),
    );

    assert!(outcome.is_err(), "内层永不就绪 ⇒ 期限先到");
    assert!(dropped.load(Ordering::SeqCst), "超时应当丢弃内层 future");
    assert_eq!(timer.len(), 0usize, "超时后轮盘应当清空");
}

/// 验证 `Elapsed` 的失败语义、文案与 `Error` 实现。
/// - 手段：用一个永远挂起的内层 future 触发超时，再把错误取出来。
/// - 判断：`Display` 为既定中文文案；`Error::source` 为 `None`；且 `Elapsed` 满足
///   `core::error::Error` 约束（编译期断言）。
#[test]
fn elapsed_reports_the_deadline_and_implements_error() {
    fn assert_error_<E: core::error::Error>() {}
    assert_error_::<Elapsed>();

    let clock = FakeClock_::default();
    let timer = Timer::new(clock.clone());
    let outcome = drive_(
        &clock,
        &timer,
        timeout(&timer, Duration::from_millis(10), pending::<u8>()),
    );
    let elapsed = outcome.expect_err("期限已到应当是错误");
    assert_eq!(elapsed.to_string(), "期限已到");
    assert!(core::error::Error::source(&elapsed).is_none());
}

/// 验证 `timeout_at` 在期限已过时**立刻**超时（内层挂起 ⇒ 输给期限）。
/// - 手段：把假时钟推到 5 ms，再 `timeout_at` 一个 4 ms 的（过去）期限。
/// - 判断：结果为 `Err`；且**没有**推进假时钟也没有唤醒（不必等）。
#[test]
fn timeout_at_in_the_past_elapses_immediately() {
    let clock = FakeClock_::default();
    let timer = Timer::new(clock.clone());
    clock.advance_(Duration::from_millis(5));
    let past = clock.now() - Duration::from_millis(1);

    let outcome = drive_(&clock, &timer, timeout_at(&timer, past, pending::<u8>()));
    let elapsed = outcome.expect_err("期限已过应当立刻超时");
    assert_eq!(elapsed.to_string(), "期限已到");
    assert_eq!(clock.now(), Instant64::<1_000_000>::new(5_000), "不该再等");
    assert_eq!(timer.len(), 0usize);
}
