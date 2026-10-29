//! **计时循环**：连接的第五个本地循环，**只做计时**。
//!
//! # 形状（R9 收缩后）
//!
//! 它不扫身份表、不做协议决策、不组帧、不发任何协议动作。每轮只有三件事：
//!
//! 1. 从 [`TimerSignal_`] 读 `main_loop` 发布的下一次期限；
//! 2. `delay` 到期（或 `main_loop` 发布新期限 / 取消）；
//! 3. 到期时 `raise_main_hint_(HINT_MAIN_TIMER)`——**只注入「到期」这一个事实**。
//!
//! 决策（保活 `PULSE`、空闲超时拆流、建流超时 `REJECT`、优雅关闭 `Finalize`、
//! `WaitClose` 墓碑回收、连接级关闭动作）全部在 `main_loop::tick_` 里做：它们是
//! 身份表写操作，而身份表的唯一写者是 `main_loop`（设计稿 §6.4 / I3）。
//!
//! # 为什么保留而不是并入 main
//!
//! `main_loop` 的 park 点保持「请求 / 公共 RX / 提示 / 取消」四类来源，不需要额外
//! 持有 `C::Rt::delay`。timer 与 main 的耦合只有一个 `AtomicU64` 与一个
//! `NotifySlot_`（[`TimerSignal_`]），它是零分配、零锁的。
//!
//! # 它持有什么
//!
//! | 持有 | 为什么 |
//! | --- | --- |
//! | [`MuxShared_`](super::session_::MuxLoopShared_) | 连接级时钟 + 空闲超时 + `ControlState_` |
//! | 取消令牌 | 与其它四个循环同一条收尾纪律 |
//!
//! 与另外四个循环一致：**不持有 [`MuxCore`](super::mux_connection::core_) 的强引用**，
//! 否则核心永远不会析构、取消令牌永远不会触发。
//!
//! # 两条时钟与「保活为什么能收敛」
//!
//! [`ChannelState_`](super::owner_::ChannelState_) 为每条子流记**两个**时刻，都由
//! 活动点在**活动发生的那一刻**写下（不是等计时扫描时打点——扫描有周期，把间隙里到的
//! 建流帧记成「扫描那一刻」会把本端的保活职责整整推后一个周期）：
//!
//! | 时钟 | 谁写 | 判什么 |
//! | --- | --- | --- |
//! | `active_millis_` | 收到的**任何**帧（含 `PULSE`）、本端写出的数据 | 存活：距它 `timeout` ⇒ 空闲超时 |
//! | `data_millis_` | **非保活**活动、以及本端上一次发出 `PULSE` | 职责：距它 `pulse_millis` ⇒ 发 `PULSE` |
//!
//! **收到对端的 `PULSE` 只刷前者、不刷后者**，这是保活能收敛的关键：若两者合一，
//! 两端会在同一时刻互相把对方的职责时钟清零、于是**都不发** `PULSE`，而各自的存活
//! 时钟又从最后一次真实活动起算——先停手的那一侧必定先到 `max_channel_timeout`
//! 被拆掉。
//!
//! 保活周期取 `timeout / 2`：两端各自按这个周期发 `PULSE`，因此每一端每隔
//! `timeout / 2` 就会收到对端一条，存活时钟的余量正好是另一半——能容忍一整个周期的
//! 抖动。**本端发出的 `PULSE` 不刷存活时钟**：否则对端已死时本端会一直给自己续命。

use core::{
    future::poll_fn,
    sync::atomic::{AtomicU64, Ordering},
    task::{Context, Poll},
    time::Duration,
};

use abs_art::TrDelay;
use abs_cancel::TrCancellationToken;

use crate::{
    connection::{Dock, TrConnCfg, session_::MuxShared_, sync_::NotifySlot_},
    flow_ctrl::WindowReport,
    x_deps::abs_cancel,
};

/// 每轮最多认领 / 投递多少条保活动作。
///
/// 它是**限频**：每条 `PULSE` 或拆流都要经事件通道投一条事件，因此在册子流很多时
/// 不能一轮全投。写满即提前返回并**立刻**再扫一轮（`TimerScan_::is_full_`），动作
/// 不会丢，只是分轮完成。
pub(crate) const K_TIMER_ACTION_BATCH: usize = 64usize;

/// 计时循环每轮认领的一个动作。
///
/// 它由注册表在**读锁内**产出（判定与认领都是原子读改写），由计时循环在**锁外**
/// 投递——这样锁内不会 `wake` 任何等待者（唤醒纪律见
/// `ChannelRegistry_::notify_timer_`）。
#[derive(Debug, Clone, Copy)]
pub(crate) enum TimerAction_ {
    /// 空槽（切片里未被认领的位置）。
    Idle,

    /// 给某条子流发一条保活 `PULSE`（携带本端当前接收窗口）。
    Pulse {
        /// 本端 dock。
        local_dock: Dock,
        /// 对端 dock。
        remote_dock: Dock,
        /// 本端当前的接收窗口快照。
        report: WindowReport,
    },

    /// 某条子流空闲超时：拆掉它（只拆子流，不拆连接）。
    Abort {
        /// 本端 dock。
        local_dock: Dock,
        /// 对端 dock。
        remote_dock: Dock,
    },

    /// 某条**建流尚未裁决**的子流超时：向对端回 `REJECT` 把它放走、释放本端身份，
    /// 并把原因留在共享状态上供应用随后读取（见 `ChannelHandle::abort_reason`）。
    ///
    /// 它与 [`TimerAction_::Abort`] 共用同一根存活时钟与同一个 `max_channel_timeout`，
    /// 差别只在「对端是不是还在等裁决」：还在等 ⇒ 必须回帧；已经建好 ⇒ 只关本端。
    /// 连接**等待关闭**时为每条未裁决子流直接产生它（不必等空闲超时——连接已经不
    /// 打算再等了）。
    RejectEstablish {
        /// 本端 dock。
        local_dock: Dock,
        /// 对端 dock。
        remote_dock: Dock,
    },

    /// 连接**等待关闭**时的一条活跃子流：把已提交字节排空，然后发 `CLOSE(FIN)`。
    ///
    /// 它就是「所有正在活动的子流排空并向对端发出关闭信号」那一步的载体。每条子流
    /// 只被认领一次（[`ChannelOwner_::claim_graceful_fin_`] 的去重位），因此投递量
    /// 与子流数成正比、与本循环扫了多少轮无关。
    ///
    /// [`ChannelOwner_::claim_graceful_fin_`]: crate::connection::owner_::ChannelOwner_::claim_graceful_fin_
    Finalize {
        /// 本端 dock。
        local_dock: Dock,
        /// 对端 dock。
        remote_dock: Dock,
    },
}

/// 计时循环本体：只等期限、到期只 raise [`HINT_MAIN_TIMER`]。
///
/// # 参数
///
/// - `R`：**运行时值**（最终二进制选中的那个）。它既报时刻（`TrClock`）又管等待
///   （`TrDelay`）——**同源**，因此虚拟时间下不会有「睡在虚拟时钟、读在墙上时钟」
///   的错配。本循环只要求 [`TrTime`]（= `TrDelay + TrClock`）。
/// - `C`：连接资源策略（`TrConnCfg`）。
///
/// # `next_millis_ == 0`
///
/// `0` 表示 main 还没有发布期限（连接刚建立 / 当前没有在册子流）。此时用
/// `channel_timeout` 作上限兜底：即使 main 漏发布，timer 也会周期性回来注入一次
/// 到期，让 tick 重新算期限——不会因为「谁都没发布」而永久睡死。
///
/// [`HINT_MAIN_TIMER`]: crate::connection::HINT_MAIN_TIMER
/// [`TrClock`]: abs_art::TrClock
/// [`TrTime`]: crate::time::TrTime
pub(crate) async fn timer_loop_async_<C, K>(shared: MuxShared_<C>, cancel: K)
where
    C: TrConnCfg,
    K: TrCancellationToken,
{
    let clock = shared.conn_clock_().clone();
    let timeout_millis = shared.channel_timeout_millis_();
    let signal = shared.control_().timer_signal_();

    loop {
        #[cfg(feature = "test-loop-probe")]
        crate::connection::loop_probe_::timer_tick_();
        if cancel.is_cancelled() {
            #[cfg(feature = "test-loop-probe")]
            crate::connection::loop_probe_::timer_exit_cancel_();
            return;
        }
        let now = clock.now_millis_();
        let next = signal.next_millis_();
        // `0` = 没发布期限：用 timeout 兜底（见类型文档）。
        let target = if next == 0u64 {
            now.saturating_add(timeout_millis)
        } else {
            next
        };
        // 期限已过（main 还没来得及重算）时至少等 1 ms：`delay(0)` 会立刻就绪，而
        // 每轮都立刻 raise 只会把合并提示位刷成忙转。
        let wait_millis = target.saturating_sub(now).max(1u64);

        let mut sleep_fut = core::pin::pin!(clock.rt_().delay(Duration::from_millis(wait_millis)));
        let cancel_fut = cancel.child_token().cancellation();
        let mut cancel_fut = core::pin::pin!(cancel_fut);
        let mut cancelled = false;
        let mut due = false;
        poll_fn(|cx| {
            if core::future::Future::poll(cancel_fut.as_mut(), cx).is_ready() {
                cancelled = true;
                return Poll::Ready(());
            }
            // main 发布了新期限：回顶部重算 wait。
            if signal.poll_wake_(cx).is_ready() {
                return Poll::Ready(());
            }
            match core::future::Future::poll(sleep_fut.as_mut(), cx) {
                Poll::Ready(()) => {
                    due = true;
                    Poll::Ready(())
                }
                Poll::Pending => Poll::Pending,
            }
        })
        .await;

        if cancelled {
            #[cfg(feature = "test-loop-probe")]
            crate::connection::loop_probe_::timer_exit_cancel_();
            return;
        }
        if due {
            // **只注入「到期」这一个事实**：决策与组帧都在 `main_loop::tick_`。
            shared
                .control_()
                .raise_main_hint_(crate::connection::HINT_MAIN_TIMER);
        }
    }
}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// 计时信号：main <-> timer
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

/// `main_loop` 与 `timer_loop` 之间的最小耦合面。
///
/// `main_loop` 每次 `tick_` 结束后写 `next_millis_` 并 `notify_`；`timer_loop` 的 park
/// 同时 poll `timer_wake_`、`delay(next - now)` 与取消令牌，到期后只
/// `raise_main_hint_(HINT_MAIN_TIMER)`。它不含身份表、不组帧、不直接发协议动作。
///
/// `0` 表示「当前没有期限」：timer 于是用 `channel_timeout` 兜底。
pub(crate) struct TimerSignal_ {
    /// 下一次必须醒来的连接内毫秒；`0` = 当前没有期限。
    next_millis_: AtomicU64,

    /// main -> timer：期限变了，重算 delay。
    timer_wake_: NotifySlot_,
}

impl TimerSignal_ {
    /// 空信号：没有期限、没有等待者。
    pub(crate) fn new_() -> Self {
        TimerSignal_ {
            next_millis_: AtomicU64::new(0u64),
            timer_wake_: NotifySlot_::new_(),
        }
    }

    /// 发布新的最早期限并唤醒 timer；`0` 表示没有期限。
    ///
    /// 幂等：同一期限重复发布只会多一次唤醒，timer 复读后按同一值重算。
    pub(crate) fn publish_(&self, next_millis: u64) {
        self.next_millis_.store(next_millis, Ordering::Release);
        self.timer_wake_.notify_();
    }

    /// 当前最早期限（连接内毫秒；`0` 表示没有）。
    pub(crate) fn next_millis_(&self) -> u64 {
        self.next_millis_.load(Ordering::Acquire)
    }

    /// timer 侧等待「期限变了」。
    pub(crate) fn poll_wake_(&self, cx: &mut Context<'_>) -> Poll<()> {
        self.timer_wake_.poll_wait_(cx)
    }

    /// 清掉期限（`0` = 没有期限）；连接收尾时用，timer 于是回落到 `channel_timeout`
    /// 兜底等待。只被单测直接调用，保留它是为了钉住「清期限 → 复读为 0」这条语义。
    #[cfg(test)]
    pub(crate) fn clear_(&self) {
        self.publish_(0u64);
    }


}

#[cfg(test)]
mod timer_signal_tests_ {
    use std::{
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        task::Wake,
    };

    use super::*;

    /// 计数唤醒器：统计 `wake` 次数。
    struct CountingWake_ {
        count_: AtomicUsize,
    }

    impl Wake for CountingWake_ {
        fn wake(self: Arc<Self>) {
            self.count_.fetch_add(1usize, Ordering::SeqCst);
        }

        fn wake_by_ref(self: &Arc<Self>) {
            self.count_.fetch_add(1usize, Ordering::SeqCst);
        }
    }

    /// 造一个计数 waker 与它的计数句柄。
    fn counting_waker_() -> (core::task::Waker, Arc<CountingWake_>) {
        let probe = Arc::new(CountingWake_ {
            count_: AtomicUsize::new(0usize),
        });
        (core::task::Waker::from(probe.clone()), probe)
    }

    /// 测试发布期限后 timer 立刻可被唤醒，并能读到期限。
    /// - 手段：`publish_(123)` 后 poll `poll_wake_`。
    /// - 判断：poll 为 `Ready`；`next_millis_()==123`；再 poll 一次为 `Pending`
    ///   （持久位已被消费）。
    ///
    /// Test goal: publishing a deadline wakes the timer and exposes the value.
    /// Method: `publish_(123)` then poll `poll_wake_`.
    /// Assertion: first poll is `Ready`; `next_millis_()==123`; second poll is
    /// `Pending` (the persistent bit was consumed).
    #[test]
    fn timer_signal_publish_wakes_and_exposes_deadline_() {
        let signal = TimerSignal_::new_();
        let (waker, _probe) = counting_waker_();
        let mut cx = Context::from_waker(&waker);

        signal.publish_(123u64);
        assert!(signal.poll_wake_(&mut cx).is_ready());
        assert_eq!(signal.next_millis_(), 123u64);
        assert!(signal.poll_wake_(&mut cx).is_pending());
    }

    /// 测试等待中的 timer 会被新的期限唤醒。
    /// - 手段：先 poll 一次（无期限，应 `Pending` 并登记 waker），再 `publish_(7)`。
    /// - 判断：首次 `Pending`；发布后唤醒计数为 1。
    ///
    /// Test goal: a parked timer is woken by a new deadline.
    /// Method: poll once with no deadline (expect `Pending`, registering the waker),
    /// then `publish_(7)`.
    /// Assertion: first poll is `Pending`; publishing increments the wake count.
    #[test]
    fn timer_signal_publish_wakes_parked_timer_() {
        let signal = TimerSignal_::new_();
        let (waker, probe) = counting_waker_();
        let mut cx = Context::from_waker(&waker);

        assert!(signal.poll_wake_(&mut cx).is_pending());
        assert_eq!(probe.count_.load(Ordering::SeqCst), 0usize);
        signal.publish_(7u64);
        assert_eq!(probe.count_.load(Ordering::SeqCst), 1usize);
    }

    /// 测试 `clear_` 把期限复位为「没有」。
    /// - 手段：发布 9 后 `clear_()`。
    /// - 判断：`next_millis_()==0`。
    ///
    /// Test goal: `clear_` resets the deadline to "none".
    /// Method: publish 9, then `clear_()`.
    /// Assertion: `next_millis_()==0`.
    #[test]
    fn timer_signal_clear_resets_deadline_() {
        let signal = TimerSignal_::new_();
        signal.publish_(9u64);
        assert_eq!(signal.next_millis_(), 9u64);
        signal.clear_();
        assert_eq!(signal.next_millis_(), 0u64);
    }
}
