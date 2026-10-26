//! **计时循环**：连接的第五个本地循环，负责保活（`PULSE`）与空闲超时拆流。
//!
//! # 形状（写法 B）
//!
//! 一个连接**一个**计时循环，它在完全静默时仍然会醒来，并**激活**其它可能已经
//! 静默的循环：`PULSE` 与拆流都经既有的两条事件通道投给复用 / 解复用循环，因此
//! 不需要为每条子流起一个计时任务，也不需要新增「连接级通知」之外的任何状态。
//! 决策的因果链与三处关键修正见 `dev-notes/keepalive-timer-loop-20261005-1420.md`。
//!
//! # 它持有什么
//!
//! | 持有 | 为什么 |
//! | --- | --- |
//! | [`MuxLoopShared_`](super::session_::MuxLoopShared_) | 注册表句柄 + 连接级时钟 + 空闲超时 |
//! | 两条事件通道的**生产端** | `PULSE` / `CLOSE` 投给复用循环，`Release` 投给解复用循环 |
//! | 取消令牌 | 与其它四个循环同一条收尾纪律 |
//!
//! 与另外四个循环一致：**不持有 [`MuxCore`](super::mux_connection::core_) 的强引用**，
//! 否则核心永远不会析构、取消令牌永远不会触发。等待用运行时值的 `delay`
//! （`R: TrTime`），「什么时候该醒」用连接级时钟在本地算。
//!
//! # 一轮做什么
//!
//! ```text
//! loop {
//!     扫注册表（一次读锁）：读两条时钟 / 认领 PULSE / 认领空闲拆流，并算出最早期限
//!     锁外投递：PULSE → 写循环；拆流 → 先落宽限态，再投 CLOSE×2 + PeerClosed + Release
//!     睡到 { 最早期限 | 注册新身份 | 取消 }
//! }
//! ```
//!
//! **动态睡到最早期限**而不是固定 tick：新身份登记会 `notify_timer_` 唤醒它重算
//! （见 `ChannelRegistry_::notify_timer_`），因此空闲时既不空转，也不会漏掉刚建立的
//! 子流。子流活动**不**唤醒它——活动只把期限**往后**推（两个时钟都只前进），早醒一轮
//! 只会重算一次，没有正确性问题。
//!
//! # 空闲超时的语义（本轮裁决）
//!
//! - **只拆该子流，不拆连接**：`MuxError::IdleTimeout` 是子流级失败；
//! - 拆流时**尽力**告诉对端（`CLOSE(FIN)` + `CLOSE(RESET)` 各一条），对端因此两个
//!   方向都收尾，不必等它自己的空闲计时；
//! - 应用侧从两个半部读到：环进入关闭态，且
//!   [`ChannelTx::abort_reason`](super::ChannelTx::abort_reason) /
//!   [`ChannelRx::abort_reason`](super::ChannelRx::abort_reason) 给出
//!   `Some(MuxError::IdleTimeout)`。
//!
//! # 两条时钟与「保活为什么能收敛」
//!
//! [`ChannelState_`](super::owner_::ChannelState_) 为每条子流记**两个**时刻，都由
//! 活动点在**活动发生的那一刻**写下（不是等计时循环扫描时打点——扫描有周期，把
//! 间隙里到的建流帧记成「扫描那一刻」会把本端的保活职责整整推后一个周期）：
//!
//! | 时钟 | 谁写 | 判什么 |
//! | --- | --- | --- |
//! | `active_millis_` | 收到的**任何**帧（含 `PULSE`）、本端写出的数据 | 存活：距它 `timeout` ⇒ 空闲超时 |
//! | `data_millis_` | **非保活**活动、以及本端上一次发出 `PULSE` | 职责：距它 `pulse_millis` ⇒ 发 `PULSE` |
//!
//! **收到对端的 `PULSE` 只刷前者、不刷后者**，这是保活能收敛的关键：若两者合一，
//! 两端会在同一时刻互相把对方的职责时钟清零、于是**都不发** `PULSE`，而各自的存活
//! 时钟又从最后一次真实活动起算——先停手的那一侧必定先到 `max_channel_timeout`
//! 被拆掉（`tests/keepalive.rs` 的第一版正是这么失败的：被判超时的恰好是先发出
//! `PULSE` 的那一侧）。
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
    connection::{
        Dock, FrameKind, TrConnCfg, flags,
        mux_connection::{K_CLOSE_NONE, K_CLOSE_TIMEOUT},
        session_::MuxShared_,
        signal_::{
            ControlFrame_, EventSender_, HINT_R_RELEASE, HINT_W_TX_CLOSED, ReadEvent_,
            WriteEvent_,
        },
        sync_::NotifySlot_,
    },
    flow_ctrl::WindowReport,
    metrics::{ChannelCloseReason, TrMetricsSink},
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

/// 计时循环本体。
///
/// # 参数
///
/// - `R`：**运行时值**（最终二进制选中的那个，例如
///   `abs_art_tokio::Runtime<{ FULL }>`）。它既报时刻（[`TrClock`]）又管等待
///   （[`TrDelay`]）——**同源**，因此虚拟时间下不会有「睡在虚拟时钟、读在墙上时钟」
///   的错配。本循环只要求 [`TrTime`]（= `TrDelay + TrClock`）。
/// - `C`：连接资源策略（`TrConnCfg`），提供分配器与两条传输半边。
///
/// [`TrClock`]: abs_art::TrClock
pub(crate) async fn timer_loop_async_<C, K>(
    shared: MuxShared_<C>,
    w_events: EventSender_<WriteEvent_<C::Alloc>>,
    r_events: EventSender_<ReadEvent_<C::Alloc>>,
    cancel: K,
) where
    C: TrConnCfg,
    K: TrCancellationToken,
{
    let clock = shared.conn_clock_().clone();
    let timeout_millis = shared.channel_timeout_millis_();
    // 接近超时的判据取 timeout/2：既留出「PULSE 往返 + 对端处理」的余量，又保证
    // 一个空闲期至多一条 PULSE。
    let pulse_millis = timeout_millis / 2u64;

    let mut actions = [TimerAction_::Idle; K_TIMER_ACTION_BATCH];

    loop {
        #[cfg(feature = "test-loop-probe")]
        crate::connection::loop_probe_::timer_tick_();
        if cancel.is_cancelled() {
            #[cfg(feature = "test-loop-probe")]
            crate::connection::loop_probe_::timer_exit_cancel_();
            return;
        }
        let now = clock.now_millis_();

        // 1. 扫描 + 认领（一次读锁；锁内不投递、不唤醒）。
        let scan = match shared
            .reg_()
            .timer_scan_(
                now,
                pulse_millis,
                timeout_millis,
                &mut actions,
                cancel.child_token(),
            )
            .await
        {
            Result::Ok(scan) => scan,
            // 等锁被取消 = 连接正在收尾。
            Result::Err(_) => {
                #[cfg(feature = "test-loop-probe")]
                crate::connection::loop_probe_::timer_exit_scan_err_();
                return;
            }
        };

        // 2. **连接级关闭动作优先**：等待关闭已经不需要再排空（活跃子流归零），或者
        //    关闭整体超时——两种情形都到此为止，本轮认领的逐子流动作已无意义。
        //    收尾就是取消三个令牌（与「丢弃连接」同一套机制，幂等）。
        //
        //    两条路径**不再有区别**：旧版把批量成帧留在连接写环里、由写泵排空，因此
        //    收敛路径要留写泵一条活路；本版复用循环直接写传输，连接内部没有排空阶段
        //    （见 `ChannelRegistry_::cancel_loops_` 的文档）。
        if scan.close_action_() != K_CLOSE_NONE {
            // 超时兜底要**留下显式事实**：`close_async` 的返回值靠它区分「与对端收敛」
            // 与「超时收尾」——两者都会取消令牌（见 `K_STATE_FORCED_CLOSE`）。
            if scan.close_action_() == K_CLOSE_TIMEOUT {
                shared.reg_().mark_forced_close_();
            }
            shared.reg_().cancel_loops_();
            return;
        }

        // 3. 锁外投递本轮认领到的动作。
        for action in &actions[..scan.actions_()] {
            if !deliver_action_::<C, _>(&shared, &w_events, &r_events, action, cancel.child_token())
                .await
            {
                // 事件通道的生产端已全部消失 = 连接正在收尾。
                #[cfg(feature = "test-loop-probe")]
                crate::connection::loop_probe_::timer_exit_send_fail_();
                return;
            }
        }

        // 4. 算下一次醒来：批次满则立刻再扫一轮；否则睡到最早期限，并以上限
        //    `timeout` 兜底（没有任何在册子流时也要周期性回来看看）。
        let next = if scan.is_full_() {
            now
        } else {
            scan.next_millis_()
                .min(now.saturating_add(timeout_millis))
        };

        // 5. 睡到「最早期限」，同时可被**注册新身份**与取消令牌打断。
        //
        //    两个唤醒源都不可省：不挂注册唤醒，刚建立的子流要等到本端已排好的那个
        //    期限才会被纳入考虑（登记处虽然已经用「现在」给它打了点，但它的
        //    `PULSE` 职责与存活期限都要等下一轮扫描才算）；不挂取消令牌则连接被丢弃
        //    后本任务会一直睡下去，连同它的状态永久泄漏。
        //
        //    `delay` 只吃**相对时长**：期限与「现在」都已经是自 epoch 起算的毫秒，
        //    相减即可，不需要把绝对时刻还原成后端的时间类型（`saturating_sub` 让
        //    「期限已过」退化为 `delay(0)`，按契约立刻就绪）。
        let wait_millis = next.saturating_sub(now);
        let mut sleep_fut = core::pin::pin!(clock
            .rt_()
            .delay(Duration::from_millis(wait_millis)));
        let cancel_fut = cancel.child_token().cancellation();
        let mut cancel_fut = core::pin::pin!(cancel_fut);
        let mut cancelled = false;
        poll_fn(|cx| {
            if core::future::Future::poll(cancel_fut.as_mut(), cx).is_ready() {
                cancelled = true;
                return Poll::Ready(());
            }
            if shared.reg_().poll_timer_wake_(cx).is_ready() {
                return Poll::Ready(());
            }
            core::future::Future::poll(sleep_fut.as_mut(), cx)
        })
        .await;
        if cancelled {
            #[cfg(feature = "test-loop-probe")]
            crate::connection::loop_probe_::timer_exit_cancel_();
            return;
        }
    }
}

/// 投递一条保活动作；返回 `false` 表示连接正在收尾（调用方应当退出）。
async fn deliver_action_<C, K>(
    shared: &MuxShared_<C>,
    w_events: &EventSender_<WriteEvent_<C::Alloc>>,
    // 读方向只剩合并提示；参数保留到事件通道整体清理时删除。
    _r_events: &EventSender_<ReadEvent_<C::Alloc>>,
    action: &TimerAction_,
    cancel: K,
) -> bool
where
    C: TrConnCfg,
    K: TrCancellationToken,
{
    match action {
        TimerAction_::Idle => true,

        TimerAction_::Finalize {
            local_dock,
            remote_dock,
        } => {
            // 复用**既有的**「应用丢弃发送半边」路径：状态字上的
            // `APP_TX_CLOSED` 就是事实，写循环按位在发送环排空之后补
            // `CLOSE(FIN)`。这里只负责置位 + 唤醒写循环，不再投事件。
            let owner = match shared
                .reg_()
                .channel_owner_(*local_dock, *remote_dock, cancel.child_token())
                .await
            {
                Result::Ok(owner) => owner,
                Result::Err(_) => return false,
            };
            let Option::Some(owner) = owner else {
                // 身份已经释放：没有可排空的东西。
                return true;
            };
            let _ = owner.set_app_tx_closed_();
            shared.reg_().raise_write_hint_(HINT_W_TX_CLOSED);
            true
        }

        TimerAction_::Pulse {
            local_dock,
            remote_dock,
            report,
        } => {
            // `PULSE` 与 `WINDOW_UPDATE` 共用「携带窗口通告」的形状：保活的同时
            // 顺带把本端接收窗口重同步一次（协议 §7.1 的既定形状）。
            let frame = ControlFrame_::with_window_(
                FrameKind::Pulse,
                if report.is_reset() {
                    flags::K_TOTAL_RESET
                } else {
                    0u8
                },
                *local_dock,
                *remote_dock,
                Option::Some((report.recv_total(), report.window())),
                Vec::new(),
            );
            w_events.try_send_durable_event_(WriteEvent_::Control { frame_: frame })
        }

        TimerAction_::Abort {
            local_dock,
            remote_dock,
        } => {
            let (local, remote) = (*local_dock, *remote_dock);

            // 1. 先取状态句柄：等下面把身份转成宽限态之后，注册表就再也交不出它了。
            let owner = match shared
                .reg_()
                .channel_owner_(local, remote, cancel.child_token())
                .await
            {
                Result::Ok(owner) => owner,
                Result::Err(_) => return false,
            };

            // 2. **先**把身份转成宽限态：此后到达的在途帧一律静默丢弃。这一步必须
            //    早于下面两条本地事件——否则读循环可能在宽限态落定之前把一个在途
            //    数据帧判成「未知子流」而终止整条连接。
            match shared
                .reg_()
                .release_channel_(
                    local,
                    remote,
                    shared.conn_clock_().now_millis_(),
                    cancel.child_token(),
                )
                .await
            {
                // 返回值（活跃子流是否归零）在这里无关：计时循环拆流之后，
                // 「最后一条收口」由复用循环在它自己的释放路径上发现。
                Result::Ok(_) => {}
                Result::Err(_) => return false,
            }

            // 3. 记下两个方向都已经「协议收尾」：发送环里剩下的字节被主动放弃
            //    （`FIN` 只在排空后发，而这里是明确不排空了），接收方向也不再收。
            //    这同时让应用随后丢弃两个半部时不会再补发重复的 `CLOSE`。
            if let Option::Some(owner) = owner {
                owner.set_local_fin_sent_();
                let _ = owner.claim_local_reset_();
                // 身份已经被本路径直接释放（绕过 `claim_release_` 的两方向判据）：
                // 补置 `RELEASED`，读循环据此摘本地读表并关接收环写端。
                owner.mark_released_();
                // 两个方向就此收尾（`FIN` / `RESET` 均已投递）：叫醒应用侧的收尾
                // 等待者，让 `flush_async` / `close_async` 按「空闲超时」的终局返回，
                // 而不是继续 park 在一条再也不会被排空的环上。原因（`IdleTimeout`）
                // 已由计时循环在认领动作时发布，因此这里顺序无关。
                owner.notify_both_progress_();
                // 上报子流关闭（空闲超时）。这条路径**不经过** `maybe_release_`——它
                // 直接释放身份，因此关闭回调只能在这里发（见 [`crate::metrics`] 模块
                // 文档的插入点说明）。
                let now_millis = shared.conn_clock_().now_millis_();
                shared.metrics_().on_channel_closed(
                    local,
                    remote,
                    ChannelCloseReason::IdleTimeout,
                    now_millis.saturating_sub(owner.created_millis_()),
                );
            }

            // 4. 尽力告诉对端：两个方向都收尾。`FIN` 关掉对端的接收方向、`RESET` 关掉
            //    对端的发送方向，一条帧只能表达一个方向，因此发两条。
            let fin = ControlFrame_::plain_(FrameKind::Close, flags::K_FIN, local, remote);
            if !w_events.try_send_durable_event_(WriteEvent_::Control { frame_: fin }) {
                return false;
            }
            let reset = ControlFrame_::plain_(FrameKind::Close, flags::K_RESET, local, remote);
            if !w_events.try_send_durable_event_(WriteEvent_::Control { frame_: reset }) {
                return false;
            }

            // 5. 本地两个循环各自摘掉本地表项：复用循环放弃这条子流（含「欠一条
            //    FIN」的登记）、并**显式关闭发送环的消费端**（应用随即看到发送方向
            //    已关闭），解复用循环丢掉接收环写端（应用随即读到 EOF）。
            if !w_events.try_send_durable_event_(WriteEvent_::LocalAbort {
                local_dock: local,
                remote_dock: remote,
            }) {
                return false;
            }
            // 身份已释放：读循环按 `RELEASED` 位摘表并关接收环写端。
            shared.reg_().raise_read_hint_(HINT_R_RELEASE);
            true
        }

        TimerAction_::RejectEstablish {
            local_dock,
            remote_dock,
        } => {
            let (local, remote) = (*local_dock, *remote_dock);

            // 1. 先取状态句柄：身份一旦释放就取不到了，而下面还要用它唤醒建流等待方。
            let owner = match shared
                .reg_()
                .channel_owner_(local, remote, cancel.child_token())
                .await
            {
                Result::Ok(owner) => owner,
                Result::Err(_) => return false,
            };

            // 2. 对端此刻一定在等裁决（发起方已发出 `OPEN`、响应方的 `OPEN` 已到达），
            //    因此必须明确回一条 `REJECT` 把它放走——这正是本条与 `Abort` 的分界：
            //    后者关掉的是一条对端早已在传数据的子流，对端不在等建流结果。
            let reject = ControlFrame_::plain_(FrameKind::Reject, 0u8, local, remote);
            if !w_events.try_send_durable_event_(WriteEvent_::Control { frame_: reject }) {
                return false;
            }

            // 3. 释放身份（进拆流宽限，让在途帧被静默丢弃）。此刻还没有环、也没有
            //    数据流，因此不像 `Abort` 那样再投两条本地关闭事件。
            let _ = shared
                .reg_()
                .release_channel_(
                    local,
                    remote,
                    shared.conn_clock_().now_millis_(),
                    cancel.child_token(),
                )
                .await;

            // 4. 唤醒建流等待方：原因位已在扫描时经 `claim_abort_` 写下，它会读到
            //    `IdleTimeout`。同一时刻把「已裁决」置上，免得同一根时钟在下一轮扫描
            //    里再把它当成活跃子流拆一次。
            if let Option::Some(owner) = owner {
                owner.set_establish_settled_();
                owner.notify_establish_();
                // 上报子流关闭（**建流未裁决**即超时）：与 `Abort` 同理，这条路径不经过
                // `maybe_release_`。
                let now_millis = shared.conn_clock_().now_millis_();
                shared.metrics_().on_channel_closed(
                    local,
                    remote,
                    ChannelCloseReason::EstablishTimeout,
                    now_millis.saturating_sub(owner.created_millis_()),
                );
            }
            true
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
/// `0` 表示「当前没有期限」：timer 于是只等 `timer_wake_` / 取消令牌。
#[allow(dead_code)]
pub(crate) struct TimerSignal_ {
    /// 下一次必须醒来的连接内毫秒；`0` = 当前没有期限。
    next_millis_: AtomicU64,

    /// main -> timer：期限变了，重算 delay。
    timer_wake_: NotifySlot_,
}

#[allow(dead_code)]
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

    /// 清掉期限（连接收尾时由 main 发布）。
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
