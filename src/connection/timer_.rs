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
//! 否则核心永远不会析构、取消令牌永远不会触发。等待用后端计时
//! （`S: TrTime` 的 `delay`），「什么时候该醒」用连接级时钟在本地算。
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
    task::Poll,
};

use abs_art::TrTime;
use buffex::x_deps::abs_cancel::TrCancellationToken;

use crate::{
    connection::{
        Dock, FrameKind, TrConnCfg, flags,
        session_::MuxShared_,
        signal_::{ControlFrame_, EventSender_, ReadEvent_, TrEventSender_, WriteEvent_},
    },
    flow_ctrl::WindowReport,
    time::TrDeadline,
};

/// 每轮最多认领 / 投递多少条保活动作。
///
/// 它是**限频**：每条 `PULSE` 或拆流都要经 `flume` 投一条事件（一次全局分配），
/// 因此在册子流很多时不能一轮全投。写满即提前返回并**立刻**再扫一轮
/// （`TimerScan_::is_full_`），动作不会丢，只是分轮完成。
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
}

/// 计时循环本体。
///
/// # 参数
///
/// - `S`：**后端计时类型**（最终二进制选中的那个，例如 `abs_art_tokio::LocalScope`）。
///   它只需实现 [`TrTime`]；本循环不要求它是作用域，也不持有任何作用域值。
/// - `C`：连接资源策略（`TrConnCfg`），提供分配器与**连接级时钟**类型。
pub(crate) async fn timer_loop_async_<C, S, K>(
    shared: MuxShared_<C>,
    w_events: EventSender_<WriteEvent_<C::Buff, C::Alloc>>,
    r_events: EventSender_<ReadEvent_<C::Buff, C::Alloc>>,
    cancel: K,
) where
    C: TrConnCfg,
    S: TrTime,
    K: TrCancellationToken,
{
    let clock = shared.conn_clock_().clone();
    let timeout_millis = shared.channel_timeout_millis_();
    // 接近超时的判据取 timeout/2：既留出「PULSE 往返 + 对端处理」的余量，又保证
    // 一个空闲期至多一条 PULSE。
    let pulse_millis = timeout_millis / 2u64;

    let mut actions = [TimerAction_::Idle; K_TIMER_ACTION_BATCH];

    loop {
        if cancel.is_cancelled() {
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
            Result::Err(_) => return,
        };

        // 2. 锁外投递本轮认领到的动作。
        for action in &actions[..scan.actions_()] {
            if !deliver_action_::<C, _>(&shared, &w_events, &r_events, action, cancel.child_token())
                .await
            {
                // 事件通道的生产端已全部消失 = 连接正在收尾。
                return;
            }
        }

        // 3. 算下一次醒来：批次满则立刻再扫一轮；否则睡到最早期限，并以上限
        //    `timeout` 兜底（没有任何在册子流时也要周期性回来看看）。
        let next = if scan.is_full_() {
            now
        } else {
            scan.next_millis_()
                .min(now.saturating_add(timeout_millis))
        };

        // 4. 睡到「最早期限」，同时可被**注册新身份**与取消令牌打断。
        //
        //    两个唤醒源都不可省：不挂注册唤醒，刚建立的子流要等到本端已排好的那个
        //    期限才会被纳入考虑（登记处虽然已经用「现在」给它打了点，但它的
        //    `PULSE` 职责与存活期限都要等下一轮扫描才算）；不挂取消令牌则连接被丢弃
        //    后本任务会一直睡下去，连同它的状态永久泄漏。
        let deadline = clock.deadline_(next);
        let clock_value = clock.clock_();
        let mut sleep_fut = core::pin::pin!(<S as TrDeadline>::sleep_until(clock_value, deadline));
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
            return;
        }
    }
}

/// 投递一条保活动作；返回 `false` 表示连接正在收尾（调用方应当退出）。
async fn deliver_action_<C, K>(
    shared: &MuxShared_<C>,
    w_events: &EventSender_<WriteEvent_<C::Buff, C::Alloc>>,
    r_events: &EventSender_<ReadEvent_<C::Buff, C::Alloc>>,
    action: &TimerAction_,
    cancel: K,
) -> bool
where
    C: TrConnCfg,
    K: TrCancellationToken,
{
    match action {
        TimerAction_::Idle => true,

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
            w_events.try_send_event_(WriteEvent_::Control { frame_: frame })
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
                Result::Ok(()) => {}
                Result::Err(_) => return false,
            }

            // 3. 记下两个方向都已经「协议收尾」：发送环里剩下的字节被主动放弃
            //    （`FIN` 只在排空后发，而这里是明确不排空了），接收方向也不再收。
            //    这同时让应用随后丢弃两个半部时不会再补发重复的 `CLOSE`。
            if let Option::Some(owner) = owner {
                owner.set_local_fin_sent_();
                let _ = owner.claim_local_reset_();
            }

            // 4. 尽力告诉对端：两个方向都收尾。`FIN` 关掉对端的接收方向、`RESET` 关掉
            //    对端的发送方向，一条帧只能表达一个方向，因此发两条。
            let fin = ControlFrame_::plain_(FrameKind::Close, flags::K_FIN, local, remote);
            if !w_events.try_send_event_(WriteEvent_::Control { frame_: fin }) {
                return false;
            }
            let reset = ControlFrame_::plain_(FrameKind::Close, flags::K_RESET, local, remote);
            if !w_events.try_send_event_(WriteEvent_::Control { frame_: reset }) {
                return false;
            }

            // 5. 本地两个循环各自摘掉本地表项：复用循环放弃这条子流（含「欠一条
            //    FIN」的登记）、并**显式关闭发送环的消费端**（应用随即看到发送方向
            //    已关闭），解复用循环丢掉接收环写端（应用随即读到 EOF）。
            if !w_events.try_send_event_(WriteEvent_::LocalAbort {
                local_dock: local,
                remote_dock: remote,
            }) {
                return false;
            }
            let _ = r_events.try_send_event_(ReadEvent_::Release {
                local_dock: local,
                remote_dock: remote,
            });
            true
        }
    }
}
