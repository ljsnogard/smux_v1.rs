//! 循环活动探针：回答「某个本地循环到底还在不在跑」。
//!
//! 挂起类问题（例如「连接空闲之后再建一条子流就卡住」）的第一分岔点是：
//! **循环停了**，还是**循环在跑但不推进协议**？这两种的修法完全不同，而现象都是
//! 「测试挂住」。本模块给出一条可采样的证据。
//!
//! # 为什么是 thread-local
//!
//! 连接的本地循环是**同线程**的（`spawn_local`），因此一个 `Cell<u64>` 就够，
//! 不需要任何跨线程同步，也不给热路径增加原子操作。
//!
//! # 为什么单独成模块、而不放进 `test_support_`
//!
//! `test_support_` 是 `cfg(test)` 的：它依赖 `MuxConnection::new_test_`（同样
//! `cfg(test)`）。而**集成测试**链接的是正常编译的库，`cfg(test)` 对它不可见——探针
//! 必须在集成测试里可用，所以它挂在自己的 feature（`test-loop-probe`）上，不依赖
//! 任何测试专用构造路径。
//!
//! # 它不在生产 feature 集里
//!
//! `test-loop-probe` 只由两个测试 feature 打开；生产构建里本模块整个不存在，
//! 循环里那一次自增也因此消失（`#[cfg]` 掉）。
//!
//! # 关闭流程的探针（`CLOSE_*` / `TX_CLOSED` / `LOCAL_FIN`）
//!
//! 优雅关闭是**跨三个任务**的协作（计时循环认领动作 → 复用循环排空并发 `FIN` →
//! 收口），挂在任何一处都看不出「链条断在哪一环」。这几条计数把链条逐段摊开：
//! 「认领了没有」「事件处理了没有」「`FIN` 发出去了没有」「收尾判定了没有」，
//! 每段各有一次自增，采样它们的差值即可定位断点。

use core::cell::Cell;
use std::sync::atomic::{AtomicU64, Ordering};

std::thread_local! {
    /// 解复用循环每跑一轮就自增。
    static DEMUX_TICKS: Cell<u64> = const { Cell::new(0) };

    /// 计时循环在等待关闭状态下**认领**「排空 + `FIN`」动作的次数。
    static CLOSE_FIN_CLAIMED: Cell<u64> = const { Cell::new(0) };

    /// 复用循环**成功发出一条 `CLOSE(FIN)`** 的次数（`finalize_entry_` 走完）。
    static LOCAL_FIN_SENT: Cell<u64> = const { Cell::new(0) };

    /// 计时循环判定「等待关闭且已经没有活跃子流」的次数。
    static CLOSE_IDLE_HITS: Cell<u64> = const { Cell::new(0) };

    /// 计时循环判定「等待关闭整体超时」的次数。
    static CLOSE_TIMEOUT_HITS: Cell<u64> = const { Cell::new(0) };

    /// 计时循环**扫描时看到 `closing == true`** 的次数（每轮至多一次）。
    static CLOSE_SEEN: Cell<u64> = const { Cell::new(0) };

    /// 计时循环因**取消令牌**退出。
    static TIMER_EXIT_CANCEL: Cell<u64> = const { Cell::new(0) };

    /// 计时循环因**扫描等锁被取消**退出。
    static TIMER_EXIT_SCAN_ERR: Cell<u64> = const { Cell::new(0) };

    /// 计时循环因**事件通道生产端消失**退出。
    static TIMER_EXIT_SEND_FAIL: Cell<u64> = const { Cell::new(0) };

    /// `finalize_entry_` 判定「发送环里还有数据」（`Some(true)`）的次数。
    static FIN_SEE_DATA: Cell<u64> = const { Cell::new(0) };

    /// `finalize_entry_` 判定「发送环已排空」（`Some(false)`）的次数。
    static FIN_SEE_EMPTY: Cell<u64> = const { Cell::new(0) };

    /// `finalize_entry_` 判定「不可判定」（`None`）的次数。
    static FIN_SEE_UNKNOWN: Cell<u64> = const { Cell::new(0) };

    /// 复用循环每跑一轮就自增。
    static MUX_TICKS: Cell<u64> = const { Cell::new(0) };

    /// 计时循环每跑一轮就自增。
    static TIMER_TICKS: Cell<u64> = const { Cell::new(0) };

    /// `MuxCore::drop` 被调用的次数（每次连接恰好一次）。
    static CORE_DROPS: Cell<u64> = const { Cell::new(0) };

    /// `MuxCore::drop` 里「首次进入等待关闭」成功的次数。
    static CORE_ENTERED_CLOSE: Cell<u64> = const { Cell::new(0) };
}

/// 解复用循环记一次活动（由 `session_::demux_loop_async_` 在每轮开头调用）。
pub(crate) fn demux_tick_() {
    DEMUX_TICKS.with(|c| c.set(c.get().wrapping_add(1)));
}

/// 读解复用环计数（**不清零**，便于反复采样同一段区间）。
pub(crate) fn demux_ticks_() -> u64 {
    DEMUX_TICKS.with(Cell::get)
}

/// 记一次「计时循环认领了某条子流的收尾动作」。
pub(crate) fn close_fin_claimed_() {
    CLOSE_FIN_CLAIMED.with(|c| c.set(c.get().wrapping_add(1)));
}

/// 读「认领收尾动作」计数。
pub fn close_fin_claimed_ticks_() -> u64 {
    CLOSE_FIN_CLAIMED.with(Cell::get)
}

/// 记一次「本端发出 `CLOSE(FIN)`」。
pub(crate) fn local_fin_sent_() {
    LOCAL_FIN_SENT.with(|c| c.set(c.get().wrapping_add(1)));
}

/// 读「发出 `CLOSE(FIN)`」计数。
pub fn local_fin_sent_ticks_() -> u64 {
    LOCAL_FIN_SENT.with(Cell::get)
}

/// 记一次「等待关闭且已经没有活跃子流」。
pub(crate) fn close_idle_hit_() {
    CLOSE_IDLE_HITS.with(|c| c.set(c.get().wrapping_add(1)));
}

/// 读「等待关闭且已无活跃子流」计数。
pub fn close_idle_hits_() -> u64 {
    CLOSE_IDLE_HITS.with(Cell::get)
}

/// 记一次「等待关闭整体超时」。
pub(crate) fn close_timeout_hit_() {
    CLOSE_TIMEOUT_HITS.with(|c| c.set(c.get().wrapping_add(1)));
}

/// 读「等待关闭整体超时」计数。
pub fn close_timeout_hits_() -> u64 {
    CLOSE_TIMEOUT_HITS.with(Cell::get)
}

/// 复用循环记一轮活动。
pub(crate) fn mux_tick_() {
    MUX_TICKS.with(|c| c.set(c.get().wrapping_add(1)));
}

/// 读复用循环轮数。
pub fn mux_ticks_() -> u64 {
    MUX_TICKS.with(Cell::get)
}

/// 计时循环记一轮活动。
pub(crate) fn timer_tick_() {
    TIMER_TICKS.with(|c| c.set(c.get().wrapping_add(1)));
}

/// 读计时循环轮数。
pub fn timer_ticks_() -> u64 {
    TIMER_TICKS.with(Cell::get)
}

/// 记一次「核心析构」。
pub(crate) fn core_drop_() {
    CORE_DROPS.with(|c| c.set(c.get().wrapping_add(1)));
}

/// 读核心析构计数。
pub fn core_drops_() -> u64 {
    CORE_DROPS.with(Cell::get)
}

/// 记一次「核心析构时首次进入等待关闭」。
pub(crate) fn note_core_entered_close_() {
    CORE_ENTERED_CLOSE.with(|c| c.set(c.get().wrapping_add(1)));
}

/// 读「核心首次进入等待关闭」计数。
pub fn core_entered_close_() -> u64 {
    CORE_ENTERED_CLOSE.with(Cell::get)
}

/// 记一次「计时循环扫描时看到等待关闭状态」。
pub(crate) fn close_seen_() {
    CLOSE_SEEN.with(|c| c.set(c.get().wrapping_add(1)));
}

/// 读「扫描时看到等待关闭」计数。
pub fn close_seen_ticks_() -> u64 {
    CLOSE_SEEN.with(Cell::get)
}

/// 记「计时循环因取消退出」。
pub(crate) fn timer_exit_cancel_() {
    TIMER_EXIT_CANCEL.with(|c| c.set(c.get().wrapping_add(1)));
}

/// 记「计时循环因扫描失败退出」。
pub(crate) fn timer_exit_scan_err_() {
    TIMER_EXIT_SCAN_ERR.with(|c| c.set(c.get().wrapping_add(1)));
}

/// 记「计时循环因投递失败退出」。
pub(crate) fn timer_exit_send_fail_() {
    TIMER_EXIT_SEND_FAIL.with(|c| c.set(c.get().wrapping_add(1)));
}

/// 读三条计时循环退出计数 `(取消, 扫描失败, 投递失败)`。
pub fn timer_exits_() -> (u64, u64, u64) {
    (
        TIMER_EXIT_CANCEL.with(Cell::get),
        TIMER_EXIT_SCAN_ERR.with(Cell::get),
        TIMER_EXIT_SEND_FAIL.with(Cell::get),
    )
}

/// 记一次「`finalize_entry_` 看到环里还有数据」。
#[allow(dead_code, reason = "R6 起 finalize_entry_ 删除；probe 保留待下一轮清理")]
pub(crate) fn fin_see_data_() {
    FIN_SEE_DATA.with(|c| c.set(c.get().wrapping_add(1)));
}

/// 记一次「`finalize_entry_` 看到环已排空」。
#[allow(dead_code, reason = "R6 起 finalize_entry_ 删除；probe 保留待下一轮清理")]
pub(crate) fn fin_see_empty_() {
    FIN_SEE_EMPTY.with(|c| c.set(c.get().wrapping_add(1)));
}

/// 记一次「`finalize_entry_` 无法判定」。
#[allow(dead_code, reason = "R6 起 finalize_entry_ 删除；probe 保留待下一轮清理")]
pub(crate) fn fin_see_unknown_() {
    FIN_SEE_UNKNOWN.with(|c| c.set(c.get().wrapping_add(1)));
}

/// 读三个判定计数 `(有数据, 已排空, 不可判定)`。
pub fn fin_sees_() -> (u64, u64, u64) {
    (
        FIN_SEE_DATA.with(Cell::get),
        FIN_SEE_EMPTY.with(Cell::get),
        FIN_SEE_UNKNOWN.with(Cell::get),
    )
}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// 事件通道诊断（**全局原子**）
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
//
// 事件生产者既可能跑在循环线程（读循环投写事件、写循环投读事件），也可能跑在
// API 线程（跨线程句柄），所以这里不能用 thread-local。只在 `test-loop-probe`
// feature 下编译，生产构建里整个消失。

/// `TxReady`：满足通知条件、进入投递流程的次数。
static TX_READY_ATTEMPT: AtomicU64 = AtomicU64::new(0);
/// `TxReady`：因为「进入函数时发送环为空」而通知的次数。
static TX_READY_EMPTY_RING: AtomicU64 = AtomicU64::new(0);
/// `TxReady`：因为「积压超过临界水位」而通知的次数。
static TX_READY_CONGESTED: AtomicU64 = AtomicU64::new(0);
/// `TxReady`：因为「积压状态发生边沿变化」而通知的次数。
static TX_READY_EDGE: AtomicU64 = AtomicU64::new(0);
/// `TxReady`：被 `mark_tx_queued_` 去重位挡下的次数（重复通知）。
static TX_READY_DEDUP: AtomicU64 = AtomicU64::new(0);
/// `TxReady`：真正投递成功的次数。
static TX_READY_SENT: AtomicU64 = AtomicU64::new(0);
/// `TxReady`：投递时环满/消费者消失的次数。
static TX_READY_FULL: AtomicU64 = AtomicU64::new(0);

/// `RxConsumed`：进入投递流程的次数。
static RX_CONSUMED_ATTEMPT: AtomicU64 = AtomicU64::new(0);
/// `RxConsumed`：被去重位挡下的次数。
static RX_CONSUMED_DEDUP: AtomicU64 = AtomicU64::new(0);
/// `RxConsumed`：真正投递成功的次数。
static RX_CONSUMED_SENT: AtomicU64 = AtomicU64::new(0);
/// `RxConsumed`：投递时环满/消费者消失的次数。
static RX_CONSUMED_FULL: AtomicU64 = AtomicU64::new(0);

/// 可重建事件因环满被丢弃的次数。
static EVENT_FULL_RECOVERABLE: AtomicU64 = AtomicU64::new(0);
/// 不可重建事件成功进入 `durable_` 暂存的次数。
static EVENT_DURABLE_STASHED: AtomicU64 = AtomicU64::new(0);
/// 不可重建事件投递时消费者已消失的次数。
static EVENT_DURABLE_CLOSED: AtomicU64 = AtomicU64::new(0);
/// 消费者看到 `EventTake_::Rescan` 的次数。
static EVENT_RESCAN: AtomicU64 = AtomicU64::new(0);

/// 事件通道诊断快照（绝对值，调用方自行做前后差值）。
#[derive(Debug, Clone, Copy, Default)]
pub struct EventProbeSnapshot {
    /// `TxReady` 进入投递流程的次数。
    pub tx_ready_attempt: u64,
    /// 其中「发送环为空」分支的次数。
    pub tx_ready_empty_ring: u64,
    /// 其中「积压超过临界水位」分支的次数。
    pub tx_ready_congested: u64,
    /// 其中「积压边沿变化」分支的次数。
    pub tx_ready_edge: u64,
    /// 被去重位挡下的重复 `TxReady` 次数。
    pub tx_ready_dedup: u64,
    /// 真正投递成功的 `TxReady` 次数。
    pub tx_ready_sent: u64,
    /// `TxReady` 投递失败（环满 / 消费者消失）次数。
    pub tx_ready_full: u64,
    /// `RxConsumed` 进入投递流程的次数。
    pub rx_consumed_attempt: u64,
    /// 被去重位挡下的重复 `RxConsumed` 次数。
    pub rx_consumed_dedup: u64,
    /// 真正投递成功的 `RxConsumed` 次数。
    pub rx_consumed_sent: u64,
    /// `RxConsumed` 投递失败（环满 / 消费者消失）次数。
    pub rx_consumed_full: u64,
    /// 可重建事件因环满被丢弃的次数。
    pub event_full_recoverable: u64,
    /// 不可重建事件进入兜底暂存的次数。
    pub event_durable_stashed: u64,
    /// 不可重建事件投递时消费者已消失的次数。
    pub event_durable_closed: u64,
    /// 消费者看到 `Rescan` 的次数。
    pub event_rescan: u64,
}

/// 记一次 `TxReady` 通知尝试，并记下触发分支。
pub(crate) fn event_tx_ready_attempt_(empty_ring: bool, congested: bool, edge: bool) {
    TX_READY_ATTEMPT.fetch_add(1, Ordering::Relaxed);
    if empty_ring {
        TX_READY_EMPTY_RING.fetch_add(1, Ordering::Relaxed);
    }
    if congested {
        TX_READY_CONGESTED.fetch_add(1, Ordering::Relaxed);
    }
    if edge {
        TX_READY_EDGE.fetch_add(1, Ordering::Relaxed);
    }
}

/// 记一次被去重位挡下的 `TxReady`。
pub(crate) fn event_tx_ready_dedup_() {
    TX_READY_DEDUP.fetch_add(1, Ordering::Relaxed);
}

/// 记一次 `TxReady` 投递结果。
pub(crate) fn event_tx_ready_result_(sent: bool) {
    if sent {
        TX_READY_SENT.fetch_add(1, Ordering::Relaxed);
    } else {
        TX_READY_FULL.fetch_add(1, Ordering::Relaxed);
    }
}

/// 记一次 `RxConsumed` 通知尝试。
pub(crate) fn event_rx_consumed_attempt_() {
    RX_CONSUMED_ATTEMPT.fetch_add(1, Ordering::Relaxed);
}

/// 记一次被去重位挡下的 `RxConsumed`。
pub(crate) fn event_rx_consumed_dedup_() {
    RX_CONSUMED_DEDUP.fetch_add(1, Ordering::Relaxed);
}

/// 记一次 `RxConsumed` 投递结果。
pub(crate) fn event_rx_consumed_result_(sent: bool) {
    if sent {
        RX_CONSUMED_SENT.fetch_add(1, Ordering::Relaxed);
    } else {
        RX_CONSUMED_FULL.fetch_add(1, Ordering::Relaxed);
    }
}

/// 记一次「可重建事件因环满被丢弃」。
pub(crate) fn event_full_recoverable_() {
    EVENT_FULL_RECOVERABLE.fetch_add(1, Ordering::Relaxed);
}

/// 记一次「不可重建事件进入兜底暂存」。
pub(crate) fn event_durable_stashed_() {
    EVENT_DURABLE_STASHED.fetch_add(1, Ordering::Relaxed);
}

/// 记一次「不可重建事件投递时消费者已消失」。
pub(crate) fn event_durable_closed_() {
    EVENT_DURABLE_CLOSED.fetch_add(1, Ordering::Relaxed);
}

/// 记一次「消费者看到 `Rescan`」。
pub(crate) fn event_rescan_() {
    EVENT_RESCAN.fetch_add(1, Ordering::Relaxed);
}

/// 读事件通道诊断快照。
pub fn event_probe_snapshot_() -> EventProbeSnapshot {
    EventProbeSnapshot {
        tx_ready_attempt: TX_READY_ATTEMPT.load(Ordering::Relaxed),
        tx_ready_empty_ring: TX_READY_EMPTY_RING.load(Ordering::Relaxed),
        tx_ready_congested: TX_READY_CONGESTED.load(Ordering::Relaxed),
        tx_ready_edge: TX_READY_EDGE.load(Ordering::Relaxed),
        tx_ready_dedup: TX_READY_DEDUP.load(Ordering::Relaxed),
        tx_ready_sent: TX_READY_SENT.load(Ordering::Relaxed),
        tx_ready_full: TX_READY_FULL.load(Ordering::Relaxed),
        rx_consumed_attempt: RX_CONSUMED_ATTEMPT.load(Ordering::Relaxed),
        rx_consumed_dedup: RX_CONSUMED_DEDUP.load(Ordering::Relaxed),
        rx_consumed_sent: RX_CONSUMED_SENT.load(Ordering::Relaxed),
        rx_consumed_full: RX_CONSUMED_FULL.load(Ordering::Relaxed),
        event_full_recoverable: EVENT_FULL_RECOVERABLE.load(Ordering::Relaxed),
        event_durable_stashed: EVENT_DURABLE_STASHED.load(Ordering::Relaxed),
        event_durable_closed: EVENT_DURABLE_CLOSED.load(Ordering::Relaxed),
        event_rescan: EVENT_RESCAN.load(Ordering::Relaxed),
    }
}
