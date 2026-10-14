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

std::thread_local! {
    /// 解复用循环每跑一轮就自增。
    static DEMUX_TICKS: Cell<u64> = const { Cell::new(0) };

    /// 计时循环在等待关闭状态下**认领**「排空 + `FIN`」动作的次数。
    static CLOSE_FIN_CLAIMED: Cell<u64> = const { Cell::new(0) };

    /// 复用循环**处理** `WriteEvent_::TxClosed` 的次数（应用丢弃发送半边，或连接
    /// 在关闭流程里代为投递——两者同一条路径）。
    static TX_CLOSED_HANDLED: Cell<u64> = const { Cell::new(0) };

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

/// 记一次「复用循环处理了 `TxClosed`」。
pub(crate) fn tx_closed_handled_() {
    TX_CLOSED_HANDLED.with(|c| c.set(c.get().wrapping_add(1)));
}

/// 读「处理 `TxClosed`」计数。
pub fn tx_closed_handled_ticks_() -> u64 {
    TX_CLOSED_HANDLED.with(Cell::get)
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
pub(crate) fn fin_see_data_() {
    FIN_SEE_DATA.with(|c| c.set(c.get().wrapping_add(1)));
}

/// 记一次「`finalize_entry_` 看到环已排空」。
pub(crate) fn fin_see_empty_() {
    FIN_SEE_EMPTY.with(|c| c.set(c.get().wrapping_add(1)));
}

/// 记一次「`finalize_entry_` 无法判定」。
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
