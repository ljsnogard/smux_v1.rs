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

use core::cell::Cell;

std::thread_local! {
    /// 解复用循环每跑一轮就自增。
    static DEMUX_TICKS: Cell<u64> = const { Cell::new(0) };
}

/// 解复用循环记一次活动（由 `session_::demux_loop_async_` 在每轮开头调用）。
pub(crate) fn demux_tick_() {
    DEMUX_TICKS.with(|c| c.set(c.get().wrapping_add(1)));
}

/// 读解复用环计数（**不清零**，便于反复采样同一段区间）。
pub(crate) fn demux_ticks_() -> u64 {
    DEMUX_TICKS.with(Cell::get)
}
