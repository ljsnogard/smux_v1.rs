//! 保活与空闲超时的 **compio** 薄壳：只做「建立作用域 + 选运行时」。
//!
//! 场景本体与设计说明在 `tests/keepalive_common.inc`；tokio 侧见 `keepalive.rs`。
//!
//! 跑本 target 时必须**显式关掉** `test-tokio-runtime`（两个 feature 都在缺省里，
//! 共享文件按它分派作用域类型）：
//!
//! ```bash
//! cargo test --test keepalive_compio --no-default-features --features test-compio-runtime
//! ```
//!
//! `--all-targets`（两个 feature 都在默认里）下本 target 会被 cfg 掉、不重复跑 compio。

#![cfg(not(feature = "test-tokio-runtime"))]

#[path = "common/mod.rs"]
mod common;

#[path = "keepalive_common.inc"]
mod keepalive_common;

/// 测试目标、手段、判断见 [`keepalive_common::idle_channel_times_out_`]。
#[compio::test]
async fn idle_channel_times_out_compio_() {
    let scope = abs_art_compio::LocalScope::new();
    keepalive_common::idle_channel_times_out_(&scope).await;
}

/// 测试目标、手段、判断见 [`keepalive_common::keepalive_pulses_`]。
#[compio::test]
async fn keepalive_pulses_compio_() {
    let scope = abs_art_compio::LocalScope::new();
    keepalive_common::keepalive_pulses_(&scope).await;
}
