//! 保活与空闲超时的 **tokio** 薄壳：只做「建立作用域 + 选运行时」。
//!
//! 场景本体与设计说明在 `tests/keepalive_common.inc`；compio 侧见 `keepalive_compio.rs`。

#![cfg(feature = "test-tokio-runtime")]

#[path = "common/mod.rs"]
mod common;

#[path = "keepalive_common.inc"]
mod keepalive_common;

/// 测试目标、手段、判断见 [`keepalive_common::idle_channel_times_out_`]。
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn idle_channel_times_out_tokio_() {
    let scope = abs_art_tokio::LocalScope::new();
    keepalive_common::idle_channel_times_out_(&scope).await;
}

/// 测试目标、手段、判断见 [`keepalive_common::keepalive_pulses_`]。
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn keepalive_pulses_tokio_() {
    let scope = abs_art_tokio::LocalScope::new();
    keepalive_common::keepalive_pulses_(&scope).await;
}
