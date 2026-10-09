//! 保活与空闲超时的 **compio** 薄壳：只做「建运行时 + 取运行时值 / 作用域 + 驱动虚拟时间」。
//!
//! 场景本体与设计说明在 `tests/common/keepalive_common.rs`；tokio 侧见 `keepalive.rs`。
//!
//! 缺省 feature 集就是 compio，因此 `cargo test --all-targets` 直接跑到本 target；
//! 显式指定的等价写法是：
//!
//! ```bash
//! cargo test --test keepalive_compio --no-default-features --features test-compio-runtime
//! ```
//!
//! 换 tokio / smol 装配时才需要 `--no-default-features`（smux 的 `DefaultRt_` 是
//! `test-*-runtime` 三选一）。
//!
//! # 作用域与时间
//!
//! 与 tokio 侧同理：作用域不依赖 thread_local，由 `Runtime::with_runtime(rt)` 造出的
//! 运行时值交出（`local_scope()` 每次新建一条队列）；时间用 `ManualTime` 装饰成虚拟的。
//!
//! # 为什么这里是同步 `#[test]` 而不是 `#[compio::test]`
//!
//! `LocalScope::block_on_advancing` 内部会调 `compio::runtime::Runtime::block_on`——在
//! compio 运行时上下文**内**再 `block_on` 是不允许的。因此本壳用普通 `#[test]` 自建
//! 一个 compio 运行时，在**上下文之外**用 `block_on_advancing` 跑场景（该入口内部会把
//! compio 执行器的 `run()` 当作 tick 钩子）。

#![cfg(not(feature = "test-tokio-runtime"))]
#![feature(allocator_ext)]

#[path = "common/mod.rs"]
mod common;

#[path = "common/keepalive_common.rs"]
mod keepalive_common;

use abs_art_mock_clock::{ManualClock, ManualTime};

/// 测试目标、手段、判断见 [`keepalive_common::idle_channel_times_out_`]。
///
/// 手段补充：运行时值由 `Runtime::with_runtime` 构造，再用 `ManualTime` 装饰成虚拟时间；
/// 场景由 `scope.block_on_advancing` 在 compio 上下文之外驱动。
#[test]
fn idle_channel_times_out_compio_() {
    let rt = compio::runtime::Runtime::new().expect("建 compio 运行时应当成功");
    let value: abs_art_bridge::CompioRuntime =
        abs_art_bridge::CompioRuntime::with_runtime(rt);
    // 第一句就说清本用例的前提：驱动虚拟时间的那套 API 属于 compio。
    common::assert_runtime_is_(&value, abs_art_bridge::RuntimeTag::Compio);
    let scope = value.local_scope();
    let clock = ManualClock::new();
    let timed = ManualTime::new(value, clock.clone());
    scope.block_on_advancing(
        &clock,
        keepalive_common::idle_channel_times_out_(&timed),
    );
}

/// 测试目标、手段、判断见 [`keepalive_common::keepalive_pulses_`]。
///
/// 手段补充：同 [`idle_channel_times_out_compio_`]，仍走虚拟时间。
#[test]
fn keepalive_pulses_compio_() {
    let rt = compio::runtime::Runtime::new().expect("建 compio 运行时应当成功");
    let value: abs_art_bridge::CompioRuntime =
        abs_art_bridge::CompioRuntime::with_runtime(rt);
    // 第一句就说清本用例的前提：驱动虚拟时间的那套 API 属于 compio。
    common::assert_runtime_is_(&value, abs_art_bridge::RuntimeTag::Compio);
    let scope = value.local_scope();
    let clock = ManualClock::new();
    let timed = ManualTime::new(value, clock.clone());
    scope.block_on_advancing(
        &clock,
        keepalive_common::keepalive_pulses_(&timed),
    );
}

/// 测试目标、手段、判断见 [`keepalive_common::establish_timeout_on_initiator_`]。
#[test]
fn establish_timeout_on_initiator_compio_() {
    let rt = compio::runtime::Runtime::new().expect("建 compio 运行时应当成功");
    let value: abs_art_bridge::CompioRuntime = abs_art_bridge::CompioRuntime::with_runtime(rt);
    common::assert_runtime_is_(&value, abs_art_bridge::RuntimeTag::Compio);
    let scope = value.local_scope();
    let clock = ManualClock::new();
    let timed = ManualTime::new(value, clock.clone());
    scope.block_on_advancing(
        &clock,
        keepalive_common::establish_timeout_on_initiator_(&timed),
    );
}

/// 测试目标、手段、判断见 [`keepalive_common::establish_timeout_on_responder_`]。
#[test]
fn establish_timeout_on_responder_compio_() {
    let rt = compio::runtime::Runtime::new().expect("建 compio 运行时应当成功");
    let value: abs_art_bridge::CompioRuntime = abs_art_bridge::CompioRuntime::with_runtime(rt);
    common::assert_runtime_is_(&value, abs_art_bridge::RuntimeTag::Compio);
    let scope = value.local_scope();
    let clock = ManualClock::new();
    let timed = ManualTime::new(value, clock.clone());
    scope.block_on_advancing(
        &clock,
        keepalive_common::establish_timeout_on_responder_(&timed),
    );
}

/// 测试目标、手段、判断见 [`keepalive_common::cancel_accept_notifies_peer_`]。
#[test]
fn cancel_accept_notifies_peer_compio_() {
    let rt = compio::runtime::Runtime::new().expect("建 compio 运行时应当成功");
    let value: abs_art_bridge::CompioRuntime = abs_art_bridge::CompioRuntime::with_runtime(rt);
    common::assert_runtime_is_(&value, abs_art_bridge::RuntimeTag::Compio);
    let scope = value.local_scope();
    let clock = ManualClock::new();
    let timed = ManualTime::new(value, clock.clone());
    scope.block_on_advancing(
        &clock,
        keepalive_common::cancel_accept_notifies_peer_(&timed),
    );
}
