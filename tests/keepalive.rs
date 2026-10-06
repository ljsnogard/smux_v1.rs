//! 保活与空闲超时的 **tokio** 薄壳：只做「建运行时 + 取运行时值 / 作用域 + 驱动虚拟时间」。
//!
//! 场景本体与设计说明在 `tests/keepalive_common.inc`；compio 侧见 `keepalive_compio.rs`。
//!
//! # 作用域怎么来（不需要 tokio 上下文）
//!
//! `abs_art` 的 `LocalScope` **不依赖任何 thread_local**：`Runtime::local_scope()` 就是
//! `LocalScope::with_handle(self.handle_.clone())`，每次新建一条 `LocalSet`。因此拿到
//! 任意运行时把手后，用 `Runtime::with_handle(..)` 造一个运行时值、再由它交出作用域
//! 即可——不必先进 tokio 上下文。
//!
//! # 时间怎么变虚的
//!
//! 用 `ManualTime` 把运行时值装饰成「时间由手动时钟提供」，再用
//! `LocalScope::block_on_advancing(&clock, body)` 驱动：它内部的 `Supervisor` 在没有别的
//! 活可干时把虚拟时钟推进到下一个到期时刻并驱动本地队列。于是「等 1.6 秒 / 2.5 秒」
//! 在真实时间里几乎瞬间完成，而保活与超时的**事件顺序**与真实时间下相同。
//!
//! # 为什么这里是同步 `#[test]` 而不是 `#[tokio::test]`
//!
//! `block_on_advancing` 在「已处于 tokio 上下文内」时走 `block_in_place` 分支；本仓实测
//! 那条分支与 `LocalSet` 的驱动组合在一起会让场景挂起（子流建立不起来）。改成与
//! `abs_art-tokio` 自己的 `mock_clock_tests_` 相同的方式：普通 `#[test]` 自建一个多线程
//! tokio 运行时，在**上下文之外**调 `block_on_advancing`（此时它直接
//! `Handle::block_on` + `LocalSet::run_until`）。

#![cfg(feature = "test-tokio-runtime")]

#[path = "common/mod.rs"]
mod common;

#[path = "keepalive_common.inc"]
mod keepalive_common;

use abs_art_mock_clock::{ManualClock, ManualTime};

/// 建一个多线程 tokio 运行时（`LocalSet::run_until` + `Handle::block_on` 需要它）。
fn tokio_rt_() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .expect("建 tokio 运行时应当成功")
}

/// 测试目标、手段、判断见 [`keepalive_common::idle_channel_times_out_`]。
///
/// 手段补充：运行时值由 `Runtime::with_handle` 构造（不进上下文），再用 `ManualTime`
/// 装饰成虚拟时间；场景由 `scope.block_on_advancing` 在上下文之外驱动。
#[test]
fn idle_channel_times_out_tokio_() {
    let rt = tokio_rt_();
    let value: abs_art_bridge::TokioRuntime =
        abs_art_bridge::TokioRuntime::with_handle(rt.handle().clone());
    let scope = value.local_scope();
    let clock = ManualClock::new();
    let timed = ManualTime::new(value, clock.clone());
    scope.block_on_advancing(
        &clock,
        keepalive_common::idle_channel_times_out_(&timed, &scope),
    );
}

/// 测试目标、手段、判断见 [`keepalive_common::keepalive_pulses_`]。
///
/// 手段补充：同 [`idle_channel_times_out_tokio_`]，仍走虚拟时间。
#[test]
fn keepalive_pulses_tokio_() {
    let rt = tokio_rt_();
    let value: abs_art_bridge::TokioRuntime =
        abs_art_bridge::TokioRuntime::with_handle(rt.handle().clone());
    let scope = value.local_scope();
    let clock = ManualClock::new();
    let timed = ManualTime::new(value, clock.clone());
    scope.block_on_advancing(
        &clock,
        keepalive_common::keepalive_pulses_(&timed, &scope),
    );
}
