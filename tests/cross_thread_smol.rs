//! 跨线程验收的 **smol** 壳：只做「建立运行时值 + 作用域 + 给 worker 线程一个上下文」。
//!
//! 场景本体在 `tests/common/scenarios_/cross_thread_.rs`，三个壳共用同一份实现。
//!
//! # 与前两个壳的差别
//!
//! - smol **没有隐式运行时上下文**：本地队列在本线程的 `thread_local!` 里，必须由调用方
//!   驱动。因此这里是普通 `#[test]`，整段场景挂在 `smol::block_on(scope.run_until(..))`
//!   上：`block_on` 是外部阻塞驱动源，`run_until` 在其中顺带推进本线程的本地队列。
//! - worker 线程：smol 的运行时值是**零大小标记**（`CurrentConnCfg` 的取用前提在 smol 上
//!   没有否定答案），但 `worker_body_` 里的 future 仍需要一个执行体去 poll，
//!   所以 worker 用 `smol::block_on` 驱动自己的那一段。
//!
//! # 怎么跑
//!
//! ```bash
//! cargo test --test cross_thread_smol --no-default-features --features test-smol-runtime
//! ```
//!
//! 必须 `--no-default-features`：缺省的 `test-compio-runtime` 若同时打开，smux 自己的
//! `DefaultRt_` 就没有唯一解（它是 `test-*-runtime` 三选一）；bridge 的缺省后端不受影响。

#![cfg(feature = "test-smol-runtime")]
#![feature(allocator_ext)]

use abs_art::TrLocalScope;

#[path = "common/mod.rs"]
mod common;

/// 测试目标：**建连线程与句柄操作线程分离**时，跨线程的句柄仍能正常 bind 并开大量
/// 子流完成双向通信（smol 装配）。
///
/// - 手段：主线程用 `CurrentConnCfg` 在两条内存被动环之间完成握手建连；随后起一条 worker
///   线程，该线程用 `smol::block_on` 驱动 [`common::worker_body_`]——在其中并发
///   `bind_async`（每条子流一个独有临时 dock）并向主线程监听的 dock 发起
///   `K_CROSS_THREAD_CHANNELS` 条 `open_channel_async`；主线程 meanwhile 在
///   `smol::block_on(scope.run_until(..))` 里驱动五个循环并收齐同样多条入向子流，两侧各自
///   写入载荷、校验对端载荷、半关闭并等 EOF。
/// - 判断：全部 `K_CROSS_THREAD_CHANNELS` 条 open / accept 成功；每条子流的载荷逐字节相等（校验靠载荷 tag 复算，
///   不依赖 open / accept 的配对顺序）；半关闭后双方读到 EOF。任一侧任一条失败，或整个
///   场景挂住，即测试失败。
#[test]
fn cross_thread_bind_and_channels_smol_() {
    // 走后端 crate 的自由函数 `current()`：返回 `Runtime<FULL>`，避免 `Runtime::current()`
    // 的 `CAPS` 推断问题（同 `inmem_mux.rs`）。
    let rt = abs_art_smol::current();
    common::assert_runtime_is_(&rt, abs_art_bridge::RuntimeTag::Smol);
    let scope = rt.local_scope();

    smol::block_on(common::with_watchdog_(
        &rt,
        scope.run_until(common::run_cross_thread_scenario_(|conn| {
            // worker 线程自己驱动那一段：smol 无上下文前提，但仍需一个执行体。
            smol::block_on(common::worker_body_(conn, common::K_CROSS_THREAD_CHANNELS))
        })),
    ));
}
