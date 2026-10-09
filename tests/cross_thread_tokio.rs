//! 跨线程验收的 **tokio** 壳：只做「建立运行时值 + 作用域 + 给 worker 线程一个上下文」。
//!
//! 场景本体在 `tests/common/scenarios_/cross_thread_.rs`，三个壳共用同一份实现。
//!
//! # 两个线程各自需要什么
//!
//! - **主线程**（本测试线程）：建连、把五个循环经 `spawn_local` 投递到**本线程**的本地
//!   队列、并驱动它们（`scope.run_until(..)` 包住整段场景）。它必须处于 tokio 上下文内——
//!   `#[tokio::test]` 满足。
//! - **worker 线程**：拿连接的**克隆**做句柄操作（`bind_async` / `open_channel_async` /
//!   收发）。连接用的是 `CurrentConnCfg`（**不存储**运行时值），于是每次取「现在几点」
//!   都要求调用点处于所选后端的上下文内——worker 自建一个 `current_thread` 运行时并
//!   `block_on` 整段操作，正是为了满足这条**调用者责任**（见 `TrRtCurrent` 的文档）。

#![cfg(feature = "test-tokio-runtime")]
#![feature(allocator_ext)]

use abs_art::TrLocalScope;

#[path = "common/mod.rs"]
mod common;

/// 测试目标：**建连线程与句柄操作线程分离**时，跨线程的句柄仍能正常 bind 并开大量
/// 子流完成双向通信（tokio 装配）。
///
/// - 手段：主线程用 `CurrentConnCfg`（不存储运行时值，故连接 `Send + Sync`）在两条内存
///   被动环之间完成握手建连，并让 `scope.run_until(..)` 驱动本线程的五个循环；随后起一条
///   worker 线程，该线程**自建一个 tokio 运行时**并 `block_on` [`common::worker_body_`]
///   ——在其中并发 `bind_async`（每条子流一个独有临时 dock）并向主线程监听的 dock 发起
///   `K_CROSS_THREAD_CHANNELS` 条 `open_channel_async`；主线程同时收齐同样多条入向子流，
///   两侧各自写入载荷、校验对端载荷、半关闭并等 EOF。
/// - 判断：全部 `K_CROSS_THREAD_CHANNELS` 条 open / accept 成功；每条子流的载荷逐字节相等（校验靠载荷 tag 复算，
///   不依赖 open / accept 的配对顺序）；半关闭后双方读到 EOF。任一侧任一条失败，或整个
///   场景挂住，即测试失败。
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cross_thread_bind_and_channels_tokio_() {
    let rt = abs_art_bridge::current();
    common::assert_runtime_is_(&rt, abs_art_bridge::RuntimeTag::Tokio);
    let scope = rt.local_scope();

    common::with_watchdog_(
        &rt,
        scope.run_until(common::run_cross_thread_scenario_(|conn| {
            // worker 线程**自己的** tokio 上下文：`CurrentConnCfg` 的取用前提在这里被满足。
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("worker 线程的 tokio 运行时应当创建成功");
            rt.block_on(common::worker_body_(conn, common::K_CROSS_THREAD_CHANNELS))
        })),
    )
    .await;
}
