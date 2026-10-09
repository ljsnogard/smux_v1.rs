//! 跨线程验收的 **compio** 壳：只做「建立运行时值 + 作用域 + 给 worker 线程一个上下文」。
//!
//! 场景本体在 `tests/common/scenarios_/cross_thread_.rs`，三个壳共用同一份实现。
//!
//! # 本壳是这套验收里最关键的一格
//!
//! compio 的运行时值是 `!Send + !Sync`（线程本地执行器）。**改造前**它被存进
//! `DefaultConnCfg`，于是 `MuxConnection` 也是 `!Send + !Sync`，连接句柄根本过不了线程
//! ——「另一条线程 bind」这套用法在本装配下无法表达。改用 `CurrentConnCfg`（不存储
//! 运行时值）之后连接是 `Send + Sync`，本用例才成立。
//!
//! # 两个线程各自需要什么
//!
//! - **主线程**：建连 + `scope.run_until(..)` 驱动五个循环；`#[compio::test]` 提供上下文。
//! - **worker 线程**：拿连接克隆做句柄操作；compio 的运行时**绑定创建它的线程**，因此该
//!   线程必须自建一个 compio 运行时并 `block_on`——这正是 `CurrentConnCfg` 契约里的
//!   调用者责任。做不到这一点时 debug 构建会先给出 smux 自己的断言提示。
//!
//! 跑本 target 时必须显式关掉 `test-tokio-runtime`（smux 的 `DefaultRt_` 是
//! `test-*-runtime` 三选一）：
//!
//! ```bash
//! cargo test --test cross_thread_compio --no-default-features --features test-compio-runtime
//! ```

#![cfg(not(feature = "test-tokio-runtime"))]
#![feature(allocator_ext)]

use abs_art::{TrAsyncRuntime, TrLocalScope};

#[path = "common/mod.rs"]
mod common;

/// 测试目标：**建连线程与句柄操作线程分离**时，跨线程的句柄仍能正常 bind 并开大量
/// 子流完成双向通信（compio 装配）——即「句柄可以走、reactor 不走」在 compio 上成立。
///
/// - 手段：主线程用 `CurrentConnCfg` 在两条内存被动环之间完成握手建连，并让
///   `scope.run_until(..)` 驱动本线程的五个循环；随后起一条 worker 线程，该线程**自建一个
///   compio 运行时**并 `block_on` [`common::worker_body_`]——在其中并发 `bind_async`
///   （每条子流一个独有临时 dock）并向主线程监听的 dock 发起 `K_CROSS_THREAD_CHANNELS`
///   条 `open_channel_async`；主线程同时收齐同样多条入向子流，两侧各自写入载荷、校验对端
///   载荷、半关闭并等 EOF。
/// - 判断：全部 `K_CROSS_THREAD_CHANNELS` 条 open / accept 成功；每条子流的载荷逐字节相等（校验靠载荷 tag 复算，
///   不依赖 open / accept 的配对顺序）；半关闭后双方读到 EOF。任一侧任一条失败，或整个
///   场景挂住，即测试失败。
#[compio::test]
async fn cross_thread_bind_and_channels_compio_() {
    const FULL: usize = <abs_art_bridge::CompioRuntime as TrAsyncRuntime>::FULL_CAP;
    let rt = abs_art_bridge::CompioRuntime::<FULL>::current();
    common::assert_runtime_is_(&rt, abs_art_bridge::RuntimeTag::Compio);
    let scope = rt.local_scope();

    common::with_watchdog_(
        &rt,
        scope.run_until(common::run_cross_thread_scenario_(|conn| {
            // worker 线程**自己的** compio 运行时：线程本地运行时不能借用主线程那一份。
            let rt = compio::runtime::Runtime::new()
                .expect("worker 线程的 compio 运行时应当创建成功");
            rt.block_on(common::worker_body_(conn, common::K_CROSS_THREAD_CHANNELS))
        })),
    )
    .await;
}
