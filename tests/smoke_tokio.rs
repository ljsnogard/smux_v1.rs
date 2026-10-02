//! tokio（多线程 flavor）运行时下的 smux v1 集成冒烟测试。
//!
//! 场景本体在 `tests/common/mod.rs`，与 compio 版逐字相同；本文件只负责运行时
//! 特化的接线：建立 tokio UNIX socket 对，把两个半边经 `abs_buff_tokio_adapt`
//! 的设备级适配（经 `buffex_tokio_adapt::x_deps` 重导出）交给 `common` 的
//! **调用方驱动泵**。
//!
//! 本文件有两个用例：
//!
//! - [`mux_smoke_tokio_multi_thread_`]：`16 dock × 64 条 = 1024` 条子流的整链路
//!   冒烟（真 UDS、握手、建连接、双向收发、半关闭）；
//! - [`mux_small_socket_tokio_`]：同一条链路的小场景（`2 dock × 2 条 = 4` 条），
//!   作为快速传输回归——它覆盖传输装配，秒级返回。
//!
//! 两者都只跑**缺省（单线程）feature 配置**：`multi-thread` 配置下连接驱动尚未
//! 实现（`MuxConnection::new` 仍是 `todo!()`，见 `src/connection/channel_.rs` 的
//! `#[cfg(feature = "multi-thread")]` 实现），因此用 `#[cfg]` 跳过而不是失败。

mod common;

use buffex::x_deps::abs_buff::io::{TrInput, TrOutput};
use buffex_tokio_adapt::x_deps::abs_buff_tokio_adapt::{ReadAsInput, WriteAsOutput};
#[cfg(not(feature = "multi-thread"))]
use tokio::net::UnixStream;
use tokio::net::unix::{OwnedReadHalf, OwnedWriteHalf};

/// 连接层使用的运行时标记类型。
///
/// 同时声明 `SPAWN_SEND` 与 `SPAWN_LOCAL` 两种能力：缺省（单线程）配置下
/// `MuxConnection::new` 要求 `Rt: TrSpawnLocal`，开启 `multi-thread` 后要求
/// `Rt: TrSpawnSend`；两种都声明，同一份测试才能在两种 feature 配置下都通过
/// 编译检查。
#[cfg(not(feature = "multi-thread"))]
type SmokeRt =
    abs_art_tokio::Runtime<{ abs_art_tokio::SPAWN_SEND | abs_art_tokio::SPAWN_LOCAL }>;

/// 编译期断言：tokio socket 的两个半边经设备级适配后，分别满足 `common` 对输入 /
/// 输出设备的契约（`TrInput<u8>` / `TrOutput<u8>`）。函数体刻意不执行，只为让这条
/// 接线在类型层面被编译检查。
#[allow(dead_code)]
fn assert_socket_adapters_fit_(
    read: &mut OwnedReadHalf,
    write: &mut OwnedWriteHalf,
) {
    fn assert_input_<I: TrInput<u8>>(_: I) {}
    fn assert_output_<O: TrOutput<u8>>(_: O) {}

    assert_input_(ReadAsInput::new(read));
    assert_output_(WriteAsOutput::new(write));
}

/// 测试目标：tokio 运行时下，smux v1 的「握手 → 拆连接 → 16 dock × 1024 子流收发」
/// 整条链路的冒烟级端到端行为，且传输是真实的 UNIX domain socket。
///
/// - 手段：`tokio::net::UnixStream::pair()` 建立**一条**全双工 UNIX domain
///   socket 连接——`pair()` 返回的是这**同一条连接的两个端点**（socketpair），
///   不是两条连接；每端各自 `into_split()` 得到自己的读 / 写半边，再分别包成
///   `ReadAsInput` / `WriteAsOutput`（设备级适配），交给
///   [`common::run_socket_scenario_`]；后者用四条**调用方驱动**的 async 泵把
///   「socket ↔ 全被动环」串起来（避开 buffex 主动端「提交时单次非阻塞 poll」
///   与 `TrBuffWrite` 无 flush 钩子两个坑，见 `tests/common/mod.rs`），再跑与
///   运行时无关的 [`common::run_smoke_scenario_`]；四个泵与场景用
///   `futures::select` 在同一任务内并发推进（不 spawn，故不要求 `Send`）。连接
///   内部的两个循环经 `Rt::spawn_local` 投递，因此必须跑在 `LocalSet` 里。
/// - 判断：两侧各 16 个 dock 上的 1024 次 `open_channel_async` 与 1024 次
///   `accept_async` 全部返回成功；每条子流按 `(dock, index)` 自洽校验载荷（不依赖
///   open / accept 的配对顺序），丢弃发送半边后对端读到 `Closing`（EOF）。任一
///   不满足即 panic，测试失败。
#[cfg(not(feature = "multi-thread"))]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mux_smoke_tokio_multi_thread_() {
    let (stream_a, stream_b) = UnixStream::pair().expect("建立 tokio UNIX socket 对应当成功");
    let (mut a_read, mut a_write) = stream_a.into_split();
    let (mut b_read, mut b_write) = stream_b.into_split();

    // 缺省（单线程）配置下连接内部用 `Rt::spawn_local`，tokio 侧必须在
    // `LocalSet` 里跑（compio 的运行时本身就是线程本地的，无需这一步）。
    let local = tokio::task::LocalSet::new();
    local
        .run_until(common::run_socket_scenario_::<_, _, _, _, SmokeRt>(
            ReadAsInput::new(&mut a_read),
            WriteAsOutput::new(&mut a_write),
            ReadAsInput::new(&mut b_read),
            WriteAsOutput::new(&mut b_write),
        ))
        .await;
}

/// 测试目标：与 [`mux_smoke_tokio_multi_thread_`] 同一条链路，但只跑
/// `2 dock × 2 条 = 4` 条子流的小场景，作为**快速传输回归**。
///
/// - 手段：同样用 `UnixStream::pair()` + `into_split()` + 设备级适配 + 调用方驱动
///   泵，只是把场景换成 [`common::run_small_socket_scenario_`]。
/// - 判断：4 条子流的 open / accept / 双向收发 / 半关闭全部成功；失败即 panic。
#[cfg(not(feature = "multi-thread"))]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mux_small_socket_tokio_() {
    let (stream_a, stream_b) = UnixStream::pair().expect("建立 tokio UNIX socket 对应当成功");
    let (mut a_read, mut a_write) = stream_a.into_split();
    let (mut b_read, mut b_write) = stream_b.into_split();

    let local = tokio::task::LocalSet::new();
    local
        .run_until(common::run_small_socket_scenario_::<_, _, _, _, SmokeRt>(
            ReadAsInput::new(&mut a_read),
            WriteAsOutput::new(&mut a_write),
            ReadAsInput::new(&mut b_read),
            WriteAsOutput::new(&mut b_write),
        ))
        .await;
}
