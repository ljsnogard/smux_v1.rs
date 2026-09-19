//! tokio（多线程 flavor）运行时下的 smux v1 集成冒烟测试。
//!
//! 场景本体在 `tests/common/mod.rs`，与 compio 版逐字相同；本文件只负责运行时
//! 特化的接线：建立 tokio UNIX socket 对，把两个半边经 `abs_buff_tokio_adapt`
//! 的设备级适配交给 [`common::run_socket_scenario_`] 的四条调用方驱动泵。

mod common;

use abs_buff::io::{TrInput, TrOutput};
use abs_buff_tokio_adapt::{ReadAsInput, WriteAsOutput};
use tokio::net::UnixStream;
use tokio::net::unix::{OwnedReadHalf, OwnedWriteHalf};

/// 编译期断言：tokio socket 的两个半边经设备级适配后，分别满足
/// [`common::run_socket_scenario_`] 对输入 / 输出设备的契约
/// （`TrInput<u8>` / `TrOutput<u8>`）。函数体刻意不执行，只为让这条接线在类型
/// 层面被编译检查。
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

/// 测试目标：tokio 多线程运行时下，smux v1 的「握手 → 拆连接 → 16 dock × 1024
/// 子流收发」整条链路的冒烟级端到端行为，且传输是真实的 UNIX domain socket。
///
/// - 手段：`tokio::net::UnixStream::pair()` 建立**一条**全双工 UNIX domain
///   socket 连接——`pair()` 返回的是这**同一条连接的两个端点**（socketpair），
///   不是两条连接；每端各自 `into_split()` 得到自己的读 / 写半边，再分别包成
///   `ReadAsInput` / `WriteAsOutput`（设备级适配），交给
///   [`common::run_socket_scenario_`]；后者用四条**调用方驱动**的
///   async 泵把「socket ↔ 全被动环」串起来（避开 buffex 主动端「提交时单次非阻塞
///   poll」与 `TrBuffWrite` 无 flush 钩子两个坑，见 `tests/common/mod.rs`），再跑
///   与运行时无关的 [`common::run_smoke_scenario_`]；四个泵与场景用 `futures::select`
///   在同一任务内并发推进（不 spawn，故不要求 `Send`）。
/// - 判断：两侧各 16 个 dock 上的 1024 次 `open_channel_async` 与 1024 次
///   `accept_async` 全部返回成功，且每条子流收到的载荷与按 `(dock, index)` 生成
///   的期望载荷逐字节相等；任何一次失败都会让场景 panic 从而测试失败。
///
/// 说明：连接层当前是 `todo!()` 骨架，因此本测试被标记为 `#[ignore]`，默认
/// `cargo test` 不会执行；实现落地后移除该属性。`cargo test -- --ignored`
/// 会在 `MuxConnection::split` 处以 `not yet implemented` panic——这正说明测试
/// 已经编译、并成功跑过了运行时接线与握手。
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "连接层尚未实现（todo!），实现落地后移除本属性"]
async fn mux_smoke_tokio_multi_thread_() {
    let (stream_a, stream_b) = UnixStream::pair().expect("建立 tokio UNIX socket 对应当成功");
    let (mut a_read, mut a_write) = stream_a.into_split();
    let (mut b_read, mut b_write) = stream_b.into_split();

    common::run_socket_scenario_(
        ReadAsInput::new(&mut a_read),
        WriteAsOutput::new(&mut a_write),
        ReadAsInput::new(&mut b_read),
        WriteAsOutput::new(&mut b_write),
    )
    .await;
}
