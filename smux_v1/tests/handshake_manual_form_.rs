//! 回归：**握手路径自己会把帧推出去**，因此"按需形态"的传输也能建连。
//!
//! # 这条测试钉住什么
//!
//! 旧版的静默挂起是这样的：调用方用 `BuffWrite::try_new`（按需驱动，背后没有自启动的
//! 泵）装好传输，握手把 `INVITE` 提交进环之后就 park 在自己的读上等 `ACCEPT`——而
//! "环 → 设备"那一跳**没有任何人负责**，于是 `INVITE` 永久留在环里，两端互等。
//! 症状是**无错误、无超时、永久挂起**。
//!
//! 现在 [`HandshakeAgent`] 在每一帧写出后都会 `flush_async`（见
//! `handshake::agent::flush_frame_async_`），因此这里刻意**不装配任何排空泵**：
//!
//! ```text
//! 写半边 = BuffWrite::try_new（按需驱动，无人推） ──> 真实 UDS socket
//! ```
//!
//! 若哪天有人把那次 flush 删掉，本测试会**挂住**（外层有 10 秒看门狗把它变成失败）。

#![cfg(feature = "test-tokio-runtime")]

use std::time::Duration;

use buffex_tokio_adapt::{BuffRead, BuffWrite, DefaultAllocConfig};
use smux_v1::{
    handshake::{
        agent::{AcceptAllEntries, HandshakeAgent},
        opts::BasicOpts,
    },
    x_deps::abs_art::TrLocalScope,
};
use tokio::net::UnixStream;

/// 把一条 tokio UNIX socket 拆成"按需驱动形态"的两条适配器半边。
///
/// 关键在于**写侧用 `try_new`**：它不 spawn 任何后台泵，"环 → 设备"只能由握手自己
/// 的 flush 推进——这正是被测的性质。
fn make_manual_halves_(
    stream: UnixStream,
    ring_cap: usize,
) -> (
    BuffWrite<tokio::net::unix::OwnedWriteHalf, DefaultAllocConfig>,
    BuffRead<tokio::net::unix::OwnedReadHalf, DefaultAllocConfig>,
) {
    let (rd, wr) = stream.into_split();
    let rx = BuffRead::<_, DefaultAllocConfig>::try_new(rd, ring_cap, DefaultAllocConfig)
        .expect("读环容量合法");
    let tx = BuffWrite::<_, DefaultAllocConfig>::try_new(wr, ring_cap, DefaultAllocConfig)
        .expect("写环容量合法");
    (tx, rx)
}

/// 测试：**按需形态**的两条半边（没有任何排空泵）能完成一次完整握手。
/// - 测试目标：握手自己负责把每一帧 flush 出去，调用方不必为此装配自驱动泵。
/// - 测试手段：`UnixStream::pair` 建一对真实 socket，两侧都用 `try_new`（按需驱动）
///   装环；在同一个本地作用域里用 `futures::join!` 并发推进主动方与被动方，外层套
///   10 秒看门狗。
/// - 判断：双方都返回 `Ok`，且交付物的 `max_packet_size` 等于双方声明的缺省值；
///   旧版行为（不 flush）会让本用例在看门狗处失败。
#[test]
fn manual_form_handshakes_without_any_pump_() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("建 tokio 运行时");

    rt.block_on(async {
        // 运行时值与作用域都取"当前装配的默认后端"（本 target 由 feature 固定为 tokio）。
        let scope = smux_v1::x_deps::abs_art_facade::default_rt_().local_scope();

        let (a, b) = UnixStream::pair().expect("建立 UNIX socket 对");
        let (tx_a, rx_a) = make_manual_halves_(a, 1024usize);
        let (tx_b, rx_b) = make_manual_halves_(b, 1024usize);

        let listener = HandshakeAgent::new(tx_a, rx_a);
        let inviter = HandshakeAgent::new(tx_b, rx_b);
        let local_opts = BasicOpts::default();
        let proposed = BasicOpts::default();

        let pair = scope.run_until(async move {
            futures::join!(
                async move { listener.listen_async(&local_opts, AcceptAllEntries).await },
                async move { inviter.invite_async(&proposed, AcceptAllEntries).await },
            )
        });

        let (accepted, invited) = tokio::time::timeout(Duration::from_secs(10), pair)
            .await
            .expect("握手必须在看门狗内完成——超时即说明握手帧没被推出去");

        let accepted = accepted.expect("等待方握手应当成功");
        let invited = invited.expect("发起方握手应当成功");
        assert_eq!(
            accepted.opts.basic_opts.max_packet_size,
            invited.opts.basic_opts.max_packet_size,
            "双方协商出的规格应当一致"
        );
    });
}
