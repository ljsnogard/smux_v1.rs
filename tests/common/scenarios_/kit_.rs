//! 场景公共件：多个场景共用的「驱动一端」「单条子流收发校验 + 半关闭」以及\n//! 「建连接 → 跑一个场景」的骨架。它们不构成对外接口，只在 `scenarios_` 内部复用。

use buffex::x_deps::abs_buff::{TrBuffRead, TrBuffWrite};
use abs_smux::{
    chan::TrChannelHalf,
    conf::TrMuxConfig,
    conn::{TrChannelListener, TrConnection, TrDockBinding},
};
use smux_v1::connection::{Dock, MuxConnection, TrConnCfg};

use crate::common::{
    AcceptAsyncClosureExt,
    SmokeBuff,
    SmokeMuxConfig,
    TrSmokeRt,
    TrSmokeScope,
    connect_pair_,
    expect_eof_,
    make_channel_buff_,
    make_payload_,
    read_channel_exact_,
    write_channel_all_,
};

/// 与 [`run_small_mux_scenario_`] 相同，但 dock 数量与每个 dock 的子流数量可调。
///
/// 这是全部场景的唯一实现：握手 → 建两个 [`MuxConnection`]（内部各自 spawn 读 / 写
/// 循环）→ 两端并发跑「`dock_count` 个 dock × 每个 `per_dock` 条子流」的双向收发与
/// 半关闭（[`drive_side_`]）。1024 条的冒烟场景只是它的 `16 × 64` 特例。
///
/// `rt` 是**运行时值**（进连接的类型参数），`scope` 是**本地作用域**（投递五个循环）。
pub(super) async fn run_mux_scenario_<RA, WA, RB, WB, S, RT>(
    rt: &RT,
    scope: &S,
    tx_a: WA,
    rx_a: RA,
    tx_b: WB,
    rx_b: RB,
    dock_count: u32,
    per_dock: usize,
) where
    RA: TrBuffRead<u8> + 'static,
    WA: TrBuffWrite<u8> + 'static,
    RB: TrBuffRead<u8> + 'static,
    WB: TrBuffWrite<u8> + 'static,
    S: TrSmokeScope + Clone + 'static,
    RT: TrSmokeRt + smux_v1::connection::ScopeHost,
{
    let (conn_a, conn_b) =
        connect_pair_::<
            SmokeMuxConfig<WA, RA, RT>,
            SmokeMuxConfig<WB, RB, RT>,
            RA,
            WA,
            RB,
            WB,
            S,
        >(rt, scope, tx_a, rx_a, tx_b, rx_b)
        .await;

    futures::join!(
        drive_side_(&conn_a, 0u32, dock_count, per_dock),
        drive_side_(&conn_b, 1u32, dock_count, per_dock),
    );
}


/// 为一端（`side` = 0/1）跑完场景：在 `1..=dock_count` 上监听，同时向对端的
/// 同名 dock 发起 `dock_count × per_dock` 条子流（每条用不同的临时 local_dock）。
///
/// 发起侧的临时 dock 取值 `0x1000 + side·0x100_0000 + dock·0x100 + index`：
///
/// - 与监听 dock（`1..=dock_count`）天然不重叠；
/// - 同侧不同 `(dock, index)` 互不相同（`per_dock ≤ 0x100` 时不会串到下一个 dock），
///   满足 §4.1「每条并发子流一个互不相同的 local_dock」；
/// - `side` 抬高一整段，保证两侧的临时 dock 区间不重叠。
///
/// # Panics
///
/// 任何一次 open / accept / 读写 / 半关闭校验失败都会 panic——失败即测试失败。
pub(super) async fn drive_side_<C>(
    conn: &MuxConnection<C>,
    side: u32,
    dock_count: u32,
    per_dock: usize,
) where
    C: TrConnCfg + TrMuxConfig<Buff = SmokeBuff>,
{
    let conn_ref = conn;

    let mut accept_tasks = Vec::new();
    for dock in 1..=dock_count {
        accept_tasks.push(async move {
            let mut binding = conn_ref
                .bind_async(Dock::new(dock))
                .await
                .expect("绑定监听 dock 应当成功");
            let mut listener = binding
                .listen_async()
                .await
                .expect("在本地 dock 上建立 listener 应当成功");
            for index in 0..per_dock {
                let mut handle = listener
                    .income_async()
                    .await
                    .expect("应当取到下一条入向建流请求");
                assert_eq!(
                    handle.local_dock(),
                    Dock::new(dock),
                    "被动方的 local_dock 应当是监听 dock（镜像语义）"
                );
                let mut welcome_buf: [u8; 0] = [];
                let mut welcome: &mut [u8] = &mut welcome_buf[..];
                let (tx, mut rx) = handle
                    .accept_async_closure(&mut welcome, || {
                        (make_channel_buff_(), make_channel_buff_())
                    })
                    .await
                    .expect("accept 入向子流应当成功");
                exchange_and_half_close_(tx, &mut rx, dock, index).await;
            }
        });
    }

    let mut open_tasks = Vec::new();
    for dock in 1..=dock_count {
        for index in 0..per_dock {
            // 每条并发子流一个**互不相同**的临时 local_dock（Q2 裁决）。
            let local = Dock::new(
                0x1000u32 + side * 0x100_0000u32 + dock * 0x100u32 + index as u32,
            );
            open_tasks.push(async move {
                let mut binding = conn_ref
                    .bind_async(local)
                    .await
                    .expect("绑定发起 dock 应当成功");
                let mut message: &[u8] = &[];
                // `open_channel_async` 现在只交出**半建立**句柄：本端 `OPEN` 要
                // 等最终裁决（`accept_async`）时拿到缓冲后才发。
                let mut handle = binding
                    .open_channel_async(Dock::new(dock), &mut message)
                    .await
                    .expect("向对端 dock 发起子流应当成功");
                let mut welcome_buf: [u8; 0] = [];
                let mut welcome: &mut [u8] = &mut welcome_buf[..];
                let (tx, mut rx) = handle
                    .accept_async_closure(&mut welcome, || {
                        (make_channel_buff_(), make_channel_buff_())
                    })
                    .await
                    .expect("发起方最终裁决子流应当成功");
                exchange_and_half_close_(tx, &mut rx, dock, index).await;
            });
        }
    }

    futures::join!(
        futures::future::join_all(open_tasks),
        futures::future::join_all(accept_tasks),
    );
}


/// 一条子流上的完整交互：写本端载荷 → 读对端载荷并校验 → 丢弃发送半边（半关闭）
/// → 在接收半边等到 EOF。
///
/// 载荷校验**不依赖 open / accept 的配对顺序**：先读 4 字节 tag，tag 里编码了发送方
/// 的 `(dock, index)`，据此推出对方的完整载荷再逐字节比对。这样即使两侧的临时
/// dock 分配与 accept 顺序不同，校验依然成立。
pub(super) async fn exchange_and_half_close_<T, R>(tx: T, rx: &mut R, dock: u32, index: usize)
where
    T: TrBuffWrite<u8>,
    R: TrBuffRead<u8>,
{
    let payload = make_payload_(dock, index);
    let mut tx = tx;
    write_channel_all_(&mut tx, &payload)
        .await
        .expect("子流写入本端载荷应当成功");

    let mut tag = [0u8; 4];
    read_channel_exact_(rx, &mut tag)
        .await
        .expect("子流读取对端载荷 tag 应当成功");
    let raw = u32::from_be_bytes(tag);
    let expected = make_payload_(raw >> 16, (raw & 0xFFFF) as usize);
    assert_eq!(tag, expected[..4], "对端载荷 tag 应当自洽");

    let mut rest = vec![0u8; expected.len() - 4];
    read_channel_exact_(rx, &mut rest)
        .await
        .expect("子流读取对端载荷正文应当成功");
    let got = [tag.as_slice(), rest.as_slice()].concat();
    assert_eq!(got, expected, "对端载荷应逐字节相等");

    // 半关闭：丢弃发送半边（= 发 FIN），对端应当在读尽后看到 EOF。
    drop(tx);
    expect_eof_(rx).await;
}
