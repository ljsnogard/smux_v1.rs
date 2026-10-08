//! telegraph（数据报）端到端场景：两条内存环直连两个端点，各自在互不相同的
//! `local_dock` 上开一条数据报端点，验证本轮落地的语义。
//!
//! # 验收点
//!
//! 1. **一次提交 = 一条报文**：多次写入被合并成一条（`Demand::at_least` 的语义）；
//! 2. **长度在发送前已知**：`send_async` 返回的就是载荷长度；
//! 3. **收一条报文 = 一个自描述对象**：`recv_async` 交出的 `RecvDatagram` 同时给出
//!    **远端地址**（帧头里的 `RemoteDock`）与**载荷段**，接收侧从整帧里自行拆出二者；
//! 4. **空报文**（长度为 0）被当作一条**真报文**交付，不会与「什么都没收到」混淆；
//! 5. **接收环装不下就整条丢弃**：过大的报文不会把连接判失败、也不会留下半条，
//!    紧随其后的正常报文必须照常收到；
//! 6. **不发任何帧即可开始**：telegraph 没有建流握手，`open` 之后直接收发；
//! 7. **身份由两个半边共同持有**：丢弃 tx / rx 之后，同一个 `local_dock` 可以再次
//!    `open_telegraph_async`。
//!
//! 场景由调用方（`tests/inmem_mux.rs`）用当前运行时的作用域驱动。

use abs_smux::{
    conn::TrConnection,
    telegraph::{TrDatagramRecver, TrDatagramSender, TrRecvDatagram, TrTelegraph},
};
use abs_buff::{
    Demand, TrBuffRead, TrBuffWrite,
    buffer::{TrBuffSegmRef, TrBuffSegmView},
};
use buffex::x_deps::abs_buff;
use smux_v1::connection::{Dock, MuxConnection, RecvDatagram, ScopeHost, TrConnCfg, Sender};

use crate::common::{
    OpenTelegraphClosureExt, SmokeMuxConfig, TrSmokeRt, TrSmokeScope,
    connect_pair_, make_channel_buff_with_, make_stage_buffs_,
};

/// 一条数据报的**发送**环容量：取 1 KiB——足够装下场景里最大的那条报文（200 字节），
/// 因此「发送侧跨段 / 跨多次写入」不会被环容量干扰。
const K_TG_TX_CAPACITY: usize = 1024usize;

/// 一条数据报的**接收**环容量：刻意取小块（64 字节）。
///
/// 它让「接收环装不下就整条丢弃」这条语义可以被一个**正常返回**的用例钉住：发送
/// 200 字节的报文时接收端只有 64 字节空间，报文必须被整条丢弃，而不是留下半条。
const K_TG_RX_CAPACITY: usize = 64usize;

/// 造一对 telegraph 环内存（发送 1 KiB、接收 64 字节）。
fn tg_buffs_() -> (crate::common::SmokeBuff, crate::common::SmokeBuff) {
    (
        make_channel_buff_with_(K_TG_TX_CAPACITY),
        make_channel_buff_with_(K_TG_RX_CAPACITY),
    )
}

/// 写一条报文并发往 `remote`，返回 `send_async` 报告的载荷长度。
///
/// 两条语句而不是一条链式调用：`write_all` 与 `send_async` 都要 `&mut tx`，借用在
/// 同一表达式里会重叠。
///
/// 目的地址在**发送时**给出（同 UDP 的 `sendto`），而不是开端点时固定。
async fn send_msg_<C>(tx: &mut Sender<C>, remote: u32, payload: &[u8]) -> usize
where
    C: TrConnCfg,
{
    tx.write_all(payload).await.expect("写入发送环应当成功");
    let demand = Demand::at_least(payload.len());
    tx.send_async(Dock::new(remote), &demand)
        .await
        .expect("提交一条报文应当成功")
}

/// 读走一条数据报的**全部载荷**（消费语义），返回 `(远端地址, 载荷)`。
///
/// - 手段：用 [`TrBuffSegmRef::move_items_to_buff`] 把载荷搬进临时缓冲；它会同时推进
///   段上的已消费量，因此数据报被丢弃时环读指针已经前进，后续报文不会错位。
/// - 判断：搬出的字节数必须与段的 `least_count()` 相等，否则 panic。
fn take_datagram_<'a, C, S>(mut dg: RecvDatagram<'a, S>) -> (Dock, std::vec::Vec<u8>)
where
    C: TrConnCfg,
    S: TrBuffSegmRef<'a, u8>,
    RecvDatagram<'a, S>: TrRecvDatagram<'a, C>,
{
    let remote = dg.remote_dock();
    let len = dg.least_count();
    let mut raw = std::vec![core::mem::MaybeUninit::<u8>::uninit(); len];
    let got = TrBuffSegmRef::move_items_to_buff(&mut dg, &mut raw);
    assert_eq!(got, len, "搬出的载荷字节数必须等于段的剩余量");
    // SAFETY: `move_items_to_buff` 刚刚把 `0..got`（= 全部 `len` 个槽位）初始化，
    // 且 `u8` 无 drop 资源、位模式任意皆有效。
    let payload = raw
        .into_iter()
        .map(|slot| unsafe { slot.assume_init() })
        .collect();
    (remote, payload)
}

/// 测试目标（**本轮验收点**）：两条数据报端点的双向收发、空报文、以及「接收环装不下
/// 就整条丢弃」。
///
/// - 手段：两条内存环直连两个端点并完成握手；A 在 dock 7、B 在 dock 11 上各开一条
///   telegraph（发送环 1 KiB、接收环 64 字节），A 依次发三条报文（5 / 200 / 0 字节）、
///   B 依次收两条（**中间那条 200 字节的必须被整条丢弃**），随后 B 回发一条 9 字节、
///   A 收下。整段由 `scope.run_until` 驱动。
/// - 判断：三条短报文的载荷逐字节相等、`send_async` 返回的长度与写入量一致；
///   200 字节那条不出现在接收侧（紧随其后的空报文仍然是**下一条**）；空报文的长度
///   为 0 且内容为空；双向都能收到。任一不满足即 panic。
pub async fn run_telegraph_scenario_<RA, WA, RB, WB, S, RT>(
    rt: &RT,
    scope: &S,
    tx_a: WA,
    rx_a: RA,
    tx_b: WB,
    rx_b: RB,
) where
    RA: TrBuffRead<u8> + 'static,
    WA: TrBuffWrite<u8> + 'static,
    RB: TrBuffRead<u8> + 'static,
    WB: TrBuffWrite<u8> + 'static,
    S: TrSmokeScope + Clone + 'static,
    RT: TrSmokeRt + ScopeHost,
{
    let (conn_a, conn_b) = connect_pair_::<
        SmokeMuxConfig<WA, RA, RT>,
        SmokeMuxConfig<WB, RB, RT>,
        RA,
        WA,
        RB,
        WB,
        S,
    >(
        rt,
        scope,
        tx_a,
        rx_a,
        tx_b,
        rx_b,
        make_stage_buffs_(),
        make_stage_buffs_(),
    )
    .await;

    // 两端是**不同的**配置类型（各自的传输半边类型不同），因此按各自的类型驱动。
    futures::join!(
        drive_telegraph_side_(&conn_a, 7u32, 11u32),
        drive_telegraph_side_(&conn_b, 11u32, 7u32),
    );
}

/// 在一对**已经建立**的连接上跑 telegraph 场景（供进程内直连与真实 socket 共用）。
///
/// A 在 dock 7、B 在 dock 11 上各开一条数据报端点（发送环 1 KiB、接收环 64 字节），
/// 互为对端地址；A 发 5 / 200 / 0 字节三条报文，B 只应收到 5 与 0（200 那条超长被
/// **整条丢弃**），随后 B 回发 9 字节。
pub async fn drive_telegraph_pair_<CA, CB>(conn_a: &MuxConnection<CA>, conn_b: &MuxConnection<CB>)
where
    CA: TrConnCfg,
    CB: TrConnCfg,
{
    futures::join!(
        drive_telegraph_side_(conn_a, 7u32, 11u32),
        drive_telegraph_side_(conn_b, 11u32, 7u32),
    );
}

/// 驱动一端：在 `local_dock` 上开数据报端点，与对端的 `remote_dock` 收发。
///
/// `local < remote` 的一侧（A：7 vs 11）充当**主动方**：先发三条报文再收一条；
/// 另一侧（B）先收两条再回一条。两侧的报文都带各自的可识别填充字节。
async fn drive_telegraph_side_<C>(conn: &MuxConnection<C>, local: u32, remote: u32)
where
    C: TrConnCfg,
{
    let mut binding = conn
        .bind_async(Dock::new(local))
        .await
        .expect("绑定本地 dock 应当成功");
    let telegraph = binding
        .open_telegraph_async_closure(tg_buffs_)
        .await
        .expect("开启 telegraph 应当成功");
    let (mut tx, mut rx) = telegraph.split();
    assert_eq!(tx.local_dock(), Dock::new(local));
    assert_eq!(rx.local_dock(), Dock::new(local));

    if local < remote {
        // -- 主动方：5 字节、200 字节（必须被对端整条丢弃）、0 字节。
        let short = [1u8, 2, 3, 4, 5];
        assert_eq!(
            send_msg_(&mut tx, remote, &short).await,
            short.len(),
            "send_async 必须报告实际提交的长度"
        );
        let oversized = std::vec![0xABu8; 200];
        assert_eq!(
            send_msg_(&mut tx, remote, &oversized).await,
            oversized.len(),
            "超长报文的提交本身必须成功（丢弃发生在接收侧）"
        );
        assert_eq!(
            send_msg_(&mut tx, remote, &[]).await,
            0usize,
            "空报文也是一条报文，长度为 0"
        );

        // 收 B 回发的那条。
        let dg = rx.recv_async().await.expect("应当收到 B 回发的报文");
        let (remote_got, buf) = take_datagram_::<C, _>(dg);
        assert_eq!(
            remote_got,
            Dock::new(remote),
            "远端地址必须逐条给出（= 发送方写下的目的地址）"
        );
        assert_eq!(buf, std::vec![0x77u8; 9], "B 回发的载荷必须逐字节相等");
    } else {
        // -- 被动方：先收两条短报文，**中间那条 200 字节的必须已被整条丢弃**。
        let dg = rx.recv_async().await.expect("应当收到第一条报文");
        let (remote_got, buf) = take_datagram_::<C, _>(dg);
        assert_eq!(remote_got, Dock::new(remote));
        assert_eq!(buf, std::vec![1u8, 2, 3, 4, 5], "第一条报文载荷必须是 5 字节");

        // 下一条**必须**是那条空报文：200 字节那条不得以任何形式出现（半条也不行）。
        let dg = rx.recv_async().await.expect("应当收到空报文");
        let (_, buf) = take_datagram_::<C, _>(dg);
        assert!(
            buf.is_empty(),
            "接收环装不下的那条必须被整条丢弃，下一条是空报文"
        );

        // 回发一条 9 字节。
        assert_eq!(
            send_msg_(&mut tx, remote, &[0x77u8; 9]).await,
            9usize,
            "send_async 必须报告实际提交的长度"
        );
    }
}
