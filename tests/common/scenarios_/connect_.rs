//! 握手建连：A 端发起、B 端等待，并由交付物建出两个 `MuxConnection`。

use buffex::x_deps::abs_buff::{TrBuffRead, TrBuffWrite};
use mm_ptr::x_deps::abs_mm::CoreAlloc;
use smux_v1::{
    connection::{MuxConnection, TrConnCfg},
    handshake::{
        agent::{AcceptAllEntries, HandshakeAgent},
        opts::BasicOpts,
    },
};

use crate::common::{SmokeBuff, TrSmokeScope};

/// 握手（A 端发起、B 端等待）并由交付物建立两个 [`MuxConnection`]。
///
/// 抽出来给「收发场景」与「绑定独占性场景」共用，保证两者走的是**同一套**
/// 连接建立路径；`tests/thread_safety.rs` 也直接用它装配连接。
///
/// **连接配置是泛型参数** `C`：默认调用点是 [`SmokeMuxConfig`]，而「极小帧暂存」
/// 验收用例传入一个只把 `make_stage_buffs` 换成一字节缓冲的同构配置——两条子流环
/// 与其余策略完全一致，避免把「容量」以外的差异带进对照。
pub async fn connect_pair_<CA, CB, RA, WA, RB, WB, S>(
    scope: &S,
    tx_a: WA,
    rx_a: RA,
    tx_b: WB,
    rx_b: RB,
) -> (MuxConnection<CA, S>, MuxConnection<CB, S>)
where
    // 连接把 Rx / Tx 移交给 `'static` 的读写循环（`spawn_local` 要求 `'static`；
    // 本地投递**不要求** `Send`，因此 `!Send` 的传输也能直接当 `Rx` / `Tx`）。
    RA: TrBuffRead<u8> + 'static,
    WA: TrBuffWrite<u8> + 'static,
    RB: TrBuffRead<u8> + 'static,
    WB: TrBuffWrite<u8> + 'static,
    S: TrSmokeScope + Clone + 'static,
    // 两端的连接配置各自泛化：默认用例两侧都是 [`SmokeMuxConfig`]，「极小帧暂存」
    // 用例传入一对只把 `make_stage_buffs` 换成一字节缓冲的同构配置——两条子流环与
    // 其余策略完全一致，避免把「容量」以外的差异带进对照。
    CA: TrConnCfg<
            ConnRx = RA,
            ConnTx = WA,
            Alloc = CoreAlloc,
            Buff = SmokeBuff,
            StageBuff = SmokeBuff,
        > + Default
        + Clone
        + 'static,
    CB: TrConnCfg<
            ConnRx = RB,
            ConnTx = WB,
            Alloc = CoreAlloc,
            Buff = SmokeBuff,
            StageBuff = SmokeBuff,
        > + Default
        + Clone
        + 'static,
    CA::StageBuff: Send + Sync,
    CB::StageBuff: Send + Sync,
{
    let invite_opts = BasicOpts::default();
    let listen_opts = BasicOpts::default();
    let invite_fut = HandshakeAgent::new(tx_a, rx_a).invite_async(&invite_opts, AcceptAllEntries);
    let listen_fut = HandshakeAgent::new(tx_b, rx_b).listen_async(&listen_opts, AcceptAllEntries);
    let (invited, accepted) = futures::join!(async { invite_fut.await }, async { listen_fut.await });
    let delivery_a = invited.expect("发起方握手应当成功");
    let delivery_b = accepted.expect("等待方握手应当成功");

    // 两块连接级缓冲**由配置提供**（`TrConnCfg::make_stage_buffs`）：容量是连接级
    // 策略，测试装配不该在这里另写一份固定的 64 KiB。
    let config_a = CA::default();
    let config_b = CB::default();
    let (stage_ar, stage_aw) = config_a
        .make_stage_buffs(config_a.allocator())
        .expect("A 侧连接级帧暂存应当分配成功");
    let (stage_br, stage_bw) = config_b
        .make_stage_buffs(config_b.allocator())
        .expect("B 侧连接级帧暂存应当分配成功");
    (
        MuxConnection::new(scope, delivery_a, config_a, stage_ar, stage_aw),
        MuxConnection::new(scope, delivery_b, config_b, stage_br, stage_bw),
    )
}
