//! 缓冲相关的验收场景：建流时**拒绝**调用方给的环内存（容量不足），以及逐子流
//! 分配与配额的行为。

use buffex::x_deps::abs_buff::{TrBuffRead, TrBuffWrite};
use abs_smux::conn::{TrChannelListener, TrConnection, TrDockBinding};
use smux_v1::connection::{Dock, HandleError};

use crate::common::{
    AcceptAsyncClosureExt,
    K_CHANNEL_CAPACITY,
    SmokeMuxConfig,
    TrSmokeRt,
    TrSmokeScope,
    connect_pair_,
    make_channel_buff_,
    make_channel_buff_with_,
};

use super::kit_::exchange_and_half_close_;

/// 场景：**拒绝接受环内存**（容量不足以建环）时连接的行为。
///
/// - 目标：连接对「不合约的内存」只**拒绝接受**，不替调用方改尺寸，也不因此拆掉连接。
/// - 手段：A 发起一条子流到 `remote_b`；B 取到待决句柄后用容量 `0` 的缓冲裁决
///   （环的下限是 `1`），A 用正常容量的缓冲裁决；随后在**同一条连接**上再走一遍
///   正常的建流。
/// - 判断：B 侧裁决必须得到 [`HandleError::RingRejected`]；A 侧必须得到
///   [`HandleError::Refused`]（拒绝发生在发出任何帧之前，B 按角色补 `REJECT`）；
///   随后的那条子流必须成功——若拒绝把连接或注册表弄脏了，这一段会失败。
pub async fn run_ring_rejected_scenario_<RA, WA, RB, WB, S, RT>(
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
    RT: TrSmokeRt,
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

    let local_a = Dock::new(0x3100u32);
    let remote_b = Dock::new(0x3101u32);
    let mut binding_a = conn_a
        .bind_async(local_a)
        .await
        .expect("A 侧绑定应当成功");
    let mut listener_b = conn_b
        .bind_async(remote_b)
        .await
        .expect("B 侧绑定应当成功")
        .listen_async()
        .await
        .expect("B 侧开始监听应当成功");

    // -- 第 1 段：B 给一块容量 0 的缓冲 ⇒ 必须被拒绝。
    let mut message: &[u8] = &[];
    let mut handle_a = binding_a
        .open_channel_async(remote_b, &mut message)
        .await
        .expect("发起应当成功");
    let mut welcome_buf: [u8; 0] = [];
    let mut welcome: &mut [u8] = &mut welcome_buf[..];
    let (verdict_a, verdict_b) = futures::join!(
        async {
            handle_a
                .accept_async_closure(&mut welcome, || {
                    (make_channel_buff_(), make_channel_buff_())
                })
                .await
        },
        async {
            let mut handle_b = listener_b
                .income_async()
                .await
                .expect("B 侧应当取到发起请求");
            let mut peer_welcome_buf: [u8; 0] = [];
            let mut peer_welcome: &mut [u8] = &mut peer_welcome_buf[..];
            handle_b
                .accept_async_closure(&mut peer_welcome, || {
                    // 容量 0 < 环下限 1 ⇒ 连接应当拒绝这两份内存。
                    (make_channel_buff_with_(0), make_channel_buff_with_(0))
                })
                .await
        },
    );
    assert!(
        matches!(verdict_b, Result::Err(HandleError::RingRejected)),
        "容量 0 不足以建环时，accept_async 必须报 RingRejected"
    );
    assert!(
        matches!(verdict_a, Result::Err(HandleError::Refused)),
        "响应方拒绝接受内存后，主动方应当收到 Refused"
    );

    // -- 第 2 段：同一条连接上正常建流仍然成功（拒绝没有弄脏连接 / 注册表）。
    let remote_c = Dock::new(0x3102u32);
    let mut listener_c = conn_b
        .bind_async(remote_c)
        .await
        .expect("B 侧再绑定一个 dock 应当成功")
        .listen_async()
        .await
        .expect("B 侧第二个监听应当成功");
    let mut message2: &[u8] = &[];
    let mut handle_a2 = binding_a
        .open_channel_async(remote_c, &mut message2)
        .await
        .expect("第二次发起应当成功");
    let (a_done, b_done) = futures::join!(
        async {
            let mut welcome2_buf: [u8; 0] = [];
            let mut welcome2: &mut [u8] = &mut welcome2_buf[..];
            let (tx, mut rx) = handle_a2
                .accept_async_closure(&mut welcome2, || {
                    (make_channel_buff_(), make_channel_buff_())
                })
                .await
                .expect("拒绝之后，正常建流应当仍然成功");
            exchange_and_half_close_(tx, &mut rx, 0x3103u32, 0usize).await;
        },
        async {
            let mut handle_b2 = listener_c
                .income_async()
                .await
                .expect("B 侧应当取到第二次发起");
            let mut peer_welcome_buf: [u8; 0] = [];
            let mut peer_welcome: &mut [u8] = &mut peer_welcome_buf[..];
            let (tx, mut rx) = handle_b2
                .accept_async_closure(&mut peer_welcome, || {
                    (make_channel_buff_(), make_channel_buff_())
                })
                .await
                .expect("B 侧正常建流应当成功");
            exchange_and_half_close_(tx, &mut rx, 0x3103u32, 0usize).await;
        },
    );
    let _ = (a_done, b_done);
}


/// 逐条子流自行分配场景：**同一条连接**（一种声明的环存储类型）上，两条子流各自
/// 决定「分配多少、从哪块内存来」。
///
/// - 手段：完成握手得到两个连接；两条子流的 `accept_async` 各自在闭包里现造缓冲
///   （[`leak_buff_`]），容量分别是 4096 与 8192。
/// - 判断：两条子流都建立成功、载荷逐字节相符、半关闭后读到 EOF（两条的接收窗口与
///   环大小不同，因此这也顺带验证「容量是逐条的」）。
///
/// # Panics
///
/// 握手 / 绑定 / 监听 / 交互任一环节失败都会 panic。
pub async fn run_per_channel_alloc_scenario_<RA, WA, RB, WB, S, RT>(
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
    RT: TrSmokeRt,
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

    let local_a = Dock::new(0x4000u32);
    let dock_small = Dock::new(0x4001u32);
    let dock_large = Dock::new(0x4002u32);

    let mut binding_a = conn_a
        .bind_async(local_a)
        .await
        .expect("A 侧绑定应当成功");
    let mut listener_small = conn_b
        .bind_async(dock_small)
        .await
        .expect("B 侧绑定 dock_small 应当成功")
        .listen_async()
        .await
        .expect("B 侧监听 dock_small 应当成功");
    let mut listener_large = conn_b
        .bind_async(dock_large)
        .await
        .expect("B 侧绑定 dock_large 应当成功")
        .listen_async()
        .await
        .expect("B 侧监听 dock_large 应当成功");

    // -- 第一条：两侧各自按 4096 分配。
    let small = K_CHANNEL_CAPACITY;
    let (a_done, b_done) = futures::join!(
        async {
            let mut message: &[u8] = &[];
            let mut handle = binding_a
                .open_channel_async(dock_small, &mut message)
                .await
                .expect("发起第一条子流应当成功");
            let mut welcome_buf: [u8; 0] = [];
            let mut welcome: &mut [u8] = &mut welcome_buf[..];
            let (tx, mut rx) = handle
                .accept_async_closure(&mut welcome, move || (make_channel_buff_with_(small), make_channel_buff_with_(small)))
                .await
                .expect("第一条子流裁决应当成功");
            exchange_and_half_close_(tx, &mut rx, 0x4001u32, 0usize).await;
        },
        async {
            let mut handle = listener_small
                .income_async()
                .await
                .expect("B 侧应当取到第一条子流");
            let mut welcome_buf: [u8; 0] = [];
            let mut welcome: &mut [u8] = &mut welcome_buf[..];
            let (tx, mut rx) = handle
                .accept_async_closure(&mut welcome, move || (make_channel_buff_with_(small), make_channel_buff_with_(small)))
                .await
                .expect("B 侧第一条子流裁决应当成功");
            exchange_and_half_close_(tx, &mut rx, 0x4001u32, 0usize).await;
        },
    );
    let _ = (a_done, b_done);

    // -- 第二条：两侧各自按 8192 分配（**另一种容量**）。
    let large = K_CHANNEL_CAPACITY * 2;
    let (a_done, b_done) = futures::join!(
        async {
            let mut message: &[u8] = &[];
            let mut handle = binding_a
                .open_channel_async(dock_large, &mut message)
                .await
                .expect("发起第二条子流应当成功");
            let mut welcome_buf: [u8; 0] = [];
            let mut welcome: &mut [u8] = &mut welcome_buf[..];
            let (tx, mut rx) = handle
                .accept_async_closure(&mut welcome, move || (make_channel_buff_with_(large), make_channel_buff_with_(large)))
                .await
                .expect("第二条子流裁决应当成功");
            exchange_and_half_close_(tx, &mut rx, 0x4002u32, 1usize).await;
        },
        async {
            let mut handle = listener_large
                .income_async()
                .await
                .expect("B 侧应当取到第二条子流");
            let mut welcome_buf: [u8; 0] = [];
            let mut welcome: &mut [u8] = &mut welcome_buf[..];
            let (tx, mut rx) = handle
                .accept_async_closure(&mut welcome, move || (make_channel_buff_with_(large), make_channel_buff_with_(large)))
                .await
                .expect("B 侧第二条子流裁决应当成功");
            exchange_and_half_close_(tx, &mut rx, 0x4002u32, 1usize).await;
        },
    );
    let _ = (a_done, b_done);
}
