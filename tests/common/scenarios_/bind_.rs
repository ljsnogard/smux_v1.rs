//! 绑定与建流生命周期场景：dock 绑定的独占性，以及「发起方句柄尚未裁决就被丢弃」。

use buffex::x_deps::abs_buff::{TrBuffRead, TrBuffWrite};
use abs_smux::conn::{TrChannelListener, TrConnection, TrDockBinding};
use smux_v1::connection::{BindError, Dock, HandleError};

use crate::common::{
    AcceptAsyncClosureExt,
    SmokeMuxConfig,
    TrSmokeRt,
    TrSmokeScope,
    connect_pair_,
    make_channel_buff_,
};

/// 绑定独占性场景：验证 [`TrConnection::bind_async`] 对同一个 `local_dock` 拒绝
/// 第二次绑定，且丢弃 binding 后可以重绑。
///
/// 只做本地注册表行为验证，不建子流、不交换业务字节；但**必须**先完成握手并建出
/// 两个真实 `MuxConnection`，因为 binding 是连接对象上的东西。
///
/// # Panics
///
/// 握手失败、绑定出现的错误类型不是 `DockInUse`、或解绑后重绑失败都会 panic。
///
/// [`TrConnection::bind_async`]: abs_smux::conn::TrConnection::bind_async
pub async fn run_bind_exclusivity_scenario_<RA, WA, RB, WB, S, RT>(
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
        >(rt, scope, tx_a, rx_a, tx_b, rx_b, crate::common::make_stage_buffs_(), crate::common::make_stage_buffs_())
        .await;

    // 探测用的 dock 取值远离收发场景用的 `1..=16` 与 `0x1000..`，避免歧义。
    let dock = Dock::new(0x2000u32);
    let other = Dock::new(0x2001u32);

    // 1) 首次绑定成功；同一 dock 第二次绑定必须报 `DockInUse`，而不是又发一个
    //    binding。
    let first = conn_a.bind_async(dock).await.expect("首次绑定应当成功");
    assert!(
        matches!(
            conn_a.bind_async(dock).await,
            Result::Err(BindError::DockInUse)
        ),
        "同一 local_dock 第二次 bind_async 应当报 DockInUse"
    );

    // 2) 另一个 dock 不受影响。
    let second = conn_a
        .bind_async(other)
        .await
        .expect("不同 local_dock 应当可以绑定");

    // 3) 丢弃 binding 即解绑，之后同一个 dock 可以重绑。
    drop(first);
    let third = conn_a
        .bind_async(dock)
        .await
        .expect("解绑后应当可以重新绑定");

    // 4) 绑定是**每条连接**独立的状态：对端用同一个 dock 值不受本端影响。
    let peer_binding = conn_b
        .bind_async(dock)
        .await
        .expect("另一条连接上的同名 local_dock 应当可以绑定");

    drop((second, third, peer_binding));
}


/// 半建立句柄的收尾场景：验证「最终裁决之前丢弃句柄」在两个角色下都**不留垃圾、
/// 不悬着对端**。
///
/// - 手段：两条内存环直连并完成握手，取两个真实 `MuxConnection`；A 侧绑定一个
///   dock、B 侧在同一 dock 上 `listen_async`。然后跑两小段：
///   1. A **未 accept 就 drop** 发起方句柄，随即用**同一个 dock 对**再开一次，这次
///      两侧都正常 `accept_async`；
///   2. A 发起并 `accept_async`（会等对端裁决），同时 B 取到入向句柄后**未裁决就
///      drop**。
///
///   整个场景由 `scope.run_until` 驱动。
/// - 判断：第 1 段中第二次 `open_channel_async` 必须**立刻成功**——若丢弃发起方句柄
///   只是把它放进拆流宽限期，这里会报 `WaitClose`/`Duplicate`（发起方此刻还没发过
///   `OPEN`，对端不可能有在途帧，所以理应立刻可复用）；第 2 段中 A 的
///   `accept_async` 必须返回 `HandleError::Refused`——若丢弃响应方句柄不发 `REJECT`，
///   A 会永远悬着，测试会超时。任一不满足即 panic。
///
/// # Panics
///
/// 握手/绑定/监听失败，或上述两条判断不成立，都会 panic。
pub async fn run_unsettled_handle_scenario_<RA, WA, RB, WB, S, RT>(
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
        >(rt, scope, tx_a, rx_a, tx_b, rx_b, crate::common::make_stage_buffs_(), crate::common::make_stage_buffs_())
        .await;

    // 探测用的 dock 取值远离收发场景用的 `1..=16` 与 `0x1000..`，避免歧义。
    let local_a = Dock::new(0x3000u32);
    let remote_b = Dock::new(0x3001u32);

    let mut binding_a = conn_a
        .bind_async(local_a)
        .await
        .expect("A 侧绑定应当成功");
    let mut listener_b = conn_b
        .bind_async(remote_b)
        .await
        .expect("B 侧绑定应当成功")
        .listen_async_default()
        .await
        .expect("B 侧开始监听应当成功");

    // -- 第 1 段：发起方未裁决就丢弃 ⇒ 登记被彻底撤销，同一 dock 对立刻可复用。
    let mut message: &[u8] = &[];
    let abandoned = binding_a
        .open_channel_async(remote_b, &mut message)
        .await
        .expect("第一次发起应当成功");
    drop(abandoned);

    let mut again = binding_a
        .open_channel_async(remote_b, &mut message)
        .await
        .expect("撤销登记后，同一个 dock 对应当立刻可以再次发起");
    let mut welcome_buf: [u8; 0] = [];
    let mut welcome: &mut [u8] = &mut welcome_buf[..];
    let (opened, incoming) = futures::join!(
        async {
            again
                .accept_async_closure(&mut welcome, || {
                    (make_channel_buff_(), make_channel_buff_())
                })
                .await
        },
        async {
            let mut handle = listener_b
                .income_async()
                .await
                .expect("B 侧应当取到第二次发起的请求");
            let mut peer_welcome_buf: [u8; 0] = [];
            let mut peer_welcome: &mut [u8] = &mut peer_welcome_buf[..];
            handle
                .accept_async_closure(&mut peer_welcome, || {
                    (make_channel_buff_(), make_channel_buff_())
                })
                .await
        },
    );
    let (tx, rx) = opened.expect("A 侧最终裁决应当成功");
    let (peer_tx, peer_rx) = incoming.expect("B 侧最终裁决应当成功");
    drop((tx, rx, peer_tx, peer_rx));

    // -- 第 2 段：响应方未裁决就丢弃 ⇒ 主动方必须收到 `Refused`，而不是悬着。
    //
    // 换一个 dock 对：上一条子流已经拆掉，同一 dock 对会处于拆流宽限期。
    let remote_c = Dock::new(0x3002u32);
    let mut listener_c = conn_b
        .bind_async(remote_c)
        .await
        .expect("B 侧再绑定一个 dock 应当成功")
        .listen_async_default()
        .await
        .expect("B 侧第二个监听应当成功");
    let mut message2: &[u8] = &[];
    let mut waiter = binding_a
        .open_channel_async(remote_c, &mut message2)
        .await
        .expect("第二次发起应当成功");
    let mut welcome2_buf: [u8; 0] = [];
    let mut welcome2: &mut [u8] = &mut welcome2_buf[..];
    let (verdict, ()) = futures::join!(
        async {
            waiter
                .accept_async_closure(&mut welcome2, || {
                    (make_channel_buff_(), make_channel_buff_())
                })
                .await
        },
        async {
            let handle = listener_c
                .income_async()
                .await
                .expect("B 侧应当取到第三次发起的请求");
            // 未裁决就丢弃：按契约应当给对端补一条 `REJECT`。
            drop(handle);
        },
    );
    assert!(
        matches!(verdict, Result::Err(HandleError::Refused)),
        "响应方丢弃待决句柄后，主动方的 accept_async 应当得到 Refused"
    );
}
