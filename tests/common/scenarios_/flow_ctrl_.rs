//! 流控验收：真实 socket 上「小接收环 + 大发送」，载荷比窗口大两个数量级，窗口必然
//! 被用尽并回补；慢读（每次 [`K_FLOW_CTRL_READ_STEP`] 字节）把窗口压在阈值附近。

use buffex::x_deps::abs_buff::{TrBuffRead, TrBuffWrite};
use abs_smux::{
    chan::TrChannelHalf,
    conn::{TrChannelListener, TrConnection, TrDockBinding},
};
use smux_v1::connection::Dock;

use crate::common::{
    AcceptAsyncClosureExt,
    FlowCtrlConfig,
    K_FLOW_CTRL_PAYLOAD_LEN,
    K_FLOW_CTRL_READ_STEP,
    K_FLOW_CTRL_RING_CAPACITY,
    K_FLOW_CTRL_SMALL_LEN,
    TrSmokeScope,
    connect_pair_,
    expect_eof_,
    make_channel_buff_with_,
    make_flow_payload_,
};

/// 流控验收场景①：**真实 socket** 上「小接收环 + 大发送」的定向传输。
///
/// # 这条用例要钉住什么
///
/// 载荷（[`K_FLOW_CTRL_PAYLOAD_LEN`] = 64 KiB）比接收窗口
/// （[`K_FLOW_CTRL_RING_CAPACITY`] = 512 字节）大 **128 倍**，因此发送方一定会经历
/// 「额度用尽 → 等 `WINDOW_UPDATE` → 继续」的循环；接收方一定会经历「窗口降到 0 并
/// 通告 → 应用消费 → 窗口回补并通告」。这正是流控被真正执行的形状——窗口永远用不完
/// 的用例不会执行到这些代码。
///
/// 接收方**慢读**（每次只读 [`K_FLOW_CTRL_READ_STEP`] 字节），窗口因此在 0 与阈值
/// 之间反复往返；发送方**写完全部载荷之后才 `drop(tx)`**，所以对端的 `FIN` 必须排在
/// 全部数据之后到达。
///
/// - 手段：真实 socket 装配（设备级适配 + 四条调用方驱动的泵 + 全被动环）→
///   [`connect_pair_`] 建连（两侧都用 [`FlowCtrlConfig`]，子流环上钳到 512 字节）→
///   A 端 `open_channel_async` 后 `write_all` 全部载荷再丢发送半边，B 端
///   `income_async` 后按 64 字节一步步读、逐字节比对，最后 [`expect_eof_`]。
/// - 判断：(1) B 端读到的字节与 A 端发出的**逐字节相等且长度相同**——窗口用尽与回补
///   不丢任何字节；(2) 收尾读到 `Closing`——`FIN` 确实到达且排在全部数据之后（若实现
///   提前发 `FIN`，第 (1) 条会先失败）；(3) 场景在超时前返回——回补通告若被误判为
///   过期，这条用例会**死锁**而不是通过。
pub async fn run_flow_ctrl_socket_scenario_<RA, WA, RB, WB, S>(
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
{
    let (conn_a, conn_b) = connect_pair_::<
        FlowCtrlConfig<WA, RA>,
        FlowCtrlConfig<WB, RB>,
        RA,
        WA,
        RB,
        WB,
        S,
    >(scope, tx_a, rx_a, tx_b, rx_b)
    .await;

    let listener_dock = Dock::new(1u32);
    let payload = make_flow_payload_(0xF10C_0001u32, K_FLOW_CTRL_PAYLOAD_LEN);

    // 两端各自启动；`conn_a` / `conn_b` 由本函数持有到场景结束（传输泵必须在连接
    // 活着期间持续搬运）。
    let a_side = async {
        let mut binding = conn_a
            .bind_async(Dock::new(0x5001u32))
            .await
            .expect("A 侧绑定应当成功");
        let mut opening: &[u8] = b"flow";
        let mut handle = binding
            .open_channel_async(listener_dock, &mut opening)
            .await
            .expect("A 侧发起子流应当成功");
        let mut welcome_buf: [u8; 0] = [];
        let mut welcome: &mut [u8] = &mut welcome_buf[..];
        let (mut tx, rx) = handle
            .accept_async_closure(&mut welcome, || {
                (
                    make_channel_buff_with_(K_FLOW_CTRL_RING_CAPACITY),
                    make_channel_buff_with_(K_FLOW_CTRL_RING_CAPACITY),
                )
            })
            .await
            .expect("A 侧建流裁决应当成功");

        tx.write_all(&payload)
            .await
            .expect("写入本端子流发送环应当成功（连接必须持续给窗口）");
        // 半关闭：`FIN` 必须等这批字节全部上线之后才发。
        drop(tx);
        // **接收半边必须活到场景结束**——本场景只声明「丢发送半边」。若在这里顺带把它
        // 丢掉，本端会立刻向对端发 `CLOSE(RESET)`，而对端收到 `RESET` 会连自己的**接收
        // 环写端**一并关掉，于是本端仍在途的最后一个窗口（512 字节）被丢弃，接收侧读到的
        // 载荷会短一截（实测 65024/65536）。因此把它交还给调用方持有：`join!` 结束时才
        // 随结果一起丢弃。
        //
        // 「`RESET` 该不该动对端的接收方向」本轮裁决**先不动**，半 RESET 记为后续项，
        // 见 `dev-notes/flow-ctrl-20261005-0115.md` §3.3。
        rx
    };

    let b_side = async {
        let mut listener = conn_b
            .bind_async(listener_dock)
            .await
            .expect("B 侧绑定应当成功")
            .listen_async()
            .await
            .expect("B 侧开始监听应当成功");
        let mut incoming = listener.income_async().await.expect("B 侧应当取到入向子流");
        let mut welcome_buf: [u8; 0] = [];
        let mut welcome: &mut [u8] = &mut welcome_buf[..];
        let (_tx, mut rx) = incoming
            .accept_async_closure(&mut welcome, || {
                (
                    make_channel_buff_with_(K_FLOW_CTRL_RING_CAPACITY),
                    make_channel_buff_with_(K_FLOW_CTRL_RING_CAPACITY),
                )
            })
            .await
            .expect("B 侧建流裁决应当成功");

        let mut got = vec![0u8; payload.len()];
        // 慢读：每次只取 64 字节，迫使窗口在 0 与阈值之间往复。
        for chunk in got.chunks_mut(K_FLOW_CTRL_READ_STEP) {
            rx.read_exact(chunk)
                .await
                .expect("按慢速节奏读完对端载荷应当成功");
        }
        assert_eq!(
            got, payload,
            "窗口用尽 / 回补之后，对端载荷必须逐字节完好（不丢包、不重排）"
        );
        // 数据读尽之后才应当看到 EOF；提前看到说明 `FIN` 抢在数据前面。
        expect_eof_(&mut rx).await;
    };

    futures::join!(a_side, b_side);
}


/// 流控验收场景②：**一条子流的窗口被用尽，不拖累同连接上的其它子流**。
///
/// 流控是**逐条子流**的：`A` 在子流 1 上写满接收窗口之后停下来等通告，此时子流 2
/// 上的双向收发必须照常完成。若窗口记账被做成连接级共享（或发送额度被误当成整条
/// 连接的额度），子流 2 会被一起卡住。
///
/// - 手段：同一条真实 socket 连接上开两条子流。子流 1：`A` 恰好写满接收窗口
///   （[`K_FLOW_CTRL_RING_CAPACITY`] 字节），而 `B` **完全不读**，它的额度因此归零
///   并停在那里；子流 2：`A` 写 [`K_FLOW_CTRL_SMALL_LEN`] 字节、`B` 读尽并等到
///   `Closing`。
/// - 判断：子流 2 的载荷逐字节相等且 `FIN` 正常到达；子流 1 的 `Tx` 仍可用
///   （`is_tx_closed()` 为假）——被阻塞的只是它自己的发送方向。
pub async fn run_flow_ctrl_isolation_scenario_<RA, WA, RB, WB, S>(
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
{
    use abs_smux::chan::TrChannelHalf;

    let (conn_a, conn_b) = connect_pair_::<
        FlowCtrlConfig<WA, RA>,
        FlowCtrlConfig<WB, RB>,
        RA,
        WA,
        RB,
        WB,
        S,
    >(scope, tx_a, rx_a, tx_b, rx_b)
    .await;

    let dock_blocked = Dock::new(1u32);
    let dock_progress = Dock::new(2u32);
    let fill = make_flow_payload_(0xB10C_0001u32, K_FLOW_CTRL_RING_CAPACITY);
    let small = make_flow_payload_(0xB10C_0002u32, K_FLOW_CTRL_SMALL_LEN);

    let a_side = async {
        let mut binding = conn_a
            .bind_async(Dock::new(0x5002u32))
            .await
            .expect("A 侧绑定应当成功");

        // -- 子流 1：恰好写满接收窗口，然后**保持打开、不再写**。
        //    B 完全不读它，因此该方向的额度归零且停在 0。
        let mut opening: &[u8] = b"";
        let mut blocked_handle = binding
            .open_channel_async(dock_blocked, &mut opening)
            .await
            .expect("A 侧发起子流 1 应当成功");
        let mut welcome_buf: [u8; 0] = [];
        let mut welcome: &mut [u8] = &mut welcome_buf[..];
        let (mut blocked_tx, _blocked_rx) = blocked_handle
            .accept_async_closure(&mut welcome, || {
                (
                    make_channel_buff_with_(K_FLOW_CTRL_RING_CAPACITY),
                    make_channel_buff_with_(K_FLOW_CTRL_RING_CAPACITY),
                )
            })
            .await
            .expect("A 侧子流 1 裁决应当成功");
        blocked_tx
            .write_all(&fill)
            .await
            .expect("写满接收窗口应当成功");

        // -- 子流 2：正常收发 + 半关闭。
        let mut opening2: &[u8] = &[];
        let mut free_handle = binding
            .open_channel_async(dock_progress, &mut opening2)
            .await
            .expect("A 侧发起子流 2 应当成功");
        let (mut free_tx, _free_rx) = free_handle
            .accept_async_closure(&mut welcome, || {
                (
                    make_channel_buff_with_(K_FLOW_CTRL_RING_CAPACITY),
                    make_channel_buff_with_(K_FLOW_CTRL_RING_CAPACITY),
                )
            })
            .await
            .expect("A 侧子流 2 裁决应当成功");
        free_tx
            .write_all(&small)
            .await
            .expect("子流 2 写入应当成功");
        drop(free_tx);

        // 子流 1 只是**额度用尽**，它的发送半边不该被判为关闭。
        assert!(
            !blocked_tx.is_tx_closed(),
            "额度用尽不等于发送方向关闭：子流 1 的 Tx 必须仍然可用"
        );
        drop(blocked_tx);
    };

    let b_side = async {
        let mut listener_blocked = conn_b
            .bind_async(dock_blocked)
            .await
            .expect("B 侧绑定 dock 1 应当成功")
            .listen_async()
            .await
            .expect("B 侧监听 dock 1 应当成功");
        let mut listener_progress = conn_b
            .bind_async(dock_progress)
            .await
            .expect("B 侧绑定 dock 2 应当成功")
            .listen_async()
            .await
            .expect("B 侧监听 dock 2 应当成功");

        let mut welcome_buf: [u8; 0] = [];
        let mut welcome: &mut [u8] = &mut welcome_buf[..];

        // 先接子流 1 并**故意不读**：环保持满，窗口停在 0。
        let mut incoming_blocked = listener_blocked
            .income_async()
            .await
            .expect("B 侧应当取到子流 1");
        let (_blocked_tx, blocked_rx) = incoming_blocked
            .accept_async_closure(&mut welcome, || {
                (
                    make_channel_buff_with_(K_FLOW_CTRL_RING_CAPACITY),
                    make_channel_buff_with_(K_FLOW_CTRL_RING_CAPACITY),
                )
            })
            .await
            .expect("B 侧子流 1 裁决应当成功");
        let _never_read = blocked_rx;

        // 子流 2：读尽并等 EOF。
        let mut incoming_progress = listener_progress
            .income_async()
            .await
            .expect("B 侧应当取到子流 2");
        let (_progress_tx, mut progress_rx) = incoming_progress
            .accept_async_closure(&mut welcome, || {
                (
                    make_channel_buff_with_(K_FLOW_CTRL_RING_CAPACITY),
                    make_channel_buff_with_(K_FLOW_CTRL_RING_CAPACITY),
                )
            })
            .await
            .expect("B 侧子流 2 裁决应当成功");
        let mut got = vec![0u8; small.len()];
        progress_rx
            .read_exact(&mut got)
            .await
            .expect("子流 2 必须能在子流 1 额度耗尽时照常读完");
        assert_eq!(got, small, "子流 2 的载荷必须逐字节完好");
        expect_eof_(&mut progress_rx).await;
    };

    futures::join!(a_side, b_side);
}
