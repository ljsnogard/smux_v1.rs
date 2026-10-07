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
    TrSmokeRt,
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
///   A 端 `open_channel_async` 后 `write_all` 全部载荷，**随即丢掉两个半边**，B 端
///   `income_async` 后按 64 字节一步步读、逐字节比对，最后 [`expect_eof_`]。
/// - 判断：(1) B 端读到的字节与 A 端发出的**逐字节相等且长度相同**——窗口用尽与回补
///   不丢任何字节；(2) 收尾读到 `Closing`——`FIN` 确实到达且排在全部数据之后（若实现
///   提前发 `FIN`，第 (1) 条会先失败）；(3) 场景在超时前返回——回补通告若被误判为
///   过期，这条用例会**死锁**而不是通过。
pub async fn run_flow_ctrl_socket_scenario_<RA, WA, RB, WB, S, RT>(
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
    let (conn_a, conn_b) = connect_pair_::<
        FlowCtrlConfig<WA, RA, RT>,
        FlowCtrlConfig<WB, RB, RT>,
        RA,
        WA,
        RB,
        WB,
        S,
    >(rt, scope, tx_a, rx_a, tx_b, rx_b)
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
        // 接收半边**就地丢弃**（真实用法）：本端随即向对端发 `CLOSE(RESET)`，声明
        // 「我不再接收」。`RESET` 只关对端的**发送方向**，不再连对端的接收环写端一起
        // 关；本端仍在途的最后一个窗口因此照常送达接收侧（T1 修掉的行为，见
        // `dev-notes/outlook-concurrency…` §12 T1）。
        drop(rx);
    };

    let b_side = async {
        let mut listener = conn_b
            .bind_async(listener_dock)
            .await
            .expect("B 侧绑定应当成功")
            .listen_async_default()
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
pub async fn run_flow_ctrl_isolation_scenario_<RA, WA, RB, WB, S, RT>(
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
    use abs_smux::chan::TrChannelHalf;

    let (conn_a, conn_b) = connect_pair_::<
        FlowCtrlConfig<WA, RA, RT>,
        FlowCtrlConfig<WB, RB, RT>,
        RA,
        WA,
        RB,
        WB,
        S,
    >(rt, scope, tx_a, rx_a, tx_b, rx_b)
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
            .listen_async_default()
            .await
            .expect("B 侧监听 dock 1 应当成功");
        let mut listener_progress = conn_b
            .bind_async(dock_progress)
            .await
            .expect("B 侧绑定 dock 2 应当成功")
            .listen_async_default()
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

/// 收尾验收：**应用丢掉接收半边之后到达的数据必须被静默丢弃，连接必须活着**。
///
/// # 这条用例要钉住什么
///
/// `drop(rx)` 会关掉接收环的消费端，并让写循环向对端发 `CLOSE(RESET)`。对端在收到
/// `RESET` 之前已经在途的数据帧仍会到达本端，此时有两种错误处理方式都会**误杀整条
/// 连接**：
///
/// - 把数据写进一条消费端已关闭的环 → `buffex` 返回 `ProducerError::Closing`，而
///   解复用循环把它当成传输错误；
/// - 读侧表项已经被提前摘掉 → 帧掉进「未知子流」分支，按协议违例处理。
///
/// 正确行为是：数据**静默丢弃**（本端已经声明不再接收），连接继续服务其它子流。
/// 因此本用例在丢掉子流 1 的接收半边之后，用**同一条连接**上的子流 2 做一次完整
/// 往返——连接若被杀掉，这一往返必然失败（而不是悄悄通过）。
///
/// - 手段：两个端点直连（子流环上钳到 [`K_FLOW_CTRL_RING_CAPACITY`]）→ A 在子流 1
///   上写满**一整个窗口**的载荷（保证有数据帧在途）、并保持 `tx` 打开；B 接受子流 1
///   后**立刻丢掉 `rx`**；随后 B 服务子流 2，A 在子流 2 上完成一次收发。
/// - 判断：子流 2 的载荷逐字节相等且半关闭后读到 EOF；场景在超时前返回。
///
/// 说明：在途帧恰好落在「表项还在且已标记不再接收」还是「已进墓碑」这两条路径之一，
/// 取决于调度；两者都必须安全。本用例不强制定时，但任何一条走成连接级错误都会让
/// 子流 2 失败。
pub async fn run_recv_dropped_scenario_<RA, WA, RB, WB, S, RT>(
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
    let (conn_a, conn_b) = connect_pair_::<
        FlowCtrlConfig<WA, RA, RT>,
        FlowCtrlConfig<WB, RB, RT>,
        RA,
        WA,
        RB,
        WB,
        S,
    >(rt, scope, tx_a, rx_a, tx_b, rx_b)
    .await;

    let drop_dock = Dock::new(1u32);
    let probe_dock = Dock::new(2u32);
    // 恰好一整个接收窗口：写完之后一定有数据帧在途（而不会把发送环写满到阻塞）。
    let fill = make_flow_payload_(0xD40F_0001u32, K_FLOW_CTRL_RING_CAPACITY);
    let small = make_flow_payload_(0xC0DE_0001u32, K_FLOW_CTRL_SMALL_LEN);
    let echo = make_flow_payload_(0xC0DE_0002u32, K_FLOW_CTRL_SMALL_LEN);

    let a_side = async {
        let mut binding = conn_a
            .bind_async(Dock::new(0x6001u32))
            .await
            .expect("A 侧绑定应当成功");

        // -- 子流 1：写满一整个窗口；`tx` 保持打开（本用例只验证「丢 rx」）。
        let mut opening: &[u8] = b"drop";
        let mut handle = binding
            .open_channel_async(drop_dock, &mut opening)
            .await
            .expect("A 侧发起子流 1 应当成功");
        let mut welcome_buf: [u8; 0] = [];
        let mut welcome: &mut [u8] = &mut welcome_buf[..];
        let (mut tx, _rx) = handle
            .accept_async_closure(&mut welcome, || {
                (
                    make_channel_buff_with_(K_FLOW_CTRL_RING_CAPACITY),
                    make_channel_buff_with_(K_FLOW_CTRL_RING_CAPACITY),
                )
            })
            .await
            .expect("A 侧子流 1 裁决应当成功");
        tx.write_all(&fill)
            .await
            .expect("写满一整个窗口应当成功");

        // -- 子流 2：同一条连接上的双向往返；连接被杀掉时这一步必然失败。
        let mut opening2: &[u8] = b"probe";
        let mut handle2 = binding
            .open_channel_async(probe_dock, &mut opening2)
            .await
            .expect("A 侧发起子流 2 应当成功");
        let (mut tx2, mut rx2) = handle2
            .accept_async_closure(&mut welcome, || {
                (
                    make_channel_buff_with_(K_FLOW_CTRL_RING_CAPACITY),
                    make_channel_buff_with_(K_FLOW_CTRL_RING_CAPACITY),
                )
            })
            .await
            .expect("A 侧子流 2 裁决应当成功");
        tx2.write_all(&small)
            .await
            .expect("子流 2 写入应当成功");
        drop(tx2);
        // 本端接收方向也要活着：B 会在同一条子流上回一份 `echo`。
        let mut got = vec![0u8; echo.len()];
        rx2.read_exact(&mut got)
            .await
            .expect("子流 2 必须能在对端丢弃另一条子流的接收半边后照常读完");
        assert_eq!(got, echo, "子流 2 的回程载荷必须逐字节完好");
        expect_eof_(&mut rx2).await;
    };

    let b_side = async {
        let mut listener_drop = conn_b
            .bind_async(drop_dock)
            .await
            .expect("B 侧绑定 dock 1 应当成功")
            .listen_async_default()
            .await
            .expect("B 侧监听 dock 1 应当成功");
        let mut incoming_drop = listener_drop
            .income_async()
            .await
            .expect("B 侧应当取到子流 1");
        let mut welcome_buf: [u8; 0] = [];
        let mut welcome: &mut [u8] = &mut welcome_buf[..];
        let (_tx_drop, rx_drop) = incoming_drop
            .accept_async_closure(&mut welcome, || {
                (
                    make_channel_buff_with_(K_FLOW_CTRL_RING_CAPACITY),
                    make_channel_buff_with_(K_FLOW_CTRL_RING_CAPACITY),
                )
            })
            .await
            .expect("B 侧子流 1 裁决应当成功");
        // **立刻丢掉接收半边**：此后到达子流 1 的数据帧都必须被静默丢弃。
        drop(rx_drop);

        // 子流 2：先收 A 的 `small`，回一份 `echo`，再等 EOF。连接被杀掉时这一步失败。
        let mut listener_probe = conn_b
            .bind_async(probe_dock)
            .await
            .expect("B 侧绑定 dock 2 应当成功")
            .listen_async_default()
            .await
            .expect("B 侧监听 dock 2 应当成功");
        let mut incoming_probe = listener_probe
            .income_async()
            .await
            .expect("B 侧应当取到子流 2");
        let (mut tx_probe, mut rx_probe) = incoming_probe
            .accept_async_closure(&mut welcome, || {
                (
                    make_channel_buff_with_(K_FLOW_CTRL_RING_CAPACITY),
                    make_channel_buff_with_(K_FLOW_CTRL_RING_CAPACITY),
                )
            })
            .await
            .expect("B 侧子流 2 裁决应当成功");
        let mut got = vec![0u8; small.len()];
        rx_probe
            .read_exact(&mut got)
            .await
            .expect("子流 2 必须能读完");
        assert_eq!(got, small, "子流 2 的载荷必须逐字节完好");
        tx_probe
            .write_all(&echo)
            .await
            .expect("子流 2 回程写入应当成功");
        drop(tx_probe);
        expect_eof_(&mut rx_probe).await;
    };

    futures::join!(a_side, b_side);
}
