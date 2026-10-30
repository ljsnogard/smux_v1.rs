//! 优雅关闭（`TrConnection::close_async`）的端到端场景。
//!
//! # 验收点
//!
//! 1. **排空**：关闭**之前**已经写进子流、但还没上网的字节，必须在 `FIN` 之前全部
//!    送达对端——对端读到的字节与写入的逐字节相等；
//! 2. **对端确认**：`close_async` 只有在**对端也宣告不再发送**（本端收到它的
//!    `CLOSE(FIN)`）之后才返回 `true`；本端单方面关闭时它必须一直等到超时；
//! 3. **收尾**：返回之后连接已经断开——对端读到 EOF，本端不再受理新的子流
//!    （`bind_async` 拿到「连接已关闭」）。
//!
//! 场景与传输、运行时都无关：两个端点用两条内存环直连（同 `small_`），只是这里
//! 由**连接级**的 `close_async` 而不是「丢弃最后一个句柄」来收尾。

use buffex::x_deps::abs_buff::{
    TrBuffFlush, TrBuffRead, TrBuffTryWrite, TrBuffWrite,
    buffer::{TrProducerState, TrConsumerState},
};
use abs_art::TrDelay;
use abs_smux::{
    chan::TrChannelHalf,
    conn::{TrChannelListener, TrConnection, TrDockBinding},
};
use smux_v1::{
    connection::Dock,
    handshake::opts::BasicOpts,
};
use core::time::Duration;

use crate::common::{
    AcceptAsyncClosureExt,
    SmokeMuxConfig,
    TrSmokeRt,
    TrSmokeScope,
    connect_pair_,
    connect_pair_with_opts_,
    expect_eof_,
    make_channel_buff_,
    read_channel_exact_,
    try_write_channel_,
    write_channel_all_,
};

/// 关闭前写入的载荷长度。
///
/// 取 1024：**小于**一条子流环的容量（4096），因此排空一定能在「一次额度」内完成，
/// 用例不依赖窗口回补——它要验的是「关闭会等已提交数据上网」，不是流控。
const K_CLOSE_PAYLOAD_LEN: usize = 1024;

/// A 端用来发起子流的临时 `local_dock`。
const K_CLOSE_CLIENT_DOCK: u32 = 0x100;

/// B 端监听的 dock。
const K_CLOSE_SERVER_DOCK: u32 = 9;

/// 优雅关闭的**成功路径**：本端排空 → 对端确认 → 收尾，返回 `true`。
///
/// # 手段
///
/// 1. 两端用内存环直连建立连接，B 在 [`K_CLOSE_SERVER_DOCK`] 上监听，A 在
///    [`K_CLOSE_CLIENT_DOCK`] 上开一条子流并完成 `accept`；
/// 2. A 先写入 [`K_CLOSE_PAYLOAD_LEN`] 字节——**不**丢弃发送半边、也**不**读，
///    于是这批字节在关闭发起时还留在 A 的发送环里；
/// 3. A 调用 `close_async`（它要先排空、再发 `FIN`），**与此同时** B 读数据、等
///    A 的 `FIN`（读到 EOF）、随后丢弃自己的发送半边（向 A 发 `FIN`——这就是
///    A 等的「对端确认」）；
/// 4. 回到 A：断言 `close_async` 返回 `true`，且此后再 `bind_async` 会被拒绝。
///
/// # 判断
///
/// - B 读到的载荷与 A 写入的逐字节相等（**排空**成立）；
/// - B 在 A 的 `FIN` 之后读到 EOF（A 的发送方向确实收尾了）；
/// - A 的 `close_async` 返回 `true`（**对端确认**成立；若它提前返回、或等到超时，
///   本断言即失败——超时上限是协商的 `max_channel_timeout`，默认 30 秒，远大于本
///   场景的完成时间）；
/// - 关闭之后 `bind_async` 返回错误且不再是「成功绑定」（**收尾**成立）。
///
/// # Panics
///
/// 建流、读写、EOF 判定或上述任一断言不成立即 panic——失败即测试失败。
pub async fn run_graceful_close_scenario_<RA, WA, RB, WB, S, RT>(
    rt: &RT,
    scope: &S,
    tx_a: WA,
    rx_a: RA,
    tx_b: WB,
    rx_b: RB,
) -> bool
where
    RA: TrBuffRead<u8> + TrConsumerState + 'static,
    WA: TrBuffWrite<u8> + TrBuffFlush<u8> + TrProducerState + 'static,
    RB: TrBuffRead<u8> + TrConsumerState + 'static,
    WB: TrBuffWrite<u8> + TrBuffFlush<u8> + TrProducerState + 'static,
    S: TrSmokeScope + Clone + 'static,
    RT: TrSmokeRt,
{
    let (mut conn_a, conn_b) =
        connect_pair_::<
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
        )
        .await;

    // 1. 建一条子流：B 监听、A 发起。两侧的 `accept` 都必须完成，子流才真正露面。
    let b_side = async {
        let mut binding = conn_b
            .bind_async(Dock::new(K_CLOSE_SERVER_DOCK))
            .await
            .expect("B 绑定监听 dock 应当成功");
        let mut listener = binding
            .listen_async_default()
            .await
            .expect("B 建立 listener 应当成功");
        let mut handle = listener
            .income_async()
            .await
            .expect("B 应当取到入向建流请求");
        let mut welcome: &[u8] = &[];
        handle
            .accept_async_closure(&mut welcome, || {
                (make_channel_buff_(), make_channel_buff_())
            })
            .await
            .expect("B 接受子流应当成功")
    };
    let a_side = async {
        let mut binding = conn_a
            .bind_async(Dock::new(K_CLOSE_CLIENT_DOCK))
            .await
            .expect("A 绑定发起 dock 应当成功");
        let mut message: &[u8] = &[];
        let mut handle = binding
            .open_channel_async(Dock::new(K_CLOSE_SERVER_DOCK), &mut message)
            .await
            .expect("A 发起子流应当成功");
        let mut welcome: &[u8] = &[];
        handle
            .accept_async_closure(&mut welcome, || {
                (make_channel_buff_(), make_channel_buff_())
            })
            .await
            .expect("A 最终裁决子流应当成功")
    };
    let ((a_tx, a_rx), (b_tx, b_rx)) = futures::join!(a_side, b_side);

    // 2. A 写入待排空的载荷（先不关、不读），随后发起优雅关闭。
    let payload: Vec<u8> = (0..K_CLOSE_PAYLOAD_LEN)
        .map(|idx| (idx % 251usize) as u8)
        .collect();
    let a_close = async {
        let mut a_tx = a_tx;
        write_channel_all_(&mut a_tx, &payload)
            .await
            .expect("A 写入载荷应当成功");
        // 关键一步：此时载荷可能还在发送环里，`close_async` 必须先把它送上传输、
        // 再发 `FIN`，并一直等到对端也 `FIN`。
        conn_a.close_async().await
    };
    // 3. 对端：读全载荷 → 等 A 的 `FIN`（EOF）→ 丢弃发送半边（回 `FIN`）。
    let b_drain = async {
        let mut b_rx = b_rx;
        let mut got = vec![0u8; K_CLOSE_PAYLOAD_LEN];
        read_channel_exact_(&mut b_rx, &mut got)
            .await
            .expect("B 读取载荷应当成功");
        assert_eq!(got, payload, "优雅关闭之前已提交的载荷必须完整送达");
        expect_eof_(&mut b_rx).await;
        drop(b_tx);
    };
    let (closed, ()) = futures::join!(a_close, b_drain);

    // 4. 收尾之后不再受理新子流。
    let after_close = conn_a.bind_async(Dock::new(0x200u32)).await;
    assert!(
        after_close.is_err(),
        "连接关闭之后不应再受理新的 binding"
    );
    // A 的接收半边与发送半边在关闭之后都进入关闭态（此处只断言对象仍可用，
    // 「读到 EOF」由 B 侧那条更早的断言覆盖）。
    drop(a_rx);

    closed
}

/// 优雅关闭的**超时路径**：对端始终不宣告不再发送时，`close_async` 必须等到协商的
/// `max_channel_timeout`，走强制收尾，并返回 `false`。
///
/// # 为什么必须有一条「不成功」的用例
///
/// 「等对端确认」这条语义只能由反例钉住：若实现是「投递完收尾请求就返回 `true`」，
/// 上面那条成功路径照样通过。本用例把 `max_channel_timeout` 压到 1 秒（默认 30 秒），
/// 让「对端不回应」在可接受的时间内得出结论。
///
/// # 手段
///
/// 1. 两端用 **1 秒** `max_channel_timeout` 的协商项建连；
/// 2. A 开一条子流、写入少量载荷，随即 `close_async`；B 接受之后**永不**丢弃自己的
///    发送半边（因此永远不回 `FIN`），但**每 100 ms 写 1 字节**——目的是让 A 侧的
///    存活时钟持续刷新，计时循环的空闲超时不会抢先拆掉这条子流（否则本用例验的就
///    成了「空闲超时」，而不是「`close` 自己的超时」）；
/// 3. A 等待 `close_async` 返回。
///
/// # 判断
///
/// `close_async` 返回 `false`（超时走强制路径）。若它返回 `true`，说明「对端确认」
/// 被漏判了——那正是本用例要拦住的实现缺陷。
///
/// # Panics
///
/// 建流或读写失败即 panic；A 侧返回 `true` 时 panic（见上）。
pub async fn run_graceful_close_timeout_scenario_<RA, WA, RB, WB, S, RT>(
    rt: &RT,
    scope: &S,
    tx_a: WA,
    rx_a: RA,
    tx_b: WB,
    rx_b: RB,
) -> bool
where
    RA: TrBuffRead<u8> + TrConsumerState + 'static,
    WA: TrBuffWrite<u8> + TrBuffFlush<u8> + TrProducerState + 'static,
    RB: TrBuffRead<u8> + TrConsumerState + 'static,
    WB: TrBuffWrite<u8> + TrBuffFlush<u8> + TrProducerState + 'static,
    S: TrSmokeScope + Clone + 'static,
    RT: TrSmokeRt,
{
    // 只有超时上限被压低，其余协商量与默认用例逐字相同。
    let opts = BasicOpts {
        max_channel_timeout: Duration::from_secs(1u64),
        ..BasicOpts::DEFAULT
    };
    let (mut conn_a, conn_b) =
        connect_pair_with_opts_::<
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
            opts.clone(),
            opts,
        )
        .await;

    // 1. 建一条子流（与成功路径同一套步骤）。
    let b_side = async {
        let mut binding = conn_b
            .bind_async(Dock::new(K_CLOSE_SERVER_DOCK))
            .await
            .expect("B 绑定监听 dock 应当成功");
        let mut listener = binding
            .listen_async_default()
            .await
            .expect("B 建立 listener 应当成功");
        let mut handle = listener
            .income_async()
            .await
            .expect("B 应当取到入向建流请求");
        let mut welcome: &[u8] = &[];
        handle
            .accept_async_closure(&mut welcome, || {
                (make_channel_buff_(), make_channel_buff_())
            })
            .await
            .expect("B 接受子流应当成功")
    };
    let a_side = async {
        let mut binding = conn_a
            .bind_async(Dock::new(K_CLOSE_CLIENT_DOCK))
            .await
            .expect("A 绑定发起 dock 应当成功");
        let mut message: &[u8] = &[];
        let mut handle = binding
            .open_channel_async(Dock::new(K_CLOSE_SERVER_DOCK), &mut message)
            .await
            .expect("A 发起子流应当成功");
        let mut welcome: &[u8] = &[];
        handle
            .accept_async_closure(&mut welcome, || {
                (make_channel_buff_(), make_channel_buff_())
            })
            .await
            .expect("A 最终裁决子流应当成功")
    };
    let ((a_tx, _a_rx), (b_tx, b_rx)) = futures::join!(a_side, b_side);

    // 2. A 写入数据并关闭；B 保持子流活跃但**从不**回 FIN。
    //
    // 两个半部刻意留在**外层作用域**：B 的任务是「始终不宣告不再发送」，而
    // `drop(tx)` 会立刻替它发一条 `FIN`（半关闭是协议义务，见 README §5）——
    // 若把 `b_tx` move 进下面那个 future，循环一结束它就随 future 析构，本用例
    // 便退化成「对端其实回了 FIN」，什么都验不到。
    let mut b_tx = b_tx;
    let b_rx = b_rx;
    let a_close = async {
        let mut a_tx = a_tx;
        write_channel_all_(&mut a_tx, &[0x5Au8; 64usize])
            .await
            .expect("A 写入载荷应当成功");
        conn_a.close_async().await
    };
    let b_keepalive = async {
        // 上限 15 次 × 100 ms = 1.5 秒只是兜底（它必须盖过 A 侧那 1 秒超时）：
        // 正常情况下 A 排空后立刻发出 `FIN`，本端**接收环的生产端**随之被解复用
        // 循环关闭——`is_tx_closed` 读的正是那一位（`ChannelRx` 的「生产端」是
        // 写数据的对方），那就是「可以收工」的信号，循环随即退出。
        for _ in 0..15u32 {
            if write_channel_all_(&mut b_tx, &[0xA5u8]).await.is_err() {
                break;
            }
            rt.delay(Duration::from_millis(100u64)).await;
            if b_rx.is_tx_closed() {
                break;
            }
        }
    };
    let (closed, ()) = futures::join!(a_close, b_keepalive);
    closed
}

/// `drop` 的**尽力交付**验收：丢弃整条连接（最后一个应用面引用消失）之后，连接上
/// **已经提交但还没上网**的字节仍然必须被送出去。
///
/// # 这条用例为什么能区分新旧实现
///
/// 旧实现里 `MuxCore::drop` **立即**触发取消令牌：五个循环在下一个 await 点退出，
/// 发送环里还没搬走的字节随循环一起消失。新实现把它换成与 `close_async` 相同的状态
/// 驱动流程——置「等待关闭」、由计时循环推进排空与 `FIN`、等对端 `FIN` 收口。
///
/// 为了让「数据确实还在环里」这件事**确定**，载荷用非阻塞的
/// [`try_write_channel_`](crate::common::try_write_channel_) 写入，并且写入之后到丢弃
/// 连接之间**没有任何 await 点**：单线程本地执行器下循环拿不到运行机会，因此旧实现
/// 必然丢掉这批字节。
///
/// # 手段
///
/// 1. 两端内存环直连，A 开一条子流、B 接受；
/// 2. A 非阻塞写入 [`K_CLOSE_PAYLOAD_LEN`] 字节，随后**立即丢弃**自己全部的连接面
///    对象（两个半部、binding、连接）——核心就此析构；
/// 3. B 侧读到全载荷之后等 EOF，然后丢弃自己的发送半边（回 `FIN`，让 A 侧收口）。
///
/// # 判断
///
/// B 读到的载荷与 A 写入的**逐字节相等**。丢掉任何一个字节都说明「丢弃连接」仍然是
/// 「立即拆」，而不是「尽力把话说完」。
///
/// # Panics
///
/// 建流、非阻塞写入、读取或上述断言失败即 panic。
pub async fn run_drop_flush_scenario_<RA, WA, RB, WB, S, RT>(
    rt: &RT,
    scope: &S,
    tx_a: WA,
    rx_a: RA,
    tx_b: WB,
    rx_b: RB,
) where
    RA: TrBuffRead<u8> + TrConsumerState + 'static,
    WA: TrBuffWrite<u8> + TrBuffFlush<u8> + TrBuffTryWrite<u8> + TrProducerState + 'static,
    RB: TrBuffRead<u8> + TrConsumerState + 'static,
    WB: TrBuffWrite<u8> + TrBuffFlush<u8> + TrProducerState + 'static,
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
        >(
            rt,
            scope,
            tx_a,
            rx_a,
            tx_b,
            rx_b,
        )
        .await;

    // 1. 建一条子流（与成功路径同一套步骤）。
    let b_side = async {
        let mut binding = conn_b
            .bind_async(Dock::new(K_CLOSE_SERVER_DOCK))
            .await
            .expect("B 绑定监听 dock 应当成功");
        let mut listener = binding
            .listen_async_default()
            .await
            .expect("B 建立 listener 应当成功");
        let mut handle = listener
            .income_async()
            .await
            .expect("B 应当取到入向建流请求");
        let mut welcome: &[u8] = &[];
        let (tx, rx) = handle
            .accept_async_closure(&mut welcome, || {
                (make_channel_buff_(), make_channel_buff_())
            })
            .await
            .expect("B 接受子流应当成功");
        (binding, tx, rx)
    };
    let a_side = async {
        let mut binding = conn_a
            .bind_async(Dock::new(K_CLOSE_CLIENT_DOCK))
            .await
            .expect("A 绑定发起 dock 应当成功");
        let mut message: &[u8] = &[];
        let mut handle = binding
            .open_channel_async(Dock::new(K_CLOSE_SERVER_DOCK), &mut message)
            .await
            .expect("A 发起子流应当成功");
        let mut welcome: &[u8] = &[];
        let (tx, rx) = handle
            .accept_async_closure(&mut welcome, || {
                (make_channel_buff_(), make_channel_buff_())
            })
            .await
            .expect("A 最终裁决子流应当成功");
        (binding, tx, rx)
    };
    let ((a_binding, mut a_tx, a_rx), (b_binding, b_tx, mut b_rx)) = futures::join!(a_side, b_side);

    // 2. 非阻塞写入 + 立即丢弃全部连接面对象（中间没有 await）。
    let payload: Vec<u8> = (0..K_CLOSE_PAYLOAD_LEN)
        .map(|idx| (idx % 251usize) as u8)
        .collect();
    let put = try_write_channel_(&mut a_tx, &payload).expect("A 非阻塞写入应当成功");
    assert_eq!(put, payload.len(), "载荷应当一次全部装进发送环");
    // 两个半部与 `binding` 都持有连接克隆，必须**一起**丢弃——否则核心不会析构，本用例
    // 验的就不是「丢弃连接」这条路径了。丢弃之间没有任何 `await`：单线程本地执行器下
    // 循环拿不到运行机会，这批字节因此必然还压在发送环里（这正是旧实现丢数据的场景）。
    drop((a_tx, a_rx, a_binding, conn_a));

    // 3. 对端读全载荷 + EOF，然后回 `FIN`。
    let mut got = vec![0u8; K_CLOSE_PAYLOAD_LEN];
    read_channel_exact_(&mut b_rx, &mut got)
        .await
        .expect("B 读取载荷应当成功");
    assert_eq!(
        got, payload,
        "丢弃连接之后，已经提交的载荷必须仍然完整送达"
    );
    expect_eof_(&mut b_rx).await;
    drop((b_tx, b_binding, conn_b));
}
