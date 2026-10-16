//! 子流级**显式收尾**（`TrBuffFlush` / `TrChannelRx::close_async`）的端到端场景。
//!
//! # 验收点
//!
//! 1. **发送方向**：`TrBuffFlush::close_async` 的完成点是「`CLOSE(FIN)` 已经写进
//!    传输」，而不是「封口动作已经投递」——在 `FIN` 上线之前它必须挂起；
//! 2. **数据的顺序**：`close_async` 返回之后，对端必须能读到**全部**已提交载荷并
//!    随后读到 EOF（`FIN` 绝不越过未上网的数据）；
//! 3. **接收方向**：`TrChannelRx::close_async` 的完成点是「`CLOSE(RESET)` 已经写进
//!    传输」；返回之后本端接收方向已关（再读得到 `Closing`），而对端随后写入的字节
//!    被静默丢弃——**连接不得因此而判成协议错误**（历史回归点见 `layered_rpc` 的 F8）。
//!
//! 场景与运行时、与传输都无关：两个端点用两条内存环直连（同 `graceful_close_`）。

use abs_art::TrDelay;
use abs_smux::{
    chan::{TrChannelHalf, TrChannelRx, TrChannelTx},
    conn::{TrChannelListener, TrConnection, TrDockBinding},
};
use buffex::x_deps::abs_buff::{
    TrBuffFlush, TrBuffRead, TrBuffWrite,
    buffer::{TrConsumerState, TrProducerState},
};
use core::future::IntoFuture;
use futures::future::poll_immediate;
use smux_v1::connection::Dock;

use crate::common::{
    AcceptAsyncClosureExt,
    SmokeMuxConfig,
    TrSmokeRt,
    TrSmokeScope,
    connect_pair_,
    expect_eof_,
    make_channel_buff_,
    read_channel_exact_,
    write_channel_all_,
};

/// 发送方向用例的载荷长度。
///
/// 取 512：远小于一条子流环的容量（4096），因此「排空 + 发 `FIN`」不依赖窗口回补，
/// 本用例验的是收尾语义而不是流控。
const K_TX_CLOSE_PAYLOAD_LEN: usize = 512usize;

/// 子流 1 的临时 `local_dock`（发送方向用例）。
const K_CLOSE_CLIENT_DOCK_1: u32 = 0x301;

/// 子流 2 的临时 `local_dock`（接收方向用例）。
///
/// 与子流 1 必须不同：帧头没有 channel 标识字段，`(local_dock, remote_dock)` 就是
/// 身份（见 `tests/common/mod.rs` 的跨文件约定 3）。
const K_CLOSE_CLIENT_DOCK_2: u32 = 0x302;

/// 被动端监听的 dock（两条子流共用）。
const K_CLOSE_SERVER_DOCK: u32 = 7;

/// 显式收尾的场景主体：先验发送方向，再验接收方向。
///
/// # 手段
///
/// 1. 两端用内存环直连建立连接，B 在 [`K_CLOSE_SERVER_DOCK`] 上监听；A 用两个互不
///    相同的临时 dock 开两条子流，B 逐条 `accept`；
/// 2. **发送方向**（子流 1）：A 写入 [`K_TX_CLOSE_PAYLOAD_LEN`] 字节后调用
///    `TrBuffFlush::close_async`，并在**首次推进**时断言它仍挂起（此刻对端一个字节
///    都还没读走，`FIN` 不可能已经上线）；与此同时 B 读全载荷并等 EOF；
/// 3. **接收方向**（子流 2）：B 调用 `TrChannelRx::close_async`，同样在首次推进时
///    断言它仍挂起（`RESET` 还没上线）；随后断言接收方向已关、再读得到 `Closing`；
///    A 在这条子流上继续写入并封口，连接不得因此报错。
///
/// # 判断
///
/// - 两个 `close_async` 的**首次推进都必须是挂起**：若实现把「投递」当成「完成」，
///   本断言即失败——这正是本用例要拦住的缺陷；
/// - B 读到的载荷与 A 写入的逐字节相等，随后读到 EOF（数据先于 `FIN`）；
/// - B 关闭接收方向之后再读得到 `Closing`；A 侧继续写入并封口之后整个场景不 panic，
///   即连接没有进入失败态（否则后续动作会以传输错误收场）。
///
/// # Panics
///
/// 建流、读写、EOF 判定或上述任一断言不成立即 panic——失败即测试失败。
pub async fn run_channel_close_scenario_<RA, WA, RB, WB, S, RT>(
    rt: &RT,
    scope: &S,
    tx_a: WA,
    rx_a: RA,
    tx_b: WB,
    rx_b: RB,
)
where
    RA: TrBuffRead<u8> + TrConsumerState + 'static,
    WA: TrBuffWrite<u8> + TrBuffFlush<u8> + TrProducerState + 'static,
    RB: TrBuffRead<u8> + TrConsumerState + 'static,
    WB: TrBuffWrite<u8> + TrBuffFlush<u8> + TrProducerState + 'static,
    S: TrSmokeScope + Clone + 'static,
    RT: TrSmokeRt,
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
    )
    .await;

    // 1. 两条子流：B 监听一次、接受两次；A 用两个临时 dock 各开一条。
    let b_side = async {
        let mut binding = conn_b
            .bind_async(Dock::new(K_CLOSE_SERVER_DOCK))
            .await
            .expect("B 绑定监听 dock 应当成功");
        let mut listener = binding
            .listen_async_default()
            .await
            .expect("B 建立 listener 应当成功");
        let mut accepted = Vec::with_capacity(2usize);
        for _ in 0..2usize {
            let mut handle = listener
                .income_async()
                .await
                .expect("B 应当取到入向建流请求");
            let mut welcome_buf: [u8; 0] = [];
            let mut welcome: &mut [u8] = &mut welcome_buf[..];
            let halves = handle
                .accept_async_closure(&mut welcome, || {
                    (make_channel_buff_(), make_channel_buff_())
                })
                .await
                .expect("B 接受子流应当成功");
            accepted.push(halves);
        }
        let mut it = accepted.into_iter();
        let first = it.next().expect("两条子流都已接受");
        let second = it.next().expect("两条子流都已接受");
        (first, second)
    };
    let a_side = async {
        let mut opened = Vec::with_capacity(2usize);
        for local in [K_CLOSE_CLIENT_DOCK_1, K_CLOSE_CLIENT_DOCK_2] {
            let mut binding = conn_a
                .bind_async(Dock::new(local))
                .await
                .expect("A 绑定发起 dock 应当成功");
            let mut message: &[u8] = &[];
            let mut handle = binding
                .open_channel_async(Dock::new(K_CLOSE_SERVER_DOCK), &mut message)
                .await
                .expect("A 发起子流应当成功");
            let mut welcome_buf: [u8; 0] = [];
            let mut welcome: &mut [u8] = &mut welcome_buf[..];
            let halves = handle
                .accept_async_closure(&mut welcome, || {
                    (make_channel_buff_(), make_channel_buff_())
                })
                .await
                .expect("A 最终裁决子流应当成功");
            opened.push(halves);
        }
        let mut it = opened.into_iter();
        let first = it.next().expect("两条子流都已建立");
        let second = it.next().expect("两条子流都已建立");
        (first, second)
    };
    let (
        ((b_tx1, b_rx1), (b_tx2, b_rx2)),
        ((a_tx1, a_rx1), (a_tx2, a_rx2)),
    ) = futures::join!(b_side, a_side);

    // 2. 发送方向：A 写载荷 → `close_async` 等 `FIN` 上线；B 读全 + EOF。
    let payload: Vec<u8> = (0..K_TX_CLOSE_PAYLOAD_LEN)
        .map(|idx| (idx % 251usize) as u8)
        .collect();
    let a_close = async {
        let mut a_tx1 = a_tx1;
        write_channel_all_(&mut a_tx1, &payload)
            .await
            .expect("A 写入载荷应当成功");
        // `FIN` 必须等载荷全部上网之后才发，因此首次推进**一定**挂起：对端此刻甚至
        // 还没开始读。把「投递」当「完成」的实现会在这里立刻就绪。
        let mut close = core::pin::pin!(IntoFuture::into_future(
            TrBuffFlush::close_async(&mut a_tx1)
        ));
        assert!(
            poll_immediate(close.as_mut()).await.is_none(),
            "close_async 不应在 CLOSE(FIN) 上线之前就返回"
        );
        close.await.expect("A 的 close_async 应当成功");
    };
    let b_drain = async {
        let mut b_rx1 = b_rx1;
        let mut got = vec![0u8; K_TX_CLOSE_PAYLOAD_LEN];
        read_channel_exact_(&mut b_rx1, &mut got)
            .await
            .expect("B 读取载荷应当成功");
        assert_eq!(got, payload, "close_async 之前已提交的载荷必须完整送达");
        expect_eof_(&mut b_rx1).await;
    };
    futures::join!(a_close, b_drain);
    // 子流 1 的反向（B → A）没有期待，随作用域收尾即可。
    drop(a_rx1);
    drop(b_tx1);

    // 3. 接收方向：B 关闭接收半边并等 `RESET` 上线；A 继续写也不得让连接报错。
    let b_close = async {
        let mut b_rx2 = b_rx2;
        {
            let mut close = core::pin::pin!(IntoFuture::into_future(
                TrChannelRx::close_async(&mut b_rx2)
            ));
            assert!(
                poll_immediate(close.as_mut()).await.is_none(),
                "close_async 不应在 CLOSE(RESET) 上线之前就返回"
            );
            close.await.expect("B 的 close_async 应当成功");
        }
        assert!(
            b_rx2.is_rx_closed(),
            "close_async 返回时接收方向应当已经关闭"
        );
        // 消费端已关：再读得到 `Closing`，既不是挂起、也不是数据。
        expect_eof_(&mut b_rx2).await;
        drop(b_tx2);
    };
    let a_write2 = async {
        let mut a_tx2 = a_tx2;
        // 对端已宣告不再接收：这些字节会被静默丢弃，连接**不得**判成协议错误。
        let _ = write_channel_all_(&mut a_tx2, &[1u8, 2u8, 3u8]).await;
        // 封口。对端已 `RESET` 时这一步无需等到 `FIN` 上线，两种路径都应当成功。
        let _ = TrBuffFlush::close_async(&mut a_tx2).await;
        drop(a_rx2);
    };
    futures::join!(b_close, a_write2);
}
