//! 帧上限回归场景：**发送环里一次积压超过「一个帧装得下的量」时，写路径必须拆帧送出**，
//! 而不是判 `FrameTooLarge` 让连接失败。
//!
//! # 背景（本仓实测）
//!
//! 写循环每次最多从子流环搬走**一个逻辑读段**（上界 `K_MAX_DATA_CHUNK = 16 KiB`），却在
//! 搬之前检查 `段长 + 帧头 > max_packet_size`，一超就直接返回
//! `MuxError::FrameTooLarge`。应用只要一口气把发送环写满（一次积压 ≥
//! `max_packet_size − 帧头`），这条检查必然命中：连接被判失败，而连接级失败**不会唤醒**
//! 在册子流上的等待者，应用侧因此表现为**挂死**（0 CPU），看起来像丢唤醒。
//!
//! 缺省 `max_packet_size = 4096`、帧头上界 64，因此只要「一次积压 > 4032 字节」就会踩到。
//! 跨进程真机上一次 8 MiB 的写入必然踩到；本场景用「子流环 8 KiB + 一次写入 6 KiB」把
//! 它在**进程内**稳定复现，作为回归闸门。
//!
//! # 为什么需要看门狗
//!
//! 症状是挂死而不是断言失败：本场景在等读之前让出有限轮执行权，超限即 panic，
//! 于是回归时看到的是一条可读的 panic，而不是 CI 挂到超时。

use abs_smux::conn::{TrChannelListener, TrConnection, TrDockBinding};
use buffex::x_deps::abs_buff::{TrBuffRead, TrBuffWrite};
use futures::{
    future::{Either, select},
    join,
};
use smux_v1::connection::Dock;

use crate::common::{
    SmokeMuxConfig, TrSmokeRt, TrSmokeScope, connect_pair_, expect_eof_, read_channel_exact_,
    write_channel_all_,
};

/// 子流环容量（字节）：**大于**一次要写入的载荷，因此应用可以「一口气写满」。
const K_FRAME_CAP_RING: usize = 8usize * 1024usize;

/// 一次写入的载荷长度（字节）。
///
/// 必须满足 `K_FRAME_CAP_RING ≥ 它`（好让应用一次写完、不必等写循环腾地方）且
/// `它 > max_packet_size − K_MAX_FRAME_HEADER = 4032`（命中本场景要钉的那条路径）。
const K_FRAME_CAP_PAYLOAD: usize = 6usize * 1024usize;

/// 看门狗允许的「让出执行权」轮数上限。
///
/// 正常路径只需个位数轮（写循环当轮就把这段拆成两帧发完），这个值取得极宽松：
/// 它只用来把「挂死」变成一条 panic。
const K_FRAME_CAP_WATCHDOG_YIELDS: usize = 4096;

/// 让出一次执行权（让本地队列推进一轮）。
macro_rules! yield_once_ {
    () => {{
        let mut yielded = false;
        core::future::poll_fn(|cx| {
            if yielded {
                core::task::Poll::Ready(())
            } else {
                yielded = true;
                cx.waker().wake_by_ref();
                core::task::Poll::Pending
            }
        })
        .await
    }};
}

/// 让出 `rounds` 轮之后 panic 的看门狗。
async fn watchdog_(rounds: usize) {
    for _ in 0..rounds {
        yield_once_!();
    }
    panic!(
        "看门狗：发送环里一次积压超过一个帧的容量之后，数据没有被拆帧送出（\
         疑似撞上 FrameTooLarge 导致连接静默失败）"
    );
}

/// `connect_pair_` 需要的冒烟配置别名（只把两侧的传输类型参数化）。
type PassiveSmokeCfg_<W, R, RT> = SmokeMuxConfig<W, R, RT>;

/// 场景：建连 → 一条子流（环 [`K_FRAME_CAP_RING`]）→ 一次写入 [`K_FRAME_CAP_PAYLOAD`]
/// → 在有看门狗保护的等待里读回并逐字节比对 → 双向半关闭等 EOF。
///
/// # Panics
///
/// 建流失败、载荷不一致、半关闭后读不到 EOF，或看门狗超限（连接静默失败）都会 panic。
pub async fn run_frame_cap_scenario_<RA, WA, RB, WB, S, RT>(
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
        PassiveSmokeCfg_<WA, RA, RT>,
        PassiveSmokeCfg_<WB, RB, RT>,
        RA,
        WA,
        RB,
        WB,
        S,
    >(rt, scope, tx_a, rx_a, tx_b, rx_b)
    .await;

    let dock_b = Dock::new(0x7201u32);
    let mut listener_b = conn_b
        .bind_async(dock_b)
        .await
        .expect("B 侧绑定应当成功")
        .listen_async_default()
        .await
        .expect("B 侧监听应当成功");

    let dock_a = Dock::new(0x7202u32);
    let mut binding_a = conn_a
        .bind_async(dock_a)
        .await
        .expect("A 侧绑定应当成功");
    let mut message: &[u8] = &[];
    let mut handle = binding_a
        .open_channel_async(dock_b, &mut message)
        .await
        .expect("A 侧发起子流应当成功");

    // 两侧的最终裁决必须**并发**推进（主动方的 `OPEN` 在 `accept` 里才发出）。
    // 环容量两边都取 [`K_FRAME_CAP_RING`]：本场景钉的是**发送侧**读自己发送环时的那条路径。
    let (opened, accepted) = join!(
        async {
            let mut welcome: [u8; 0] = [];
            let mut welcome: &mut [u8] = &mut welcome[..];
            handle
                .accept_async_managed(&mut welcome, K_FRAME_CAP_RING)
                .await
        },
        async {
            let mut incoming = listener_b
                .income_async()
                .await
                .expect("B 侧应当取到入向请求");
            let mut welcome: [u8; 0] = [];
            let mut welcome: &mut [u8] = &mut welcome[..];
            incoming
                .accept_async_managed(&mut welcome, K_FRAME_CAP_RING)
                .await
        },
    );
    let (mut tx_a, mut rx_a) = opened.expect("A 侧最终裁决应当成功");
    let (tx_b, mut rx_b) = accepted.expect("B 侧最终裁决应当成功");

    // **一口气写满**：整段载荷一次写进发送环（环装得下），于是写循环下次看到的逻辑读段
    // 就是整段——正是触发条件。
    let payload = crate::common::make_flow_payload_(0x7202u32, K_FRAME_CAP_PAYLOAD);
    write_channel_all_(&mut tx_a, &payload)
        .await
        .expect("A 侧写入应当成功");

    let mut got = vec![0u8; K_FRAME_CAP_PAYLOAD];
    {
        let read_fut = core::pin::pin!(read_channel_exact_(&mut rx_b, &mut got));
        let guard_fut = core::pin::pin!(watchdog_(K_FRAME_CAP_WATCHDOG_YIELDS));
        match select(read_fut, guard_fut).await {
            // 读先完成：这段积压被正确地拆成了多帧。
            Either::Left((res, _guard)) => res.expect("B 侧读满应当成功"),
            // 看门狗先完成只有一种可能：它自己 panic 了。
            Either::Right((_, _read)) => unreachable!("看门狗已经 panic"),
        }
    }
    assert_eq!(got, payload[..], "读回的载荷应当与写入逐字节相等");

    // 收尾：半关闭 + 等 EOF。
    drop(tx_a);
    drop(tx_b);
    expect_eof_(&mut rx_a).await;
    expect_eof_(&mut rx_b).await;
}
