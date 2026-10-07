//! 丢唤醒回归场景：**安静的连接上，第一次写入低于临界水位也必须被搬运**。
//!
//! # 背景
//!
//! [`ChannelTx::notify_tx_ready_`] 原来只在「积压超过临界水位（`容量 / 4`）」或该状态的
//! **边沿**上发 `TxReady`。于是一条安静子流**第一次**写入 512 B（< `4096 / 4 = 1024`）
//! 时一条事件都不发：写循环此时 `last_ready` 为空（或指向别的子流），既收不到事件、
//! 也没有对应的子流环可供 park，环里的数据就此无人搬运——**丢唤醒**。
//! 该路径由 `tests/alloc_count.rs` 的基线用例稳定复现（单条安静子流写 512 B 直接挂死）。
//!
//! 现在「进入写入时环为空」也通知（去重位保证每条子流至多一条待处理事件），本场景把它
//! 钉住。
//!
//! # 为什么需要看门狗
//!
//! 丢唤醒的表现是**挂死**而不是断言失败。本场景在读之前让出有限轮执行权，超限即 panic：
//! 于是回归时看到的是一条可读的 panic，而不是 CI 挂到超时。
//!
//! [`ChannelTx::notify_tx_ready_`]: smux_v1::connection::ChannelTx

use crate::common::AcceptAsyncClosureExt;
use abs_smux::conn::{TrChannelListener, TrConnection, TrDockBinding};
use buffex::x_deps::abs_buff::{TrBuffRead, TrBuffWrite};
use futures::{
    future::{Either, select},
    join,
};
use smux_v1::connection::Dock;

use crate::common::{
    K_CHANNEL_CAPACITY, SmokeMuxConfig, TrSmokeRt, TrSmokeScope, connect_pair_, expect_eof_,
    read_channel_exact_, write_channel_all_,
};

/// 本场景的载荷长度（字节）。
///
/// 必须**低于**发送环的临界水位（`K_CHANNEL_CAPACITY / 4 = 1024`），否则命中的是
/// 「积压超过临界水位」那条**会**通知的路径，测不到本场景要钉的东西。
const K_IDLE_PAYLOAD: usize = 512;

/// 看门狗允许的「让出执行权」轮数上限。
///
/// 正常路径只需个位数轮（写循环当轮就会把数据搬走），这个值取得极宽松：它只用来把
/// 「挂死」变成一条 panic。
const K_IDLE_WATCHDOG_YIELDS: usize = 4096;

/// 让出一次执行权（让本地队列推进一轮）。
///
/// 与 `tests/inmem_mux.rs` 里的同名宏同义，这里自带一份是因为场景文件不依赖那个目标。
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
    panic!("看门狗：安静的连接上低于临界水位的首次写入没有被搬运（疑似丢唤醒）");
}

/// `connect_pair_` 需要的冒烟配置别名（只把两侧的传输类型参数化）。
type PassiveSmokeCfg_<W, R, RT> = SmokeMuxConfig<W, R, RT>;

/// 场景：建连 → 一条静默子流 → 写 512 B → 在有看门狗保护的等待里读回。
///
/// # Panics
///
/// 建流失败、载荷不一致、半关闭后读不到 EOF，或看门狗超限（丢唤醒）都会 panic。
pub async fn run_idle_small_write_scenario_<RA, WA, RB, WB, S, RT>(
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
    >(rt, scope, tx_a, rx_a, tx_b, rx_b, crate::common::make_stage_buffs_(), crate::common::make_stage_buffs_())
    .await;

    let dock_b = Dock::new(0x7101u32);
    let mut listener_b = conn_b
        .bind_async(dock_b)
        .await
        .expect("B 侧绑定应当成功")
        .listen_async_default()
        .await
        .expect("B 侧监听应当成功");

    let dock_a = Dock::new(0x7102u32);
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
    let (opened, accepted) = join!(
        async {
            let mut welcome: [u8; 0] = [];
            let mut welcome: &mut [u8] = &mut welcome[..];
            handle
                .accept_async_closure(&mut welcome, || (crate::common::make_channel_buff_with_(K_CHANNEL_CAPACITY), crate::common::make_channel_buff_with_(K_CHANNEL_CAPACITY)))
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
                .accept_async_closure(&mut welcome, || (crate::common::make_channel_buff_with_(K_CHANNEL_CAPACITY), crate::common::make_channel_buff_with_(K_CHANNEL_CAPACITY)))
                .await
        },
    );
    let (mut tx_a, mut rx_a) = opened.expect("A 侧最终裁决应当成功");
    let (tx_b, mut rx_b) = accepted.expect("B 侧最终裁决应当成功");

    // 安静的连接：此前没有任何数据往来，`last_ready` 为空。
    let payload = crate::common::make_flow_payload_(0x7101u32, K_IDLE_PAYLOAD);
    write_channel_all_(&mut tx_a, &payload)
        .await
        .expect("A 侧写入应当成功");

    // 读之前先让出一次执行权：把「写循环还没被调度」与「写循环收不到通知」区分开。
    yield_once_!();

    let mut got = [0u8; K_IDLE_PAYLOAD];
    {
        let read_fut = core::pin::pin!(read_channel_exact_(&mut rx_b, &mut got));
        let guard_fut = core::pin::pin!(watchdog_(K_IDLE_WATCHDOG_YIELDS));
        match select(read_fut, guard_fut).await {
            // 读先完成：丢唤醒不存在（载荷随后逐字节比对）。
            Either::Left((res, _guard)) => res.expect("B 侧读满应当成功"),
            // 看门狗先完成只有一种可能：它自己 panic 了。
            Either::Right((_, _read)) => unreachable!("看门狗已经 panic"),
        }
    }
    assert_eq!(got, payload[..], "读回的载荷应当与写入逐字节相等");

    // 收尾：半关闭 + 等 EOF（顺带覆盖「安静子流」的收尾路径）。
    drop(tx_a);
    drop(tx_b);
    expect_eof_(&mut rx_a).await;
    expect_eof_(&mut rx_b).await;
}
