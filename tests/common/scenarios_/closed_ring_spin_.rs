//! 回归场景：**「生产端已关闭且已排空」的发送环不得被复用循环当作「有数据可发」**，
//! 且**待收尾的子流不得被前面一条推不动的子流饿死**。
//!
//! # 背景（跨进程真机实测）
//!
//! 复用循环在「没有可发数据」时 park 在「最近一次收到 `TxReady` 的那条发送环」上，
//! 就绪判据原先只看环读 future 是否 `Ready`。但这条 future 在**生产端已关闭**
//! （应用 `drop(tx)` 之后环里已排空）时也会**立刻完成**（返回 `Closing`）——那不是
//! 「有数据可发」：`drain_one_` 一样取不到段。于是整圈在「回到顶部 → drain 失败 →
//! 立刻又就绪」之间**纯空转**：CPU 打满，同一条本地队列上的解复用循环、两个泵与
//! 应用任务全部被饿死，两端不再有任何字节流动，直到子流被空闲超时兜底拆掉。
//!
//! 触发它还需要一个伴生条件：**待收尾集合里有一条被阻塞的子流排在前面**。原先的
//! 收尾扫描每轮只取最小的一条、推不动就 `break`，于是后面那条「环已关闭且已排空」
//! 的子流一直留在本地表里，成了上面那条空转路径的**持久化来源**（真机现场与因果链
//! 见 `smux_v1_sock_demo/dev-notes/intermittent-stall-20261007-0200.md`）。
//!
//! # 本场景怎么把两个条件同时造出来
//!
//! - `P`（local dock 较小）：对端给出的窗口小于载荷、且**永不消费**，于是 `P` 的发送
//!   环里永远有积压、额度永远为 0——应用 `drop(tx)` 之后它进待收尾集合、**一直推不动**，
//!   并且因为键序在前，会挡住后面的条目；
//! - `Q`（local dock 较大）：载荷**略大于**窗口。应用写完就 `drop(tx)`（此时额度已用尽、
//!   环里还有余量 → 进待收尾集合），随后对端把已收到的部分读走 → 窗口回补 → 复用循环
//!   把 `Q` 环里剩下的字节发完，`Q` 于是变成「生产端已关闭 **且** 已排空、额度为正」。
//!
//! # 判据
//!
//! 对端必须在有界看门狗内读满 `Q` 的全部载荷**并读到 `EOF`**：
//!
//! - 只修「就绪判据」不修「收尾扫描」→ `Q` 的 `FIN` 被 `P` 饿死，看门狗 panic；
//! - 两处都不修 → 空转把看门狗一起饿死，用例挂到测试超时；
//! - 两处都修 → 对端读到完整载荷与 `EOF`，场景正常返回。

use crate::common::AcceptAsyncClosureExt;
use abs_smux::conn::{TrChannelListener, TrConnection, TrDockBinding};
use buffex::x_deps::abs_buff::{TrBuffRead, TrBuffWrite};
use futures::{
    future::{Either, select},
    join,
};
use smux_v1::connection::Dock;

use crate::common::{
    SmokeMuxConfig, TrSmokeRt, TrSmokeScope, connect_pair_, expect_eof_, make_flow_payload_,
    read_channel_exact_, write_channel_all_,
};

/// 对端（接收方）给出的接收窗口，同时就是它对端接收环的容量。
///
/// 取 [`K_CHANNEL_CAPACITY`](crate::common::K_CHANNEL_CAPACITY) 同量级的小值：本场景要
/// 让「额度用尽 → 环里留有余量 → 回补后发完」这条路径必然被走到。
const K_WINDOW: usize = 4096usize;

/// 发起方自己的子流环容量（收发两条环都是它）：要装得下本场景一次性写入的载荷。
const K_RING: usize = 16usize * 1024usize;

/// `P` 的载荷：**大于**窗口，因此额度用尽之后发送环里仍有积压（永远推不动）。
const K_PAYLOAD_P: usize = 8usize * 1024usize;

/// `Q` 的载荷：**略大于**窗口，好让「先发满窗口、回补之后再发完」发生。
const K_PAYLOAD_Q: usize = 6usize * 1024usize;

/// 每一步之间让出的执行权轮数（让本地队列把事件与环推进落实）。
const K_SETTLE_YIELDS: usize = 1024usize;

/// 看门狗允许的让出轮数上限：正常路径只需几百轮，这里取得极宽松。
const K_WATCHDOG_YIELDS: usize = 200_000usize;

/// 让出一次执行权（让本地队列推进一轮）。
async fn yield_once_() {
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
}

/// 让出 `rounds` 轮之后 panic 的看门狗（把「挂死」变成一条可读的失败）。
async fn watchdog_(rounds: usize) {
    for _ in 0..rounds {
        yield_once_().await;
    }
    panic!(
        "看门狗：生产端已关闭且已排空的发送环没有被正确收尾——\
         疑似复用循环把它误判成「可读」而空转，或它的 FIN 被前面推不动的子流饿死"
    );
}

/// 让出 [`K_SETTLE_YIELDS`] 轮。
async fn settle_() {
    for _ in 0..K_SETTLE_YIELDS {
        yield_once_().await;
    }
}

/// `connect_pair_` 需要的冒烟配置别名（只把两侧的传输类型参数化）。
type PassiveSmokeCfg_<W, R, RT> = SmokeMuxConfig<W, R, RT>;

/// 场景：建连 → 两条子流 `P` / `Q`（见模块文档）→ 应用写完即半关闭 → 对端读满 `Q`
/// 并等到 `EOF`。
///
/// # Panics
///
/// 建流 / 读写失败、载荷不一致、或看门狗超限（收尾被饿死或复用循环空转）都会 panic。
pub async fn run_closed_ring_spin_scenario_<RA, WA, RB, WB, S, RT>(
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

    // B 侧只绑一个监听 dock：两条入向子流都建在它上面（对端的临时 dock 区分身份）。
    let listener_dock = Dock::new(0x7401u32);
    let dock_p = Dock::new(0x7402u32);
    let dock_q = Dock::new(0x7403u32);

    let mut listener_b = conn_b
        .bind_async(listener_dock)
        .await
        .expect("B 侧绑定应当成功")
        .listen_async_default()
        .await
        .expect("B 侧监听应当成功");

    let mut binding_p = conn_a
        .bind_async(dock_p)
        .await
        .expect("A 侧绑定 P 应当成功");
    let mut binding_q = conn_a
        .bind_async(dock_q)
        .await
        .expect("A 侧绑定 Q 应当成功");

    // 先按 P、Q 的顺序发起：B 侧的入向队列因此也是 P 在前（`pending_fin` 的键序同理）。
    let mut empty: &[u8] = &[];
    let mut handle_p = binding_p
        .open_channel_async(listener_dock, &mut empty)
        .await
        .expect("A 侧发起 P 应当成功");
    let mut handle_q = binding_q
        .open_channel_async(listener_dock, &mut empty)
        .await
        .expect("A 侧发起 Q 应当成功");

    // 两条子流的最终裁决必须**并发**推进（主动方的 `OPEN` 在 `accept` 里才发出）。
    // A 侧环容量取 [`K_RING`]（装得下一次性写入的载荷），B 侧取 [`K_WINDOW`]
    // （它同时就是 B 向 A 通告的接收窗口）。
    let (opened_p, accepted_p) = join!(
        async {
            let mut welcome: [u8; 0] = [];
            let mut welcome: &mut [u8] = &mut welcome[..];
            handle_p.accept_async_closure(&mut welcome, || (crate::common::make_channel_buff_with_(K_RING), crate::common::make_channel_buff_with_(K_RING))).await
        },
        async {
            let mut incoming = listener_b
                .income_async()
                .await
                .expect("B 侧应当取到 P 的入向请求");
            let mut welcome: [u8; 0] = [];
            let mut welcome: &mut [u8] = &mut welcome[..];
            incoming.accept_async_closure(&mut welcome, || (crate::common::make_channel_buff_with_(K_WINDOW), crate::common::make_channel_buff_with_(K_WINDOW))).await
        },
    );
    let (opened_q, accepted_q) = join!(
        async {
            let mut welcome: [u8; 0] = [];
            let mut welcome: &mut [u8] = &mut welcome[..];
            handle_q.accept_async_closure(&mut welcome, || (crate::common::make_channel_buff_with_(K_RING), crate::common::make_channel_buff_with_(K_RING))).await
        },
        async {
            let mut incoming = listener_b
                .income_async()
                .await
                .expect("B 侧应当取到 Q 的入向请求");
            let mut welcome: [u8; 0] = [];
            let mut welcome: &mut [u8] = &mut welcome[..];
            incoming.accept_async_closure(&mut welcome, || (crate::common::make_channel_buff_with_(K_WINDOW), crate::common::make_channel_buff_with_(K_WINDOW))).await
        },
    );

    let (mut tx_p, mut _rx_p) = opened_p.expect("P 在 A 侧的最终裁决应当成功");
    let (tx_bp, _rx_bp) = accepted_p.expect("P 在 B 侧的最终裁决应当成功");
    let (mut tx_q, mut _rx_q) = opened_q.expect("Q 在 A 侧的最终裁决应当成功");
    let (_tx_bq, mut rx_bq) = accepted_q.expect("Q 在 B 侧的最终裁决应当成功");
    // 两条子流的四个「不会被本场景读写」的半部必须**活着**到场景结束：丢掉它们会给
    // 对端发多余的 `CLOSE`，把本场景要钉的收尾路径搅乱。
    let _keep_alive = (tx_bp, _rx_bp, _rx_p, _rx_q, _tx_bq);

    // A 侧一口气写完两条子流的载荷（都装得进自己的发送环）。
    let payload_p = make_flow_payload_(0x7402u32, K_PAYLOAD_P);
    let payload_q = make_flow_payload_(0x7403u32, K_PAYLOAD_Q);
    write_channel_all_(&mut tx_p, &payload_p)
        .await
        .expect("A 侧写 P 应当成功");
    write_channel_all_(&mut tx_q, &payload_q)
        .await
        .expect("A 侧写 Q 应当成功");

    // 让复用循环把「额度允许的那部分」发出去：P 发满窗口即停（环里仍有积压），
    // Q 也发满窗口（环里还剩一点点）。
    settle_().await;

    // 应用写完就半关闭：两条都进待收尾集合，而 P（键序在前）永远推不动。
    drop(tx_p);
    drop(tx_q);
    settle_().await;

    // B 侧把 Q 已收到的部分读走 → 窗口回补 → A 把 Q 剩下的字节发完，
    // 于是 Q 变成「生产端已关闭且已排空、额度为正」——修复前正是这里开始空转。
    //
    // 接收环容量只有 [`K_WINDOW`]，因此按窗口大小分步读：`exactly(6144)` 会超过环容量，
    // 那是 `Unsatisfiable`，不是本场景要钉的路径。
    let mut got_q = vec![0u8; K_PAYLOAD_Q];
    let mut off = 0usize;
    while off < K_PAYLOAD_Q {
        let step = core::cmp::min(K_WINDOW, K_PAYLOAD_Q - off);
        let read_fut = core::pin::pin!(read_channel_exact_(
            &mut rx_bq,
            &mut got_q[off..off + step]
        ));
        let guard_fut = core::pin::pin!(watchdog_(K_WATCHDOG_YIELDS));
        match select(read_fut, guard_fut).await {
            Either::Left((res, _guard)) => res.expect("B 侧读满 Q 应当成功"),
            // 看门狗先完成只有一种可能：它自己 panic 了。
            Either::Right((_, _read)) => unreachable!("看门狗已经 panic"),
        }
        off += step;
    }
    assert_eq!(got_q, payload_q[..], "Q 读回的载荷应当与写出的逐字节相等");

    // 关键判据：Q 的发送方向已收尾，对端必须读到 EOF（而不是被 P 饿死）。
    {
        let eof_fut = core::pin::pin!(expect_eof_(&mut rx_bq));
        let guard_fut = core::pin::pin!(watchdog_(K_WATCHDOG_YIELDS));
        match select(eof_fut, guard_fut).await {
            Either::Left(((), _guard)) => {}
            Either::Right((_, _eof)) => unreachable!("看门狗已经 panic"),
        }
    }
}
