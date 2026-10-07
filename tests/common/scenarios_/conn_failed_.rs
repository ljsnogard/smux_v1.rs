//! 连接级失败回归场景：**判定不可恢复之后，所有在册子流都必须被唤醒，并读到原因**。
//!
//! # 这条用例要钉住什么
//!
//! 连接级失败此前**不唤醒在册子流上的读写等待者**：应用停在 `read_async` /
//! `write_async` 上无期限挂起，只能靠子流空闲超时兜底（见
//! `smux_v1_sock_demo/dev-notes/known-stall-20261007-0130.md` §3.2）。现在要求两件事
//! 同时成立：
//!
//! 1. **醒得来**：所有在册子流的两个方向都被唤醒。`buffex` 的环半部 **drop 不置关闭
//!    位、也不唤醒对端**，所以「循环退出把表丢掉」本身不产生任何唤醒——必须由持有
//!    那一半的循环在退出时显式 `close()`；
//! 2. **说得清**：应用被唤醒后读到的不是笼统的 `Closing`，而是
//!    `abort_reason() == Some(ConnFailed(..))`——连接级原因压过子流级原因。
//!
//! # 怎么制造一次**确定性**的连接级失败
//!
//! 用 [`FailableTx_`] 包住 A 侧的传输**写**半边：置位之后所有写都返回
//! `ProducerError::Closing`。连接内部的写泵拿到它就 `fail_pump_` →
//! `mark_failed_(Transport { write: true })`，整条连接终结。触发时机由用例完全控制：
//! 先让各子流双向阻塞，再置位故障、丢掉一条子流的发送半边——那条 `CLOSE(FIN)` 会让
//! 写泵去碰传输，从而把这次失败兜出来。因此不依赖任何竞态。
//!
//! # 判据
//!
//! A 侧每条子流的读与写都在有界看门狗内返回错误，且两个半部的 `abort_reason()` 都是
//! `ConnFailed(Transport)`。挂起会先被看门狗抓成 panic（本用例在修复前正是**挂死**）。

use core::{
    cell::Cell,
    future::Future,
    marker::PhantomData,
    pin::Pin,
    task::{Context, Poll},
};
use std::rc::Rc;

use abs_smux::{
    chan::TrChannelHalf,
    conn::{TrChannelListener, TrConnection, TrDockBinding},
};
use buffex::{
    ring::ProducerError,
    x_deps::{
        abs_buff::{Demand, TrBuffRead, TrBuffTryWrite, TrBuffWrite},
        abs_cancel,
        anylr::SomeOf,
    },
};
use futures::{
    future::{Either, join_all, select},
    join,
};
use smux_v1::{
    connection::{Dock, MuxError},
    metrics::ConnCloseReason,
};

use crate::common::{
    SmokeMuxConfig, TrSmokeRt, TrSmokeScope, connect_pair_, make_flow_payload_,
    read_channel_exact_, write_channel_all_,
};

/// 子流条数：每条都会「双向阻塞」，因此每条都必须被唤醒、并读到原因。
const K_CHANNELS: usize = 3usize;

/// 两侧都用的子流环容量（同时也是各自向对端通告的接收窗口）。
const K_RING: usize = 4096usize;

/// 每条子流要写的字节数：**大于「环容量 + 初始窗口」**，所以写方向必然阻塞。
const K_WRITE_LEN: usize = 64usize * 1024usize;

/// 让出执行权让本地队列推进的轮数（建流、让额度用尽、让 `CLOSE` 上线触发失败）。
const K_SETTLE_YIELDS: usize = 2048usize;

/// 看门狗允许的让出轮数上限：正常路径只需几千轮，这里取得极宽松。
const K_WATCHDOG_YIELDS: usize = 400_000usize;

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// 故障注入：可置位的传输写半边
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

/// 可置位故障的传输**写**半边。
///
/// 未置位时逐字节转发（连握手都用它）；置位后 `try_write` / `write_async` 一律返回
/// `ProducerError::Closing`——对连接来说这就是「传输写失败」，于是写泵 `fail_pump_`
/// 把连接判死。内部持 `Rc<Cell<bool>>`（`!Send`），顺带再次证明「本地投递不要求
/// `Send`」。
struct FailableTx_<H> {
    /// 被包住的真实写半边。
    half_: H,

    /// 故障标志（用例持有另一半句柄，置位即注入失败）。
    broken_: Rc<Cell<bool>>,
}

impl<H> FailableTx_<H> {
    /// 包住一个写半边，并返回共享的故障标志。
    fn new_(half: H) -> (Self, Rc<Cell<bool>>) {
        let broken = Rc::new(Cell::new(false));
        (
            FailableTx_ {
                half_: half,
                broken_: broken.clone(),
            },
            broken,
        )
    }
}

impl<H> TrBuffTryWrite<u8> for FailableTx_<H>
where
    H: TrBuffTryWrite<u8, Err = ProducerError<usize>>,
{
    type SegmMut<'f>
        = H::SegmMut<'f>
    where
        Self: 'f;

    type Err = ProducerError<usize>;

    fn try_write<'f>(
        &'f mut self,
        demand: &'f Demand<usize>,
    ) -> SomeOf<Self::SegmMut<'f>, Self::Err> {
        if self.broken_.get() {
            return SomeOf::new_right(ProducerError::Closing);
        }
        self.half_.try_write(demand)
    }
}

impl<H> TrBuffWrite<u8> for FailableTx_<H>
where
    H: TrBuffWrite<u8, Err = ProducerError<usize>>,
{
    type WriteAsync<'f>
        =
        FailableWrite_<<H::WriteAsync<'f> as core::future::IntoFuture>::IntoFuture, H::SegmMut<'f>>
    where
        Self: 'f;

    fn write_async<'f>(&'f mut self, demand: &'f Demand<usize>) -> Self::WriteAsync<'f> {
        FailableWrite_ {
            fut_: core::future::IntoFuture::into_future(self.half_.write_async(demand)),
            broken_: self.broken_.clone(),
            _segm_: PhantomData,
        }
    }
}

/// [`FailableTx_`] 的异步等待体：置位后立刻给出「写失败」，否则转发内层 future。
///
/// 与 `tests/inmem_mux.rs` 的 `IgnoresCancel_` 同款：只做转发，**不登记**取消等待者
/// （测试替身不需要；连接内部的每一次 park 都自己与取消令牌竞争）。
struct FailableWrite_<F, S> {
    /// 被包住的真实 future。
    fut_: F,

    /// 故障标志（与 [`FailableTx_`] 共享）。
    broken_: Rc<Cell<bool>>,

    /// 借出段的类型占位。
    _segm_: PhantomData<fn() -> S>,
}

impl<F, S> Future for FailableWrite_<F, S>
where
    F: Future<Output = SomeOf<S, ProducerError<usize>>>,
{
    type Output = SomeOf<S, ProducerError<usize>>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // SAFETY: 本类型是普通结构体、无自引用，且不实现 `Unpin` 之外的投影逻辑；
        // 把 `self` 的固定性传给内部字段是 `pin-project` 标准做法的手工等价形式
        // （与 `tests/inmem_mux.rs` 的 `IgnoresCancel_` 逐字同款）。
        let this = unsafe { self.get_unchecked_mut() };
        if this.broken_.get() {
            return Poll::Ready(SomeOf::new_right(ProducerError::Closing));
        }
        let fut = unsafe { Pin::new_unchecked(&mut this.fut_) };
        fut.poll(cx)
    }
}

impl<'a, F, S> abs_cancel::TrMayCancel<'a> for FailableWrite_<F, S>
where
    F: Future<Output = SomeOf<S, ProducerError<usize>>> + 'a,
    S: 'a,
{
    type MayCancelFuture<'f, C>
        = FailableWrite_<F, S>
    where
        'f: 'a,
        Self: 'f,
        C: 'f + abs_cancel::TrCancellationToken;

    type MayCancelOutput = F::Output;

    /// 恒等变换：**不登记**任何取消等待者。
    fn may_cancel_with<C>(self, _cancel: C) -> Self::MayCancelFuture<'a, C>
    where
        C: 'a + abs_cancel::TrCancellationToken,
    {
        self
    }
}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// 场景
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

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

/// 让出 `K_SETTLE_YIELDS` 轮。
async fn settle_() {
    for _ in 0..K_SETTLE_YIELDS {
        yield_once_().await;
    }
}

/// 让出 `rounds` 轮就返回的看门狗：超时**不在这里 panic**，把「等超时了」交回调用方
/// ——调用方要先打印现场（各半部的关闭态与原因）再判失败。
async fn watchdog_(rounds: usize) {
    for _ in 0..rounds {
        yield_once_().await;
    }
}

/// 场景：建连（A 的传输写半部可注入故障）→ 建 `K_CHANNELS` 条子流 → 各子流双向阻塞
/// → 注入传输写失败让连接判死 → 断言每条子流都被唤醒且原因正确。
///
/// # Panics
///
/// 建流 / 读写装配失败、看门狗超限（挂死）、或唤醒后的原因不是连接级失败，都会 panic。
pub async fn run_conn_failed_wakes_scenario_<RA, WA, RB, WB, S, RT>(
    rt: &RT,
    scope: &S,
    tx_a: WA,
    rx_a: RA,
    tx_b: WB,
    rx_b: RB,
) where
    RA: TrBuffRead<u8> + 'static,
    WA: TrBuffWrite<u8> + TrBuffTryWrite<u8, Err = ProducerError<usize>> + 'static,
    RB: TrBuffRead<u8> + 'static,
    WB: TrBuffWrite<u8> + 'static,
    S: TrSmokeScope + Clone + 'static,
    RT: TrSmokeRt,
{
    // A 侧的传输写半边包上故障注入（未置位时完全透明，握手照常走）。
    let (tx_a, broken) = FailableTx_::new_(tx_a);
    let (conn_a, conn_b) = connect_pair_::<
        SmokeMuxConfig<FailableTx_<WA>, RA, RT>,
        SmokeMuxConfig<WB, RB, RT>,
        RA,
        FailableTx_<WA>,
        RB,
        WB,
        S,
    >(rt, scope, tx_a, rx_a, tx_b, rx_b)
    .await;

    let listener_dock = Dock::new(0x7601u32);
    let mut listener_b = conn_b
        .bind_async(listener_dock)
        .await
        .expect("B 侧绑定应当成功")
        .listen_async()
        .await
        .expect("B 侧监听应当成功");

    // A 侧：每条子流一个互不相同的临时 local dock，全部指向 B 的监听 dock。
    let mut bindings = Vec::with_capacity(K_CHANNELS);
    let mut handles = Vec::with_capacity(K_CHANNELS);
    for i in 0..K_CHANNELS {
        let dock = Dock::new(0x7610u32 + i as u32);
        let mut binding = conn_a.bind_async(dock).await.expect("A 侧绑定应当成功");
        let mut empty: &[u8] = &[];
        let handle = binding
            .open_channel_async(listener_dock, &mut empty)
            .await
            .expect("A 侧发起子流应当成功");
        bindings.push(binding);
        handles.push(handle);
    }

    // 逐条并发裁决（A 侧的 accept 与 B 侧的 income/accept 必须同时推进）。
    let mut a_channels = Vec::with_capacity(K_CHANNELS);
    let mut b_channels = Vec::with_capacity(K_CHANNELS);
    for mut handle in handles {
        let (opened, accepted) = join!(
            async {
                let mut welcome: [u8; 0] = [];
                let mut welcome: &mut [u8] = &mut welcome[..];
                handle.accept_async_managed(&mut welcome, K_RING).await
            },
            async {
                let mut incoming = listener_b
                    .income_async()
                    .await
                    .expect("B 侧应当取到入向请求");
                let mut welcome: [u8; 0] = [];
                let mut welcome: &mut [u8] = &mut welcome[..];
                incoming.accept_async_managed(&mut welcome, K_RING).await
            },
        );
        a_channels.push(opened.expect("A 侧最终裁决应当成功"));
        b_channels.push(accepted.expect("B 侧最终裁决应当成功"));
    }

    // 最后一条子流**不参与收发**，专门用来在注入故障之后上线一条 `CLOSE(FIN)`：
    // 写泵只有「有字节要写」时才会去碰传输，没有这条帧，注入的失败不会被兜出来。
    let (trigger_tx, trigger_rx) = a_channels.pop().expect("至少有一条触发用子流");
    let b_trigger = b_channels.pop().expect("B 侧同一条子流");

    // 其余子流：B 侧从不读、也不写，于是 A 侧两条方向都阻塞在环上
    // （读等 B 的数据；写因对端不读、窗口用尽而停在环满）——正是要被唤醒的等待者。
    let write_payload = make_flow_payload_(0x76F0u32, K_WRITE_LEN);
    let a_ops = async {
        join_all(a_channels.iter_mut().map(|(tx, rx)| {
            let payload = &write_payload;
            async move {
                let mut sink = vec![0u8; K_RING];
                let read_fut = read_channel_exact_(rx, &mut sink);
                let write_fut = write_channel_all_(tx, payload);
                join!(read_fut, write_fut)
            }
        }))
        .await
    };

    // 触发：先让位（A 侧各子流的「环满 + 额度为零」就位），再注入写失败，
    // 最后丢掉触发子流的发送半边——其 `CLOSE(FIN)` 会让写泵去碰传输并失败。
    let trigger_ops = async {
        settle_().await;
        broken.set(true);
        drop(trigger_tx);
        settle_().await;
    };

    let outcomes = {
        let both = core::pin::pin!(async {
            let (ops, ()) = join!(a_ops, trigger_ops);
            ops
        });
        let guard = core::pin::pin!(watchdog_(K_WATCHDOG_YIELDS));
        match select(both, guard).await {
            Either::Left((outcomes, _guard)) => Option::Some(outcomes),
            Either::Right((_, _ops)) => Option::None,
        }
    };
    let Some(outcomes) = outcomes else {
        for (idx, (tx, rx)) in a_channels.iter().enumerate() {
            eprintln!(
                "[diag] 子流 {idx}: tx_reason={:?} rx_reason={:?} \
                 tx_tx_closed={} tx_rx_closed={} rx_tx_closed={} rx_rx_closed={}",
                tx.abort_reason(),
                rx.abort_reason(),
                tx.is_tx_closed(),
                tx.is_rx_closed(),
                rx.is_tx_closed(),
                rx.is_rx_closed(),
            );
        }
        panic!(
            "看门狗：连接级失败之后，在册子流上的等待者没有被唤醒\
             （应用会无期限挂起，直到子流空闲超时兜底）"
        );
    };

    // 每条子流的两个方向都必须返回错误（而不是挂住）。
    for (idx, (read_res, write_res)) in outcomes.into_iter().enumerate() {
        assert!(
            read_res.is_err(),
            "子流 {idx} 的读方向应当在连接级失败后返回错误"
        );
        assert!(
            write_res.is_err(),
            "子流 {idx} 的写方向应当在连接级失败后返回错误"
        );
    }

    // 原因必须落到**连接级失败**上（压过任何子流级原因）。
    let want = Option::Some(MuxError::ConnFailed(ConnCloseReason::Transport));
    for (idx, (tx, rx)) in a_channels.iter().enumerate() {
        assert_eq!(
            tx.abort_reason(),
            want,
            "子流 {idx} 的发送半部应当读到连接级失败"
        );
        assert_eq!(
            rx.abort_reason(),
            want,
            "子流 {idx} 的接收半部应当读到连接级失败"
        );
    }
    // 触发子流的发送半部已经丢弃；接收半部同样应当读到连接级失败。
    assert_eq!(trigger_rx.abort_reason(), want, "触发子流的接收半部");

    // 把不参与断言的句柄活到场景结束（B 侧半部与 binding 都是连接状态的一部分）。
    drop((bindings, b_channels, b_trigger, trigger_rx));
}
