//! 进程内（无 socket）的复用连接验收测试。
//!
//! # 为什么另起一个测试目标
//!
//! `tests/smoke_*.rs` 把连接挂在「真实 UNIX socket + 调用方驱动的泵 + 全被动环」
//! 的传输上；该装配在**并发**场景下会出现「一侧的 socket 读不再被唤醒」的问题
//! （见 dev-notes `connection-20261002-0548.md` §7 的记录），与本轮要验收的连接层
//! 行为无关。本文件改用**进程内直连**：两条全被动环分别承载 A→B 与 B→A，四个环
//! 半部直接交给两个 `MuxConnection` 当作 `Rx` / `Tx`，中间没有任何泵。
//!
//! 这样既保留了「两个真实端点、双向并发、真实运行时任务」的全部连接层行为，
//! 又把传输层的未知问题隔离在外。
//!
//! # 运行方式：作用域由调用方取得并驱动
//!
//! 连接的两个循环经 `abs_art` 的 [`TrLocalScope`] **值**投递（不再有运行时类型
//! 参数），因此每个用例自己做两件事：取得作用域、驱动它。tokio 用
//! `scope.run_until(..)`，compio 的运行时自己驱动线程本地队列。

mod common;

use core::cell::Cell;
use std::rc::Rc;

use abs_art::TrLocalScope;
use abs_art_tokio::LocalScope;
use abs_smux::conn::TrConnection;
use buffex::x_deps::abs_buff::{Demand, TrBuffRead, TrBuffTryRead, TrBuffTryWrite, TrBuffWrite};
use buffex::x_deps::anylr::SomeOf;
use buffex::x_deps::abs_cancel;
use smux_v1::{
    connection::{Dock, MuxConnection},
    handshake::{
        agent::{AcceptAllEntries, HandshakeAgent},
        opts::BasicOpts,
    },
};

/// 测试目标（**本轮验收点**）：两个端点之间 2 个 dock × 各 2 条 channel 并发通信。
///
/// - 手段：用 `make_passive_ring_` 建两条容量 64 KiB 的内存环（一条 A→B、一条
///   B→A），取得一个 tokio `LocalScope` 并把它与四个半部一起交给
///   [`common::run_small_mux_scenario_`]；后者完成握手、建立两个 `MuxConnection`
///   （内部各自经作用域 `spawn_local` 读 / 写循环），并在 `1..=2` 两个 dock 上并发
///   跑 4 条 open + 4 条 accept（每条子流使用互不相同的临时 local_dock），双向收发
///   后丢弃发送半边、在对端等 EOF。整个场景由 `scope.run_until` 驱动。
/// - 判断：所有 open / accept 成功；每条子流的载荷逐字节相等；半关闭后读到
///   `Closing`（EOF）。任一不满足即 panic。
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mux_small_inmem_tokio_() {
    let (a_tx, b_rx) = common::make_passive_ring_(common::K_NET_BUFFER_SIZE);
    let (b_tx, a_rx) = common::make_passive_ring_(common::K_NET_BUFFER_SIZE);

    // tokio 的本地队列归作用域值所有，由 `run_until` 驱动；连接在核心留了一份克隆
    // 保活，因此队列不会先于连接消失。
    let scope = LocalScope::new();
    let scenario = common::run_small_mux_scenario_(&scope, a_rx, a_tx, b_rx, b_tx);
    scope.run_until(scenario).await;
}

/// 测试目标：与 tokio 版逐字相同的验收场景，改用 **compio 运行时**。
///
/// - 手段：同样用两条内存环直连两个端点，交给
///   [`common::run_small_mux_scenario_`]；区别只是作用域换成
///   `abs_art_compio::LocalScope`（零大小，因为 compio 的队列归运行时所有并由它
///   自己驱动），并跑在 `#[compio::test]` 里。
/// - 判断：与 tokio 版相同——载荷逐字节相等、半关闭后读到 EOF。
#[compio::test]
async fn mux_small_inmem_compio_() {
    let (a_tx, b_rx) = common::make_passive_ring_(common::K_NET_BUFFER_SIZE);
    let (b_tx, a_rx) = common::make_passive_ring_(common::K_NET_BUFFER_SIZE);

    let scope = abs_art_compio::LocalScope::new();
    let scenario = common::run_small_mux_scenario_(&scope, a_rx, a_tx, b_rx, b_tx);
    scope.run_until(scenario).await;
}

/// 测试目标（**本轮验收点**）：**最终裁决**（`accept_async`）成为建流的唯一提交点
/// ——在它之前丢弃半建立句柄，两个角色都不留垃圾、不悬着对端。
///
/// - 手段：两条内存环直连两个端点并完成握手，交给
///   [`common::run_unsettled_handle_scenario_`]：发起方在 `accept` 前丢弃句柄后立刻
///   复用同一个 dock 对；响应方在 `accept` 前丢弃句柄，主动方等它的裁决。场景由
///   `scope.run_until` 驱动。
/// - 判断：丢弃发起方句柄后同一 dock 对**立刻**可复用（报 `WaitClose`/`Duplicate`
///   即失败）；丢弃响应方句柄后主动方拿到 `HandleError::Refused`（若不发 `REJECT`，
///   主动方会永远悬着，测试超时即失败）。
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mux_unsettled_handle_tokio_() {
    let (a_tx, b_rx) = common::make_passive_ring_(common::K_NET_BUFFER_SIZE);
    let (b_tx, a_rx) = common::make_passive_ring_(common::K_NET_BUFFER_SIZE);

    let scope = LocalScope::new();
    let scenario = common::run_unsettled_handle_scenario_(&scope, a_rx, a_tx, b_rx, b_tx);
    scope.run_until(scenario).await;
}

/// 测试目标：与 tokio 版逐字相同的「半建立句柄收尾」验收，改用 **compio 运行时**。
///
/// - 手段：同样两条内存环直连，交给
///   [`common::run_unsettled_handle_scenario_`]；作用域换成
///   `abs_art_compio::LocalScope`，队列由运行时驱动。
/// - 判断：与 tokio 版相同——发起方丢弃后同一 dock 对立刻可复用；响应方丢弃后主动
///   方拿到 `Refused`。
#[compio::test]
async fn mux_unsettled_handle_compio_() {
    let (a_tx, b_rx) = common::make_passive_ring_(common::K_NET_BUFFER_SIZE);
    let (b_tx, a_rx) = common::make_passive_ring_(common::K_NET_BUFFER_SIZE);

    let scope = abs_art_compio::LocalScope::new();
    let scenario = common::run_unsettled_handle_scenario_(&scope, a_rx, a_tx, b_rx, b_tx);
    scope.run_until(scenario).await;
}

/// 测试目标：tokio 下 `bind_async` 对同一个 `local_dock` 是**独占**的——首次绑定
/// 成功，第二次绑定报 `BindError::DockInUse`，丢弃 binding 后可重绑。
///
/// - 手段：两条内存环直连两个端点并完成握手，交给
///   [`common::run_bind_exclusivity_scenario_`]；后者在 A 侧对同一个 dock 连续
///   `bind_async`、在另一个 dock 上正常绑定、`drop` 首个 binding 后再绑定，并在
///   B 侧用同一个 dock 值绑定以证明绑定是每条连接独立的状态。场景由
///   `scope.run_until` 驱动（缺省配置下两个循环经作用域 `spawn_local`）。
/// - 判断：第二次绑定必须是 `BindError::DockInUse`（不是再次成功）；不同 dock、
///   解绑后的重绑、以及对端同名 dock 的绑定都必须成功。任一不满足即 panic。
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mux_bind_is_exclusive_tokio_() {
    let (a_tx, b_rx) = common::make_passive_ring_(common::K_NET_BUFFER_SIZE);
    let (b_tx, a_rx) = common::make_passive_ring_(common::K_NET_BUFFER_SIZE);

    let scope = LocalScope::new();
    let scenario = common::run_bind_exclusivity_scenario_(&scope, a_rx, a_tx, b_rx, b_tx);
    scope.run_until(scenario).await;
}

/// 测试目标：与 tokio 版逐字相同的绑定独占性验收，改用 **compio 运行时**。
///
/// - 手段：同样两条内存环直连，交给
///   [`common::run_bind_exclusivity_scenario_`]；作用域换成
///   `abs_art_compio::LocalScope`，队列由运行时驱动。
/// - 判断：与 tokio 版相同——同一 dock 第二次绑定报 `DockInUse`；不同 dock、解绑后
///   重绑、对端同名 dock 绑定都成功。
#[compio::test]
async fn mux_bind_is_exclusive_compio_() {
    let (a_tx, b_rx) = common::make_passive_ring_(common::K_NET_BUFFER_SIZE);
    let (b_tx, a_rx) = common::make_passive_ring_(common::K_NET_BUFFER_SIZE);

    let scope = abs_art_compio::LocalScope::new();
    let scenario = common::run_bind_exclusivity_scenario_(&scope, a_rx, a_tx, b_rx, b_tx);
    scope.run_until(scenario).await;
}

/// 测试目标（**本轮验收点**）：环存储的**类型**由使用环境声明、**分配**由 accept 端
/// 当场决定——同一条连接上两条子流可以切不同容量、来自不同段内存，全程零装箱。
///
/// - 手段：两条内存环直连并完成握手，交给
///   [`common::run_per_channel_alloc_scenario_`]：策略声明
///   `Buff = &'static mut [MaybeUninit<u8>]`，测试方先泄漏出一块 arena，两条子流各自
///   在 `accept_async` 里按 4096 / 8192 切出自己那对缓冲；两条都双向收发并半关闭。
///   场景由 `scope.run_until` 驱动。
/// - 判断：两条子流都建立成功、载荷逐字节相符、半关闭后读到 EOF。若把 `prepare` 的
///   缓冲类型换成别的类型，本用例**编译不过**（连接声明了环类型，因此静态派发）——这
///   正是「类型归实现方、分配归调用方」的分工。
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mux_per_channel_alloc_tokio_() {
    let (a_tx, b_rx) = common::make_passive_ring_(common::K_NET_BUFFER_SIZE);
    let (b_tx, a_rx) = common::make_passive_ring_(common::K_NET_BUFFER_SIZE);

    let scope = LocalScope::new();
    let scenario = common::run_per_channel_alloc_scenario_(&scope, a_rx, a_tx, b_rx, b_tx);
    scope.run_until(scenario).await;
}

/// 测试目标：与 tokio 版逐字相同的「逐条子流自行分配」验收，改用 **compio 运行时**。
///
/// - 手段：同样两条内存环直连，交给
///   [`common::run_per_channel_alloc_scenario_`]；作用域换成
///   `abs_art_compio::LocalScope`，队列由运行时驱动。
/// - 判断：与 tokio 版相同——两条子流都建立成功且数据逐字节相符。
#[compio::test]
async fn mux_per_channel_alloc_compio_() {
    let (a_tx, b_rx) = common::make_passive_ring_(common::K_NET_BUFFER_SIZE);
    let (b_tx, a_rx) = common::make_passive_ring_(common::K_NET_BUFFER_SIZE);

    let scope = abs_art_compio::LocalScope::new();
    let scenario = common::run_per_channel_alloc_scenario_(&scope, a_rx, a_tx, b_rx, b_tx);
    scope.run_until(scenario).await;
}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// 收尾：丢弃连接 ⇒ 两个循环退出并把传输交还
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

/// 包住一个 future，但**忽略取消令牌**（`may_cancel_with` 是恒等变换）。
///
/// 用来把传输伪装成「只做转发、不在取消时唤醒 park」的适配层：`buffex` 的环半部
/// 会把 `cancellation()` 存进自己的 park 状态并在取消时唤醒，而这样的适配层不会。
/// 于是「丢弃连接 ⇒ 循环退出」这条不变量如果成立，只能是因为**循环自己**把每次
/// park 与取消令牌竞争了（见 `session_::race_cancel_`）——这正是本文件的收尾用例
/// 要钉住的东西。
struct IgnoresCancel_<F> {
    /// 被包住的真实 future。
    fut_: F,
}

impl<F> core::future::Future for IgnoresCancel_<F>
where
    F: core::future::Future,
{
    type Output = F::Output;

    fn poll(
        self: core::pin::Pin<&mut Self>,
        cx: &mut core::task::Context<'_>,
    ) -> core::task::Poll<Self::Output> {
        // SAFETY: 本类型是 `#[repr(Rust)]` 的单字段结构体，且不实现 `Unpin` 之外的
        // 任何投影逻辑；把 `self` 的固定性传给内部字段是 `pin-project` 的标准做法
        // 在此处的等价手工形式（内部字段与结构体同地址、同对齐）。
        let this = unsafe { self.get_unchecked_mut() };
        let fut = unsafe { core::pin::Pin::new_unchecked(&mut this.fut_) };
        fut.poll(cx)
    }
}

impl<'a, F> abs_cancel::TrMayCancel<'a> for IgnoresCancel_<F>
where
    F: core::future::Future + 'a,
{
    type MayCancelFuture<'f, C>
        = IgnoresCancel_<F>
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

/// 传输**读**半边的析构探针：逐项转发（并忽略取消令牌），`drop` 时置位标志。
///
/// 探针内部持 `Rc<Cell<bool>>`（`!Send`），这正是「本地投递不要求 `Send`」的顺带
/// 证明：把它当 `Rx` 交给连接完全合法。
struct DropProbeRx_<H> {
    /// 被包住的真实半边。
    half_: H,

    /// 析构标志。
    dropped_: Rc<Cell<bool>>,
}

impl<H> DropProbeRx_<H> {
    /// 包住一个读半边，并共享一个析构标志。
    fn new_(half: H, dropped: Rc<Cell<bool>>) -> Self {
        DropProbeRx_ {
            half_: half,
            dropped_: dropped,
        }
    }
}

impl<H> TrBuffTryRead<u8> for DropProbeRx_<H>
where
    H: TrBuffTryRead<u8>,
{
    type SegmRef<'f>
        = H::SegmRef<'f>
    where
        Self: 'f;

    type Err = H::Err;

    fn try_read<'f>(
        &'f mut self,
        demand: &'f Demand<usize>,
    ) -> SomeOf<Self::SegmRef<'f>, Self::Err> {
        self.half_.try_read(demand)
    }
}

impl<H> TrBuffRead<u8> for DropProbeRx_<H>
where
    H: TrBuffRead<u8>,
{
    type ReadAsync<'f>
        = IgnoresCancel_<<H::ReadAsync<'f> as core::future::IntoFuture>::IntoFuture>
    where
        Self: 'f;

    fn read_async<'f>(&'f mut self, demand: &'f Demand<usize>) -> Self::ReadAsync<'f> {
        IgnoresCancel_ {
            fut_: core::future::IntoFuture::into_future(self.half_.read_async(demand)),
        }
    }
}

impl<H> Drop for DropProbeRx_<H> {
    fn drop(&mut self) {
        self.dropped_.set(true);
    }
}

/// 传输**写**半边的析构探针；语义与 [`DropProbeRx_`] 对称。
struct DropProbeTx_<H> {
    /// 被包住的真实半边。
    half_: H,

    /// 析构标志。
    dropped_: Rc<Cell<bool>>,
}

impl<H> DropProbeTx_<H> {
    /// 包住一个写半边，并共享一个析构标志。
    fn new_(half: H, dropped: Rc<Cell<bool>>) -> Self {
        DropProbeTx_ {
            half_: half,
            dropped_: dropped,
        }
    }
}

impl<H> TrBuffTryWrite<u8> for DropProbeTx_<H>
where
    H: TrBuffTryWrite<u8>,
{
    type SegmMut<'f>
        = H::SegmMut<'f>
    where
        Self: 'f;

    type Err = H::Err;

    fn try_write<'f>(
        &'f mut self,
        demand: &'f Demand<usize>,
    ) -> SomeOf<Self::SegmMut<'f>, Self::Err> {
        self.half_.try_write(demand)
    }
}

impl<H> TrBuffWrite<u8> for DropProbeTx_<H>
where
    H: TrBuffWrite<u8>,
{
    type WriteAsync<'f>
        = IgnoresCancel_<<H::WriteAsync<'f> as core::future::IntoFuture>::IntoFuture>
    where
        Self: 'f;

    fn write_async<'f>(&'f mut self, demand: &'f Demand<usize>) -> Self::WriteAsync<'f> {
        IgnoresCancel_ {
            fut_: core::future::IntoFuture::into_future(self.half_.write_async(demand)),
        }
    }
}

impl<H> Drop for DropProbeTx_<H> {
    fn drop(&mut self) {
        self.dropped_.set(true);
    }
}

/// 测试目标：**丢弃连接即关闭连接**——最后一个应用面对象被丢弃后，两个循环必须
/// 退出并把各自的传输半边交还（drop）。
///
/// 这条不变量是本轮架构的关键：循环**不持有**核心强引用，因此「最后一个应用面对象
/// 消失」能让 `MuxCore` 真正析构，其 `Drop` 触发两个取消令牌。若哪天有人改成循环
/// 持有强引用，本用例会失败（核心永不析构 ⇒ 令牌永不触发 ⇒ 传输永不释放）。
///
/// - 手段：四个传输半边各包一层「析构探针」（drop 时置位 `Rc<Cell<bool>>`）；
///   在作用域里完成握手、建出两个连接与一个 binding，然后丢弃它们；再循环
///   `yield_now()` 让本地队列推进（每次 yield 都可能轮到两个循环被 poll），
///   每次检查四个标志。
/// - 判断：限定轮数内四个探针标志**全部置位**（两个循环都退出、传输都释放）；
///   超时未置位说明循环没有响应「连接被丢弃」，判为失败。
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dropping_connection_stops_both_loops_tokio_() {
    let scope = LocalScope::new();
    let dropped: [Rc<Cell<bool>>; 4] = core::array::from_fn(|_| Rc::new(Cell::new(false)));

    let (a_tx, b_rx) = common::make_passive_ring_(common::K_NET_BUFFER_SIZE);
    let (b_tx, a_rx) = common::make_passive_ring_(common::K_NET_BUFFER_SIZE);

    let scenario = async {
        let invite_opts = BasicOpts::default();
        let listen_opts = BasicOpts::default();
        let invite_fut = HandshakeAgent::new(
            DropProbeRx_::new_(a_rx, dropped[0].clone()),
            DropProbeTx_::new_(a_tx, dropped[1].clone()),
        )
        .invite_async(&invite_opts, AcceptAllEntries);
        let listen_fut = HandshakeAgent::new(
            DropProbeRx_::new_(b_rx, dropped[2].clone()),
            DropProbeTx_::new_(b_tx, dropped[3].clone()),
        )
        .listen_async(&listen_opts, AcceptAllEntries);
        let (invited, accepted) =
            futures::join!(async { invite_fut.await }, async { listen_fut.await });

        let conn_a = MuxConnection::new(
            &scope,
            invited.expect("发起方握手应当成功"),
            common::SmokeMuxConfig,
        );
        let conn_b = MuxConnection::new(
            &scope,
            accepted.expect("等待方握手应当成功"),
            common::SmokeMuxConfig,
        );
        // 会话句柄也持有一份连接克隆：逐个丢弃，最后一个消失时核心才析构。
        let binding = conn_a
            .bind_async(Dock::new(0x3000u32))
            .await
            .expect("绑定应当成功");
        let forked = conn_a.clone();
        drop(binding);
        assert!(
            !dropped.iter().any(|flag| flag.get()),
            "连接与句柄都还在时，两个循环不应退出"
        );

        drop((conn_a, conn_b, forked));

        for _ in 0..256usize {
            if dropped.iter().all(|flag| flag.get()) {
                return;
            }
            tokio::task::yield_now().await;
        }
        panic!("丢弃连接后两个循环没有退出：析构标志 = {dropped:?}");
    };

    scope.run_until(scenario).await;
}

/// 测试目标：**只丢弃一侧连接时，该侧的两个循环也必须退出**——即使对端仍然活着且
/// 完全空闲（既不发送、也不关闭自己的方向）。
///
/// 这条比「两侧同时丢弃」严格得多：后者可以靠「先退出的那个循环关掉对端环」把另一个
/// 循环顺带唤醒，从而掩盖「取消令牌唤不醒 park」的问题。本用例把那条捷径掐掉，
/// 因此它真正钉住的是 [`race_cancel_`](smux_v1) 的价值——传输的 `may_cancel_with`
/// 只做转发、不在取消时唤醒时，循环仍能靠自己的竞争醒来退出。
///
/// 本文件里的四个传输半边都经 [`IgnoresCancel_`] 包装成**忽略取消令牌**的形态，
/// 正是为了模拟这种「不配合」的传输。
///
/// - 手段：与上一个用例同样的探针接线，但只丢弃 `conn_a`（以及它的句柄），
///   `conn_b` 继续存活；随后循环 `yield_now()` 让本地队列推进。
/// - 判断：限定轮数内 **A 侧**两个探针标志全部置位；超时未置位即失败。
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dropping_one_side_stops_its_loops_tokio_() {
    let scope = LocalScope::new();
    let dropped: [Rc<Cell<bool>>; 4] = core::array::from_fn(|_| Rc::new(Cell::new(false)));

    let (a_tx, b_rx) = common::make_passive_ring_(common::K_NET_BUFFER_SIZE);
    let (b_tx, a_rx) = common::make_passive_ring_(common::K_NET_BUFFER_SIZE);

    let scenario = async {
        let opts_a = BasicOpts::default();
        let opts_b = BasicOpts::default();
        let invite_fut = HandshakeAgent::new(
            DropProbeRx_::new_(a_rx, dropped[0].clone()),
            DropProbeTx_::new_(a_tx, dropped[1].clone()),
        )
        .invite_async(&opts_a, AcceptAllEntries);
        let listen_fut = HandshakeAgent::new(
            DropProbeRx_::new_(b_rx, dropped[2].clone()),
            DropProbeTx_::new_(b_tx, dropped[3].clone()),
        )
        .listen_async(&opts_b, AcceptAllEntries);
        let (invited, accepted) =
            futures::join!(async { invite_fut.await }, async { listen_fut.await });

        let conn_a = MuxConnection::new(
            &scope,
            invited.expect("发起方握手应当成功"),
            common::SmokeMuxConfig,
        );
        let conn_b = MuxConnection::new(
            &scope,
            accepted.expect("等待方握手应当成功"),
            common::SmokeMuxConfig,
        );

        // 只丢 A：B 仍然活着、空闲，其写循环不会关掉 A 的接收环。
        drop(conn_a);
        for _ in 0..256usize {
            if dropped[0].get() && dropped[1].get() {
                drop(conn_b);
                return;
            }
            tokio::task::yield_now().await;
        }
        let state = (dropped[0].get(), dropped[1].get());
        drop(conn_b);
        panic!("只丢弃一侧后，该侧两个循环未退出：A 读/写传输析构标志 = {state:?}");
    };

    scope.run_until(scenario).await;
}

/// 测试目标（**本轮验收点**）：连接**拒绝接受**不合约的环内存，且因此不弄脏连接。
///
/// - 手段：两条内存环直连两个端点并完成握手，交给
///   [`common::run_ring_rejected_scenario_`]：响应方用容量 `1` 的缓冲裁决（环下限为
///   `2`），随后在同一条连接上再走一遍正常建流。场景由 `scope.run_until` 驱动。
/// - 判断：响应方必须得到 `HandleError::RingRejected`，主动方必须得到
///   `HandleError::Refused`；随后那条子流必须成功（若拒绝路径把连接或注册表弄脏，
///   这一段会失败）。
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mux_ring_rejected_tokio_() {
    let (a_tx, b_rx) = common::make_passive_ring_(common::K_NET_BUFFER_SIZE);
    let (b_tx, a_rx) = common::make_passive_ring_(common::K_NET_BUFFER_SIZE);

    let scope = LocalScope::new();
    let scenario = common::run_ring_rejected_scenario_(&scope, a_rx, a_tx, b_rx, b_tx);
    scope.run_until(scenario).await;
}

/// 测试目标：与 tokio 版逐字相同的「拒绝环内存」验收，改用 **compio 运行时**。
///
/// - 手段：同样两条内存环直连，交给
///   [`common::run_ring_rejected_scenario_`]；作用域换成 `abs_art_compio::LocalScope`，
///   队列由运行时驱动。
/// - 判断：与 tokio 版相同——响应方 `RingRejected`、主动方 `Refused`，随后建流成功。
#[compio::test]
async fn mux_ring_rejected_compio_() {
    let (a_tx, b_rx) = common::make_passive_ring_(common::K_NET_BUFFER_SIZE);
    let (b_tx, a_rx) = common::make_passive_ring_(common::K_NET_BUFFER_SIZE);

    let scope = abs_art_compio::LocalScope::new();
    let scenario = common::run_ring_rejected_scenario_(&scope, a_rx, a_tx, b_rx, b_tx);
    scope.run_until(scenario).await;
}
