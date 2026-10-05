//! 堆分配计数基线（`dev-notes/audit-heap-alloc-20261004-1122.md` §12-T4）。
//!
//! # 这个测试目标为什么单独存在、且只有一条用例、一个运行时
//!
//! `#[global_allocator]` 是**进程级**的：整个测试二进制只能装一个，而且它统计的是
//! **所有线程**的分配。因此：
//!
//! - 本文件只放一条用例（`tokio` 一个运行时）。若照惯例用
//!   [`dual_runtime_test_!`](smux_v1::dual_runtime_test_) 生成两个变体，它们会在同一
//!   个二进制里**并行**执行，把彼此的计数搅在一起；
//! - 计数只在**武装期间**累加（[`arm_`] / [`disarm_`]），因此打印、断言与测试框架
//!   自身的分配不会混进被测区间。
//!
//! # 它度量什么、断言什么
//!
//! 度量两个分配器上的**次数与字节数**：
//!
//! - **全局**分配器（本文件的 [`CountingGlobal`]）——`flume` 通道、每帧一个 `Vec`
//!   之类「隐式落到全局分配器」的分配点（audit §3.1）；
//! - **注入**分配器（[`CountingAlloc`]，由 [`CountMuxConfig`] 交给连接）——子流环
//!   存储、环节点、每条子流的状态节点、以及循环的本地表与暂存。
//!
//! 断言只落在**结构性稳定**的事实上（例如「稳态搬运不产生注入分配」）；次数本身随
//! 实现演进，一律**打印**出来，作为 `audit-heap-alloc` 里那些「待整改」条目的量纲。
//!
//! # 两条测量纪律（都踩过坑）
//!
//! 1. **脚手架与被测对象分开**：测试自己的 `Vec` / 载荷缓冲一律在**武装之前**分配。
//!    第一版把它们放在武装区间里，「监听 + 绑定」阶段的 3 次全局分配里有 2 次其实是
//!    两条 `Vec::with_capacity`；
//! 2. **两个计数器必须正交**：计数注入分配器**不能**转发给 `Global`——那正是本文件
//!    注册的计数全局分配器，会让每笔注入分配被重复计入全局（见 `audit-heap-alloc`
//!    §F.4）。它转发 `System`。
//!
//! # 为什么不复用 `common::connect_pair_`
//!
//! 它的 where 约束把分配器与缓冲类型固定成了 `CoreAlloc` / `SmokeBuff`
//! （见 `tests/common/scenarios_/connect_.rs`），注入分配器换不进去。本文件因此自带
//! 一份**只换分配器**的建连辅助（[`connect_counting_`]），其余装配与冒烟用例一致。

#![feature(allocator_ext)]

mod common;

use core::{
    alloc::{AllocError, Allocator, AllocatorClone, Layout},
    marker::PhantomData,
    mem::MaybeUninit,
    ptr::NonNull,
    sync::atomic::{AtomicBool, AtomicUsize, Ordering},
};
use std::alloc::{GlobalAlloc, System};

use abs_art::TrLocalScope;
use abs_art_tokio::LocalScope;
use abs_smux::{
    conf::TrMuxConfig,
    conn::{TrChannelListener, TrConnection, TrDockBinding},
};
use buffex::x_deps::abs_buff::{TrBuffRead, TrBuffWrite};
use futures::join;
use mm_ptr::{Owned, x_deps::abs_mm::CoreAlloc};
use smux_v1::{
    connection::{
        BuffAllocError, BufferedRx, BufferedTx, ChannelListener, ChannelRx, ChannelTx, Dock,
        K_STAGE_RING_CAPACITY, MuxChanBuff, MuxConnection, TrConnCfg,
    },
    flow_ctrl::DefaultPolicy,
    handshake::{
        agent::{AcceptAllEntries, HandshakeAgent},
        opts::BasicOpts,
    },
};

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// 计数用全局分配器
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

/// 是否处于**武装**状态：只有武装期间的全局分配会被计数。
static G_ARMED: AtomicBool = AtomicBool::new(false);

/// 武装期间全局分配的次数。
static G_ALLOCS: AtomicUsize = AtomicUsize::new(0usize);

/// 武装期间全局分配的字节数（只计 `alloc` / `alloc_zeroed` / `realloc` 的**新**布局）。
static G_BYTES: AtomicUsize = AtomicUsize::new(0usize);

/// 会计数的全局分配器：转发给 [`System`]，仅在 [`G_ARMED`] 为真时累加。
///
/// 释放（`dealloc`）不计：本文件要的是**分配次数**，它与释放一一对应，重复计一遍
/// 只会让数字翻倍而没有新信息。
struct CountingGlobal;

unsafe impl GlobalAlloc for CountingGlobal {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        count_global_(layout.size());
        // SAFETY: 转发给系统分配器；`layout` 由 `GlobalAlloc` 契约保证合法。
        unsafe { System.alloc(layout) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        count_global_(layout.size());
        // SAFETY: 同 `alloc`。
        unsafe { System.alloc_zeroed(layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        count_global_(new_size);
        // SAFETY: 由调用方保证 `ptr` 由本分配器按 `layout` 分配、`new_size` 合法。
        unsafe { System.realloc(ptr, layout, new_size) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: 由调用方保证 `ptr` 由本分配器按 `layout` 分配。
        unsafe { System.dealloc(ptr, layout) }
    }
}

/// 记一次全局分配（仅在武装期间）。
fn count_global_(bytes: usize) {
    if G_ARMED.load(Ordering::Relaxed) {
        G_ALLOCS.fetch_add(1usize, Ordering::Relaxed);
        G_BYTES.fetch_add(bytes, Ordering::Relaxed);
    }
}

#[global_allocator]
static GLOBAL_ALLOC_: CountingGlobal = CountingGlobal;

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// 计数用**注入**分配器与连接配置
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

/// 注入分配器上的分配次数。
static INJ_ALLOCS: AtomicUsize = AtomicUsize::new(0usize);

/// 注入分配器上的分配字节数。
static INJ_BYTES: AtomicUsize = AtomicUsize::new(0usize);

/// 计数用的**注入**分配器：每次分配累加，内存直接向 [`System`] 要。
///
/// # 为什么是 `System` 而不是 `Global`
///
/// `Global` 就是本文件注册的**计数全局分配器**：经它分配会让同一笔注入分配被
/// **同时计入全局**（第一版就是这么写错的，于是「监听期全局 +1/个」其实是 8 个身份
/// 节点被重复计数）。`System` 是操作系统分配器，绕开计数，两个计数器才互不串台。
///
/// 它**不需要武装开关**：连接只会在真正需要分配时调用它，而本文件的所有连接都在
/// 被测区间里运行。
#[derive(Clone, Copy, Debug, Default)]
struct CountingAlloc;

unsafe impl Allocator for CountingAlloc {
    fn allocate(&self, layout: Layout) -> Result<NonNull<[u8]>, AllocError> {
        INJ_ALLOCS.fetch_add(1usize, Ordering::Relaxed);
        INJ_BYTES.fetch_add(layout.size(), Ordering::Relaxed);
        System.allocate(layout)
    }

    unsafe fn deallocate(&self, ptr: NonNull<u8>, layout: Layout) {
        // SAFETY: `ptr` 由本分配器按同一 `layout` 向 `System` 分配（调用方保证）。
        unsafe { System.deallocate(ptr, layout) }
    }
}

unsafe impl AllocatorClone for CountingAlloc {}

/// 子流环存储的类型（与冒烟配置同构，只换分配器）。
type CountBuff = Owned<[MaybeUninit<u8>], CountingAlloc>;

/// 用[注入分配器](CountingAlloc)记账的连接配置。
#[derive(Debug)]
struct CountMuxConfig<W, R> {
    /// 传输写半边的类型占位。
    _use_w_: PhantomData<fn() -> W>,

    /// 传输读半边的类型占位。
    _use_r_: PhantomData<fn() -> R>,
}

impl<W, R> Copy for CountMuxConfig<W, R> {}

impl<W, R> Clone for CountMuxConfig<W, R> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<W, R> Default for CountMuxConfig<W, R> {
    fn default() -> Self {
        CountMuxConfig {
            _use_w_: PhantomData,
            _use_r_: PhantomData,
        }
    }
}

/// 测试用流控策略（与冒烟配置一致）。
static COUNT_POLICY: DefaultPolicy = DefaultPolicy;

impl<W, R> TrMuxConfig for CountMuxConfig<W, R>
where
    W: TrBuffWrite<u8> + 'static,
    R: TrBuffRead<u8> + 'static,
{
    type Data = u8;
    type Dock = Dock;
    type Buff = CountBuff;
}

impl<W, R> TrConnCfg for CountMuxConfig<W, R>
where
    W: TrBuffWrite<u8> + 'static,
    R: TrBuffRead<u8> + 'static,
{
    type Alloc = CountingAlloc;
    type Policy = DefaultPolicy;
    type ConnTx = W;
    type ConnRx = R;
    type StageBuff = CountBuff;

    fn allocator(&self) -> Self::Alloc {
        CountingAlloc
    }

    fn policy(&self) -> &Self::Policy {
        &COUNT_POLICY
    }

    fn make_ring_buffs(
        &self,
        alloc: Self::Alloc,
        capacity: usize,
    ) -> Result<(Self::Buff, Self::Buff), BuffAllocError> {
        Result::Ok((
            Owned::new_uninit_slice(capacity, alloc),
            Owned::new_uninit_slice(capacity, alloc),
        ))
    }

    fn make_stage_buffs(
        &self,
        alloc: Self::Alloc,
    ) -> Result<(Self::StageBuff, Self::StageBuff), BuffAllocError> {
        Result::Ok((
            Owned::new_uninit_slice(K_STAGE_RING_CAPACITY, alloc),
            Owned::new_uninit_slice(K_STAGE_RING_CAPACITY, alloc),
        ))
    }
}

/// 与 [`CountMuxConfig`] 同构，但**子流环存储**换成擦除分配器的 [`MuxChanBuff`]
/// （`DefaultConnCfg` 那条路径）。
///
/// 唯一的差别就是这一处：`MuxChanBuff::pair_from_alloc_` 每次调用都要
/// `Arc::new(alloc)` 把分配器擦除成一个 trait object——**每侧一次全局分配**
/// （audit-heap-alloc §3.1 #5 / §5.5-A）。本配置存在的意义就是量出这笔开销，
/// 为「#5 怎么改（公开 API 加性新增还是改签名）」提供数字。
#[derive(Debug)]
struct ErasedMuxConfig<W, R> {
    /// 传输写半边的类型占位。
    _use_w_: PhantomData<fn() -> W>,

    /// 传输读半边的类型占位。
    _use_r_: PhantomData<fn() -> R>,
}

impl<W, R> Copy for ErasedMuxConfig<W, R> {}

impl<W, R> Clone for ErasedMuxConfig<W, R> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<W, R> Default for ErasedMuxConfig<W, R> {
    fn default() -> Self {
        ErasedMuxConfig {
            _use_w_: PhantomData,
            _use_r_: PhantomData,
        }
    }
}

impl<W, R> TrMuxConfig for ErasedMuxConfig<W, R>
where
    W: TrBuffWrite<u8> + 'static,
    R: TrBuffRead<u8> + 'static,
{
    type Data = u8;
    type Dock = Dock;

    /// 与 [`CountMuxConfig`] 的 `CountBuff` 的**唯一**差别。
    type Buff = MuxChanBuff;
}

impl<W, R> TrConnCfg for ErasedMuxConfig<W, R>
where
    W: TrBuffWrite<u8> + 'static,
    R: TrBuffRead<u8> + 'static,
{
    type Alloc = CountingAlloc;
    type Policy = DefaultPolicy;
    type ConnTx = W;
    type ConnRx = R;

    /// 连接级帧暂存仍用 `Owned`（与基准场景一致），把差异**隔离在子流环存储**上。
    type StageBuff = CountBuff;

    fn allocator(&self) -> Self::Alloc {
        CountingAlloc
    }

    fn policy(&self) -> &Self::Policy {
        &COUNT_POLICY
    }

    fn make_ring_buffs(
        &self,
        alloc: Self::Alloc,
        capacity: usize,
    ) -> Result<(Self::Buff, Self::Buff), BuffAllocError> {
        MuxChanBuff::pair_from_alloc_(alloc, capacity).map_err(|_| BuffAllocError)
    }

    fn make_stage_buffs(
        &self,
        alloc: Self::Alloc,
    ) -> Result<(Self::StageBuff, Self::StageBuff), BuffAllocError> {
        Result::Ok((
            Owned::new_uninit_slice(K_STAGE_RING_CAPACITY, alloc),
            Owned::new_uninit_slice(K_STAGE_RING_CAPACITY, alloc),
        ))
    }
}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// 度量的取值与打印
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

/// 一段区间内的分配用量。
#[derive(Clone, Copy, Debug, Default)]
struct Usage_ {
    /// 分配次数。
    allocs_: usize,

    /// 分配字节数。
    bytes_: usize,
}

impl Usage_ {
    /// 两次快照之差（`after - before`）。
    fn since_(after: Usage_, before: Usage_) -> Self {
        Usage_ {
            allocs_: after.allocs_.saturating_sub(before.allocs_),
            bytes_: after.bytes_.saturating_sub(before.bytes_),
        }
    }
}

/// 当前全局分配用量。
fn global_usage_() -> Usage_ {
    Usage_ {
        allocs_: G_ALLOCS.load(Ordering::Relaxed),
        bytes_: G_BYTES.load(Ordering::Relaxed),
    }
}

/// 当前注入分配用量。
fn injected_usage_() -> Usage_ {
    Usage_ {
        allocs_: INJ_ALLOCS.load(Ordering::Relaxed),
        bytes_: INJ_BYTES.load(Ordering::Relaxed),
    }
}

/// 开始计数全局分配。
fn arm_() {
    G_ARMED.store(true, Ordering::Relaxed);
}

/// 停止计数全局分配。
fn disarm_() {
    G_ARMED.store(false, Ordering::Relaxed);
}

/// 打印一个阶段的用量（必须在 [`disarm_`] 之后调用，避免把打印自身的分配算进去）。
fn report_(phase: &str, global: Usage_, injected: Usage_) {
    println!(
        "[alloc] {phase:<22} 全局 {:>6} 次 / {:>9} B    注入 {:>6} 次 / {:>9} B",
        global.allocs_, global.bytes_, injected.allocs_, injected.bytes_
    );
}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// 场景
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

/// 被动环两端的类型（与 `common::make_passive_ring_` 的返回一致）。
type RingTx = BufferedTx<common::SmokeBuff, CoreAlloc>;
type RingRx = BufferedRx<common::SmokeBuff, CoreAlloc>;

/// 本文件用的连接类型（分配器换成计数用的那个）。
type CountingConn<S> = MuxConnection<CountMuxConfig<RingTx, RingRx>, S>;

/// 本文件用的子流半边类型。
type CountingTx<S> = ChannelTx<CountMuxConfig<RingTx, RingRx>, S>;
type CountingRx<S> = ChannelRx<CountMuxConfig<RingTx, RingRx>, S>;

/// 本文件用的监听器类型。
type CountingListener<S> = ChannelListener<CountMuxConfig<RingTx, RingRx>, S>;


/// 两次用量相加。
fn add_usage_(a: Usage_, b: Usage_) -> Usage_ {
    Usage_ {
        allocs_: a.allocs_ + b.allocs_,
        bytes_: a.bytes_ + b.bytes_,
    }
}

/// 测量一个 future 完成期间发生的**全局**分配（武装只在被测区间生效）。
async fn measure_global_<F, T>(fut: F) -> Usage_
where
    F: core::future::Future<Output = T>,
{
    let before = global_usage_();
    arm_();
    let out = fut.await;
    disarm_();
    drop(out);
    Usage_::since_(global_usage_(), before)
}

/// 建连：握手 + 两个 `MuxConnection`，配置由调用方给出（分配器已是计数用的那个）。
///
/// 与 `common::connect_pair_` 逐语句同构，**只**把配置换成泛型参数，以免把
/// 「容量 / 策略」以外的差异带进计数。
async fn connect_with_<C, S>(
    scope: &S,
    tx_a: RingTx,
    rx_a: RingRx,
    tx_b: RingTx,
    rx_b: RingRx,
) -> (MuxConnection<C, S>, MuxConnection<C, S>)
where
    S: common::TrSmokeScope + Clone + 'static,
    C: TrConnCfg<ConnRx = RingRx, ConnTx = RingTx> + Default + Clone + 'static,
    C::StageBuff: Send + Sync,
{
    let invite_opts = BasicOpts::default();
    let listen_opts = BasicOpts::default();
    let invite_fut = HandshakeAgent::new(tx_a, rx_a).invite_async(&invite_opts, AcceptAllEntries);
    let listen_fut = HandshakeAgent::new(tx_b, rx_b).listen_async(&listen_opts, AcceptAllEntries);
    let (invited, accepted) = join!(async { invite_fut.await }, async { listen_fut.await });
    let delivery_a = invited.expect("发起方握手应当成功");
    let delivery_b = accepted.expect("等待方握手应当成功");

    let config_a = C::default();
    let config_b = C::default();
    let (stage_ar, stage_aw) = config_a
        .make_stage_buffs(config_a.allocator())
        .expect("A 侧连接级帧暂存应当分配成功");
    let (stage_br, stage_bw) = config_b
        .make_stage_buffs(config_b.allocator())
        .expect("B 侧连接级帧暂存应当分配成功");
    (
        MuxConnection::new(scope, delivery_a, config_a, stage_ar, stage_aw),
        MuxConnection::new(scope, delivery_b, config_b, stage_br, stage_bw),
    )
}

/// 基准场景的建连（配置 = [`CountMuxConfig`]）。
async fn connect_counting_<S>(
    scope: &S,
    tx_a: RingTx,
    rx_a: RingRx,
    tx_b: RingTx,
    rx_b: RingRx,
) -> (CountingConn<S>, CountingConn<S>)
where
    S: common::TrSmokeScope + Clone + 'static,
{
    connect_with_::<CountMuxConfig<RingTx, RingRx>, S>(scope, tx_a, rx_a, tx_b, rx_b).await
}

/// 每条子流在稳态阶段往返的轮数。
const K_ROUNDS: usize = 4;

/// 稳态阶段每次发送的载荷长度（字节）。`4 × 512 = 2048 < 4096`（子流环容量），
/// 因此「先写完再读回」不会因为窗口用尽而互等。
const K_PAYLOAD: usize = 512;

/// 建流的子流条数。
const K_CHANNELS: usize = 8;

/// 分配计数的基线场景（四个阶段：建连 / 绑定监听 / 建流 / 稳态搬运 / 拆流）。
async fn run_baseline_<S>(scope: &S) -> (Usage_, Usage_)
where
    S: common::TrSmokeScope + Clone + 'static,
{
    // 两条全被动环：`(a_tx, b_rx)` 承载 A→B，`(b_tx, a_rx)` 承载 B→A。环的两端分别
    // 交给两个连接的传输半边，中间没有泵（与 `tests/inmem_mux.rs` 的装配一致）。
    let (a_tx, b_rx) = common::make_passive_ring_(common::K_NET_BUFFER_SIZE);
    let (b_tx, a_rx) = common::make_passive_ring_(common::K_NET_BUFFER_SIZE);

    // -- 阶段 1：建连（握手 + 两个连接对象 + 四个循环启动）。
    let g0 = global_usage_();
    let i0 = injected_usage_();
    arm_();
    let (conn_a, conn_b) = connect_counting_(scope, a_tx, a_rx, b_tx, b_rx).await;
    disarm_();
    report_("建连", Usage_::since_(global_usage_(), g0), Usage_::since_(injected_usage_(), i0));

    // -- 阶段 2：绑定 + 监听。
    //
    // 一条 `DockBinding` 的 `local_dock` 是**身份的一半**，因此同一 binding 对同一个
    // `remote_dock` 只能同时存在一条子流（§4.1）。本用例要 N 条**并发**子流，于是让
    // B 侧在 N 个 dock 上各建一个 binding + listener，A 侧用一个 binding 依次发往
    // 这 N 个 dock——N 个 dock 对互不相同。
    let dock_a = Dock::new(0x5001u32);
    // 测试脚手架自己的 `Vec` 一律在**武装之前**分配：它们走全局分配器，落在被测区间
    // 里就成了噪声（第一版正是如此，于是「监听期 3 次全局分配」里有一次半是脚手架）。
    let mut listeners_b: Vec<CountingListener<S>> = Vec::with_capacity(K_CHANNELS);
    let mut dock_bs: Vec<Dock> = Vec::with_capacity(K_CHANNELS);

    let g0 = global_usage_();
    let i0 = injected_usage_();
    arm_();
    let mut binding_a = conn_a
        .bind_async(dock_a)
        .await
        .expect("A 侧绑定应当成功");
    for index in 0..K_CHANNELS {
        let dock_b = Dock::new(0x5100u32 + index as u32);
        let mut binding = conn_b
            .bind_async(dock_b)
            .await
            .expect("B 侧绑定应当成功");
        let listener = binding
            .listen_async()
            .await
            .expect("B 侧监听应当成功");
        dock_bs.push(dock_b);
        listeners_b.push(listener);
    }
    disarm_();
    let bind_global = Usage_::since_(global_usage_(), g0);
    let bind_injected = Usage_::since_(injected_usage_(), i0);
    report_("绑定 + 监听", bind_global, bind_injected);
    // C3 之后：每个被监听的 dock 只用**注入**分配建一个身份节点（入向通知是内联槽），
    // 全局侧只允许一次性初始化那种量级（实测 1 次 / 48 B）。
    assert!(
        bind_global.allocs_ <= 2usize,
        "绑定 + 监听阶段发生了 {} 次全局分配（9 个 dock）——listener 通知可能又回到了堆上",
        bind_global.allocs_
    );
    assert_eq!(
        bind_injected.allocs_, 11usize,
        "9 个 dock（1 binding + 8 listener）应当只发生 11 次注入分配，实际 {}",
        bind_injected.allocs_
    );

    // -- 阶段 3：建 K_CHANNELS 条子流（不含数据）；逐条打印注入分配的增量。
    // 脚手架容器在**武装之前**分配（同「绑定 + 监听」阶段：它们走全局分配器）。
    let mut txs_a: Vec<CountingTx<S>> = Vec::with_capacity(K_CHANNELS);
    let mut rxs_a: Vec<CountingRx<S>> = Vec::with_capacity(K_CHANNELS);
    let mut txs_b: Vec<CountingTx<S>> = Vec::with_capacity(K_CHANNELS);
    let mut rxs_b: Vec<CountingRx<S>> = Vec::with_capacity(K_CHANNELS);

    let g0 = global_usage_();
    let i0 = injected_usage_();
    for index in 0..K_CHANNELS {
        arm_();
        let g_before = global_usage_();
        let i_before = injected_usage_();
        let mut message: &[u8] = &[];
        let mut handle = binding_a
            .open_channel_async(dock_bs[index], &mut message)
            .await
            .expect("A 侧发起子流应当成功");
        // 两侧的最终裁决必须**并发**推进：主动方的 `OPEN` 是在 `accept` 里发出的，
        // 被动方的 `income_async` 要等它到达（顺序执行会互等，见 `inmem_mux` 的同款装配）。
        let listener = &mut listeners_b[index];
        let (opened, accepted) = join!(
            async {
                let mut welcome: [u8; 0] = [];
                let mut welcome: &mut [u8] = &mut welcome[..];
                handle
                    .accept_async_managed(&mut welcome, common::K_CHANNEL_CAPACITY)
                    .await
            },
            async {
                let mut incoming = listener
                    .income_async()
                    .await
                    .expect("B 侧应当取到入向请求");
                let mut welcome: [u8; 0] = [];
                let mut welcome: &mut [u8] = &mut welcome[..];
                incoming
                    .accept_async_managed(&mut welcome, common::K_CHANNEL_CAPACITY)
                    .await
            },
        );
        let (tx_a, rx_a) = opened.expect("A 侧最终裁决应当成功");
        let (tx_b, rx_b) = accepted.expect("B 侧最终裁决应当成功");
        disarm_();
        let step = Usage_::since_(injected_usage_(), i_before);
        // 每一条子流的**数据面**至少要有：两块环存储 + 两个环节点 + 一个状态节点；
        // 其余增量来自身份表的索引节点（`bindings_` / `remote_index_` / `docks_`），
        // 因此这里只断言下界，具体数字打印出来。
        assert!(
            step.allocs_ >= 5usize,
            "建第 {index} 条子流只发生了 {} 次注入分配，少于「2 块存储 + 2 个环 + 1 个状态」",
            step.allocs_
        );
        println!(
            "[alloc]   └ 第 {index} 条子流      全局 {:>4} 次 / {:>7} B    注入 {:>4} 次 / {:>7} B",
            Usage_::since_(global_usage_(), g_before).allocs_,
            Usage_::since_(global_usage_(), g_before).bytes_,
            step.allocs_,
            step.bytes_
        );
        txs_a.push(tx_a);
        rxs_a.push(rx_a);
        txs_b.push(tx_b);
        rxs_b.push(rx_b);
    }
    let build_global = Usage_::since_(global_usage_(), g0);
    let build_injected = Usage_::since_(injected_usage_(), i0);
    report_(&format!("建流 ×{K_CHANNELS}"), build_global, build_injected);

    // -- 阶段 4：稳态搬运（每条子流双向各 K_ROUNDS 帧）。
    //
    // 载荷（每条一个 `Vec<u8>`）**在武装之前**生成：它是测试脚手架，不该混进「smux 每帧
    // 分配多少」这个量里。读回用的缓冲区是栈上定长数组，本身不分配。
    let payloads: Vec<Vec<u8>> = (0..K_CHANNELS)
        .map(|index| common::make_flow_payload_(index as u32, K_PAYLOAD))
        .collect();

    let g0 = global_usage_();
    let i0 = injected_usage_();
    // 逐段归因：稳态的分配到底发生在「写」还是「读」（都只累加，打印在武装之外）。
    let mut usage_write_ab_ = Usage_::default();
    let mut usage_read_b_ = Usage_::default();
    let mut usage_write_ba_ = Usage_::default();
    let mut usage_read_a_ = Usage_::default();
    arm_();
    for index in 0..K_CHANNELS {
        let payload = &payloads[index];
        let mut got = [0u8; K_PAYLOAD];
        for round in 0..K_ROUNDS {
            usage_write_ab_ = add_usage_(
                usage_write_ab_,
                measure_global_(common::write_channel_all_(&mut txs_a[index], payload)).await,
            );
            usage_read_b_ = add_usage_(
                usage_read_b_,
                measure_global_(common::read_channel_exact_(&mut rxs_b[index], &mut got)).await,
            );
            assert_eq!(got, payload[..], "第 {index} 条子流第 {round} 帧内容应当一致");

            usage_write_ba_ = add_usage_(
                usage_write_ba_,
                measure_global_(common::write_channel_all_(&mut txs_b[index], payload)).await,
            );
            usage_read_a_ = add_usage_(
                usage_read_a_,
                measure_global_(common::read_channel_exact_(&mut rxs_a[index], &mut got)).await,
            );
            assert_eq!(got, payload[..], "第 {index} 条子流第 {round} 帧内容应当一致");
        }
    }
    disarm_();
    let transfer_global = Usage_::since_(global_usage_(), g0);
    let transfer_injected = Usage_::since_(injected_usage_(), i0);
    report_("稳态搬运", transfer_global, transfer_injected);
    report_("  └ 写 A→B", usage_write_ab_, Usage_::default());
    report_("  └ 读 B", usage_read_b_, Usage_::default());
    report_("  └ 写 B→A", usage_write_ba_, Usage_::default());
    report_("  └ 读 A", usage_read_a_, Usage_::default());

    // 稳态的**结构性**断言：搬运本身不需要任何注入分配（环与表都在建流期就位）。
    assert_eq!(
        transfer_injected.allocs_, 0usize,
        "稳态搬运不应当产生注入分配，实际 {} 次",
        transfer_injected.allocs_
    );
    // 全局分配：**#1（栈上帧头 + 两段入环）之后，每帧不再有那个 `Vec`**。剩下的是
    // 事件通道的固有成本（`flume` 消息与 park 登记，见 audit §F.5 的逐段归因）。
    // 这里钉一个**回归上限**：实测 1 504（8 子流 × 4 轮 × 双向 = 64 帧），#1 之前是
    // 1 608。数字本身打印出来，上限只用来让「每帧又冒出一次分配」立刻失败。
    const K_TRANSFER_GLOBAL_CEILING: usize = 1_600usize;
    assert!(
        transfer_global.allocs_ <= K_TRANSFER_GLOBAL_CEILING,
        "稳态搬运的全局分配（{} 次）超过上限 {K_TRANSFER_GLOBAL_CEILING}——每帧成帧可能又回到堆上了",
        transfer_global.allocs_
    );

    // -- 阶段 5：拆流（丢弃发送半边 → 等对端 EOF）。
    drop(txs_a);
    drop(txs_b);
    let g0 = global_usage_();
    let i0 = injected_usage_();
    arm_();
    for rx in rxs_b.iter_mut() {
        common::expect_eof_(rx).await;
    }
    for rx in rxs_a.iter_mut() {
        common::expect_eof_(rx).await;
    }
    disarm_();
    report_("拆流 + EOF", Usage_::since_(global_usage_(), g0), Usage_::since_(injected_usage_(), i0));
    drop(rxs_a);
    drop(rxs_b);
    (build_global, build_injected)
}

/// 擦除缓冲（`MuxChanBuff`）场景：只测**建流**，用来量出 audit #5 的「每侧一次
/// `Arc::new(alloc)`」。
///
/// 与 [`run_baseline_`] 的建流阶段逐语句同构，只有子流环存储的类型不同。
async fn run_erased_buffers_<S>(scope: &S) -> (Usage_, Usage_)
where
    S: common::TrSmokeScope + Clone + 'static,
{
    let (a_tx, b_rx) = common::make_passive_ring_(common::K_NET_BUFFER_SIZE);
    let (b_tx, a_rx) = common::make_passive_ring_(common::K_NET_BUFFER_SIZE);
    let (conn_a, conn_b) =
        connect_with_::<ErasedMuxConfig<RingTx, RingRx>, S>(scope, a_tx, a_rx, b_tx, b_rx).await;

    let dock_a = Dock::new(0x6001u32);
    let mut listeners_b: Vec<ChannelListener<ErasedMuxConfig<RingTx, RingRx>, S>> =
        Vec::with_capacity(K_CHANNELS);
    let mut dock_bs: Vec<Dock> = Vec::with_capacity(K_CHANNELS);
    let mut binding_a = conn_a
        .bind_async(dock_a)
        .await
        .expect("A 侧绑定应当成功");
    for index in 0..K_CHANNELS {
        let dock_b = Dock::new(0x6100u32 + index as u32);
        let listener = conn_b
            .bind_async(dock_b)
            .await
            .expect("B 侧绑定应当成功")
            .listen_async()
            .await
            .expect("B 侧监听应当成功");
        dock_bs.push(dock_b);
        listeners_b.push(listener);
    }

    // 四个半边**全部留着**（与基准场景一致）：一旦在这里丢掉 B 侧半边，上一条子流的
    // 拆流（`TxClosed` / `RxClosed` → FIN/RESET → 释放）就会被算进**下一条**的窗口，
    // 数字立刻不可比（第一版就是这么错的）。
    let mut txs_a: Vec<ChannelTx<ErasedMuxConfig<RingTx, RingRx>, S>> =
        Vec::with_capacity(K_CHANNELS);
    let mut rxs_a: Vec<ChannelRx<ErasedMuxConfig<RingTx, RingRx>, S>> =
        Vec::with_capacity(K_CHANNELS);
    let mut txs_b: Vec<ChannelTx<ErasedMuxConfig<RingTx, RingRx>, S>> =
        Vec::with_capacity(K_CHANNELS);
    let mut rxs_b: Vec<ChannelRx<ErasedMuxConfig<RingTx, RingRx>, S>> =
        Vec::with_capacity(K_CHANNELS);

    let g0 = global_usage_();
    let i0 = injected_usage_();
    for index in 0..K_CHANNELS {
        arm_();
        let mut message: &[u8] = &[];
        let mut handle = binding_a
            .open_channel_async(dock_bs[index], &mut message)
            .await
            .expect("A 侧发起子流应当成功");
        let listener = &mut listeners_b[index];
        let (opened, accepted) = join!(
            async {
                let mut welcome: [u8; 0] = [];
                let mut welcome: &mut [u8] = &mut welcome[..];
                handle
                    .accept_async_managed(&mut welcome, common::K_CHANNEL_CAPACITY)
                    .await
            },
            async {
                let mut incoming = listener
                    .income_async()
                    .await
                    .expect("B 侧应当取到入向请求");
                let mut welcome: [u8; 0] = [];
                let mut welcome: &mut [u8] = &mut welcome[..];
                incoming
                    .accept_async_managed(&mut welcome, common::K_CHANNEL_CAPACITY)
                    .await
            },
        );
        let (tx_a, rx_a) = opened.expect("A 侧最终裁决应当成功");
        let (tx_b, rx_b) = accepted.expect("B 侧最终裁决应当成功");
        txs_a.push(tx_a);
        rxs_a.push(rx_a);
        txs_b.push(tx_b);
        rxs_b.push(rx_b);
        disarm_();
    }
    let erased_global = Usage_::since_(global_usage_(), g0);
    let erased_injected = Usage_::since_(injected_usage_(), i0);
    report_(&format!("擦除缓冲 建流 ×{K_CHANNELS}"), erased_global, erased_injected);
    // 收尾在武装之外：这一步会触发拆流（FIN/RESET），不属本场景要量的「建流」。
    drop(txs_a);
    drop(rxs_a);
    drop(txs_b);
    drop(rxs_b);
    drop(listeners_b);
    (erased_global, erased_injected)
}

/// 分配计数基线（tokio 单运行时，理由见文件头）。
///
/// - 手段：进程级计数全局分配器 + 计数注入分配器，跑「建连 → 绑定监听 → 建 8 条
///   子流 → 每条双向 4 帧 × 512 B → 拆流等 EOF」，逐阶段打印次数 / 字节数。
/// - 判断：稳态搬运的**注入**分配必须为 0（环与本地表都在建流期就位）；稳态搬运的
///   **全局**分配不超过回归上限（#1 之后每帧不再有那个 `Vec`）；建每条子流的注入分配
///   不少于 5 次（两块环存储 + 两个环节点 + 一个状态节点）；绑定 + 监听阶段全局分配
///   不超过 2 次（listener 通知已内联，audit #6）。
/// - 判断（#5 的量）：擦除缓冲路径的**全局**分配恰好比 `Owned` 路径多
///   `2 × K_CHANNELS` 次（每侧一次 `Arc::new(alloc)`），**注入**侧完全相同——这就是
///   「`MuxChanBuff::pair_from_alloc_` 每次调用擦除一个分配器」的价格。C4 的 A/B 任一
///   方案落地后，右边应当变成「基线 + 1」（每连接一份），本断言随之收紧。
#[tokio::test]
async fn alloc_count_baseline_tokio_() {
    let scope = LocalScope::new();
    let (build_global, build_injected) = scope.run_until(run_baseline_(&scope)).await;

    // 第二个场景（擦除缓冲）另起一个作用域：两个连接的循环互不干扰。
    let scope = LocalScope::new();
    let (erased_global, erased_injected) = scope.run_until(run_erased_buffers_(&scope)).await;

    assert_eq!(
        erased_injected.allocs_, build_injected.allocs_,
        "两条路径的**注入**分配应当完全一致（只有擦除那一步不同）"
    );
    assert_eq!(
        erased_global.allocs_,
        build_global.allocs_ + 2usize * K_CHANNELS,
        "擦除缓冲路径应当恰好每侧多一次 `Arc::new(alloc)`（audit #5）；若这里变了，\
         说明 `MuxChanBuff` 的擦除次数或 `pair_from_alloc_` 的实现变了"
    );
}
