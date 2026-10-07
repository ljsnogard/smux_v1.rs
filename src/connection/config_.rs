//! 复用连接的资源策略 [`TrConnCfg`]。
//!
//! 上游 [`abs_smux::conf::TrMuxConfig`] 只剩 `Data` / `Dock` 两个类型；本 trait 补上
//! 连接内部还需要、但上游不关心的几样东西：内部结构使用的分配器、流控策略、两条传输
//! 半边，以及运行时值。公开类型因此只需要 `MuxConnection<C>` 一个参数。
//!
//! **缓冲类型不在配置里**：一条子流用哪种智能指针持有它的 ring 内存，是调用方在
//! `accept_async` 时**当场**交出的；连接级帧暂存缓冲同理（`MuxConnection::new` 的
//! 实参）。配置只回答「内部结构用什么分配器、跑在哪个运行时」。
//!
//! # 缓冲从哪来
//!
//! 子流环与连接级帧暂存的缓冲都由**调用方当场交出**（前者经 `accept_async` 的
//! `prepare`，后者是 `MuxConnection::new` 的实参），类型是任意 `TrUnique` 的智能
//! 指针。连接把它们一律吸收成 `ring_` 模块里的类型无关句柄，内部签名里看不到具体
//! 指针类型；分配次数与容量都由调用方决定，连接只校验容量是否可用。

extern crate alloc;

use core::{
    alloc::AllocatorClone,
    marker::PhantomData,
};

use buffex::x_deps::abs_buff::{TrBuffRead, TrBuffWrite};
use abs_smux::conf::TrMuxConfig;
use abs_mm::CoreAlloc;
use mm_ptr::x_deps::abs_mm;

use crate::{
    connection::Dock,
    flow_ctrl::{DefaultPolicy, TrFlowCtrlPolicy},
    handshake::agent::HandshakeDelivery,
    metrics::{NoMetrics, TrMetricsSink},
    time::TrTime,
};

#[allow(unused)]
pub trait TrMuxAllocConfig {
    type RegistryAlloc: AllocatorClone;

    /// Allocator for ChannelOwner
    type ChanOwnerAlloc: AllocatorClone;
}

/// **连接级**帧暂存环的容量常数（两块：读环、写环）。
///
/// **它不是正确性下限**：整条收发路径都是流式推进的——读侧帧头由逐字节状态机解析
/// （`frame_parser_`）、载荷按底层实际给出的段长分次搬入（`ReadCursor::read_async_`
/// 每次只索要 1 字节起步），写侧入环写入按空闲空间分块推进（`session_.rs` 的
/// `enqueue_frame_`）。因此两块环都只要 ≥ 环原语的最小容量（1 字节）就能跑通；
/// `tests/inmem_mux.rs` 的 `mux_min_stage_inmem_dual_` 就以 1 字节钉住了这一点。
///
/// 取 64 KiB 是**吞吐 / 时延**上的选择：环越大，外侧泵与内侧循环之间的往返越少、
/// 单次借出的段越长。默认协商值是 4096 字节，实际部署建议把 `max_packet_size` 定在
/// **64 KiB 以下**；超过此值时应由配置实现者按自己的 `max_packet_size` 放大本容量。
pub const K_STAGE_RING_CAPACITY: usize = 64usize * 1024usize;

/// 连接的配置类型。
///
/// 它是**使用环境**与连接之间的唯一约定：数据与 dock 类型由上游
/// [`TrMuxConfig`] 给出；本 trait 再补上内部结构分配器、流控策略、两条传输半边，
/// 以及**运行时值**（计时与时刻）。
/// 所有公开类型都只带 `<C>` 一个参数（配置本身），因此加一个旋钮只需改一处。
///
/// # 运行时值为什么由配置提供
///
/// `MuxCore` 必须**无条件** `Send + Sync`（它是 `mm_ptr::Shared` 的被指对象，而
/// `Shared<T, A>: Send + Sync` 要求 `T: Send + Sync`），因此核心**不能**自己持有
/// 运行时值——compio 后端的 `Runtime` 是 `!Send + !Sync`（内含线程本地执行器）。
/// 于是「现在几点」的来源改由配置回答：核心只留**建连 epoch**（一个纯数据），
/// 需要时刻时调 [`TrConnCfg::runtime`] 取一个运行时值（克隆句柄，廉价）。
///
/// 这也让后端选择**留在配置侧**：`DefaultConnCfg` 用 `abs_art-bridge` 的裸名
/// （即集成方在 `Cargo.toml` 里选定的后端），测试配置可以换成假运行时值。
pub trait TrConnCfg
where
    Self: TrMuxConfig<Data = u8, Dock = Dock> + 'static,
{
    /// **运行时值**：提供「现在几点」（[`TrClock`]）与「怎么等」（[`TrDelay`]）。
    ///
    /// 它必须是 `TrTime`（= `TrClock + TrDelay`）且可 `Clone`——核心与五个循环各自
    /// 克隆一份，因此各处看到的时刻来自**同一个**时间轴。
    ///
    /// [`TrClock`]: abs_art::TrClock
    /// [`TrDelay`]: abs_art::TrDelay
    type Rt: TrTime + Clone + 'static;

    /// 连接内部结构（帧暂存、注册表等）的分配器。
    type Alloc: AllocatorClone + Send + Sync;

    /// 流控策略。
    type Policy: TrFlowCtrlPolicy;

    /// **指标上报的接收方**（见 [`crate::metrics`]）。
    ///
    /// **每个实现都要写它**（没有默认值）：不上报就写 [`NoMetrics`]——零大小、每个方法
    /// 都是 `#[inline(always)]` 空实现；配合 [`TrConnCfg::metrics`] 返回的引用，该装配下
    /// 的上报调用点会被单态化消除。
    ///
    /// 要求 `Clone`：五个循环**不持有核心**（见 [`crate::connection`] 模块文档 §2.2），
    /// 建连时必须把 sink 克隆一份进内部共享量。因此这里适合放**廉价可克隆的句柄**
    /// （例如 `&'static T`，或需求方自己的共享句柄）；`crate::metrics` 的模块文档
    /// §3 说明了为什么 mux 不替需求方做 `Arc` / `dyn` 的适配。
    type Metrics: TrMetricsSink + Clone;

    /// 连接侧的写 / 读半边（即两条传输的缓冲类型）。
    type ConnTx: TrBuffWrite<u8>;
    type ConnRx: TrBuffRead<u8>;

    /// 取一个**运行时值**（克隆句柄；各处共享同一个时间轴与计时器）。
    ///
    /// 调用点可能不在任何运行时上下文内（例如在别的线程上 `bind_async`），因此
    /// 实现必须交出**建连时就已经抓住**的那个值，而不是临场重建——compio 的
    /// `Runtime::current()` 要求调用点已在上下文内，做不到这一点。
    fn runtime(&self) -> Self::Rt;

    /// 取连接内部结构用的分配器（按值，`buffex` 的构建器按值接收）。
    fn allocator(&self) -> Self::Alloc;

    /// 取流控策略。
    fn policy(&self) -> &Self::Policy;

    /// 取本配置携带的**指标接收方**。
    ///
    /// # 为什么它返回引用而不是 `Option<&…>`
    ///
    /// 「有没有 sink」是**编译期**由 [`TrConnCfg::Metrics`] 决定的类型事实，不是运行期
    /// 状态。用 `Option` 会让**每一个**上报调用点都多一次判空分支，而那个分支在
    /// 「不上报」的装配下本可以彻底消失。返回确定的引用之后，调用点写成
    ///
    /// ```ignore
    /// shared.metrics_().on_frame(dir, local, remote, kind, bytes);
    /// ```
    ///
    /// ——缺省时 `NoMetrics` 的空实现让整句被消除，配了 sink 时它是一次直接调用。
    ///
    /// # 为什么它是必需方法（没有默认实现）
    ///
    /// Rust 不允许默认方法体假定 `Self::Metrics == NoMetrics`，因此没有默认实现可写。
    /// 「不上报」的实现照下面一行即可（零大小类型的常量引用被提升为 `'static`，
    /// 不涉及分配）：
    ///
    /// ```ignore
    /// type Metrics = NoMetrics;
    ///
    /// fn metrics(&self) -> &Self::Metrics {
    ///     &NoMetrics
    /// }
    /// ```
    ///
    /// 要上报的实现返回自己的 sink（通常是配置里的一个廉价可克隆句柄字段）：
    ///
    /// ```ignore
    /// type Metrics = MySink;
    ///
    /// fn metrics(&self) -> &Self::Metrics {
    ///     &self.sink_
    /// }
    /// ```
    fn metrics(&self) -> &Self::Metrics;

    /// 子流环的**建议容量**：调用方自己分配那块内存时可以参考它。
    ///
    /// 它不是协议或实现的下限——容量完全由调用方决定（交多大就用多大），本项只是
    /// 「没特别想法时给一个够用的数」。
    const RING_CAPACITY: usize = K_DEFAULT_CHANNEL_RING_CAPACITY;
}

/// [`TrConnCfg::RING_CAPACITY`] 的缺省值：4 KiB。
///
/// 取这个值的理由与「子流环是逐条可调的」这件事无关——它只是「调用方没说要多大时
/// 给一个够用的数」；想逐条控制的调用方走 `accept_async_managed(.., cap)` 或自己实现
/// [`TrPrepareChannelRing`](abs_smux::chan::TrPrepareChannelRing)。
pub const K_DEFAULT_CHANNEL_RING_CAPACITY: usize = 4096usize;

/// 默认配置：`u8` 数据、[`Dock`] dock、[`CoreAlloc`] 分配、[`DefaultPolicy`]
/// 流控；**帧暂存与子流环都用** [`MuxChanBuffAlloc`]——一种把「环内存 + 释放它的
/// 分配器」打包在一起的智能指针，分配器内联在值里，因此每次建缓冲都不必再为保存
/// 分配器做一次额外堆分配。
///
/// 泛型参数 `Rt` 是**运行时值**，默认取 `abs_art-bridge` 的裸名
/// [`Runtime`](abs_art_bridge::Runtime)——即集成方在 `Cargo.toml` 里选定的后端
/// （本仓缺省是 compio）。要用另一个后端或假运行时值，显式写出 `Rt` 即可。
///
/// # 泛型参数顺序：`<W, R, M, P, Rt>`
///
/// 第三个参数 `M` 是**指标接收方**（见 [`crate::metrics`]），排在策略 `P` **之前**。
/// 因此写 `DefaultConnCfg<Tx, Rx, DefaultPolicy, Rt>` 是错的——`DefaultPolicy` 会落到
/// `M` 位上、`Rt` 落到 `P` 位上。不上报时要显式写全：
///
/// ```ignore
/// type Cfg = DefaultConnCfg<Tx, Rx, NoMetrics, DefaultPolicy, Rt>;
/// ```
///
/// # 两块缓冲为什么是同一种类型
///
/// 连接级帧暂存与子流环都是「由外部交出、供环使用的一块内存」，因此上游
/// [`TrMuxConfig::Buff`] 与本 trait 的 [`TrConnCfg::StageBuff`] 用同一个契约
/// （[`TrBoxed`]）描述它们，默认实现也就用同一个 [`MuxChanBuffAlloc`]。两种用途的差别
/// 只在**容量从哪来**：帧暂存由 [`TrConnCfg::make_stage_buffs`] 定死，子流环由调用方
/// 在最终裁决时逐条给出。
///
/// 帧暂存这一路额外要求 `Send + Sync`（[`MuxConnection::new`] 会把缓冲搬进循环），
/// 而 [`MuxChanBuffAlloc`] 内部是裸指针，因此那两条 `unsafe impl` 写在 `ring_` 模块里
/// （附安全论证）。子流环那条路不要求这两个 auto trait。
///
/// [`MuxConnection::new`]: crate::connection::MuxConnection::new
pub struct DefaultConnCfg<
    W, R,
    M = NoMetrics,
    P = DefaultPolicy,
    Rt = crate::connection::DefaultRt_>
{
    /// 运行时值（计时与时刻的来源），建连时抓住、此后按需克隆。
    rt_: Rt,
    policy_: P,
    metrics_: M,
    /// 连接侧两条半边的类型占位（它们只以类型形式参与）。
    _use_w_: PhantomData<fn() -> W>,
    _use_r_: PhantomData<fn() -> R>,
}

// `Clone` / `Copy` / `Debug` **手写**：结构里只有 `PhantomData`、策略值与运行时值，
// 不该给 `W` / `R` 加上这些约束（环半部既不 `Clone` 也不 `Debug`）。
impl<W, R, M, P, Rt> Clone for DefaultConnCfg<W, R, M, P, Rt>
where
    M: Clone,
    P: Clone,
    Rt: Clone,
{
    fn clone(&self) -> Self {
        DefaultConnCfg {
            rt_: self.rt_.clone(),
            policy_: self.policy_.clone(),
            metrics_: self.metrics_.clone(),
            _use_w_: PhantomData,
            _use_r_: PhantomData,
        }
    }
}

impl<W, R, M, P, Rt> Copy for DefaultConnCfg<W, R, M, P, Rt>
where
    M: Copy,
    P: Copy,
    Rt: Copy,
{}

impl<W, R, M: core::fmt::Debug, P: core::fmt::Debug, Rt: core::fmt::Debug> core::fmt::Debug
    for DefaultConnCfg<W, R, M, P, Rt>
{
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("DefaultConnCfg")
            .field("rt_", &self.rt_)
            .field("policy_", &self.policy_)
            .field("metrics_", &self.metrics_)
            .finish_non_exhaustive()
    }
}

impl<W, R, M, P> DefaultConnCfg<W, R, M, P, super::DefaultRt_>
where
    M: TrMetricsSink + Clone + Default,
    P: TrFlowCtrlPolicy,
{
    /// 用**默认后端**的运行时值与策略造出配置。
    ///
    /// `delivery` 原样交回：握手交付物与配置总是成对出现，调用方一行就能拿到两者，
    /// 因此这里不去拆它。
    ///
    /// # Panics
    ///
    /// 调用点不在所选后端的运行时上下文内时 panic（文案由 `abs_art` 各后端给出；
    /// tokio 为「no reactor running」、compio 为「not in a compio runtime」）。
    /// 需要显式控制运行时值时用 [`DefaultConnCfg::new_with_rt`]。
    pub fn new(
        delivery: HandshakeDelivery<W, R>,
        policy: P,
    ) -> (HandshakeDelivery<W, R>, Self) {
        let cfg = DefaultConnCfg {
            rt_: super::default_rt_(),
            policy_: policy,
            metrics_: M::default(),
            _use_w_: PhantomData,
            _use_r_: PhantomData,
        };
        (delivery, cfg)
    }
}

impl<W, R, M, P, Rt> DefaultConnCfg<W, R, M, P, Rt>
where
    M: TrMetricsSink + Clone + Default,
    P: TrFlowCtrlPolicy,
{
    /// 用**调用者给定**的运行时值与策略造出配置。
    ///
    /// 这是「特别的需要」那条接口：想在无上下文处建连、想用虚拟时钟、或想固定某个
    /// 具名后端时，用本入口把运行时值显式传进来（它会被抓住，此后只在配置内部克隆）。
    pub fn new_with_rt(
        delivery: HandshakeDelivery<W, R>,
        policy: P,
        rt: Rt,
    ) -> (HandshakeDelivery<W, R>, Self) {
        let cfg = DefaultConnCfg {
            rt_: rt,
            policy_: policy,
            metrics_: M::default(),
            _use_w_: PhantomData,
            _use_r_: PhantomData,
        };        (delivery, cfg)
    }
}

impl<W, R, M, P, Rt> TrMuxConfig for DefaultConnCfg<W, R, M, P, Rt>
where
    W: TrBuffWrite<u8> + 'static,
    R: TrBuffRead<u8> + 'static,
    P: TrFlowCtrlPolicy + 'static,
{
    type Data = u8;
    type Dock = Dock;
}

impl<W, R, M, P, Rt> TrConnCfg for DefaultConnCfg<W, R, M, P, Rt>
where
    W: TrBuffWrite<u8> + 'static,
    R: TrBuffRead<u8> + 'static,
    M: TrMetricsSink + Clone,
    P: TrFlowCtrlPolicy + 'static,
    Rt: TrTime + Clone + 'static,
{
    type Rt = Rt;
    type Alloc = CoreAlloc;
    type Policy = P;
    type ConnTx = W;
    type ConnRx = R;
    type Metrics = M;

    fn runtime(&self) -> Self::Rt {
        self.rt_.clone()
    }

    fn allocator(&self) -> Self::Alloc {
        CoreAlloc
    }

    fn policy(&self) -> &Self::Policy {
        &self.policy_
    }

    /// 取本配置**自己携带**的那一份 sink（类型由泛型参数 `M` 决定）。
    ///
    /// `metrics_` 由 [`DefaultConnCfg::new`] / [`DefaultConnCfg::new_with_rt`] 经
    /// `M::default()` 造出，因此 `M` 只支持**实现了 `Default`** 的 sink（典型是零大小的
    /// [`NoMetrics`]）。这不是缺口而是分工：要挂一个有状态、能被自己读到的采集器，
    /// **请自定义配置类型**并把 sink 句柄放进字段（见 `tests/metrics_e2e.rs` 的
    /// `MetricsCfg`）——sink 存哪、怎么共享属于需求方。
    fn metrics(&self) -> &Self::Metrics {
        &self.metrics_
    }

}
