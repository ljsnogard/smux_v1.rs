//! 复用连接的资源策略 [`TrConnCfg`]。
//!
//! 上游 [`abs_smux::conf::TrMuxConfig`] 只剩 `Data` / `Dock` 两个类型；本 trait 补上
//! 连接内部还需要、但上游不关心的几样东西：内部结构使用的分配器、流控策略、两条传输
//! 半边，以及运行时值。公开类型因此只需要 `MuxConnection<C>` 一个参数。
//!
//! **没有连接级帧暂存缓冲**：握手的交付物（`Tx` / `Rx`）本身就自带缓冲，连接的内侧
//! 两个循环直接驱动它们，因此不再需要调用方另交两块缓冲、也不再有一次搬运
//! （见 `session_` 模块文档）。传输半边因此必须**自己带缓冲**
//! ——这也正是 [`TrConnCfg::ConnTx`] / [`TrConnCfg::ConnRx`] 要求
//! `TrProducerState` / `TrConsumerState` 的原因：内侧循环要能非阻塞地问出
//! 「有多少可读 / 可写、对端是否已经封口」。
//!
//! # 缓冲从哪来
//!
//! 子流环的缓冲仍由**调用方当场交出**（经 `accept_async` 的 `prepare`），类型是任意
//! `TrUnique` 的智能指针。连接把它吸收成 `ring_` 模块里的类型无关句柄，内部签名里
//! 看不到具体指针类型；分配次数与容量都由调用方决定，连接只校验容量是否可用。

extern crate alloc;

use core::{
    alloc::AllocatorClone,
    marker::PhantomData,
};

use abs_art_facade::{self, DefaultRt_, TrRtCurrent};
use abs_buff::{
    TrBuffFlush, TrBuffRead, TrBuffWrite,
    buffer::{TrConsumerState, TrProducerState},
};
use abs_smux::conf::TrMuxConfig;
use abs_mm::CoreAlloc;

use crate::{
    connection::{Dock, pending_msg_::K_PENDING_MSG_CAP_DEFAULT},
    flow_ctrl::{DefaultPolicy, TrFlowCtrlPolicy},
    handshake::agent::HandshakeDelivery,
    metrics::{NoMetrics, TrMetricsSink},
    time::TrTime,
    x_deps::{abs_buff, abs_mm},
};


pub trait TrMuxAllocConfig {
    /// 连接内部结构的分配器。
    type RegistryAlloc: AllocatorClone + Send + Sync + 'static;

    /// 身份节点的分配器。
    type ChanOwnerAlloc: AllocatorClone + Send + Sync + 'static;
}

/// `smux_v1::connection` 内部**所有分配器用途**的聚合。
///
/// 每一项对应一类结构，而不是「一个全局分配器」：
///
/// - [`Self::RegistryAlloc`]：注册表、索引树、事件通道等连接内部结构；
/// - [`Self::ChanOwnerAlloc`]：身份节点（`ChannelOwner_` / `DockBinding_` 等）；
/// - [`Self::MpmcAlloc`]：会话内 `mpmc_chan` 通道（外层 `Arc` + 等待 chunk）。
///
/// **临时对象不走这里**：请求完成源（`Tcs_`）与循环暂存（`Owned<[u8]>`）已改由
/// 连接级 `sw_scope::Scope` 分配，见 `connection::scope_` 与
/// `dev-notes/sw-scope-20261028-2130.md`。剩下的项都服务于要求 `Allocator` /
/// `AllocatorClone` 的第三方结构，或连接级 `Shared`。
///
/// 本 trait **不要求 `'static`**；只有 [`Self::MpmcAlloc`] 的 GAT 在 `'a` 上带
/// 生命周期约束——通道把分配器类型嵌进 `MpmcChan<'static, …>`，需要
/// `MpmcAlloc<'static>: 'static`，但这条需求停在通道这一层。以后新增分配器用途时
/// 在这里加 GAT 即可。
pub trait TrConnAllocCfg
where
    Self: TrMuxAllocConfig,
{
    /// 会话内 `mpmc_chan` 通道的分配器（外层 `Arc` 与等待 chunk 都用它）。
    type MpmcAlloc<'a>: AllocatorClone + Send + Sync + 'a
    where
        Self: 'a;

    /// `Attach` 交接（`WriteAttach_` / `ReadAttach_`）的分配器。
    type AttachAlloc: AllocatorClone + Send + Sync + 'static;

    /// 数据面本地表（`WriteTable_` / `ReadTable_` / `PendingFin_` / telegraph 表）的分配器。
    type LocalTableAlloc: AllocatorClone + Send + Sync + 'static;

    /// 连接核心 / 共享量 / 取消令牌的分配器。
    type CoreAlloc: AllocatorClone + Send + Sync + 'static;

    /// 公共报文子流（`PublicChannel_` 与两条 ring）的分配器。
    type PublicAlloc: AllocatorClone + Send + Sync + 'static;

    /// 取结构分配器的一个实例。
    fn registry_alloc(&self) -> Self::RegistryAlloc;

    /// 取身份节点分配器的一个实例。
    fn chan_owner_alloc(&self) -> Self::ChanOwnerAlloc;

    /// 取通道分配器的一个实例（交给 `MpmcChan::new_in`）。
    fn mpmc_alloc(&self) -> Self::MpmcAlloc<'static>;

    /// 取 `Attach` 分配器的实例。
    fn attach_alloc(&self) -> Self::AttachAlloc;

    /// 取本地表分配器的实例。
    fn local_table_alloc(&self) -> Self::LocalTableAlloc;

    /// 取连接核心分配器的实例。
    fn core_alloc(&self) -> Self::CoreAlloc;

    /// 取公共子流分配器的实例。
    fn public_alloc(&self) -> Self::PublicAlloc;
}

/// 内建分配器配置：所有用途都用 [`CoreAlloc`]。
#[derive(Debug, Clone, Copy, Default)]
pub struct SingleConnAllocCfg<A = CoreAlloc>(PhantomData<A>)
where
    A: 'static + AllocatorClone + Default + Send + Sync;

impl<A> TrMuxAllocConfig for SingleConnAllocCfg<A>
where
    A: 'static + AllocatorClone + Default + Send + Sync,
{
    type RegistryAlloc = A;
    type ChanOwnerAlloc = A;
}

impl<A> TrConnAllocCfg for SingleConnAllocCfg<A>
where
    A: 'static + AllocatorClone + Default + Send + Sync,
{
    type MpmcAlloc<'a> = A;
    type AttachAlloc = A;
    type LocalTableAlloc = A;
    type CoreAlloc = A;
    type PublicAlloc = A;

    fn registry_alloc(&self) -> Self::RegistryAlloc {
        A::default()
    }

    fn chan_owner_alloc(&self) -> Self::ChanOwnerAlloc {
        A::default()
    }

    fn mpmc_alloc(&self) -> Self::MpmcAlloc<'static> {
        A::default()
    }

    fn attach_alloc(&self) -> Self::AttachAlloc {
        A::default()
    }

    fn local_table_alloc(&self) -> Self::LocalTableAlloc {
        A::default()
    }

    fn core_alloc(&self) -> Self::CoreAlloc {
        A::default()
    }

    fn public_alloc(&self) -> Self::PublicAlloc {
        A::default()
    }
}

/// **连接的传输环容量**（推荐值）：调用方自己造传输环时可以参考它。
///
/// 与旧版的「连接级帧暂存环容量」是同一个数，但语义变了：它不再是连接**自己**
/// 要分配的一块缓冲，而是「一条被 smux 直接驱动的传输环，取多大比较合适」的
/// 经验值。连接只要求这块环落在 `buffex::ring` 允许区间内（最小 1 字节，见
/// `tests/inmem_mux.rs` 的最小容量用例），大小只影响吞吐 / 时延：环越大，
/// 外侧泵与内侧循环之间的往返越少、单次借出的段越长。
///
/// 默认协商值是 4096 字节，实际部署建议把 `max_packet_size` 定在 **64 KiB 以下**；
/// 超过此值时应按自己的 `max_packet_size` 放大本容量——尤其**数据报**（telegraph）
/// 一条报文的帧长上限就是 `max_packet_size`，传输环必须放得下整帧（见
/// `session_::write_frame_to_stage_`）。
pub const K_TRANSPORT_RING_CAPACITY: usize = 64usize * 1024usize;

/// 连接的配置类型。
///
/// 它是**使用环境**与连接之间的唯一约定：数据与 dock 类型由上游
/// [`TrMuxConfig`] 给出；本 trait 再补上内部结构分配器、流控策略、两条传输半边，
/// 以及**运行时值**（计时与时刻）。
/// 所有公开类型都只带 `<C>` 一个参数（配置本身），因此加一个旋钮只需改一处。
///
/// # 运行时值为什么由配置提供
///
/// `MuxCore` 是 `mm_ptr::Shared` 的被指对象，而 `Shared<T, A>: Send + Sync` 要求
/// `T: Send + Sync`，因此核心**不直接**持有运行时值——compio 后端的 `Runtime` 是
/// `!Send + !Sync`（内含线程本地执行器），直接持有它就再也不可能 `Send + Sync`。
/// 于是「现在几点」的来源改由配置回答：核心只留**建连 epoch**（一个纯数据），
/// 需要时刻时调 [`TrConnCfg::runtime`] 取一个运行时值（克隆句柄，廉价）。
///
/// 这只解决「核心不直接持有」，**没有**让 `MuxCore<C>` 无条件 `Send + Sync`：核心
/// 仍持有配置本身，而 [`DefaultConnCfg`] 内含 `Self::Rt`，所以连接是否 `Send + Sync`
/// 最终由 `C::Rt` 决定——tokio 装配下是，compio 装配下否（见
/// `mux_connection::core_` 的类型文档与 `tests/thread_safety.rs` 的编译期断言）。
///
/// 这也让后端选择**留在配置侧**：[`DefaultConnCfg`] 的 `Rt` 缺省取本 crate 的
/// [`DefaultRt_`](abs_art_facade::DefaultRt_)（由 `test-*-runtime` 三选一），
/// 测试配置可以换成假运行时值。
pub trait TrConnCfg
where
    Self: TrMuxConfig<Data = u8, Dock = Dock> + 'static,
{
    /// **运行时值**：提供「现在几点」（[`TrClock`]）与「怎么等」（[`TrDelay`]）。
    ///
    /// 它必须是 `TrTime`（= `TrClock + TrDelay`）且可 `Clone`——核心与三个循环各自
    /// 克隆一份，因此各处看到的时刻来自**同一个**时间轴。
    ///
    /// [`TrClock`]: abs_art::TrClock
    /// [`TrDelay`]: abs_art::TrDelay
    type Rt: TrTime + Clone + 'static;

    /// 分配器配置：由实现者选定一个 [`TrConnAllocCfg`]。
    ///
    /// 用 GAT 只是为了携带 `Self: 'a`；实际取 `AllocCfg<'static>` 的关联类型
    /// （`TrConnCfg` 自身要求 `'static`）。这样连接配置**不必实现**分配器配置，
    /// 只是通过这个关联类型引用它。
    type AllocCfg<'a>: TrConnAllocCfg
    where
        Self: 'a;

    /// 流控策略。
    type Policy: TrFlowCtrlPolicy;

    /// **指标上报的接收方**（见 [`crate::metrics`]）。
    ///
    /// **每个实现都要写它**（没有默认值）：不上报就写 [`NoMetrics`]——零大小、每个方法
    /// 都是 `#[inline(always)]` 空实现；配合 [`TrConnCfg::metrics`] 返回的引用，该装配下
    /// 的上报调用点会被单态化消除。
    ///
    /// 要求 `Clone`：三个循环**不持有核心**（见 [`crate::connection`] 模块文档 §2.2），
    /// 建连时必须把 sink 克隆一份进内部共享量。因此这里适合放**廉价可克隆的句柄**
    /// （例如 `&'static T`，或需求方自己的共享句柄）；`crate::metrics` 的模块文档
    /// §3 说明了为什么 mux 不替需求方做 `Arc` / `dyn` 的适配。
    type Metrics: TrMetricsSink + Clone;

    /// 连接侧的写 / 读半边（即两条传输的缓冲类型）。
    ///
    /// # 为什么还要 `TrProducerState` / `TrConsumerState`
    ///
    /// 连接**不再**为传输另建两条帧暂存环：内侧两个循环直接在这两条半边上
    /// `try_read` / `try_write` / `read_async` / `write_async`。因此它们必须能
    /// **非阻塞地回答**「此刻有多少可读 / 可写、对端是否已经封口」——这正是
    /// `abs_buff` 的这两个状态 trait 提供的查询。
    ///
    /// `buffex::ring` 的两个半部（即本 crate 的 [`BufferedTx`] / [`BufferedRx`]）
    /// 与 `buffex_*_adapt` 的 `BuffRead` / `BuffWrite` 都实现了它们，因此「全被动环」
    /// 与「设备 + 环适配器」两种传输都能直接接上。
    ///
    /// # 为什么写半边还必须 `TrBuffFlush`
    ///
    /// 复用循环只把传输当"环"用：把成帧的字节提交进去就继续干别的，**从不**替传输推
    /// "环 → 设备"那一跳。对"设备 + 环适配器"这类传输，那一跳必须有主：
    ///
    /// * 自驱动形态（`try_new_local`）由适配器内部的后台泵承担，提交即迟早送达；
    /// * 按需形态（`try_new`）**没有人**承担——字节会一直留在环里，对端永久等待，
    ///   而且没有任何错误可报（静默挂起）。
    ///
    /// 要求 `TrBuffFlush` 把这件事变成**可检查的契约**：写半边必须能回答"已提交字节
    /// 是否都交出去了"（[`TrBuffTryFlush::try_flush`]），并在连接收尾时接受"封口"
    /// （[`TrBuffTryFlush::close`]，唤醒下游排空方/泵）。于是 `smux_v1` 不必再在文档里
    /// 要求使用者"必须选自驱动形态"，也不必假设调用方会替传输推泵。
    ///
    /// 全被动环（[`BufferedTx`]）由本 crate 自己实现该 trait：`try_flush` 恒等成功，
    /// `close` 转发 [`RingWriter::close`]，正是旧版缺失的那次唤醒。
    ///
    /// [`BufferedTx`]: crate::connection::BufferedTx
    /// [`BufferedRx`]: crate::connection::BufferedRx
    /// [`RingWriter::close`]: buffex::ring::RingWriter::close
    type ConnTx: TrBuffWrite<u8> + TrBuffFlush<u8> + TrProducerState;
    type ConnRx: TrBuffRead<u8> + TrConsumerState;

    /// 取一个**运行时值**（克隆句柄；各处共享同一个时间轴与计时器）。
    ///
    /// 调用点可能不在任何运行时上下文内（例如在别的线程上 `bind_async`），因此
    /// 实现必须交出**建连时就已经抓住**的那个值，而不是临场重建——compio 的
    /// `Runtime::current()` 要求调用点已在上下文内，做不到这一点。
    fn runtime(&self) -> Self::Rt;

    /// 取分配器配置（按值；内部各结构再按用途取具体分配器）。
    fn alloc_cfg(&self) -> Self::AllocCfg<'static>;

    /// 取连接内部结构用的分配器（`AllocCfg::RegistryAlloc` 的便捷投影）。
    fn allocator(&self) -> crate::connection::ConnAlloc_<Self>
    where
        Self: Sized,
    {
        self.alloc_cfg().registry_alloc()
    }

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

    /// 建流期 message 的**每个方向**载荷上限（字节）；`0` 表示只允许空 message。
    ///
    /// `open` 的 message、`accept` 的 welcome、`reject` 的 reason 都受它约束：
    /// 超出时按「outgoing 报 `MessageTooLong`（P3 接入）/ incoming 回 `REJECT`」处理。
    /// ring 实际容量是 `max(2, 2 + 本值)`，因此 `0` 仍然能写一个空的 `0x0000`。
    ///
    /// 默认值见 [`K_PENDING_MSG_CAP_DEFAULT`]（512 字节）。现有 `TrConnCfg` 实现不必
    /// 改：它带默认实现（设计稿 Q2）。
    fn pending_msg_capacity(&self) -> usize {
        K_PENDING_MSG_CAP_DEFAULT
    }

    /// 建流期 message ring 的分配器；默认复用连接内部结构的分配器。
    ///
    /// 只有需要**不同分配器实例**时才覆盖；若要不同**类型**，需要新增关联类型
    /// `MsgAlloc`（破坏性变更，本阶段不做，设计稿 Q10）。
    fn pending_msg_allocator(&self) -> crate::connection::ConnAlloc_<Self>
    where
        Self: Sized,
    {
        self.allocator()
    }

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
/// [`TrPrepareRing`](abs_smux::chan::TrPrepareRing)。
pub const K_DEFAULT_CHANNEL_RING_CAPACITY: usize = 4096usize;

/// 默认配置：`u8` 数据、[`Dock`] dock、[`CoreAlloc`] 分配、[`DefaultPolicy`]
/// 流控。
///
/// 泛型参数 `Rt` 是**运行时值**，默认取本 crate 的
/// [`DefaultRt_`](abs_art_facade::DefaultRt_)——由 smux 自己的 `test-*-runtime`
/// feature 三选一（缺省是 compio）。要用另一个后端或假运行时值，显式写出 `Rt` 即可。
///
/// # 泛型参数顺序：`<W, R, M, P, Rt>`
///
/// `W` / `R` 是**传输的两条半边**（握手的交付物交回来的那对）。第三个参数 `M` 是
/// **指标接收方**（见 [`crate::metrics`]），排在策略 `P` **之前**。因此写
/// `DefaultConnCfg<Tx, Rx, DefaultPolicy, Rt>` 是错的——`DefaultPolicy` 会落到
/// `M` 位上、`Rt` 落到 `P` 位上。不上报时要显式写全：
///
/// ```ignore
/// type Cfg = DefaultConnCfg<Tx, Rx, NoMetrics, DefaultPolicy, Rt>;
/// ```
///
/// # 缓冲
///
/// 本配置**不规定任何缓冲**：传输半边自带缓冲（见 [`TrConnCfg::ConnTx`]），子流环的
/// 内存在最终裁决时由调用方逐条给出。连接自己只做两类分配——注册表等内部结构（走
/// [`TrConnCfg::Alloc`]）与一次性的载荷暂存。
///
/// [`MuxConnection::new`]: crate::connection::MuxConnection::new
pub struct DefaultConnCfg<
    W, R,
    M = NoMetrics,
    P = DefaultPolicy,
    Rt = DefaultRt_>
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

impl<W, R, M, P> DefaultConnCfg<W, R, M, P, DefaultRt_>
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
            rt_: abs_art_facade::default_rt_(),
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
        };
        (delivery, cfg)
    }

    /// **只造配置**，不要求交出握手交付物。
    ///
    /// [`DefaultConnCfg::new`] / [`DefaultConnCfg::new_with_rt`] 都要一个
    /// [`HandshakeDelivery`] 才能返回「交付物 + 配置」这一对，那是给「握手刚做完、
    /// 马上建连」那条路用的。本入口给的是另一条路：**先把配置建好、之后再逐条连接
    /// 复用**（`SmuxListener` 的配置就要同时服务于许多条连接，每条由它自己克隆一份）。
    pub fn make_with_rt(policy: P, rt: Rt) -> Self {
        DefaultConnCfg {
            rt_: rt,
            policy_: policy,
            metrics_: M::default(),
            _use_w_: PhantomData,
            _use_r_: PhantomData,
        }
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
    W: TrBuffWrite<u8> + TrBuffFlush<u8> + TrProducerState + 'static,
    R: TrBuffRead<u8> + TrConsumerState + 'static,
    M: TrMetricsSink + Clone,
    P: TrFlowCtrlPolicy + 'static,
    Rt: TrTime + Clone + 'static,
{
    type Rt = Rt;
    type AllocCfg<'a> = SingleConnAllocCfg;
    type Policy = P;
    type ConnTx = W;
    type ConnRx = R;
    type Metrics = M;

    fn runtime(&self) -> Self::Rt {
        self.rt_.clone()
    }

    fn alloc_cfg(&self) -> Self::AllocCfg<'static> {
        SingleConnAllocCfg::default()
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

/// 与 [`DefaultConnCfg`] 同形的配置，但**不存储**运行时值：每次需要时从当前上下文取一个。
///
/// # 它存在的理由：无条件 `Send + Sync`
///
/// [`DefaultConnCfg`] 把运行时值存进字段（`rt_: Rt`），而 compio 的运行时值是
/// `!Send + !Sync`（线程本地执行器）。`MuxCore<C>` 持有的正是配置本身，于是那套装配下
/// `MuxCore<C>` / `MuxConnection<C>` 也 `!Send + !Sync`。本类型**不存**运行时值，
/// 因此只要 `P` / `M` 满足，`C: Send + Sync` 无条件成立，连接句柄也随之可以跨线程
/// （`MuxConnection` 的公开类型与 API 形状与用 [`DefaultConnCfg`] 时**完全一致**）。
///
/// # 代价：调用者保证上下文
///
/// 「不存」意味着每次取用时都要求**调用点处于所选后端的运行时上下文内**（tokio /
/// compio 的 `current()` 在上下文之外 panic；smol 无先决条件）。这是本配置的
/// **调用者责任**：跨线程使用连接时，每条使用它的线程都必须自己处于后端上下文内；
/// 语义细节见 [`TrRtCurrent`]。debug 构建下取用会先给出断言提示，release 下由后端的
/// panic 兜底。
///
/// 需要**注入**运行时值（虚拟时钟 `abs_art_mock_clock::ManualTime`、上下文之外建连、
/// 固定具名后端）时，用 [`DefaultConnCfg`]——它保留存储，代价是连接是否 `Send + Sync`
/// 由 `C::Rt` 决定。
///
/// # 泛型参数
///
/// 与 [`DefaultConnCfg`] **完全同序**（`<W, R, M, P, Rt>`），便于两者互换。
///
/// # Examples
///
/// ```ignore
/// // 默认（compio）装配下：连接句柄可以跨线程，代价是取时刻的线程必须在上下文内。
/// type Cfg = CurrentConnCfg<BufferedTx, BufferedRx>;
/// let (delivery, cfg) = CurrentConnCfg::<BufferedTx, BufferedRx>::new(delivery, DefaultPolicy);
/// let conn = MuxConnection::new(delivery, cfg);
/// ```
pub struct CurrentConnCfg<
    W, R,
    M = NoMetrics,
    P = DefaultPolicy,
    Rt = DefaultRt_,
> {
    policy_: P,
    metrics_: M,
    /// 运行时值的**类型**占位：本配置不存值，只在需要时从上下文取一个（见类型文档）。
    _use_rt_: PhantomData<fn() -> Rt>,
    /// 连接侧两条半边的类型占位（它们只以类型形式参与）。
    _use_w_: PhantomData<fn() -> W>,
    _use_r_: PhantomData<fn() -> R>,
}

// `Clone` / `Copy` / `Debug` **手写**：理由与 `DefaultConnCfg` 相同（不给 `W` / `R`
// 添加这些约束），且本类型比它少一个 `Rt` 值，约束因此更少。
impl<W, R, M, P, Rt> Clone for CurrentConnCfg<W, R, M, P, Rt>
where
    M: Clone,
    P: Clone,
{
    fn clone(&self) -> Self {
        CurrentConnCfg {
            policy_: self.policy_.clone(),
            metrics_: self.metrics_.clone(),
            _use_rt_: PhantomData,
            _use_w_: PhantomData,
            _use_r_: PhantomData,
        }
    }
}

impl<W, R, M, P, Rt> Copy for CurrentConnCfg<W, R, M, P, Rt>
where
    M: Copy,
    P: Copy,
{}

impl<W, R, M: core::fmt::Debug, P: core::fmt::Debug, Rt> core::fmt::Debug
    for CurrentConnCfg<W, R, M, P, Rt>
{
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("CurrentConnCfg")
            .field("policy_", &self.policy_)
            .field("metrics_", &self.metrics_)
            .finish_non_exhaustive()
    }
}

impl<W, R, M, P, Rt> CurrentConnCfg<W, R, M, P, Rt>
where
    M: TrMetricsSink + Clone + Default,
    P: TrFlowCtrlPolicy,
{
    /// 用策略造出配置（**不取**运行时值：本配置在需要时自己从上下文取）。
    ///
    /// `delivery` 原样交回，形状与 [`DefaultConnCfg::new`] 一致。因为不取运行时值，
    /// 本函数在**上下文之外**调用也不会 panic（真正的前提落到每次取用时）。
    pub fn new(
        delivery: HandshakeDelivery<W, R>,
        policy: P,
    ) -> (HandshakeDelivery<W, R>, Self) {
        let cfg = CurrentConnCfg {
            policy_: policy,
            metrics_: M::default(),
            _use_rt_: PhantomData,
            _use_w_: PhantomData,
            _use_r_: PhantomData,
        };
        (delivery, cfg)
    }
}

impl<W, R, M, P, Rt> TrMuxConfig for CurrentConnCfg<W, R, M, P, Rt>
where
    W: TrBuffWrite<u8> + 'static,
    R: TrBuffRead<u8> + 'static,
    P: TrFlowCtrlPolicy + 'static,
{
    type Data = u8;
    type Dock = Dock;
}

impl<W, R, M, P, Rt> TrConnCfg for CurrentConnCfg<W, R, M, P, Rt>
where
    W: TrBuffWrite<u8> + TrBuffFlush<u8> + TrProducerState + 'static,
    R: TrBuffRead<u8> + TrConsumerState + 'static,
    M: TrMetricsSink + Clone,
    P: TrFlowCtrlPolicy + 'static,
    Rt: TrTime + Clone + 'static + TrRtCurrent,
{
    type Rt = Rt;
    type AllocCfg<'a> = SingleConnAllocCfg;
    type Policy = P;
    type ConnTx = W;
    type ConnRx = R;
    type Metrics = M;

    /// 从**当前上下文**取一个运行时值。
    ///
    /// # Panics
    ///
    /// 调用点不在所选后端的运行时上下文内时 panic——这是本配置的**调用者责任**
    /// （见类型文档与 [`TrRtCurrent`]）。
    fn runtime(&self) -> Self::Rt {
        <Rt as TrRtCurrent>::current_rt()
    }

    fn alloc_cfg(&self) -> Self::AllocCfg<'static> {
        SingleConnAllocCfg::default()
    }

    fn policy(&self) -> &Self::Policy {
        &self.policy_
    }

    /// 取本配置**自己携带**的那一份 sink。
    ///
    /// 约定与 [`DefaultConnCfg`] 相同：`M` 由 `M::default()` 造出，因此要挂一个有状态、
    /// 能被自己读到的采集器，请自定义配置类型并把 sink 句柄放进字段。
    fn metrics(&self) -> &Self::Metrics {
        &self.metrics_
    }
}
