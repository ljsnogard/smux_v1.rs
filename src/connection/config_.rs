//! 复用连接的资源策略 [`TrConnCfg`]。
//!
//! 上游 [`abs_smux::conf::TrMuxConfig`] 已收敛 `Data` / `Dock` / `Buff` 三个
//! 类型；本 trait 只补上连接内部还需要、但上游不关心的两样东西：内部结构使用的
//! 分配器，以及流控策略。两条传输半边的类型也放在这里，公开类型因此只需要
//! `MuxConnection<C>` 一个参数（运行时值由本 trait 的 `Rt` 关联类型给出）。
//!
//! # 子流环的存储
//!
//! 子流环的智能指针类型由上游 [`TrMuxConfig::Buff`](abs_smux::conf::TrMuxConfig::Buff) 声明，连接侧按它静态参数化
//! 两个循环的本地表、两条事件通道与两个半部。**具体实例**（每条 channel 分配
//! 多少、从哪来）由使用环境在最终裁决建立 channel 时通过
//! [`TrPrepareChannelRing`](abs_smux::chan::TrPrepareChannelRing)（`accept_async`
//! 的 `prepare` 参数）当场交出；连接只负责校验——大小不合适就拒绝接受。
//!
//! # 连接级环的存储
//!
//! 连接级（「帧暂存」）两条环的存储类型由 [`TrConnCfg::StageBuff`] 声明，实例由
//! [`TrConnCfg::make_stage_buffs`] 造出：它与 [`TrMuxConfig::Buff`] **解耦**，
//! 因此帧暂存的容量不受「子流环容量」这一策略支配（连接级环至少要能驻留一个
//! 满帧，见 `session_` 模块文档）。

extern crate alloc;

use core::{
    alloc::AllocatorClone,
    borrow::BorrowMut,
    marker::PhantomData,
    mem::MaybeUninit,
};

use buffex::x_deps::abs_buff::{TrBuffRead, TrBuffWrite};
use abs_smux::conf::TrMuxConfig;
use mm_ptr::{Owned, x_deps::abs_mm::CoreAlloc};

use crate::{
    connection::{Dock, MuxChanBuff},
    flow_ctrl::{DefaultPolicy, TrFlowCtrlPolicy},
    handshake::agent::HandshakeDelivery,
    time::TrTime,
};

#[allow(unused)]
pub trait TrMuxAllocConfig {
    type RegistryAlloc: AllocatorClone;

    /// Allocator for ChannelOwner
    type ChanOwnerAlloc: AllocatorClone;
}

/// managed 路径构造环缓冲失败。
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("构造 channel ring 缓冲失败")]
pub struct BuffAllocError;

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

    /// 连接侧的写 / 读半边（即两条传输的缓冲类型）。
    type ConnTx: TrBuffWrite<u8>;
    type ConnRx: TrBuffRead<u8>;

    /// **连接级**两条帧暂存环的存储类型（读环一块、写环一块）。
    ///
    /// 与 [`TrMuxConfig::Buff`] 解耦：子流环容量由调用方在 `accept_async` 逐条
    /// 决定，帧暂存容量是**连接级**策略，两者不该互相绑架。容量下限由实现者保证
    /// （至少能整块驻留一个满帧，见 `session_` 模块文档），连接不再二次校验。
    ///
    /// 这里刻意**不**要求 `Send + Sync`：是否需要跨线程搬运由具体装配决定
    /// （`MuxConnection::new` 才要求 `C::StageBuff: Send + Sync`），与
    /// [`TrMuxConfig::Buff`] 的约束保持同一层级。
    type StageBuff: 'static + BorrowMut<[MaybeUninit<u8>]>;

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

    /// 用自身分配器造出一对该 channel 使用的环缓冲（Tx、Rx）。
    ///
    /// 这是 managed 路径的扩展点：`C::Buff` 是具体类型时返回具体缓冲，
    /// 因此整个数据面可以完全单态化、没有 `dyn`。
    fn make_ring_buffs(
        &self,
        alloc: Self::Alloc,
        capacity: usize,
    ) -> Result<(Self::Buff, Self::Buff), BuffAllocError>;

    /// 用自身分配器造出连接级两条帧暂存环的存储：`(读环, 写环)`。
    ///
    /// 与 [`Self::make_ring_buffs`] 的差别不只是类型：**容量在这里由配置决定**，
    /// 调用方不需要（也不应该）知道帧暂存要多大。
    ///
    /// # Errors
    ///
    /// 分配失败时返回 [`BuffAllocError`]；调用方（[`MuxConnection::new`]）把它视为
    /// 连接无法建立。
    ///
    /// [`MuxConnection::new`]: crate::connection::MuxConnection::new
    fn make_stage_buffs(
        &self,
        alloc: Self::Alloc,
    ) -> Result<(Self::StageBuff, Self::StageBuff), BuffAllocError>;

    /// 子流环的**缺省容量**：`accept` 那条「连接替你管内存」的省事路径用它。
    ///
    /// 它只是省事路径的缺省值，不是协议或实现的下限——子流环容量本身由调用方逐条
    /// 决定（[`Self::make_ring_buffs`] 收的就是逐条传进来的容量），本项给默认值，
    /// 因此实现者不必关心它。
    const RING_CAPACITY: usize = K_DEFAULT_CHANNEL_RING_CAPACITY;
}

/// [`TrConnCfg::RING_CAPACITY`] 的缺省值：4 KiB。
///
/// 取这个值的理由与「子流环是逐条可调的」这件事无关——它只是「调用方没说要多大时
/// 给一个够用的数」；想逐条控制的调用方走 `accept_async_managed(.., cap)` 或自己实现
/// [`TrPrepareChannelRing`](abs_smux::chan::TrPrepareChannelRing)。
pub const K_DEFAULT_CHANNEL_RING_CAPACITY: usize = 4096usize;

/// 默认配置：`u8` 数据、[`Dock`] dock、[`CoreAlloc`] 分配、[`DefaultPolicy`]
/// 流控；**帧暂存**用不经类型擦除的 [`Owned`]，**子流环**用把分配器擦除掉的
/// [`MuxChanBuff`]（`accept_async_managed` 那条路要求缓冲类型与调用方给出的分配器
/// 无关）。
///
/// 泛型参数 `Rt` 是**运行时值**，默认取 `abs_art-bridge` 的裸名
/// [`Runtime`](abs_art_bridge::Runtime)——即集成方在 `Cargo.toml` 里选定的后端
/// （本仓缺省是 compio）。要用另一个后端或假运行时值，显式写出 `Rt` 即可。
///
/// # 为什么两块缓冲用不同的类型
///
/// [`MuxConnection::new`] 要求 `C::StageBuff: Send + Sync`（帧暂存环的存储会在建连时
/// 被搬进循环），而 [`MuxChanBuff`] 内部持有裸指针、没有这两个 impl。帧暂存的分配器
/// 本就是配置自己定的（[`CoreAlloc`]），用 [`Owned`] 既满足约束又不必引入新的
/// `unsafe impl`；子流环则需要「分配器擦除」这一点，那条路不要求 `Send + Sync`。
///
/// [`MuxConnection::new`]: crate::connection::MuxConnection::new
pub struct DefaultConnCfg<W, R, P = DefaultPolicy, Rt = crate::connection::DefaultRt_> {
    /// 运行时值（计时与时刻的来源），建连时抓住、此后按需克隆。
    rt_: Rt,
    policy_: P,
    /// 连接侧两条半边的类型占位（它们只以类型形式参与）。
    _use_w_: PhantomData<fn() -> W>,
    _use_r_: PhantomData<fn() -> R>,
}

// `Clone` / `Copy` / `Debug` **手写**：结构里只有 `PhantomData`、策略值与运行时值，
// 不该给 `W` / `R` 加上这些约束（环半部既不 `Clone` 也不 `Debug`）。
impl<W, R, P: Clone, Rt: Clone> Clone for DefaultConnCfg<W, R, P, Rt> {
    fn clone(&self) -> Self {
        DefaultConnCfg {
            rt_: self.rt_.clone(),
            policy_: self.policy_.clone(),
            _use_w_: PhantomData,
            _use_r_: PhantomData,
        }
    }
}

impl<W, R, P: Copy, Rt: Copy> Copy for DefaultConnCfg<W, R, P, Rt> {}

impl<W, R, P: core::fmt::Debug, Rt: core::fmt::Debug> core::fmt::Debug
    for DefaultConnCfg<W, R, P, Rt>
{
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("DefaultConnCfg")
            .field("rt_", &self.rt_)
            .field("policy_", &self.policy_)
            .finish_non_exhaustive()
    }
}

impl<W, R, P> DefaultConnCfg<W, R, P, super::DefaultRt_>
where
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
            _use_w_: PhantomData,
            _use_r_: PhantomData,
        };
        (delivery, cfg)
    }
}

impl<W, R, P, Rt> DefaultConnCfg<W, R, P, Rt>
where
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
            _use_w_: PhantomData,
            _use_r_: PhantomData,
        };        (delivery, cfg)
    }
}

impl<W, R, P, Rt> TrMuxConfig for DefaultConnCfg<W, R, P, Rt>
where
    W: TrBuffWrite<u8> + 'static,
    R: TrBuffRead<u8> + 'static,
    P: TrFlowCtrlPolicy + 'static,
{
    type Data = u8;
    type Dock = Dock;
    type Buff = MuxChanBuff;
}

impl<W, R, P, Rt> TrConnCfg for DefaultConnCfg<W, R, P, Rt>
where
    W: TrBuffWrite<u8> + 'static,
    R: TrBuffRead<u8> + 'static,
    P: TrFlowCtrlPolicy + 'static,
    Rt: TrTime + Clone + 'static,
{
    type Rt = Rt;
    type Alloc = CoreAlloc;
    type Policy = P;
    type ConnTx = W;
    type ConnRx = R;
    type StageBuff = Owned<[MaybeUninit<u8>], CoreAlloc>;

    fn runtime(&self) -> Self::Rt {
        self.rt_.clone()
    }

    fn allocator(&self) -> Self::Alloc {
        CoreAlloc
    }

    fn policy(&self) -> &Self::Policy {
        &self.policy_
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
        let cap = K_STAGE_RING_CAPACITY;
        let read = Owned::try_new_uninit_slice(cap, alloc).map_err(|_| BuffAllocError)?;
        let write = Owned::try_new_uninit_slice(cap, alloc).map_err(|_| BuffAllocError)?;
        Result::Ok((read, write))
    }
}
