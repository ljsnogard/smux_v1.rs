//! 复用连接的资源策略 [`TrConnCfg`]。
//!
//! 上游 [`abs_smux::conf::TrMuxConfig`] 已收敛 `Data` / `Dock` / `Buff` 三个
//! 类型；本 trait 只补上连接内部还需要、但上游不关心的两样东西：内部结构使用的
//! 分配器，以及流控策略。两条传输半边的类型也放在这里，公开类型因此只需要
//! `MuxConnection<C, S>` 两个参数。
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
use mm_ptr::x_deps::abs_mm::CoreAlloc;

use crate::{
    connection::{Dock, MuxChanBuff},
    flow_ctrl::{DefaultPolicy, TrFlowCtrlPolicy},
    handshake::agent::HandshakeDelivery,
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
/// [`TrMuxConfig`] 给出；本 trait 再补上内部结构分配器、流控策略与两条传输半边。
/// 所有公开类型都只带 `<C, S>` 两个参数（配置 + 本地作用域），因此加一个旋钮
/// 只需要改一处。
pub trait TrConnCfg
where
    Self: TrMuxConfig<Data = u8, Dock = Dock> + 'static,
{
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
}

/// 默认配置：`u8` 数据、[`Dock`] dock、[`CoreAlloc`] 分配、[`DefaultPolicy`]
/// 流控，环存储用把分配器擦除掉的 [`MuxChanBuff`]。
#[derive(Clone, Copy, Debug)]
pub struct DefaultConnCfg<W, R, P = DefaultPolicy> {
    policy_: P,
    /// 连接侧两条半边的类型占位（它们只以类型形式参与）。
    _use_w_: PhantomData<fn() -> W>,
    _use_r_: PhantomData<fn() -> R>,
}

impl<W, R, P> DefaultConnCfg<W, R, P>
where
    P: TrFlowCtrlPolicy,
{
    /// 由流控策略造出配置。
    ///
    /// `delivery` 原样交回：握手交付物与配置总是成对出现，调用方一行就能拿到两者，
    /// 因此这里不去拆它。
    pub const fn new(
        delivery: HandshakeDelivery<W, R>,
        policy: P,
    ) -> (HandshakeDelivery<W, R>, Self) {
        let cfg = DefaultConnCfg {
            policy_: policy,
            _use_w_: PhantomData,
            _use_r_: PhantomData,
        };
        (delivery, cfg)
    }
}

impl<W, R, P> TrMuxConfig for DefaultConnCfg<W, R, P>
where
    W: TrBuffWrite<u8> + 'static,
    R: TrBuffRead<u8> + 'static,
    P: TrFlowCtrlPolicy + 'static,
{
    type Data = u8;
    type Dock = Dock;
    type Buff = MuxChanBuff;
}

impl<W, R, P> TrConnCfg for DefaultConnCfg<W, R, P>
where
    W: TrBuffWrite<u8> + 'static,
    R: TrBuffRead<u8> + 'static,
    P: TrFlowCtrlPolicy + 'static,
{
    type Alloc = CoreAlloc;
    type Policy = P;
    type ConnTx = W;
    type ConnRx = R;
    type StageBuff = MuxChanBuff;

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
        MuxChanBuff::pair_from_alloc_(alloc, K_STAGE_RING_CAPACITY)
            .map_err(|_| BuffAllocError)
    }
}
