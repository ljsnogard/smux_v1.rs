//! 复用连接的资源策略 [`TrMuxConfig`]。
//!
//! 「环存储类型 / 分配器 / 流控策略」三者在整个连接里总是成组出现，因此打包成
//! 一个由调用方实现的 trait；这样公开类型只需要一个策略参数 `C`。设计背景见
//! [`crate::connection`] 模块文档。
//!
//! 连接已改为「演员核心 + 智能指针封装」：收发半边从公开类型上消失，连接类型是
//! `MuxConnection<W, R, S, C>`。本 trait 是策略打包入口——**分配器 + 流控策略**。
//!
//! # 环存储**不在**这里
//!
//! 子流缓冲由**使用环境**在**最终裁决建立 channel** 时通过 `TrPrepareChannelBuff`
//! （`accept_async` 的 `prepare` 参数）给出，**每条子流各自决定用什么承载**（自有
//! 所有权、借用切片、池分配、`Vec`、静态区……）。连接侧对缓冲类型一无所知：它把
//! 调用方给的存储装箱进一个内部载具
//! （[`MuxChanBuff`](crate::connection::mux_connection)），于是两个循环、两条事件通道
//! 与两个半部的类型固定下来，而缓冲的承载者仍然自由。因此本 trait **没有**（也不该
//! 有）「环存储类型」这一项。

extern crate alloc;

use core::alloc::AllocatorClone;

use abs_mm::CoreAlloc;
use mm_ptr::x_deps::abs_mm;

use crate::flow_ctrl::TrFlowCtrlPolicy;

#[allow(unused)]
pub trait TrMuxAllocConfig {
    type RegistryAlloc: AllocatorClone;

    /// Allocator for ChannelOwner
    type ChanOwnerAlloc: AllocatorClone;
}

/// 复用连接的资源策略：分配器与流控策略。
///
/// 由调用方实现并注入 [`MuxConnection::new`](crate::connection::MuxConnection::new)；本 crate 只规定「必须能提供这
/// 两样东西」，不规定它们从哪来（堆、静态池、`mm_ptr`、自定义 arena 均可）。子流缓冲
/// **不在这里**（见模块文档）。
pub trait TrMuxConfig {
    /// 环内存与帧暂存的分配器。
    type Alloc: AllocatorClone + Send + Sync;

    /// 流控策略。
    type Policy: TrFlowCtrlPolicy;

    /// 取分配器（按值，`buffex` 的构建器按值接收）。
    fn allocator(&self) -> Self::Alloc;

    /// 取流控策略。
    fn policy(&self) -> &Self::Policy;

}

#[derive(Clone, Copy, Debug, Default)]
pub struct DefaultMuxConfig(crate::flow_ctrl::DefaultPolicy);

impl TrMuxConfig for DefaultMuxConfig {
    type Alloc = CoreAlloc;
    type Policy = crate::flow_ctrl::DefaultPolicy;

    fn allocator(&self) -> Self::Alloc {
        CoreAlloc
    }

    fn policy(&self) -> &Self::Policy {
        &self.0
    }
}
