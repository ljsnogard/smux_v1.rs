//! 复用连接的资源策略 [`TrMuxConfig`]。
//!
//! 「环存储类型 / 分配器 / 流控策略」三者在整个连接里总是成组出现，因此打包成
//! 一个由调用方实现的 trait；这样公开类型只需要一个策略参数 `C`。设计背景见
//! [`crate::connection`] 模块文档。
//!
//! 连接已改为「演员核心 + 智能指针封装」：收发半边从公开类型上消失，连接类型是
//! `MuxConnection<C, S, R, W>`。本 trait 仍是唯一的策略打包入口——环存储工厂、
//! 分配器与流控策略三者总是成组出现，因此打包成一个由调用方实现的 trait。

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

/// 复用连接的资源策略：环存储、分配器与流控策略。
///
/// 由调用方实现并注入 [`MuxConnection::new`](crate::connection::MuxConnection::new)；本 crate 只规定「必须能提供这
/// 三样东西」，不规定它们从哪来（堆、静态池、`mm_ptr`、自定义 arena 均可）。
///
/// # 缓冲策略
///
/// 每条子流需要一对 `buffex` 环（发送 / 接收）。[`TrMuxConfig::Buff`] 是环的
/// 存储类型，[`TrMuxConfig::channel_capacity`] 给出单条子流每个方向的容量；
/// 建流时由连接按容量实例化存储并交给 `buffex` 构建器。
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
