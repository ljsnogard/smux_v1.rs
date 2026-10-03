//! 复用连接的资源策略 [`TrMuxConfig`]。
//!
//! 「分配器 / 流控策略」两者在整个连接里总是成组出现，因此打包成
//! 一个由调用方实现的 trait；这样公开类型只需要一个策略参数 `C`。设计背景见
//! [`crate::connection`] 模块文档。
//!
//! 连接已改为「演员核心 + 智能指针封装」：收发半边从公开类型上消失，连接类型是
//! `MuxConnection<W, R, S, C>`。本 trait 是策略打包入口——**分配器 + 流控策略**。
//!
//! # 子流环的存储：类型在这里声明，分配在 accept 端决定
//!
//! 子流环的**智能指针类型**由本 trait 的 [`Buff`](TrMuxConfig::Buff) 声明——它指向
//! **用户自己**为 channel 的 ring 分配的内存（`Owned`、池句柄、`Box`、arena 指针……由
//! 使用环境选）。连接侧按它静态参数化两个循环的本地表、两条事件通道与两个半部，因此
//! 零装箱、零间接。
//!
//! **具体实例**（每条 channel 分配多少、从哪来）由使用环境在最终裁决建立 channel 时
//! 通过 `TrPrepareChannelRing`（`accept_async` 的 `prepare` 参数）当场交出；连接只负责
//! 校验——大小不合适就**拒绝接受**这两份内存。

extern crate alloc;

use core::{
    alloc::AllocatorClone,
    borrow::BorrowMut,
    mem::MaybeUninit,
};

use abs_mm::CoreAlloc;
use mm_ptr::{Owned, x_deps::abs_mm};

use crate::flow_ctrl::TrFlowCtrlPolicy;

#[allow(unused)]
pub trait TrMuxAllocConfig {
    type RegistryAlloc: AllocatorClone;

    /// Allocator for ChannelOwner
    type ChanOwnerAlloc: AllocatorClone;
}

/// 复用连接的资源策略：子流环的**智能指针类型**、分配器与流控策略。
///
/// 由调用方实现并注入 [`MuxConnection::new`](crate::connection::MuxConnection::new)；本 crate 只规定「必须能提供这
/// 三样东西」，不规定它们从哪来（堆、静态池、`mm_ptr`、自定义 arena 均可）。
pub trait TrMuxConfig {
    /// 本连接的**子流环存储智能指针类型**（使用环境选）。
    ///
    /// 连接只能面对**一种**：两个循环、两条事件通道与两个收发半边都只创建一次，类型必须
    /// 先定下来（否则就要装箱或引入别的运行时开销）。**每条 channel 分配多少、从哪来**由
    /// 使用环境在 `accept_async` 的 `prepare` 参数里当场决定，见模块文档。
    type Buff: BorrowMut<[MaybeUninit<u8>]> + Send + Sync + 'static;

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
    type Buff = Owned<[MaybeUninit<u8>], CoreAlloc>;

    type Alloc = CoreAlloc;
    type Policy = crate::flow_ctrl::DefaultPolicy;

    fn allocator(&self) -> Self::Alloc {
        CoreAlloc
    }

    fn policy(&self) -> &Self::Policy {
        &self.0
    }
}
