//! 复用连接的资源策略 [`TrMuxConfig`]。
//!
//! 「环存储类型 / 分配器 / 流控策略」三者在整个连接里总是成组出现，因此打包成
//! 一个由调用方实现的 trait；这样公开类型只需要一个策略参数 `C`。设计背景见
//! [`crate::connection`] 模块文档。
//!
//! 连接已改为「演员核心 + 智能指针封装」：收发半边从公开类型上消失，连接类型是
//! `MuxConnection<C, S, R, W>`。本 trait 仍是唯一的策略打包入口——环存储工厂、
//! 分配器与流控策略三者总是成组出现，因此打包成一个由调用方实现的 trait。

use core::{
    alloc::AllocatorClone,
    borrow::BorrowMut,
    mem::MaybeUninit,
};

use crate::flow_ctrl::TrFlowCtrlPolicy;

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
    /// 环存储类型；通常是 `mm_ptr::Owned<[MaybeUninit<u8>], Self::Alloc>`。
    type Buff: BorrowMut<[MaybeUninit<u8>]> + Send + Sync;

    /// 环内存与帧暂存的分配器。
    type Alloc: AllocatorClone + Send + Sync;

    /// 流控策略。
    type Policy: TrFlowCtrlPolicy;

    /// 取分配器（按值，`buffex` 的构建器按值接收）。
    fn allocator(&self) -> Self::Alloc;

    /// 取流控策略。
    fn policy(&self) -> &Self::Policy;

    /// 单条子流**每个方向**的环容量（字节）。
    fn channel_capacity(&self) -> usize;

    /// 用调用方注入的分配器分配一块长度为 `len` 的**环存储**。
    ///
    /// 这是子流收发环唯一的「按容量造存储」入口：`len` 取
    /// [`TrMuxConfig::channel_capacity`]，返回值随后交给环构建入口（crate 内部的
    /// 私有模块）。
    ///
    /// 连接内部的两处**帧暂存**（读循环的收帧缓冲、写循环的成帧缓冲）不经过本
    /// 方法，而是同一分配器（[`TrMuxConfig::Alloc`]）上的定长切片——它们要按字节
    /// 读写已初始化的内容，与「元素为 `MaybeUninit` 的环存储」语义不同。分配来源
    /// 与预算仍然完全由调用方掌握。
    ///
    /// # Panics
    ///
    /// 分配失败时如何表现由实现决定（标准库容器的惯例是 panic）。本 crate 不
    /// 隐式分配，因此调用方可以按 `max_channel_count × channel_capacity` 量级
    /// 准备分配器。
    fn make_buff(&self, len: usize) -> Self::Buff;
}
