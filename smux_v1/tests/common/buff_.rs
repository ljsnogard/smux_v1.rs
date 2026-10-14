//! 缓冲构造：连接级帧暂存（stage）与子流环存储（[`SmokeBuff`]）。
//!
//! 两种缓冲都由**调用方交出的智能指针**提供——测试里用 [`Owned`]（`mm_ptr` 的独占
//! 指针）当那个拥有者。连接侧把它连同环、控制块一起装进**同一次分配**（见
//! `src/connection/ring_`），因此「交出一块内存」这件事在生产与测试里走同一条路径。
//!
//! 环内存的**类型参数**就是那个智能指针：`MuxChanBuffOwnedBy<SmokeBuffVec>`（缓冲
//! 视图 + 拥有者，零堆分配），连接把它吸收成类型无关的 `MuxChanBuff_`。

use core::mem::MaybeUninit;

use mm_ptr::{Owned, x_deps::abs_mm::CoreAlloc};
use smux_v1::connection::{K_STAGE_RING_CAPACITY, MuxChanBuffOwnedBy};

use crate::common::K_CHANNEL_CAPACITY;

/// 造一对**连接级**帧暂存缓冲（容量 [`K_STAGE_RING_CAPACITY`]）。
///
/// [`MuxConnection::new`](smux_v1::connection::MuxConnection::new) 要求调用方给出两块
/// 连接级缓冲，连接把它们建成两条帧暂存环（见 `src/connection/session_.rs` 模块文档）。
pub fn make_stage_buffs_() -> (SmokeStageBuff, SmokeStageBuff) {
    make_stage_buffs_with_(K_STAGE_RING_CAPACITY)
}

/// 按指定容量造一对**连接级**帧暂存缓冲。
///
/// 「极小容量」用例（容量 1 字节）用它，验收「逐字节异步解析 ⇒ 帧暂存不需要装下
/// 整帧」这条要求。
pub fn make_stage_buffs_with_(capacity: usize) -> (SmokeStageBuff, SmokeStageBuff) {
    (
        MuxChanBuffOwnedBy::new(Owned::new_uninit_slice(capacity, CoreAlloc)),
        MuxChanBuffOwnedBy::new(Owned::new_uninit_slice(capacity, CoreAlloc)),
    )
}

/// 环内存的**拥有者**类型（元素 `u8` + `CoreAlloc`），避免类型推断歧义。
///
/// 对测试目标公开：`tests/thread_safety.rs` 需要给泛化的 `connect_pair_` 标注
/// 连接配置类型，而配置类型上带着这个存储类型。
pub type SmokeBuff = Owned<[MaybeUninit<u8>], CoreAlloc>;

/// 交给 `MuxConnection::new` 的连接级帧暂存缓冲（视图 + 拥有者）。
pub type SmokeStageBuff = MuxChanBuffOwnedBy<SmokeBuff>;

/// 造一块子流环存储（容量 [`K_CHANNEL_CAPACITY`]）。
///
/// 建流最终裁决（`accept_async`）要求调用方给出**本条子流**要用的两块缓冲；
/// 容量与来源都由调用方决定——这里就是测试自己按常量向 `CoreAlloc` 要一块。
pub fn make_channel_buff_() -> SmokeBuff {
    make_channel_buff_with_(K_CHANNEL_CAPACITY)
}

/// 按指定容量造一块缓冲（用于「每条子流自己决定分配多少」的用例）。
pub fn make_channel_buff_with_(capacity: usize) -> SmokeBuff {
    Owned::new_uninit_slice(capacity, CoreAlloc)
}
