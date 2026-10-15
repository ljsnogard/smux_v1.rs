//! 缓冲构造：**子流环**存储（[`SmokeBuff`]）。
//!
//! 连接的传输侧不再需要调用方交出缓冲——握手的交付物（`Tx` / `Rx`）自带缓冲，
//! 内侧两个循环直接驱动它们（见 `MuxConnection::new` 的文档）。因此本模块只剩子流
//! 环这一路：它的内存在最终裁决（`accept_async`）时由调用方**当场**交出，类型是
//! [`Owned`] 这样的智能指针，连接把它连同环、控制块一起装进**同一次分配**
//! （见 `src/connection/ring_`）。

use core::mem::MaybeUninit;

use mm_ptr::{Owned, x_deps::abs_mm::CoreAlloc};

use crate::common::K_CHANNEL_CAPACITY;

/// 环内存的**拥有者**类型（元素 `u8` + `CoreAlloc`），避免类型推断歧义。
///
/// 对测试目标公开：`tests/thread_safety.rs` 需要给泛化的 `connect_pair_` 标注
/// 连接配置类型，而配置类型上带着这个存储类型；`tests/common/pump_.rs` 也用它造
/// **传输环**。
pub type SmokeBuff = Owned<[MaybeUninit<u8>], CoreAlloc>;

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
