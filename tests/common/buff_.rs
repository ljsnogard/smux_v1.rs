//! 缓冲构造：连接级帧暂存（stage）与子流环存储（[`SmokeBuff`]）。

use core::mem::MaybeUninit;
use mm_ptr::{Owned, x_deps::abs_mm::CoreAlloc};
use smux_v1::connection::K_STAGE_RING_CAPACITY;

use crate::common::K_CHANNEL_CAPACITY;

/// 造一对**连接级**帧暂存缓冲（容量 [`K_STAGE_RING_CAPACITY`]）。
///
/// [`MuxConnection::new`] 要求调用方给出两块连接级缓冲，连接把它们建成两条帧暂存环
/// （见 `src/connection/session_.rs` 模块文档）。测试里直接按配置给出。
pub fn make_stage_buffs_() -> (SmokeBuff, SmokeBuff) {
    make_stage_buffs_with_(K_STAGE_RING_CAPACITY)
}


/// 按指定容量造一对**连接级**帧暂存缓冲。
///
/// 「极小容量」用例（容量 1 字节）用它，验收「逐字节异步解析 ⇒ 帧暂存不需要装下
/// 整帧」这条要求。
pub fn make_stage_buffs_with_(capacity: usize) -> (SmokeBuff, SmokeBuff) {
    (
        Owned::new_uninit_slice(capacity, CoreAlloc),
        Owned::new_uninit_slice(capacity, CoreAlloc),
    )
}


/// 环存储的具体类型（元素 `u8` + `CoreAlloc`），避免类型推断歧义。
///
/// 对测试目标公开：`tests/thread_safety.rs` 需要给泛化的 `connect_pair_` 标注
/// 连接配置类型，而配置类型上带着这个存储类型。
pub type SmokeBuff = Owned<[MaybeUninit<u8>], CoreAlloc>;


/// 造一块子流环存储（容量 [`K_CHANNEL_CAPACITY`]）。
///
/// 建流最终裁决（`accept_async`）要求调用方给出**本条子流**要用的两块缓冲；上游把
/// 「形式」固定为借用型切片（`&'static mut [MaybeUninit<u8>]`），**来源与容量由调用方
/// 决定**——测试里直接泄漏一块（生产代码里应当来自静态区或调用方的 arena）。
pub fn make_channel_buff_() -> SmokeBuff {
    Owned::new_uninit_slice(K_CHANNEL_CAPACITY, CoreAlloc)
}


/// 按指定容量造一块缓冲（用于「每条子流自己决定分配多少」的用例）。
pub fn make_channel_buff_with_(capacity: usize) -> SmokeBuff {
    Owned::new_uninit_slice(capacity, CoreAlloc)
}
