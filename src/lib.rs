#![feature(allocator_api)]
// `BTreeMap` / `BTreeSet` 的分配器参数（`new_in`）另有独立 feature 门；注册表的三张
// 索引表都要用它指定分配器，因此与 `allocator_api` 一并打开。
#![feature(btreemap_alloc)]
#![feature(impl_trait_in_assoc_type)]

#[cfg(test)]
extern crate std;

pub mod connection;
pub mod flow_ctrl;
pub mod handshake;

/// 面向 `abs_buff` 的「读满 / 写全」字节游标；握手与复用两个协议共用。
///
/// 内部实现细节，不对外导出。
mod wire_io_;

pub mod x_deps {
    pub use abs_art;
    pub use abs_async_iter;
    pub use abs_smux;
    pub use buffex;
    pub use buffex::x_deps::abs_buff;
    pub use mm_ptr;

    pub use crc;
}
