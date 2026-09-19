#![feature(impl_trait_in_assoc_type)]
#![no_std]

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
    pub use abs_buff;
    pub use abs_smux;
    pub use buffex;

    pub use crc;
}
