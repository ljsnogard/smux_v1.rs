#![feature(impl_trait_in_assoc_type)]
#![no_std]

#[cfg(test)]
extern crate std;

pub mod connection;
pub mod flow_ctrl;
pub mod handshake;

pub mod x_deps {
    pub use abs_async_iter;
    pub use abs_buff;
    pub use abs_smux;
    pub use buffex;

    pub use crc;
}
