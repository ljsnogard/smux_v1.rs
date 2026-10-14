#![feature(allocator_ext)]

#![no_std]

pub mod chan;
pub mod conf;
pub mod conn;
pub mod dock;
pub mod telegraph;

pub mod x_deps {
    pub use abs_mm;

    pub use abs_buff;
    pub use abs_buff::x_deps::{abs_cancel, anylr, funty, gen_mcf2};
}
