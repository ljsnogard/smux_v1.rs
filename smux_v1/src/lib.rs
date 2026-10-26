#![feature(allocator_ext)]
// `BTreeMap` / `BTreeSet` 的分配器参数（`new_in`）另有独立 feature 门；注册表的三张
// 索引表都要用它指定分配器，因此与 `allocator_api` 一并打开。
#![feature(impl_trait_in_assoc_type)]
// 上游 `abs_mm::res_man::TrBoxed` 把「取回自己那块内存的分配器」写成一个返回
// `impl Try<Output = &Self::Alloc>` 的方法（分配器可能存在于 `Rc` 这类共享所有权里），
// 因此**实现**该 trait 就必须能引用 `core::ops::Try`。本地不解析它的残差类型，只把
// 结果当「一定成功」使用（见 `connection::ring_::MuxChanBuffAlloc`）。
#![feature(try_trait_v2)]

extern crate alloc;

#[cfg(test)]
extern crate std;

/// 测试共用的多运行时支撑：`multi_art_test_!`。
///
/// 与上游 `buffex` 的同名模块同形：**同一个用例逻辑在 tokio / compio / smol 下各跑
/// 一遍**（三个变体无条件全部生成，见宏文档）。
///
/// 本模块**不在 `cfg(test)` 下编译**：宏需要能被 `tests/` 下的集成测试取到，而集成
/// 测试链接的是「正常编译的库」（不置 `cfg(test)`）。内容只有宏与文档，因此常驻编译
/// 没有运行期代价。
#[macro_use]
mod multi_art_test_;

/// 把 `multi_art_test_!` 放到 crate 根，供集成测试 `use smux_v1::multi_art_test_;`
/// （`#[macro_export]` 只把它放在 crate 根的宏命名空间里，不产生可 `use` 的项）。
#[doc(hidden)]
#[allow(unused_imports)]
pub use multi_art_test_ as _;

/// 同上：把 `single_runtime_test_!` 放到 crate 根，供集成测试
/// `use smux_v1::single_runtime_test_;`。
#[doc(hidden)]
#[allow(unused_imports)]
pub use single_runtime_test_ as _;

pub mod connection;
pub mod flow_ctrl;
pub mod handshake;
pub mod metrics;
pub mod time;

/// 面向 `abs_buff` 的「读满 / 写全」字节游标；握手与复用两个协议共用。
///
/// 内部实现细节，不对外导出。
mod wire_io_;

pub mod x_deps {
    pub use abs_art;
    /// **后端门面**：运行时值（[`Runtime`](abs_art_facade::Runtime) 及其具名别名
    /// `TokioRuntime` / `SmolRuntime` / `CompioRuntime`）与本地作用域的**唯一**解析点。
    ///
    /// 它替下游把 `abs_art-bridge` 的 feature 知识全揽下来了：调用方只认
    /// `abs_art-facade/rt-tokio` 一类 feature，不必知道 `backend-*` /
    /// `default-backend-*`，也不必在每个 crate 里写 `default-features = false`。
    pub use abs_art_facade;
    pub use abs_art_facade::{Runtime, RuntimeTag};

    pub use abs_async_iter;

    pub use abs_buff::x_deps::{abs_cancel, anylr, gen_mcf2};

    pub use abs_smux;

    pub use buffex;
    pub use buffex::x_deps::{abs_buff, abs_sync, atomex, atomic_sync};

    pub use mm_ptr;
    pub use mm_ptr::x_deps::abs_mm;

    pub use crc;
}
