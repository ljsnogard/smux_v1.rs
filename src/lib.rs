#![feature(allocator_ext)]
// `BTreeMap` / `BTreeSet` 的分配器参数（`new_in`）另有独立 feature 门；注册表的三张
// 索引表都要用它指定分配器，因此与 `allocator_api` 一并打开。
#![feature(btreemap_alloc)]
#![feature(impl_trait_in_assoc_type)]

extern crate alloc;

#[cfg(test)]
extern crate std;

/// 测试共用的双运行时支撑：`dual_runtime_test_!`。
///
/// 与上游 `buffex` 的同名模块同形：**同一个用例逻辑在 tokio 与 compio 下各跑一遍**。
///
/// 本模块**不在 `cfg(test)` 下编译**：宏需要能被 `tests/` 下的集成测试取到，而集成
/// 测试链接的是「正常编译的库」（不置 `cfg(test)`）。内容只有宏与文档，因此常驻编译
/// 没有运行期代价。
#[macro_use]
mod test_support_;

/// 把 `dual_runtime_test_!` 放到 crate 根，供集成测试 `use smux_v1::dual_runtime_test_;`
/// （`#[macro_export]` 只把它放在 crate 根的宏命名空间里，不产生可 `use` 的项）。
#[doc(hidden)]
#[allow(unused_imports)]
pub use dual_runtime_test_ as _;

/// 同上：把 `single_runtime_test_!` 放到 crate 根，供集成测试
/// `use smux_v1::single_runtime_test_;`。
#[doc(hidden)]
#[allow(unused_imports)]
pub use single_runtime_test_ as _;

pub mod connection;
pub mod flow_ctrl;
pub mod handshake;
pub mod time;

/// 面向 `abs_buff` 的「读满 / 写全」字节游标；握手与复用两个协议共用。
///
/// 内部实现细节，不对外导出。
mod wire_io_;

pub mod x_deps {
    pub use abs_art;
    /// **桥接 crate**：`Runtime` / `LocalScope` / `current` 的裸名解析点，后端由
    /// 集成方在 `Cargo.toml` 里选（本仓缺省 compio）。
    ///
    /// 从这里转出口，是为了让下游**只依赖 smux** 也能取到运行时值——否则调用方要
    /// 自己再加一条 `abs_art-bridge` 依赖、并保证与 smux 用的是同一个后端 feature。
    pub use abs_art_bridge;
    pub use abs_async_iter;
    pub use abs_smux;
    pub use buffex;
    pub use buffex::x_deps::abs_buff;
    pub use mm_ptr;

    pub use crc;
}
