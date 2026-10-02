//! 连接层的公共类型别名。
//!
//! 本模块只放类型定义，不放逻辑。

/// smux v1 使用的 dock 类型。
///
/// 见 [`crate::connection`] 模块文档 §4。`unspecified()` 为 0，`wildcard()` 为
/// `u32::MAX`；线格式按 1 / 2 / 4 字节自适应宽度编码。
pub type Dock = abs_smux::dock::Dock<u32>;
