//! 连接层的公共类型别名。
//!
//! 一部分是**对外**的 dock 类型，其余是内部占位别名（服务于「借用关系 + 连接
//! 泛型」的编码，避免裸 `PhantomData<...>` 触发 `clippy::type_complexity`）。
//! 本模块只放类型定义，不放逻辑。

use core::marker::PhantomData;

/// smux v1 使用的 dock 类型。
///
/// 见 [`crate::connection`] 模块文档 §4。`unspecified()` 为 0，`wildcard()` 为
/// `u32::MAX`；线格式按 1 / 2 / 4 字节自适应宽度编码。
pub type Dock = abs_smux::dock::Dock<u32>;

/// 会话派生类型（listener / handle / telegraph）的借用关系与连接泛型占位。
///
/// > **目标形状（本轮确定，迁移中）**：新架构下这些类型**各自持有一份连接智能
/// > 指针**，不再借用连接，因此本别名整体删除（见 `dev-notes` §17.7 第 2 步）。
///
/// `&'s &'f ()` 把 `'f: 's` 编码进类型本身：这些类型都派生自「借用了连接的
/// 会话」，因此连接借用的生命周期必须覆盖类型自身。用类型别名而非裸
/// `PhantomData<...>`，既避免 `clippy::type_complexity`，也让三处占位语义一致。
pub(crate) type SessionMark_<'s, 'f, R, W, C, Rt> =
    PhantomData<(&'s &'f (), fn() -> (R, W, C, Rt))>;

/// 连接对象上「收发半边 + 运行时」的类型占位（避免裸 `PhantomData<...>` 触发
/// `clippy::type_complexity`）。
pub(crate) type MuxMark_<R, W, Rt> = PhantomData<fn() -> (R, W, Rt)>;
