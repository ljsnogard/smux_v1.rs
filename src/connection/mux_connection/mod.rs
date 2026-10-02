//! [`MuxConnection`]：实现 `abs_smux::conn::TrConnection` 的复用连接对象。
//!
//! 本模块分两层：
//!
//! - `core_`：[`MuxCore`] **演员核心**——连接的全部共享状态与全部资源句柄，
//!   对它的修改经内部读写锁串行化（不使用 actor 框架、不使用消息通道）；
//! - `conn_`：[`MuxConnection`] —— 对 `Shared<MuxCore<C, S>, C::Alloc>` 的**薄封装**，
//!   `Clone` 廉价，可被任意分发到不同函数 / 结构体；建连（`new`）与 `bind_async`
//!   也在这里。
//!
//! 连接的读写循环在 `session_`，各会话对象（binding / listener / handle / 半部）
//! 在各自子模块。设计见 [`crate::connection`] 模块文档 §2 与
//! `dev-notes/connection-20261002-0548.md` §17。

mod conn_;
mod core_;
mod registry_;

pub use conn_::MuxConnection;
pub(crate) use registry_::ChannelRegistry_;
