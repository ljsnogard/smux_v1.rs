//! **tokio** 后端的入向适配：TCP 与 UNIX 域。
//!
//! # 本目录的分工
//!
//! | 文件 | 内容 |
//! | --- | --- |
//! | `shared_` | 两条半边、连接配置、通用建环函数——TCP 与 UDS 共用 |
//! | `tcp_` | [`TcpTransport`]：从 `tokio::net::TcpListener` 上接受 |
//! | `uds_` | [`UdsTransport`]：从 `tokio::net::UnixListener` 上接受 |
//!
//! 三个子模块都是**私有**的（本文件只做导出，不含实现——见 AGENTS.md 纪律 5），因此
//! 表格里它们用代码字体而不写成链接。
//!
//! # 写半边为什么用**自驱动**形态
//!
//! `buffex_tokio_adapt::BuffWrite` 的「环 → 设备」有两种驱动方式（见该 crate 文档）：
//! 按需（谁记得谁调 `flush_async`）与自驱动（构造时把泵投到本线程 `LocalSet`）。这里取
//! **自驱动**：`MuxConnection` 只把适配器当 `TrBuffWrite` 用，从不调 `flush_async`，
//! 按需形态下最后一段会永远留在环里。
//!
//! 代价是构造 `BuffWrite` 的调用点必须落在
//! [`TrLocalScope::run_until`](smux_v1::x_deps::abs_art::TrLocalScope::run_until) 之类的
//! 本地驱动栈内。`SmuxListener` 满足这一点：建环就发生在接受槽位里。

mod shared_;
mod tcp_;
mod uds_;

pub use shared_::{
    HeldRx, HeldTx, K_DEFAULT_RING_CAPACITY, TcpConnCfg, TcpRx, TcpTx, UdsConnCfg,
    UdsRx, UdsTx,
};
pub use tcp_::TcpTransport;
pub use uds_::UdsTransport;
