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
//! 按需（由调用方在收尾入口顺带推）与自驱动（构造时把泵投到本线程 `LocalSet`）。这里取
//! **自驱动**，理由是"少一次显式收尾、少一处可能忘记的调用"：
//!
//! * 自驱动形态下「提交进环」就等于「迟早送达设备」，**不需要**任何人记得收尾；
//! * 按需形态**也能**工作——`smux_v1` 自 `2026-10` 起按 `abs_buff::TrBuffFlush` 收尾
//!   （握手每帧后 `flush_async`、连接退出时 `close`）；取自驱动的代价是构造点必须落在
//!   [`TrLocalScope::run_until`](smux_v1::x_deps::abs_art::TrLocalScope::run_until) 之类的
//!   本地驱动栈内（`SmuxListener` 满足这一点：建环就发生在接受槽位里）。
//!
//! 结论：这里的形态是**性能/简洁选择**，不再是正确性前提。

mod shared_;
mod tcp_;
mod uds_;

pub use shared_::{
    HeldRx, HeldTx, K_DEFAULT_RING_CAPACITY, TcpConnCfg, TcpRx, TcpTx, UdsConnCfg,
    UdsRx, UdsTx,
};
pub use tcp_::TcpTransport;
pub use uds_::UdsTransport;
