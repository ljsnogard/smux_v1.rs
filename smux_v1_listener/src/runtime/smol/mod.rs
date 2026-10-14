//! **smol** 后端的入向适配：TCP 与 UNIX 域。
//!
//! # 本目录的分工
//!
//! | 文件 | 内容 |
//! | --- | --- |
//! | `shared_` | 两条半边、连接配置、通用建环函数——TCP 与 UDS 共用 |
//! | `tcp_` | [`TcpTransport`]：从 `smol::net::TcpListener` 上接受 |
//! | `uds_` | [`UdsTransport`]：从 `smol::net::UnixListener` 上接受 |
//!
//! 三个子模块都是**私有**的（本文件只做导出，不含实现——见 AGENTS.md 纪律 5），因此
//! 表格里它们用代码字体而不写成链接。
//!
//! # 写半边为什么用**自驱动**形态
//!
//! 理由与 tokio 侧逐字相同：`MuxConnection` 只把适配器当 `TrBuffWrite` 用，从不调
//! `flush_async`，按需形态下最后一段会永远留在环里。`try_new_local` 把泵投到本线程的
//! [`TrLocalScope`](smux_v1::x_deps::abs_art::TrLocalScope)，因此构造点必须落在
//! `scope.run_until(..)` 之类的本地驱动栈内。
//!
//! # 设备的 trait 是这一族
//!
//! smol 自己不做 IO：`smol::io` 就是 `futures_lite::io` 的重导出，`async-net` 的
//! `TcpStream` / `UnixStream` 实现的也是这一族 trait。因此 `smol::io::split` 对两者都
//! 成立，TCP 与 UNIX 域共用同一份拆分逻辑。

mod shared_;
mod tcp_;
mod uds_;

pub use shared_::{
    HeldRx, HeldTx, K_DEFAULT_RING_CAPACITY, TcpConnCfg, TcpRx, TcpTx, UdsConnCfg,
    UdsRx, UdsTx,
};
pub use tcp_::TcpTransport;
pub use uds_::UdsTransport;
