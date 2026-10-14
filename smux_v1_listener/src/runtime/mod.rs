//! 三套运行时的入向适配。
//!
//! 每个子模块把「本后端自己的 `TcpListener` / `UnixListener`」接到
//! [`TrInbound`](crate::config::TrInbound) 上，产出两条 `abs_buff` 半边。三个后端的
//! 公开形状**逐字一致**，因此本模块统一把它们重导出一遍：
//!
//! | 项 | 含义 |
//! | --- | --- |
//! | `TcpTransport` | 从本后端的 TCP 监听套接字上接受 |
//! | `UdsTransport` | 从本后端的 UNIX 域监听套接字上接受 |
//! | `TcpTx` / `TcpRx` / `UdsTx` / `UdsRx` | 四条半边的具体类型 |
//! | `TcpConnCfg` / `UdsConnCfg` | 与它们配套的**连接配置类型**（含本后端的运行时值） |
//! | `K_DEFAULT_RING_CAPACITY` | 传输环的缺省容量（64 KiB） |
//!
//! # 同时点亮多个后端
//!
//! 本模块按 feature 逐个 `pub use`，同名项因此会被**后一个覆盖前一个**，不产生歧义。
//! 需要精确指名某个后端时直接写它的子模块路径（`runtime::tokio::TcpTransport`）。
//! 实践中不必如此：连接配置里的环类型必须与传输类型对齐，选错后端就编译不过。

#[cfg(feature = "rt-compio")]
pub mod compio;

#[cfg(feature = "rt-smol")]
pub mod smol;

#[cfg(feature = "rt-tokio")]
pub mod tokio;

#[cfg(feature = "rt-compio")]
pub use compio::{
    K_DEFAULT_RING_CAPACITY, TcpConnCfg, TcpRx, TcpTransport, TcpTx, UdsConnCfg,
    UdsRx, UdsTransport, UdsTx,
};

#[cfg(feature = "rt-smol")]
pub use smol::{
    K_DEFAULT_RING_CAPACITY, TcpConnCfg, TcpRx, TcpTransport, TcpTx, UdsConnCfg,
    UdsRx, UdsTransport, UdsTx,
};

#[cfg(feature = "rt-tokio")]
pub use tokio::{
    K_DEFAULT_RING_CAPACITY, TcpConnCfg, TcpRx, TcpTransport, TcpTx, UdsConnCfg,
    UdsRx, UdsTransport, UdsTx,
};
