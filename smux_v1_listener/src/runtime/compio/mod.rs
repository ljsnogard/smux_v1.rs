//! **compio** 后端的入向适配：TCP 与 UNIX 域。
//!
//! # 本目录的分工
//!
//! | 文件 | 内容 |
//! | --- | --- |
//! | `shared_` | 两条半边、连接配置、通用建环函数——TCP 与 UDS 共用 |
//! | `tcp_` | [`TcpTransport`]：从 `compio::net::TcpListener` 上接受 |
//! | `uds_` | [`UdsTransport`]：从 `compio::net::UnixListener` 上接受 |
//!
//! 三个子模块都是**私有**的（本文件只做导出，不含实现——见 AGENTS.md 纪律 5），因此
//! 表格里它们用代码字体而不写成链接。
//!
//! # 与 tokio / smol 侧的两点不同
//!
//! 1. **拆分**：compio 的 `into_split` 是固有方法，把一条流拆成**两个同类型的流**
//!    （`(stream, stream)`），而不是「读半边 + 写半边」两个不同类型。因此
//!    [`TcpTx`] 与 [`TcpRx`] 形态相同，只是环的方向不同。
//! 2. **构造前提**：`buffex_compio_adapt` 的 `try_new` 必须在**正在运行的 compio
//!    runtime 内**调用（它要 `compio::runtime::spawn` 后台泵），否则返回
//!    `TryNewError::NotInRuntime`。`SmuxListener` 的接受槽位本身就跑在该运行时的本地
//!    队列上，因此成立；但在运行时之外手工调
//!    [`into_halves`](crate::config::TrInbound::into_halves) 会 panic。
//!
//! 这一侧不区分「按需 / 自驱动」：compio 的 `try_new` 本来就 `spawn` 后台泵（它的
//! `spawn` 不要求 `Send`，而 tokio 的多线程 `spawn` 要求）。

mod shared_;
mod tcp_;
mod uds_;

pub use shared_::{
    HeldRx, HeldTx, K_DEFAULT_RING_CAPACITY, TcpConnCfg, TcpRx, TcpTx, UdsConnCfg,
    UdsRx, UdsTx,
};
pub use tcp_::TcpTransport;
pub use uds_::UdsTransport;
