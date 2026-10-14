//! # smux_v1_listener
//!
//! 把「底层监听套接字 + 预存的握手条件」变成一条**已握手连接的异步迭代器**。
//!
//! # 它解决什么
//!
//! 一次连接建立是两段（见 `smux_v1` 的 `connection::connect_` 模块文档）：
//!
//! 1. **握手**：[`HandshakeAgent`](smux_v1::handshake::agent::HandshakeAgent) 在一条
//!    已建立的字节流上往返几个帧，协商出连接规格；
//! 2. **建连接**：把协商结果与两条半边交给
//!    [`MuxConnection::new`](smux_v1::connection::MuxConnection::new)。
//!
//! 服务端要把这两段对**每一条**连入的连接都跑一遍，还要处理「这条连接根本不是 smux
//! 客户端」的情况。本 crate 就是把这件事收成一个对象：给它一个监听套接字、一份预存的
//! 握手条件、一套协商策略，它交出
//! [`SmuxListener`](crate::listening::SmuxListener)——每次
//! [`next_async`](abs_async_iter::TrAsyncIterator::next_async) 交付一条**已经握手完成、
//! 可以直接使用**的连接。
//!
//! # 取消与关闭
//!
//! 两者是不同的事，因此 listener 的类型上**没有**令牌参数：
//!
//! - **每次等待各自可取消**：`next_async` 返回的 future 实现
//!   [`TrMayCancel`](crate::x_deps::abs_cancel::TrMayCancel)，用 `may_cancel_with` 施加
//!   调用方自己的令牌即可；取消只中止**这一次**等待，listener 仍然可用；
//! - **listener 整体关闭**：用
//!   [`close_async`](crate::listening::SmuxListener::close_async) 优雅停止（不再接受、
//!   取消在途握手、等所有任务收尾），或直接丢弃本对象——那是不优雅的停止。
//!
//! # 跨运行时
//!
//! 三套异步运行时各自提供自己的 `TcpListener` / `UnixListener`，接受入口与拆分方式都
//! 不同。本 crate 用 [`TrInbound`](crate::config::TrInbound) 把它们抹平成同一个形状，
//! 并为三套运行时 × 两种传输各给一个实现（见 [`runtime`]）：
//!
//! | 运行时 | TCP | UNIX 域 | feature |
//! | --- | --- | --- | --- |
//! | tokio | `runtime::tokio::TcpTransport` | `runtime::tokio::UdsTransport` | `rt-tokio` |
//! | compio | `runtime::compio::TcpTransport` | `runtime::compio::UdsTransport` | `rt-compio` |
//! | smol | `runtime::smol::TcpTransport` | `runtime::smol::UdsTransport` | `rt-smol` |
//!
//! 链接写在这里而不是逐项指向子模块，是因为每个子模块只在对应 feature 下存在，
//! 而本文档要在**任意** feature 组合下都能构建（见 [`runtime`] 的模块文档）。
//!
//! 运行时选择**不由 feature 决定连接行为**：连接的运行时值与本地作用域都由
//! [`TrListenerConfig`](crate::config::TrListenerConfig) 交出，`rt-*` feature 只决定
//! 「哪几套设备适配参与编译」。
//!
//! # 三个需要你自己实现的东西
//!
//! 1. [`TrListenerConfig`](crate::config::TrListenerConfig)：握手条件、连接资源策略、
//!    帧暂存缓冲从哪来；
//! 2. [`TrNegotiatorFactory`](crate::config::TrNegotiatorFactory)：拿到对端条件时由谁
//!    裁决（`smux_v1` 已经给了
//!    [`AcceptAllEntries`](smux_v1::handshake::agent::AcceptAllEntries) 这个「接受一切」
//!    的实现，可以直接拿来用）；
//! 3. 选一个 [`TrInbound`](crate::config::TrInbound)：多半直接用 [`runtime`] 里现成的。

#![feature(impl_trait_in_assoc_type)]

extern crate alloc;

pub mod config;
pub mod listening;
pub mod runtime;

mod scope_;

pub use scope_::TrScopeHost;

mod error_;

pub use error_::SmuxListenerError;

/// 依赖重导出，便于调用方在不额外声明依赖的情况下对齐版本。
pub mod x_deps {
    pub use abs_async_iter;
    pub use futures;
    pub use smux_v1;

    pub use smux_v1::x_deps::{abs_art, abs_buff, abs_cancel, abs_mm, abs_smux, buffex};
}
