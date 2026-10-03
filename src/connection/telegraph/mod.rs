//! 数据报端点：[`Telegraph`]。
//!
//! 数据报（telegraph）是类似 UDP 的短消息通道：**不需要建流握手**，但按
//! `abs_smux` 的约定，`channel` 与 `telegraph` **不得共用同一个 local_dock**，
//! 因此它在 `local_dock` 上与 channel / listener 互斥（登记在统一身份表的
//! `(local, wildcard)` / `(local, 具体值)` 之外，占 `(local, unspecified)`，
//! 见 `mux_connection::registry_`）。
//!
//! # 语义
//!
//! - **发送**（[`TrTelegraph::send_async`]）：把 `packet`（一个
//!   `TrBuffRead`）里的字节作为一条 `DATAGRAM` 帧的载荷写出；返回实际写出的
//!   字节数。一次调用恰好对应一帧，接收端也恰好收到一条。
//! - **接收**（[`TrTelegraph::recv_async`]）：等待下一条发往 `remote_dock` 的
//!   `DATAGRAM` 帧，把载荷写进 `buffer`（一个 `TrBuffWrite`），返回写入字节数。
//! - 数据报**不参与流控窗口**（没有信用回补），因此它只受连接级
//!   `max_packet_size` 约束；应用需自行避免用它替代流控子流。
//! - 数据报帧可以乱序（由底层字节流保证线序，但没有应用层序号）；v1 不保证
//!   「先发先到」以外的语义，也不做重传。

mod endpoint_;

pub use endpoint_::{
    Telegraph, TelegraphError,
};
