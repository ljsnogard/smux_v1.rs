//! 数据报端点：[`Telegraph`] 与它的两个半边 [`Sender`] / [`Receiver`]。
//!
//! 数据报（telegraph）是类似 UDP 的短消息通道：**不需要建流握手**，但按
//! `abs_smux` 的约定，`channel` 与 `telegraph` **不得共用同一个 local_dock**，
//! 因此它在 `local_dock` 上与 channel / listener 互斥（登记在统一身份表的
//! `(local, wildcard)` / `(local, 具体值)` 之外，占 `(local, unspecified)`，
//! 见 `mux_connection::registry_`）。
//!
//! # 语义
//!
//! - **发送**（`TrDatagramSender::send_async`）：应用先把载荷写进本地发送环
//!   （[`Sender`] 实现 `TrBuffWrite`），再提交。一次提交恰好对应一条 `DATAGRAM`
//!   帧，返回该报文的载荷长度；长度在**提交那一刻**就已确定，帧头的 `PayloadLen`
//!   直接照搬。目的地址是**逐次**发送的实参（同 UDP 的 `sendto`）。
//! - **接收**（`TrDatagramRecver::recv_async`）：等一条完整报文，返回一个
//!   **自描述对象** [`RecvDatagram`]——它同时给出**远端地址**与**载荷段**，应用直接
//!   从段里读内容。
//! - 数据报**不参与流控窗口**（没有信用回补、没有窗口通告），因此它只受连接级
//!   `max_packet_size` 约束；应用需自行避免用它替代流控子流。
//! - **尽力交付**：发送方向「环里有多少就送多少」，接收方向「缓存装不下就整帧丢弃」。
//!   数据报帧由底层可靠有序的字节流承载，因此线序有保证，但应用层序号 / 重传都没有。
//!
//! # 接收路径的形状（本轮定稿）
//!
//! 解复用循环**不认识 `DATAGRAM` 的字段语义**：它把「帧头 + 载荷」的整帧交给
//! [`recv_ctx_`] 的接收上下文（[`TgRecvCtx_`]），由后者写进接收环、记下帧总长；
//! 到达侧（[`recver_`]）再从环里借出整帧、**自拆**出远端地址与载荷段。没有任何接收方
//! （本地表里没有该 `local_dock` 的条目）时，整个帧直接丢弃。
//!
//! 详细语义（含两侧「报文边界」如何记录、身份由两个半边共同持有）见
//! [`endpoint_`] 的模块文档。

mod ctx_;
mod datagram_;
mod endpoint_;
mod recv_ctx_;
mod recver_;
mod sender_;

pub use datagram_::RecvDatagram;
pub use endpoint_::{DemandErr, Telegraph, TelegraphError};
pub use recver_::Receiver;
pub use sender_::Sender;

/// 由调用方交出的两块环内存建出 telegraph 的两条环（供 `open_telegraph_async` 使用）。
///
/// 它是纯本地操作（建环），不下沉到 `binding_` 里是为了让「环怎么建、失败怎么映射」
/// 与容量的口径只有一处实现。
pub(crate) use endpoint_::{TelegraphRings_, build_telegraph_rings_};

/// 解复用循环持有的接收上下文（帧头 + 载荷整帧投递落点）。
pub(crate) use recv_ctx_::TgRecvCtx_;
