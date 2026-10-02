//! 数据报端点：[`Telegraph`]。
//!
//! 数据报（telegraph）是类似 UDP 的短消息通道：**不需要建流握手**，但按
//! `abs_smux` 的约定，`channel` 与 `telegraph` **不得共用同一个 local_dock**，
//! 因此它在绑定期与 channel 互斥。
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

// 本模块目前是**骨架**：类型、签名与文档已定稿，方法体统一为 `todo!()`。
// 实现落地后必须移除本行的 `allow`（见 `dev-notes/` 的待办）。
#![allow(dead_code, unused_variables)]


use abs_buff::{TrBuffRead, TrBuffWrite, gen_may_cancel_future};
use abs_cancel::TrCancellationToken;
use abs_smux::conn::TrTelegraph;
use anylr::SomeOf;
use buffex::x_deps::{abs_buff, abs_cancel, anylr};

use crate::connection::{Dock, MuxError, TrMuxConfig};
use super::channel_::SessionMark_;

/// 数据报端点。
///
/// 由 [`TrDockBinding::open_telegraph_async`](abs_smux::conn::TrDockBinding::open_telegraph_async)
/// 在某个 binding 上建立；端点存活期间该 local_dock 被独占，不能再被 channel
/// 使用（反之亦然）。
pub struct Telegraph<'s, 'f, R, W, C, Rt> {
    /// 本端 dock。
    local_dock_: Dock,

    /// 借用关系与连接泛型的占位；语义同
    /// [`ChannelListener`](super::ChannelListener)。
    _mark_: SessionMark_<'s, 'f, R, W, C, Rt>,
}

impl<'s, 'f, R, W, C, Rt> TrTelegraph for Telegraph<'s, 'f, R, W, C, Rt>
where
    R: TrBuffRead<u8> + 'f,
    W: TrBuffWrite<u8> + 'f,
    C: TrMuxConfig + 'f,
{
    type Data = u8;
    type Dock = Dock;
    type Err = MuxError<R::Err, W::Err>;

    type SendAsync<'a, M>
        = MuxSendAsync<'a, 's, 'f, 'a, R, W, C, Rt, M>
    where
        Self: 'a,
        M: 'a + TrBuffRead<Self::Data>;

    type RecvAsync<'a, M>
        = MuxRecvAsync<'a, 's, 'f, 'a, R, W, C, Rt, M>
    where
        Self: 'a,
        M: 'a + TrBuffWrite<Self::Data>;

    fn local_dock(&self) -> Self::Dock {
        self.local_dock_
    }

    fn send_async<'a, M>(
        &'a mut self,
        remote_dock: Self::Dock,
        packet: &'a mut M,
    ) -> Self::SendAsync<'a, M>
    where
        M: TrBuffRead<Self::Data>,
    {
        MuxSendAsync::new(self, remote_dock, packet)
    }

    fn recv_async<'a, M>(
        &'a mut self,
        remote_dock: Self::Dock,
        buffer: &'a mut M,
    ) -> Self::RecvAsync<'a, M>
    where
        M: TrBuffWrite<Self::Data>,
    {
        MuxRecvAsync::new(self, remote_dock, buffer)
    }
}

/// [`TrTelegraph::send_async`] 的 step 函数。
///
/// 返回值为**实际写出**的字节数；若 `packet` 短于 `max_packet_size`，就是它的
/// 全部长度。
#[gen_may_cancel_future(MuxSend, pub)]
async fn mux_send_async_<'a, 's, 'f, R, W, C, Rt, M, K>(
    telegraph: &'a mut Telegraph<'s, 'f, R, W, C, Rt>,
    remote_dock: Dock,
    packet: &'a mut M,
    _cancel: K,
) -> SomeOf<usize, MuxError<R::Err, W::Err>>
where
    'f: 's,
    R: TrBuffRead<u8> + 'f,
    W: TrBuffWrite<u8> + 'f,
    C: TrMuxConfig + 'f,
    M: TrBuffRead<u8> + 'a,
    K: TrCancellationToken,
{
    todo!("读 packet 并写出一条 DATAGRAM 帧")
}

/// [`TrTelegraph::recv_async`] 的 step 函数。
///
/// 返回值为写入 `buffer` 的字节数。
#[gen_may_cancel_future(MuxRecv, pub)]
async fn mux_recv_async_<'a, 's, 'f, R, W, C, Rt, M, K>(
    telegraph: &'a mut Telegraph<'s, 'f, R, W, C, Rt>,
    remote_dock: Dock,
    buffer: &'a mut M,
    _cancel: K,
) -> SomeOf<usize, MuxError<R::Err, W::Err>>
where
    'f: 's,
    R: TrBuffRead<u8> + 'f,
    W: TrBuffWrite<u8> + 'f,
    C: TrMuxConfig + 'f,
    M: TrBuffWrite<u8> + 'a,
    K: TrCancellationToken,
{
    todo!("等待一条 DATAGRAM 并把载荷写进 buffer")
}
