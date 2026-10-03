// 本模块目前是**骨架**：类型、签名与文档已定稿，收发方法体统一为 `todo!()`。
// 身份登记 / 释放路径已经落地（端点存活期间独占 local_dock）。实现落地后必须移除
// `unused_variables`（`dead_code` 仍由未实现的支线需要，见 dev-notes 的待办）。
#![allow(dead_code, unused_variables)]

use abs_buff::{
    TrBuffRead, TrBuffTryRead, TrBuffTryWrite, TrBuffWrite, gen_may_cancel_future,
};
use abs_cancel::TrCancellationToken;
use abs_smux::conn::TrTelegraph;
use anylr::SomeOf;
use buffex::x_deps::{abs_buff, abs_cancel, anylr};

use crate::connection::{Dock, MuxConnection, MuxError, TrMuxConfig};

/// 数据报端点。
///
/// **不借用连接**：自己持有一份 [`MuxConnection`] 克隆，因此生命周期参数从公开
/// 类型上消失，可以存进结构体、可以从函数返回。
///
/// 由 [`TrDockBinding::open_telegraph_async`](abs_smux::conn::TrDockBinding::open_telegraph_async)
/// 在某个 binding 上建立；端点存活期间该 local_dock 被独占，不能再被 channel
/// 或 listener 使用（反之亦然，见 `ChannelRegistry_::reserve_telegraph_`）。
pub struct Telegraph<C, S, R, W>
where
    C: TrMuxConfig,
{
    /// 连接智能指针：端点被 drop 时用它解除身份登记。
    conn_: MuxConnection<C, S, R, W>,

    /// 本端 dock。
    local_dock_: Dock,
}

impl<C, S, R, W> Telegraph<C, S, R, W>
where
    C: TrMuxConfig,
{
    /// 由连接与 `local_dock` 构造（只允许 `open_telegraph_async` 调用）。
    pub(crate) fn new_(conn: MuxConnection<C, S, R, W>, local_dock: Dock) -> Self {
        Telegraph {
            conn_: conn,
            local_dock_: local_dock,
        }
    }
}

/// 丢弃端点即**解除 telegraph 身份**，使同一个 `local_dock` 之后可以再作 channel
/// 或 listener 使用（见 `ChannelRegistry_::release_telegraph_`）。
impl<C, S, R, W> Drop for Telegraph<C, S, R, W>
where
    C: TrMuxConfig,
{
    fn drop(&mut self) {
        self.conn_
            .core_()
            .reg_()
            .release_telegraph_(self.local_dock_);
    }
}

impl<C, S, R, W> TrTelegraph for Telegraph<C, S, R, W>
where
    C: TrMuxConfig,
    R: TrBuffTryRead<u8>,
    W: TrBuffTryWrite<u8>,
{
    type Data = u8;
    type Dock = Dock;
    type Err = MuxError<R, W>;

    type SendAsync<'f, M>
        = MuxSendAsync<'f, 'f, C, S, R, W, M>
    where
        Self: 'f,
        M: 'f + TrBuffRead<Self::Data>;

    type RecvAsync<'f, M>
        = MuxRecvAsync<'f, 'f, C, S, R, W, M>
    where
        Self: 'f,
        M: 'f + TrBuffWrite<Self::Data>;

    fn local_dock(&self) -> Self::Dock {
        self.local_dock_
    }

    fn send_async<'f, M>(
        &'f mut self,
        remote_dock: Self::Dock,
        packet: &'f mut M,
    ) -> Self::SendAsync<'f, M>
    where
        M: TrBuffRead<Self::Data>,
    {
        MuxSendAsync::new(self, remote_dock, packet)
    }

    fn recv_async<'f, M>(
        &'f mut self,
        remote_dock: Self::Dock,
        buffer: &'f mut M,
    ) -> Self::RecvAsync<'f, M>
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
#[gen_may_cancel_future(MuxSend, pub, new(pub(crate)))]
async fn mux_send_async_<'f, C, S, R, W, M, K>(
    telegraph: &'f mut Telegraph<C, S, R, W>,
    remote_dock: Dock,
    packet: &'f mut M,
    _cancel: K,
) -> SomeOf<usize, MuxError<R, W>>
where
    C: TrMuxConfig + 'f,
    S: 'f,
    R: TrBuffTryRead<u8> + 'f,
    W: TrBuffTryWrite<u8> + 'f,
    M: TrBuffRead<u8> + 'f,
    K: TrCancellationToken,
{
    todo!("读 packet 并写出一条 DATAGRAM 帧")
}

/// [`TrTelegraph::recv_async`] 的 step 函数。
///
/// 返回值为写入 `buffer` 的字节数。
#[gen_may_cancel_future(MuxRecv, pub, new(pub(crate)))]
async fn mux_recv_async_<'f, C, S, R, W, M, K>(
    telegraph: &'f mut Telegraph<C, S, R, W>,
    remote_dock: Dock,
    buffer: &'f mut M,
    _cancel: K,
) -> SomeOf<usize, MuxError<R, W>>
where
    C: TrMuxConfig + 'f,
    S: 'f,
    R: TrBuffTryRead<u8> + 'f,
    W: TrBuffTryWrite<u8> + 'f,
    M: TrBuffWrite<u8> + 'f,
    K: TrCancellationToken,
{
    todo!("等待一条 DATAGRAM 并把载荷写进 buffer")
}
