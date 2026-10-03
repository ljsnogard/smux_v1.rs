// 本模块目前是**骨架**：类型、签名与文档已定稿，收发方法体统一为 `todo!()`。
// 身份登记 / 释放路径已经落地（端点存活期间独占 local_dock）。实现落地后必须移除
// `unused_variables`（`dead_code` 仍由未实现的支线需要，见 dev-notes 的待办）。
#![allow(dead_code, unused_variables)]

use abs_buff::{
    TrBuffRead, TrBuffWrite,
    gen_may_cancel_future,
    x_deps::abs_cancel,
};
use abs_cancel::TrCancellationToken;
use abs_smux::conn::TrTelegraph;
use anylr::SomeOf;
use buffex::x_deps::{abs_buff, anylr};

use crate::connection::{Dock, MuxConnection, MuxError, TrConnCfg, error_::face_error_impls};

/// [`TrTelegraph`](abs_smux::conn::TrTelegraph) 的错误类型（`send_async` / `recv_async`）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TelegraphError {
    /// 报文超过协商的 `max_packet_size`。
    ///
    /// 注意 remote dock 取 `wildcard` / `unspecified` **不是**错误：那是合法目的地址。
    FrameTooLarge,

    /// 连接级失败。
    Mux(MuxError),
}

face_error_impls!(
    TelegraphError,
    TelegraphError::FrameTooLarge => "报文超过协商的最大报文长度",
);

/// 数据报端点。
///
/// **不借用连接**：自己持有一份 [`MuxConnection`] 克隆，因此生命周期参数从公开
/// 类型上消失，可以存进结构体、可以从函数返回。
pub struct Telegraph<C, S>
where
    C: TrConnCfg,
{
    /// 连接智能指针：端点被 drop 时用它解除身份登记。
    conn_: MuxConnection<C, S>,

    /// 本端 dock。
    local_dock_: Dock,
}

impl<C, S> Telegraph<C, S>
where
    C: TrConnCfg,
{
    /// 由连接与 `local_dock` 构造（只允许 `open_telegraph_async` 调用）。
    pub(crate) fn new_(conn: MuxConnection<C, S>, local_dock: Dock) -> Self {
        Telegraph {
            conn_: conn,
            local_dock_: local_dock,
        }
    }
}

/// 丢弃端点即**解除 telegraph 身份**，使同一个 `local_dock` 之后可以再作 channel
/// 或 listener 使用（见 `ChannelRegistry_::release_telegraph_`）。
impl<C, S> Drop for Telegraph<C, S>
where
    C: TrConnCfg,
{
    fn drop(&mut self) {
        self.conn_
            .core_()
            .reg_()
            .release_telegraph_(self.local_dock_);
    }
}

impl<C, S> TrTelegraph<C> for Telegraph<C, S>
where
    C: TrConnCfg,
{
    type Err = TelegraphError;

    type SendAsync<'f, M> = MuxSendAsync<'f, 'f, C, S, M>
    where
        Self: 'f,
        M: 'f + TrBuffRead<C::Data>;

    type RecvAsync<'f, M> = MuxRecvAsync<'f, 'f, C, S, M>
    where
        Self: 'f,
        M: 'f + TrBuffWrite<C::Data>;

    fn local_dock(&self) -> C::Dock {
        self.local_dock_
    }

    fn send_async<'f, M>(
        &'f mut self,
        remote_dock: C::Dock,
        packet: &'f mut M,
    ) -> Self::SendAsync<'f, M>
    where
        M: TrBuffRead<C::Data>,
    {
        MuxSendAsync::new(self, remote_dock, packet)
    }

    fn recv_async<'f, M>(
        &'f mut self,
        remote_dock: C::Dock,
        buffer: &'f mut M,
    ) -> Self::RecvAsync<'f, M>
    where
        M: TrBuffWrite<C::Data>,
    {
        MuxRecvAsync::new(self, remote_dock, buffer)
    }
}

/// [`TrTelegraph::send_async`] 的 step 函数。
#[gen_may_cancel_future(MuxSend, pub, new(pub(crate)))]
async fn mux_send_async_<'f, C, S, M, K>(
    telegraph: &'f mut Telegraph<C, S>,
    remote_dock: Dock,
    packet: &'f mut M,
    _cancel: K,
) -> SomeOf<usize, TelegraphError>
where
    C: TrConnCfg + 'f,
    S: 'f,
    M: TrBuffRead<u8> + 'f,
    K: TrCancellationToken,
{
    todo!("读 packet 并写出一条 DATAGRAM 帧")
}

/// [`TrTelegraph::recv_async`] 的 step 函数。
#[gen_may_cancel_future(MuxRecv, pub, new(pub(crate)))]
async fn mux_recv_async_<'f, C, S, M, K>(
    telegraph: &'f mut Telegraph<C, S>,
    remote_dock: Dock,
    buffer: &'f mut M,
    _cancel: K,
) -> SomeOf<usize, TelegraphError>
where
    C: TrConnCfg + 'f,
    S: 'f,
    M: TrBuffWrite<u8> + 'f,
    K: TrCancellationToken,
{
    todo!("等待一条 DATAGRAM 并把载荷写进 buffer")
}
