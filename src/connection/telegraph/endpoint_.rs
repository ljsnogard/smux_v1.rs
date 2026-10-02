// 本模块目前是**骨架**：类型、签名与文档已定稿，收发方法体统一为 `todo!()`。
// 身份登记 / 释放路径已经落地（端点存活期间独占 local_dock）。实现落地后必须移除
// `unused_variables`（`dead_code` 仍由未实现的支线需要，见 dev-notes 的待办）。
#![allow(dead_code, unused_variables)]


use core::marker::PhantomData;

use abs_buff::{TrBuffRead, TrBuffWrite, gen_may_cancel_future};
use abs_cancel::TrCancellationToken;
use abs_smux::conn::TrTelegraph;
use anylr::SomeOf;
use buffex::x_deps::{abs_buff, abs_cancel, anylr};

use crate::connection::{Dock, MuxConnection, MuxError, TrMuxConfig};
use crate::connection::types_::SessionMark_;

/// 数据报端点。///
/// > **目标形状（本轮确定，迁移中）**：本类型改为**持有一份 [`MuxConnection`] 的
/// > 克隆**（= 指向 `MuxCore` 的智能指针），不再借用连接，因此生命周期参数从公开
/// > 类型上消失，可以存进结构体、可以从函数返回。设计见
/// > [`crate::connection`] 模块文档 §2 与 `dev-notes/connection-20261002-0548.md` §17。
///
/// 由 [`TrDockBinding::open_telegraph_async`](abs_smux::conn::TrDockBinding::open_telegraph_async)
/// 在某个 binding 上建立；端点存活期间该 local_dock 被独占，不能再被 channel
/// 或 listener 使用（反之亦然，见 `ChannelRegistry_::reserve_telegraph_`）。
pub struct Telegraph<'s, 'f, R, W, C, Rt>
where
    C: TrMuxConfig,
{
    /// 连接对象：端点被 drop 时用它解除身份登记。
    conn_: &'f MuxConnection<R, W, C, Rt>,

    /// 本端 dock。
    local_dock_: Dock,

    /// 借用关系与连接泛型的占位；语义同
    /// [`ChannelListener`](super::ChannelListener)。
    _mark_: SessionMark_<'s, 'f, R, W, C, Rt>,
}

impl<'s, 'f, R, W, C, Rt> Telegraph<'s, 'f, R, W, C, Rt>
where
    C: TrMuxConfig,
{
    /// 由连接与 `local_dock` 构造（只允许 `open_telegraph_async` 调用）。
    pub(crate) fn new_(conn: &'f MuxConnection<R, W, C, Rt>, local_dock: Dock) -> Self {
        Telegraph {
            conn_: conn,
            local_dock_: local_dock,
            _mark_: PhantomData,
        }
    }
}

/// 丢弃端点即**解除 telegraph 身份**，使同一个 `local_dock` 之后可以再作 channel
/// 或 listener 使用（见 `ChannelRegistry_::release_telegraph_`）。
impl<R, W, C, Rt> Drop for Telegraph<'_, '_, R, W, C, Rt>
where
    C: TrMuxConfig,
{
    fn drop(&mut self) {
        self.conn_.reg_().release_telegraph_(self.local_dock_);
    }
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
