use smux_v1::connection::TrConnCfg;
use tokio::net::{
    UnixListener as TokioUnixListener, UnixStream as TokioUnixStream,
};

use crate::config::{AcceptOutput, InboundError, TrInbound};

use super::shared_::{K_DEFAULT_RING_CAPACITY, UdsConnCfg, make_halves_};

/// **UNIX 域**的入向适配：**持有**一个 `tokio::net::UnixListener`。
///
/// 形态与 [`TcpTransport`](super::TcpTransport) 逐字一致，理由见它的文档。
pub struct UdsTransport {
    /// 底层的 tokio UNIX 域监听套接字。
    listener_: TokioUnixListener,
}

impl UdsTransport {
    /// 由已绑定的监听套接字构造。
    pub const fn new(listener: TokioUnixListener) -> Self {
        UdsTransport { listener_: listener }
    }

    /// 取底层监听套接字。
    pub const fn listener(&self) -> &TokioUnixListener {
        &self.listener_
    }
}

impl TrInbound for UdsTransport {
    type ConnCfg = UdsConnCfg;
    type Device = TokioUnixStream;
    type Error = InboundError;

    const RING_CAPACITY: (usize, usize) =
        (K_DEFAULT_RING_CAPACITY, K_DEFAULT_RING_CAPACITY);

    async fn accept_async(&self) -> AcceptOutput<Self> {
        let (stream, _peer) = self.listener_.accept().await?;
        Result::Ok(Option::Some(stream))
    }

    fn into_halves(
        &self,
        device: Self::Device,
    ) -> (
        <Self::ConnCfg as TrConnCfg>::ConnTx,
        <Self::ConnCfg as TrConnCfg>::ConnRx,
    ) {
        let (rd, wr) = tokio::io::split(device);
        make_halves_(rd, wr)
    }
}
