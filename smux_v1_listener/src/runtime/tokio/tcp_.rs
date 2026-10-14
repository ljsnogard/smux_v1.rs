use smux_v1::connection::TrConnCfg;
use tokio::net::{
    TcpListener as TokioTcpListener, TcpStream as TokioTcpStream,
};

use crate::config::{AcceptOutput, InboundError, TrInbound};

use super::shared_::{K_DEFAULT_RING_CAPACITY, TcpConnCfg, make_halves_};

/// **TCP** 的入向适配：**持有**一个 `tokio::net::TcpListener`。
///
/// 适配器持有监听套接字（而不是让 listener 单独拿着它）是因为接受与建环都属于本 trait
/// 的职责；`SmuxListener` 用 `Arc<Self>` 共享同一个实例给所有接受槽位。
pub struct TcpTransport {
    /// 底层的 tokio TCP 监听套接字。
    listener_: TokioTcpListener,
}

impl TcpTransport {
    /// 由已绑定的监听套接字构造。
    pub const fn new(listener: TokioTcpListener) -> Self {
        TcpTransport { listener_: listener }
    }

    /// 取底层监听套接字。
    pub const fn listener(&self) -> &TokioTcpListener {
        &self.listener_
    }
}

impl TrInbound for TcpTransport {
    type ConnCfg = TcpConnCfg;
    type Device = TokioTcpStream;
    type Error = InboundError;

    const RING_CAPACITY: (usize, usize) =
        (K_DEFAULT_RING_CAPACITY, K_DEFAULT_RING_CAPACITY);

    async fn accept_async(&self) -> AcceptOutput<Self> {
        let (stream, _peer) = self.listener_.accept().await?;
        // Nagle 会把「小回执跟在数据末尾」多拖一轮，而 smux 的帧普遍很小。这里失败
        // **不**拒绝连接：TCP_NODELAY 只影响时延，不值得为它放弃一条已建好的连接。
        let _ = stream.set_nodelay(true);
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
