use async_net::{
    TcpListener as SmolTcpListener, TcpStream as SmolTcpStream,
};
use smux_v1::connection::TrConnCfg;

use crate::config::{AcceptOutput, InboundError, TrInbound};

use super::shared_::{K_DEFAULT_RING_CAPACITY, TcpConnCfg, make_halves_};

/// **TCP** 的入向适配：**持有**一个 `smol::net::TcpListener`。
///
/// 适配器持有监听套接字（而不是让 listener 单独拿着它）是因为接受与建环都属于本 trait
/// 的职责；`SmuxListener` 用 `Arc<Self>` 共享同一个实例给所有接受槽位。
pub struct TcpTransport {
    /// 底层的 `async-net` TCP 监听套接字。
    listener_: SmolTcpListener,
}

impl TcpTransport {
    /// 由已绑定的监听套接字构造。
    pub const fn new(listener: SmolTcpListener) -> Self {
        TcpTransport { listener_: listener }
    }

    /// 取底层监听套接字。
    pub const fn listener(&self) -> &SmolTcpListener {
        &self.listener_
    }
}

impl TrInbound for TcpTransport {
    type ConnCfg = TcpConnCfg;
    type Device = SmolTcpStream;
    type Error = InboundError;

    const RING_CAPACITY: (usize, usize) =
        (K_DEFAULT_RING_CAPACITY, K_DEFAULT_RING_CAPACITY);

    async fn accept_async(&self) -> AcceptOutput<Self> {
        let (stream, _peer) = self.listener_.accept().await?;
        // 与 tokio 侧同理：Nagle 对 smux 的小帧不利，但设置失败不值得放弃连接。
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
        let (rd, wr) = smol::io::split(device);
        make_halves_(rd, wr)
    }
}
