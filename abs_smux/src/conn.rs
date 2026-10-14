use abs_buff::TrBuffRead;
use abs_cancel::TrMayCancel;

use crate::{
    chan::{TrChannelHandle, TrChannelRx, TrChannelTx},
    conf::TrMuxConfig,
    x_deps::abs_cancel,
};

pub trait TrChannelListener<C>
where
    C: TrMuxConfig,
{
    type Err: core::error::Error;

    fn local_dock(&self) -> &C::Dock;

    // -- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
    // Listener income
    // -- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

    type ChannelHandle: TrChannelHandle<C>;

    type IncomeAsync<'f>: TrMayCancel<'f, MayCancelOutput =
        Result<Self::ChannelHandle, Self::Err>>
    where
        Self: 'f;

    fn income_async(&mut self) -> Self::IncomeAsync<'_>;
}

/// An smux connection.
pub trait TrConnection {
    type Config: TrMuxConfig;
    type DockBinding: TrDockBinding<Self::Config>;
    type Err: core::error::Error;

    type BindAsync<'f>: TrMayCancel<'f, MayCancelOutput =
        Result<Self::DockBinding, Self::Err>>
    where
        Self: 'f;

    /// 在某个 `local_dock` 上派生一个会话（binding）。
    ///
    /// # `unspecified`：由实现自行安排一个可用垛口
    ///
    /// # Errors
    ///
    /// `local_dock` 是 `wildcard`、或该 dock 已被占用（含自动分配时候选耗尽）时
    /// 返回错误。
    fn bind_async<'f>(
        &'f self,
        local_dock: <Self::Config as TrMuxConfig>::Dock,
    ) -> Self::BindAsync<'f>;

    type CloseAsync<'f>: TrMayCancel<'f, MayCancelOutput = bool> where Self: 'f;

    /// Gracefully disconnect.
    fn close_async(&mut self) -> Self::CloseAsync<'_>;
}

pub trait TrDockBinding<C>
where
    C: TrMuxConfig,
{
    type Err: core::error::Error;

    fn local_dock(&self) -> &C::Dock;

    // -- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
    // Listener section
    // -- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

    type ChannelHandle: TrChannelHandle<C>;

    type Listener: TrChannelListener<C>;

    type ListenAsync<'f>: TrMayCancel<'f, MayCancelOutput =
        Result<Self::Listener, Self::Err>>
    where
        Self: 'f;

    /// Listen at the dock owned by this operator, specifying the max reserve
    /// count of the incoming invitations.
    fn listen_async(
        &mut self,
        reserve: usize,
    ) -> Self::ListenAsync<'_>;

    // -- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
    // Channel definition and open_channel_async
    // -- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

    type Tx: TrChannelTx<C>;
    type Rx: TrChannelRx<C>;

    type OpenChannelAsync<'f, R>: TrMayCancel<'f, MayCancelOutput =
        Result<Self::ChannelHandle, Self::Err>>
    where
        Self: 'f,
        R: 'f + TrBuffRead<C::Data>;

    /// Initiate a channel to the remote dock
    fn open_channel_async<'f, R>(
        &'f mut self,
        remote_dock: C::Dock,
        message: &'f mut R,
    ) -> Self::OpenChannelAsync<'f, R>
    where
        R: TrBuffRead<C::Data>;
}
