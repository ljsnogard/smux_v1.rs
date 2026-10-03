use core::future::poll_fn;

use abs_buff::{gen_may_cancel_future, x_deps::abs_cancel};
use abs_cancel::TrCancellationToken;
use abs_smux::conn::TrChannelListener;
use buffex::x_deps::abs_buff;

use crate::connection::{
    Dock, MuxConnection, MuxError, TrConnCfg,
    channel_handle::ChannelHandle,
    error_::face_error_impls,
};

/// [`TrChannelListener`](abs_smux::conn::TrChannelListener) 的错误类型。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ListenerError {
    /// 本次等待被取消。
    Cancelled,

    /// 连接级失败。
    Mux(MuxError),
}

face_error_impls!(
    ListenerError,
    ListenerError::Cancelled => "本次等待被取消",
);

/// 某 `local_dock` 上的入向子流监听器（类 `TcpListener`）。
///
/// **不借用 [`DockBinding`](crate::connection::DockBinding)**：`listen_async(&mut self)`
/// 的 `&mut` 只覆盖**调用期间**，返回值是独立对象（自己持有一份 [`MuxConnection`]
/// 克隆）。因此「建 listener」与「accept 循环」可以拆成两个函数——旧模型下
/// `ChannelListener<'s,'f,…>` 借用 binding，这一步写不出来。
///
/// [`TrChannelListener::income_async`] 每次返回一个**待决句柄**
/// [`ChannelHandle`]；调用方决定 accept 还是 reject，之后该 dock 才能继续接受
/// 下一个请求（同一 dock 的请求串行化，便于用户侧实现「排队 / 限流」）。
pub struct ChannelListener<C, S>
where
    C: TrConnCfg,
{
    /// 连接智能指针：accept 时要用它的配置（`policy` 等）建子流环。
    conn_: MuxConnection<C, S>,

    /// 监听的 local_dock。
    local_dock_: Dock,
}

impl<C, S> ChannelListener<C, S>
where
    C: TrConnCfg,
{
    /// 由连接与监听 `local_dock` 构造（只允许 `listen_async` 调用）。
    pub(crate) fn new_(conn: MuxConnection<C, S>, local_dock: Dock) -> Self {
        ChannelListener {
            conn_: conn,
            local_dock_: local_dock,
        }
    }
}

/// 丢弃监听器即**解除 listener 身份**，使同一个 `local_dock` 之后可以再作
/// telegraph 使用（见 `ChannelRegistry_::release_listener_`）。
///
/// 注意这不影响该 dock 上已经建立、且在应用手里的子流半部。
impl<C, S> Drop for ChannelListener<C, S>
where
    C: TrConnCfg,
{
    fn drop(&mut self) {
        self.conn_
            .core_()
            .reg_()
            .release_listener_(self.local_dock_);
    }
}

impl<C, S> TrChannelListener<C> for ChannelListener<C, S>
where
    C: TrConnCfg,
{
    type Err = ListenerError;

    type ChannelHandle = ChannelHandle<C, S>;

    type IncomeAsync<'f> = MuxIncomeAsync<'f, 'f, C, S>
    where
        Self: 'f;

    fn local_dock(&self) -> &C::Dock {
        &self.local_dock_
    }

    fn income_async(&mut self) -> Self::IncomeAsync<'_> {
        MuxIncomeAsync::new(self)
    }
}

/// [`TrChannelListener::income_async`] 的 step 函数。
#[gen_may_cancel_future(MuxIncome, pub, new(pub(crate)))]
async fn mux_income_async_<'f, C, S, K>(
    listener: &'f mut ChannelListener<C, S>,
    cancel: K,
) -> Result<ChannelHandle<C, S>, ListenerError>
where
    C: TrConnCfg + 'f,
    S: 'f,
    K: TrCancellationToken,
{
    let conn = listener.conn_.clone();
    let local = listener.local_dock_;
    loop {
        if cancel.is_cancelled() {
            return Result::Err(ListenerError::Cancelled);
        }
        if let Option::Some(err) = conn.core_().reg_().failure_() {
            return Result::Err(ListenerError::Mux(err));
        }
        if let Option::Some(remote) = conn.core_().reg_().take_pending_inbound_(local) {
            return Result::Ok(ChannelHandle::new_(conn, local, remote));
        }
        // 先登记 waker，再复检一次：避免「检查完、还没登记」之间丢唤醒。
        poll_fn(|cx| {
            if conn.core_().reg_().has_pending_inbound_(local)
                || conn.core_().reg_().is_failed_()
            {
                return core::task::Poll::Ready(());
            }
            conn.core_()
                .reg_()
                .register_inbound_waker_(local, cx.waker());
            if conn.core_().reg_().has_pending_inbound_(local)
                || conn.core_().reg_().is_failed_()
            {
                core::task::Poll::Ready(())
            } else {
                core::task::Poll::Pending
            }
        })
        .await;
    }
}
