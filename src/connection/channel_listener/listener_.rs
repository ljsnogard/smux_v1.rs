use core::future::poll_fn;

use abs_buff::{gen_may_cancel_future, x_deps::abs_cancel};
use abs_cancel::TrCancellationToken;
use abs_smux::conn::TrChannelListener;
use flume::Receiver;
use buffex::x_deps::abs_buff;

use crate::connection::{
    Dock, MuxConnection, MuxError, TrConnCfg,
    channel_handle::ChannelHandle,
    signal_::SessionEvent_,
};

/// [`TrChannelListener`](abs_smux::conn::TrChannelListener) 的错误类型。
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum ListenerError {
    /// 本次等待被取消。
    #[error("本次等待被取消")]
    Cancelled,

    /// 连接级失败：`Display` 用内层文案，`source` 指回内层（`?` 亦直通）。
    #[error("{0}")]
    Mux(#[from] MuxError),
}

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

    /// 「本 dock 上有入向事件（新请求 / 连接失败）」的通知消费端。
    ///
    /// 由 `listen_async` 与注册表同时建立：注册表持生产端，本对象持消费端。
    notify_rx_: Receiver<()>,
}

impl<C, S> ChannelListener<C, S>
where
    C: TrConnCfg,
{
    /// 由连接、监听 `local_dock` 与通知消费端构造（只允许 `listen_async` 调用）。
    pub(crate) fn new_(
        conn: MuxConnection<C, S>,
        local_dock: Dock,
        notify_rx_: Receiver<()>,
    ) -> Self {
        ChannelListener {
            conn_: conn,
            local_dock_: local_dock,
            notify_rx_,
        }
    }
}

/// 丢弃监听器即**解除 listener 身份**（投递 [`SessionEvent_::ReleaseListener`]，
/// 由核心落实），使同一个 `local_dock` 之后可以再作 telegraph 使用
/// （见 `ChannelRegistry_::release_listener_`）。
///
/// **本 `Drop` 不取锁、不阻塞**；`listen_async` / `open_telegraph_async` 在认领身份
/// 之前会先清空释放邮箱，因此「丢弃后立刻复用同一 dock」仍然是确定的。
///
/// 注意这不影响该 dock 上已经建立、且在应用手里的子流半部。
impl<C, S> Drop for ChannelListener<C, S>
where
    C: TrConnCfg,
{
    fn drop(&mut self) {
        let _ = self
            .conn_
            .core_()
            .reg_()
            .post_session_event_(SessionEvent_::ReleaseListener {
                local_dock: self.local_dock_,
            });
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
        // 1. 连接失败优先；取注册表锁是**异步且可取消**的。
        match conn.core_().failure_(cancel.child_token()).await {
            Result::Ok(Option::Some(err)) => return Result::Err(ListenerError::Mux(err)),
            Result::Err(_) => return Result::Err(ListenerError::Cancelled),
            Result::Ok(Option::None) => {}
        }
        // 2. 有已到达的入向请求就当场取走。
        match conn
            .core_()
            .take_pending_inbound_(local, cancel.child_token())
            .await
        {
            Result::Ok(Option::Some((remote, owner))) => {
                return Result::Ok(ChannelHandle::new_(conn, local, remote, owner));
            }
            Result::Err(_) => return Result::Err(ListenerError::Cancelled),
            Result::Ok(Option::None) => {}
        }
        // 3. 等一条通知（新请求或连接失败），与取消令牌竞争。
        //    通道是持久的：第 1、2 步与这里之间的通知已经排在队列里，不会丢。
        let mut notified = core::pin::pin!(listener.notify_rx_.recv_async());
        let mut cancelled = core::pin::pin!(cancel.child_token().cancellation());
        let got = poll_fn(|cx| {
            if core::future::Future::poll(cancelled.as_mut(), cx).is_ready() {
                return core::task::Poll::Ready(false);
            }
            core::future::Future::poll(notified.as_mut(), cx).map(|_| true)
        })
        .await;
        if !got {
            return Result::Err(ListenerError::Cancelled);
        }
    }
}
