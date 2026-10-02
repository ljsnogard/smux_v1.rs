use core::{
    future::poll_fn,
    marker::PhantomData,
    task::Poll,
};

use abs_buff::{
    TrBuffRead, TrBuffWrite,
    gen_may_cancel_future,
    x_deps::abs_cancel,
};
use abs_cancel::TrCancellationToken;
use abs_smux::conn::TrChannelListener;
use buffex::x_deps::abs_buff;

use crate::connection::{
    Dock, MuxConnection, MuxError, TrMuxConfig,
    channel_handle::ChannelHandle,
    types_::SessionMark_,
};

/// 某 `local_dock` 上的入向子流监听器（类 `TcpListener`）。///
/// > **目标形状（本轮确定，迁移中）**：本类型改为**持有一份 [`MuxConnection`] 的
/// > 克隆**（= 指向 `MuxCore` 的智能指针），不再借用连接，因此生命周期参数从公开
/// > 类型上消失，可以存进结构体、可以从函数返回。设计见
/// > [`crate::connection`] 模块文档 §2 与 `dev-notes/connection-20261002-0548.md` §17。
///
/// > 特别注意：`listen_async(&mut self)` 的 `&mut` 只覆盖**调用期间**，返回值是独立
/// > 对象。因此「建 listener」与「accept 循环」可以拆成两个函数——旧模型下
/// > `ChannelListener<'s,'f,…>` 借用 `DockBinding`，这一步写不出来
/// > （`dev-notes` §16.1 F3）。
///
/// [`TrChannelListener::income_async`] 每次返回一个**待决句柄**
/// [`ChannelHandle`]；调用方决定 accept 还是 reject，之后该 dock 才能继续接受
/// 下一个请求（同一 dock 的请求串行化，便于用户侧实现「排队 / 限流」）。
pub struct ChannelListener<'s, 'f, R, W, C, Rt>
where
    C: TrMuxConfig,
{
    /// 连接对象：`accept_async` 要用它的配置（`make_buff` / `policy`）建子流环。
    conn_: &'f MuxConnection<R, W, C, Rt>,

    /// 监听的 local_dock。
    local_dock_: Dock,

    /// 借用关系与连接泛型的占位；`&'s &'f ()` 同时编码了 `'f: 's`——监听器派生自
    /// 借用了连接的会话，因此连接借用的生命周期必须覆盖监听器自身。真实共享句柄
    /// 见 [`crate::connection`] 模块文档。
    _mark_: SessionMark_<'s, 'f, R, W, C, Rt>}

impl<'s, 'f, R, W, C, Rt> ChannelListener<'s, 'f, R, W, C, Rt>
where
    C: TrMuxConfig,
{
    /// 由连接与监听 `local_dock` 构造（只允许 `listen_async` 调用）。
    pub(crate) fn new_(conn: &'f MuxConnection<R, W, C, Rt>, local_dock: Dock) -> Self {
        ChannelListener {
            conn_: conn,
            local_dock_: local_dock,
            _mark_: PhantomData}
    }
}

/// 丢弃监听器即**解除 listener 身份**，使同一个 `local_dock` 之后可以再作
/// telegraph 使用（见 `ChannelRegistry_::release_listener_`）。
///
/// 注意这不影响该 dock 上已经建立、且在应用手里的子流半部。
impl<R, W, C, Rt> Drop for ChannelListener<'_, '_, R, W, C, Rt>
where
    C: TrMuxConfig,
{
    fn drop(&mut self) {
        self.conn_.reg_().release_listener_(self.local_dock_);
    }
}

impl<'s, 'f, R, W, C, Rt> TrChannelListener for ChannelListener<'s, 'f, R, W, C, Rt>
where
    R: TrBuffRead<u8> + 'f,
    W: TrBuffWrite<u8> + 'f,
    C: TrMuxConfig + 'f,
{
    type Data = u8;
    type Dock = Dock;
    type Err = MuxError<R::Err, W::Err>;

    type ChannelHandle = ChannelHandle<'s, 'f, R, W, C, Rt>;

    type IncomeAsync<'i>
        = MuxIncomeAsync<'i, 's, 'f, 'i, R, W, C, Rt>
    where
        Self: 'i;

    fn local_dock(&self) -> &Self::Dock {
        &self.local_dock_
    }

    fn income_async(&mut self) -> Self::IncomeAsync<'_> {
        MuxIncomeAsync::new(self)
    }
}

/// [`TrChannelListener::income_async`] 的 step 函数。
#[gen_may_cancel_future(MuxIncome, pub)]
async fn mux_income_async_<'i, 's, 'f, R, W, C, Rt, K>(
    listener: &'i mut ChannelListener<'s, 'f, R, W, C, Rt>,
    cancel: K,
) -> Result<ChannelHandle<'s, 'f, R, W, C, Rt>, MuxError<R::Err, W::Err>>
where
    'f: 's,
    R: TrBuffRead<u8> + 'f,
    W: TrBuffWrite<u8> + 'f,
    C: TrMuxConfig + 'f,
    Rt: 'f,
    K: TrCancellationToken,
{
    let conn = listener.conn_;
    let local = listener.local_dock_;
    loop {
        if cancel.is_cancelled() {
            return Result::Err(MuxError::Cancelled);
        }
        if let Option::Some(kind) = conn.reg_().failure_() {
            return Result::Err(kind.into_mux_error_());
        }
        if let Option::Some(remote) = conn.reg_().take_pending_inbound_(local) {
            return Result::Ok(ChannelHandle::new_(conn, local, remote));
        }
        // 先登记 waker，再复检一次：避免「检查完、还没登记」之间丢唤醒。
        poll_fn(|cx| {
            if conn.reg_().has_pending_inbound_(local) || conn.reg_().is_failed_() {
                return Poll::Ready(());
            }
            conn.reg_().register_inbound_waker_(local, cx.waker());
            if conn.reg_().has_pending_inbound_(local) || conn.reg_().is_failed_() {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        })
        .await;
    }
}
