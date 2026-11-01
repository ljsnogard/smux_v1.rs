use core::future::poll_fn;

use abs_cancel::TrCancellationToken;
use abs_smux::conn::TrChannelListener;
use gen_mcf2::gen_may_cancel_future;

use crate::{
    connection::{
        Dock, MainLoopErr_, MainLoopReply_, MainLoopRequest_, MuxConnection, MuxError, TrConnCfg,
        TcsAbandonGuard_, Tcs_,
        channel_handle::ChannelHandle,
        owner_::LsnOwner_,
        session_::race_cancel_,
    },
    x_deps::{abs_cancel, abs_smux, gen_mcf2}
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
pub struct ChannelListener<C>
where
    C: TrConnCfg,
{
    /// 连接智能指针：accept 时要用它的配置（`policy` 等）建子流环。
    conn_: MuxConnection<C>,

    /// 监听的 local_dock。
    local_dock_: Dock,

    /// 「本 dock 上有入向事件（新请求 / 连接失败）」的**身份节点句柄**。
    ///
    /// 通知槽内联在节点里（零堆分配），本对象持句柄并在它上面 park——句柄同时保证
    /// 「只要 listener 还在，身份节点就还在」，因此等待期间不会被回收。
    rec_: LsnOwner_<crate::connection::ConnChanOwnerAlloc_<C>>,
}

impl<C> ChannelListener<C>
where
    C: TrConnCfg,
{
    /// 由连接、监听 `local_dock` 与身份节点句柄构造（只允许 `listen_async` 调用）。
    pub(crate) fn new_(
        conn: MuxConnection<C>,
        local_dock: Dock,
        rec_: LsnOwner_<crate::connection::ConnChanOwnerAlloc_<C>>,
    ) -> Self {
        ChannelListener {
            conn_: conn,
            local_dock_: local_dock,
            rec_,
        }
    }
}

/// 丢弃监听器即**解除 listener 身份**（`LsnRec_` 的 retired 位 + 合并提示，
/// 由核心落实），使同一个 `local_dock` 之后可以再作 telegraph 使用
/// （见 `ChannelRegistry_::release_listener_`）。
///
/// **本 `Drop` 不取锁、不阻塞**；`listen_async` / `open_telegraph_async` 在认领身份
/// 之前会先清空释放邮箱，因此「丢弃后立刻复用同一 dock」仍然是确定的。
///
/// 注意这不影响该 dock 上已经建立、且在应用手里的子流半部。
impl<C> Drop for ChannelListener<C>
where
    C: TrConnCfg,
{
    fn drop(&mut self) {
        // 只置位 + raise，由 main 的 retired 扫描释放。
        self.rec_.set_retired_();
        self.conn_
            .core_()
            .control_()
            .raise_main_hint_(crate::connection::HINT_MAIN_RETIRED);
    }
}

impl<C> TrChannelListener<C> for ChannelListener<C>
where
    C: TrConnCfg,
{
    type Err = ListenerError;

    type ChannelHandle = ChannelHandle<C>;

    type IncomeAsync<'f> = MuxIncomeAsync<'f, 'f, C>
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
async fn mux_income_async_<'f, C, K>(
    listener: &'f mut ChannelListener<C>,
    cancel: K,
) -> Result<ChannelHandle<C>, ListenerError>
where
    C: TrConnCfg + 'f,

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
        // 2. 有已到达的入向请求就当场取走。**经请求队列交给 main**：身份表槽由
        //    `main_loop` 独占写（I3），入向请求的取出会推进 `Pending → HandedOut`。
        let tcs = match Tcs_::new_(conn.core_().reg_().scope_()).await {
            Result::Ok(tcs) => tcs,
            Result::Err(_) => return Result::Err(ListenerError::Cancelled),
        };
        let waiter = tcs.clone();
        // 调用方取消 / 丢弃 future 时放弃结果；main 会在 `set_` 之前检查并回滚。
        let _abandon = TcsAbandonGuard_::new_(&waiter);
        if conn
            .core_()
            .control_()
            .submit_async_(
                MainLoopRequest_::TakeInbound {
                    local_dock: local,
                    reply: tcs,
                },
                cancel.child_token(),
            )
            .await
            .is_err()
        {
            return Result::Err(ListenerError::Cancelled);
        }
        let reply = race_cancel_(
            &cancel,
            poll_fn(|cx| waiter.poll_wait_(cx)),
        )
        .await;
        match reply {
            Option::Some(Option::Some(Result::Ok(MainLoopReply_::Inbound(
                Option::Some((remote, owner, msg_)),
            )))) => {
                return Result::Ok(ChannelHandle::new_(conn, local, remote, owner, msg_));
            }
            // 没有待决请求：落到下面的通知槽等待。
            Option::Some(Option::Some(Result::Ok(MainLoopReply_::Inbound(Option::None)))) => {}
            Option::Some(Option::Some(Result::Err(MainLoopErr_::Mux(err)))) => {
                return Result::Err(ListenerError::Mux(err));
            }
            // 取消 / 放弃 / 队列关闭 / 其他结果：按取消处理（调用方会重试或退出）。
            _ => return Result::Err(ListenerError::Cancelled),
        }
        // 3. 等一条通知（新请求或连接失败），与取消令牌竞争。
        //    槽的持久位是「先通知、后等待也不丢」的保证（协议见 `sync_::NotifySlot_`）：
        //    第 1、2 步与这里之间到达的通知会被 `poll_wait_` 的复检消费掉。
        let rec = &listener.rec_;
        let mut notified = core::pin::pin!(poll_fn(|cx| rec.poll_wait_(cx)));
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
