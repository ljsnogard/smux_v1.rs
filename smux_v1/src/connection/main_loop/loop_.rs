//! `main_loop`：控制面单写者循环（P1 骨架）。
//!
//! 它接管三件事：
//!
//! - **请求队列**：bind / listen / reserve 从 API future 经 [`ControlState_`] 进来；
//! - **身份表写**：所有登记 / 释放都经 [`ChannelRegistry_`] 的写锁（`main_loop`
//!   是唯一写者；`demux_loop` 后续只读）；
//! - **过渡期的释放落实**：P1 仍让 `DockBinding` / `ChannelListener` 的 `Drop` 投
//!   `SessionEvent_`，由本循环 `drain_session_events_` 落实；P4 改成 retired 位 +
//!   `HINT_MAIN_RETIRED` 后删除这条过渡路径（设计稿 §6.7）。
//!
//! 公共报文子流的 RX / TX（P2）与 tick（P5）尚未接线；park 点已经按设计稿 §6.1
//! 的形状保留「请求 / 提示 / 取消」三类唤醒源，后续加 ring 时只需再加一个 poll 分支。

use core::{alloc::AllocatorClone, future::poll_fn, pin::pin, task::Poll};

use abs_cancel::{NonCancellableToken, TrCancellationToken};
use abs_smux::dock::TrDock;

use crate::{
    connection::{
        MuxError, ReserveErr_, TrConnCfg,
        control_state_::ControlState_,
        main_loop::{MainErr_, MainReply_, MainRequest_},
        session_::MuxShared_,
    },
    x_deps::{abs_cancel, abs_smux},
};

/// 每轮最多处理多少条请求。
///
/// 防止「请求源源不断 ⇒ 提示 / 取消饿死」；控制面是冷路径，16 已远高于常见并发。
pub(crate) const K_MAIN_REQ_BATCH: usize = 16usize;

/// `main_loop` 的 park 点被唤醒的原因。
enum MainWake_<A>
where
    A: AllocatorClone + Send + Sync,
{
    /// 队列里取到一条请求。
    Request(MainRequest_<A>),

    /// 合并提示位（P1 尚无 `HINT_MAIN_*` 使用者）。
    Hint(u64),
}

/// 控制面循环本体。
///
/// 不持有 `ConnTx` / `ConnRx`，也不持有用户子流的环半部；只持共享面与请求队列句柄。
/// 退出前关闭请求队列并把仍排队的请求以 [`MuxError::Closed`] 结束，避免调用方永久
/// 等待。
pub(crate) async fn main_loop_async_<C, K>(
    shared: MuxShared_<C>,
    ctl: ControlState_<C::Alloc>,
    _public_tx_w: crate::connection::ring_::BufferedTx,
    _public_rx_r: crate::connection::ring_::BufferedRx,
    cancel: K,
) where
    C: TrConnCfg,
    K: TrCancellationToken,
{
    loop {
        if cancel.is_cancelled() {
            exit_(&ctl);
            return;
        }
        // 过渡：`Drop` 仍投 `SessionEvent_`（P4 改为 retired 位）。
        if shared
            .reg_()
            .drain_session_events_(shared.conn_clock_().now_millis_(), cancel.child_token())
            .await
            .is_err()
        {
            exit_(&ctl);
            return;
        }

        // 合并提示：P1 只取走，具体位在后续阶段接入。
        let _hints = ctl.take_main_hint_();

        // 请求：每轮最多 `K_MAIN_REQ_BATCH` 条。
        for _ in 0..K_MAIN_REQ_BATCH {
            let Some(request) = ctl.try_pop_request_() else {
                break;
            };
            handle_request_::<C, _>(&shared, request, &cancel).await;
        }

        match park_main_(&ctl, &cancel).await {
            Some(MainWake_::Request(request)) => {
                handle_request_::<C, _>(&shared, request, &cancel).await;
            }
            // 提示已由下一轮顶部的 `take_main_hint_` 取走；这里只需回顶部。
            Some(MainWake_::Hint(_bits)) => {}
            // 取消或请求队列关闭：连接正在收尾。
            None => {
                exit_(&ctl);
                return;
            }
        }
    }
}

/// 收尾：关闭队列，并把仍排队的请求以 `Closed` 结束。
fn exit_<A>(ctl: &ControlState_<A>)
where
    A: AllocatorClone + Send + Sync,
{
    ctl.close_requests_();
    while let Some(request) = ctl.try_pop_request_() {
        request.fail_(MainErr_::Mux(MuxError::Closed));
    }
}

/// 处理一条请求。
///
/// 取身份表写锁是协作式异步获取（可取消）；临界区本身只做一次登记 / 查询，不 `await`。
async fn handle_request_<C, K>(shared: &MuxShared_<C>, request: MainRequest_<C::Alloc>, cancel: &K)
where
    C: TrConnCfg,
    K: TrCancellationToken,
{
    // 过渡：`Drop` 仍投 `SessionEvent_`。登记之前先把它落实，保证「丢弃后立刻重绑 /
    // 重听 / 重新登记同一条 dock 对」与旧 API 的语义一致（P4 改为 retired 位后删除）。
    if shared
        .reg_()
        .drain_session_events_(shared.conn_clock_().now_millis_(), cancel.child_token())
        .await
        .is_err()
    {
        request.fail_(MainErr_::Mux(MuxError::Cancelled));
        return;
    }
    match request {
        MainRequest_::Bind { local_dock, reply } => {
            if reply.is_abandoned_() {
                let _ = reply.set_(Result::Err(MainErr_::Reserve(ReserveErr_::Cancelled)));
                return;
            }
            let result = if local_dock.is_unspecified() {
                shared
                    .reg_()
                    .bind_any_dock_(cancel.child_token())
                    .await
                    .map(MainReply_::Dock)
            } else {
                shared
                    .reg_()
                    .bind_dock_(local_dock, cancel.child_token())
                    .await
                    .map(|()| MainReply_::Dock(local_dock))
            };
            // 登记之后才发现等待方已放弃：**回滚刚登记的身份**，否则这个 dock 会
            // 永久占住（没有句柄能解绑它）。回滚用不可取消令牌：它是收尾动作。
            if reply.is_abandoned_()
                && let Result::Ok(MainReply_::Dock(dock)) = &result
            {
                let _ = shared
                    .reg_()
                    .unbind_dock_(*dock, NonCancellableToken::new())
                    .await;
            }
            let _ = reply.set_(result.map_err(MainErr_::Reserve));
        }
        MainRequest_::Listen {
            local_dock,
            reserve: _reserve,
            reply,
        } => {
            // `reserve` 仍未用于预分配：入向通知目前内联在身份节点里。
            if reply.is_abandoned_() {
                let _ = reply.set_(Result::Err(MainErr_::Reserve(ReserveErr_::Cancelled)));
                return;
            }
            let result = shared
                .reg_()
                .reserve_listener_(local_dock, cancel.child_token())
                .await
                .map(MainReply_::Listener);
            if reply.is_abandoned_() {
                let _ = shared
                    .reg_()
                    .release_listener_(local_dock, NonCancellableToken::new())
                    .await;
            }
            let _ = reply.set_(result.map_err(MainErr_::Reserve));
        }
        MainRequest_::ReserveChannel {
            local_dock,
            remote_dock,
            reply,
        } => {
            if reply.is_abandoned_() {
                let _ = reply.set_(Result::Err(MainErr_::Reserve(ReserveErr_::Cancelled)));
                return;
            }
            let now_millis = shared.conn_clock_().now_millis_();
            let result = shared
                .reg_()
                .reserve_channel_(local_dock, remote_dock, now_millis, cancel.child_token())
                .await
                .map(MainReply_::Channel);
            if reply.is_abandoned_() {
                let _ = shared
                    .reg_()
                    .unreserve_channel_(local_dock, remote_dock, NonCancellableToken::new())
                    .await;
            }
            let _ = reply.set_(result.map_err(MainErr_::Reserve));
        }
    }
}

/// park 到「有请求 / 有提示 / 取消」之一。
///
/// 返回 `None` 表示连接正在收尾（取消令牌触发，或请求队列被关闭）。
async fn park_main_<A, K>(ctl: &ControlState_<A>, cancel: &K) -> Option<MainWake_<A>>
where
    A: AllocatorClone + Send + Sync,
    K: TrCancellationToken,
{
    let cancel_fut = cancel.child_token().cancellation();
    let mut cancel_fut = pin!(cancel_fut);
    let mut taken: Option<MainWake_<A>> = None;
    poll_fn(|cx| {
        if core::future::Future::poll(cancel_fut.as_mut(), cx).is_ready() {
            return Poll::Ready(());
        }
        match ctl.poll_pop_request_(cx) {
            Poll::Ready(Some(request)) => {
                taken = Some(MainWake_::Request(request));
                return Poll::Ready(());
            }
            // 队列关闭：连接收尾。
            Poll::Ready(None) => return Poll::Ready(()),
            Poll::Pending => {}
        }
        if let Poll::Ready(bits) = ctl.poll_main_hint_(cx) {
            taken = Some(MainWake_::Hint(bits));
            return Poll::Ready(());
        }
        Poll::Pending
    })
    .await;
    taken
}
