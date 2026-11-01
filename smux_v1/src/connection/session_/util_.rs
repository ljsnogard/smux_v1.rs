//! 循环级小工具：失败收尾、可取消竞争、逐子流失败上报。

use core::{future::poll_fn, task::Poll};

use abs_cancel::TrCancellationToken;

use crate::{
    connection::{Dock, FrameKind, MuxError, TrConnAllocCfg, TrConnCfg, owner_::ChannelOwner_},
    metrics::{ChannelCloseReason, TrMetricsSink},
    time::TrTime,
    x_deps::abs_cancel,
};

use super::shared_::{MuxLoopShared_, MuxShared_};

/// 提示建流等待者（`open_channel_async`）：状态可能变了。
///
/// 建流状态标志在**原子字**里、通知通道是持久的，因此这一步是纯同步的一次 CAS +
/// 一次非阻塞投递——不需要取锁，也没有 `await`。
pub(crate) async fn fail_mux_loop_<C, R, M, K>(
    shared: &MuxLoopShared_<C, R, M>,
    cancel: &K,
    kind: Option<FrameKind>,
    err: &MuxError,
) where
    C: TrConnAllocCfg + 'static,
    R: TrTime,
    M: TrMetricsSink,
    K: TrCancellationToken,
{
    if !cancel.is_cancelled() {
        // 上报帧 / 协议错误：**只有真的失败才报**——取消导致的收尾不是帧错误（见上方
        // 文档）。`kind` 给出「已经解析出帧种类」时的种类，帧头都没解析出来时为
        // `None`（见 [`crate::metrics::TrMetricsSink::on_frame_error`]）。
        shared.metrics_().on_frame_error(kind, *err);
        let _ = shared.reg_.mark_failed_(err, cancel.child_token()).await;
    }
}

/// 在「取消令牌触发」与「给定 future 完成」之间竞争：取消先到返回 `None`。
///
/// # 为什么收 `IntoFuture` 而不是 `Future`
///
/// `abs_buff` 的 `read_async` / `write_async` 返回的是只承诺 [`TrMayCancel`] 的
/// **中间类型**（见 dev-notes §18.4 第 4 条），要经 `IntoFuture` 才成为 future。
/// 本函数直接在内部做这一步投影，因此调用方不需要在每个 park 点手写
/// `.into_future()`——「每一次可能长期 park 的 await 都要挂上取消」这条纪律
/// 就只剩一个落点。
///
/// # 为什么循环需要它
///
/// `may_cancel_with` 只是把令牌交给内层 future（内层在被 poll 时才检查
/// `is_cancelled`），**是否在取消时唤醒 park 中的 future 取决于那一层的实现**：
/// 例如 `buffex` 的环半部会把 `cancellation()` 存进自己的 park 状态、并在取消时
/// 唤醒，而一个只做转发的传输适配层不会。循环的收尾不能建立在这条隐含契约上——
/// 一旦某一层的 park 不被唤醒，循环就再也看不到取消令牌，任务与传输永久泄漏。
///
/// 因此循环把**每一次可能长期 park 的 await**（环读、环写、传输读、传输写）都
/// 放到这里，与 `cancellation()`（它保证登记 waker）竞争：「丢弃连接即关闭连接」
/// 由此与传输的实现细节无关。
pub(crate) async fn race_cancel_<F, K>(cancel: &K, fut: F) -> Option<F::Output>
where
    F: core::future::IntoFuture,
    K: TrCancellationToken,
{
    let mut fut = core::pin::pin!(fut.into_future());
    let mut cancelled = core::pin::pin!(cancel.child_token().cancellation());
    poll_fn(|cx| {
        if core::future::Future::poll(cancelled.as_mut(), cx).is_ready() {
            return Poll::Ready(Option::None);
        }
        core::future::Future::poll(fut.as_mut(), cx).map(Option::Some)
    })
    .await
}

/// 连接级失败牵连的**逐子流关闭上报**（恰好一次）。
///
/// 「恰好一次」由 [`ChannelOwner_::claim_release_`] 的一次性 CAS 保证：它与
/// `main_loop::flush_releasable_` 的正常收尾路径共用同一把闸门，因此同一条子流不会
/// 被报两次。
pub(super) fn report_conn_failed_<C>(
    shared: &MuxShared_<C>,
    owner: &ChannelOwner_<crate::connection::ConnChanOwnerAlloc_<C>>,
    local: Dock,
    remote: Dock,
    now_millis: u64,
) where
    C: TrConnCfg,
{
    if !owner.claim_release_() {
        return;
    }
    shared.metrics_().on_channel_closed(
        local,
        remote,
        ChannelCloseReason::ConnFailed,
        now_millis.saturating_sub(owner.created_millis_()),
    );
}
