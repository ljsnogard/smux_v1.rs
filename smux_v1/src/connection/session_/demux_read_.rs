//! 解复用循环的读侧辅助：水位复核、重扫、park 与环写入。

use core::{future::poll_fn, ops::Bound, task::Poll};

use abs_buff::{
    Demand, TrBuffRead, TrBuffWrite,
    buffer::TrBuffSegmMut,
    error::{ReadErrTag, TrTaggedError},
};
use abs_cancel::{TrCancellationToken, TrMayCancel};

use crate::{
    connection::{
        Dock, FrameHeader, MuxError, TrConnAllocCfg, TrConnCfg, owner_::ChannelOwner_,
        signal_::ReadAttach_,
    },
    flow_ctrl::{Credit, WindowReport},
    x_deps::{abs_buff, abs_cancel},
};

use super::{
    frame_io_::{Took_, took_},
    local_table_::ReadTable_,
    shared_::MuxShared_,
};

/// 从帧头取出窗口通告（`PULSE` / `WINDOW_UPDATE` / `OPEN` 上必需）。
pub(super) fn window_report_of_(header: &FrameHeader) -> Option<WindowReport> {
    let total = header.recv_total()?;
    let window = header.recv_window()?;
    if header.is_total_reset() {
        Option::Some(WindowReport::new_reset(total, window))
    } else {
        Option::Some(WindowReport::new(total, window))
    }
}

/// 非阻塞排空读事件（`RxConsumed` / `TgAttach`），返回连接级错误。
///
/// 与本模块的复用循环同理：**每轮的批有上限**。事件是持续到来的（应用每读一次就会
/// 投一条 `RxConsumed`），若「处理完为止」而不设上限，事件队列始终非空时本循环就
/// 永远走不到「读一个帧头」那一步。
///
/// 由 [`scan_released_`] 按位摘表，不再经过事件通道。
pub(super) async fn rescan_read_<C>(
    shared: &MuxShared_<C>,
    table: &mut ReadTable_<crate::connection::ConnAllocCfg_<C>>,
    now_millis: u64,
) -> Result<(), MuxError>
where
    C: TrConnCfg,
{
    let mut cursor: Option<(Dock, Dock)> = Option::None;
    loop {
        let next = match cursor {
            Option::None => table.keys().next().copied(),
            Option::Some(last) => table
                .range((Bound::Excluded(last), Bound::Unbounded))
                .next()
                .map(|(key, _)| *key),
        };
        let Option::Some(pair) = next else {
            return Result::Ok(());
        };
        cursor = Option::Some(pair);

        // 先取快照再 awaits：`entry` 的借用不能跨 `await`。
        let Option::Some((owner, buffered)) = table
            .get(pair)
            .map(|(_, entry)| (entry.owner_.clone(), entry.writer_.ring_state().data_size()))
        else {
            continue;
        };

        if owner.clear_rx_consumed_() {
            recheck_recv_level_::<C>(shared, &owner, buffered, now_millis);
        }
    }
}

/// `WINDOW_UPDATE` 由 `main_loop` 在确认公共 TX 有空间后取快照并写出。
pub(super) fn recheck_recv_level_<C>(
    shared: &MuxShared_<C>,
    owner: &ChannelOwner_<crate::connection::ConnChanOwnerAlloc_<C>>,
    buffered: usize,
    now_millis: u64,
) where
    C: TrConnCfg,
{
    // 记账与判定在本循环做（它才知道环内积压），**取快照 / 组帧 / 写出归 main**：
    // `should_report` 是消费式的，取走就必须保证帧写得出去。
    let buffered = Credit::try_from(buffered).unwrap_or(Credit::MAX);
    if owner.recv_recheck_due_(buffered, now_millis) {
        shared
            .control_()
            .raise_main_hint_(crate::connection::HINT_MAIN_WINDOW_DIRTY);
    }
}

/// 解复用循环在「传输此刻没字节」时的等待结果。
pub(super) enum ReadWake_<C>
where
    C: TrConnAllocCfg + 'static,
{
    /// 传输里来了字节。
    Bytes,

    /// 传输的**生产端已封口且已排空**：字节流到此为止（干净 EOF）。
    ///
    /// 传输的状态查询不一定能给出这一结论（例如它没实现 `TrConsumerState`），因此
    /// 这里也把「读 `Err(Closing)`」归到本变体——两种来源同义。
    Eof,

    /// 传输读失败（不是「封口」）：连接不可用。
    Fatal,

    /// 合并提示（身份释放等）：把掩码带回调用方，按状态位推进本地表。
    Hint(u64),

    /// `Attach` inbox 里来了一条新的接收环写端。
    Attach(ReadAttach_<C>),

    /// 取消令牌触发：连接正在收尾，本循环退出。
    Cancelled,
}

/// 解复用循环空转时的 park：**同时**等「传输有字节」与「读事件通道有事件」，
/// 并与取消令牌竞争。
///
/// # 三个 future 必须在闭包外 `pin` 好
///
/// 环的 park future 在 drop 时**撤回**自己的唤醒登记（`buffex::ring` 的等待槽是
/// **单槽信箱**），因此「就地建 future、poll 一次、丢掉」等于只问一句「此刻可读吗」，
/// 留不下任何唤醒——复用循环那条「park 在环上等提交」的兜底正是这么失效的（见
/// `dev-notes/flow-ctrl-20261005-0115.md` §5）。这里把三个 future 都跨 poll 持有。
pub(super) async fn park_read_wake_<C, K>(
    cancel: &K,
    rx_stage: &mut C::ConnRx,
    shared: &MuxShared_<C>,
) -> ReadWake_<crate::connection::ConnAllocCfg_<C>>
where
    C: TrConnCfg,
    K: TrCancellationToken,
{
    let demand = Demand::at_least(1usize);
    let cancel_fut = cancel.child_token().cancellation();
    let mut cancel_fut = core::pin::pin!(cancel_fut);
    let mut ring_fut = core::pin::pin!(core::future::IntoFuture::into_future(
        rx_stage.read_async(&demand),
    ));
    // `Attach` 通道的接收 future：`RecvAsync` 是参数载体，先转 future 再跨 poll 持有。
    let mut attach_fut = core::pin::pin!(core::future::IntoFuture::into_future(
        shared.reg_().recv_read_attach_async_()
    ));
    let mut cancelled = false;
    // 传输读 future 就绪时的分类（段本身借用 `ring_fut`，因此就地判、就地 drop）。
    let mut woke: Option<ReadWake_<crate::connection::ConnAllocCfg_<C>>> = Option::None;
    // 合并提示（身份释放）：把掩码带回调用方。
    let mut hint_bits = 0u64;
    // `Attach` inbox 里取出的一条。
    let mut taken_attach: Option<ReadAttach_<crate::connection::ConnAllocCfg_<C>>> = Option::None;
    poll_fn(|cx| {
        // 取消令牌优先：连接已收尾，直接退出。
        if core::future::Future::poll(cancel_fut.as_mut(), cx).is_ready() {
            cancelled = true;
            return Poll::Ready(());
        }
        // 传输来了数据：回顶部交给正式的帧头解析。借出的段在此 drop（**不消费**，
        // 已消费量为 0——字节仍留在传输自己的缓冲里）。
        //
        // 就绪的也可能是**收尾**而不是数据：`read_async` 在「生产端封口」时给出
        // `Err(Closing)`。两种情形必须分开——前者回顶部继续解析，后者是本端该干净
        // 退出的时刻；把 `Closing` 当成普通读错误会让「对端正常关闭」变成连接级失败。
        if woke.is_none() {
            match core::future::Future::poll(ring_fut.as_mut(), cx) {
                Poll::Pending => {}
                Poll::Ready(outcome) => {
                    woke = Option::Some(match took_(outcome) {
                        // 段不消费即释放：只为唤醒，数据留给随后的正式解析。
                        Took_::Segm(segm) => {
                            drop(segm);
                            ReadWake_::Bytes
                        }
                        Took_::Failed(err) => {
                            if err.err_tag() == ReadErrTag::Closing {
                                ReadWake_::Eof
                            } else {
                                ReadWake_::Fatal
                            }
                        }
                        // 既没有段也没有错误：当作「来了字节」回顶部重判一次
                        // （与旧实现同义，避免在这里自旋）。
                        Took_::Nothing => ReadWake_::Bytes,
                    });
                    return Poll::Ready(());
                }
            }
        }
        // 合并提示：身份释放不再逐条投事件，这里把掩码带回调用方。
        if let Poll::Ready(bits) = shared.reg_().poll_read_hint_(cx) {
            hint_bits = bits;
            return Poll::Ready(());
        }
        match core::future::Future::poll(attach_fut.as_mut(), cx) {
            Poll::Ready(Result::Ok(attach)) => {
                taken_attach = Option::Some(attach);
                return Poll::Ready(());
            }
            // 读侧通道关闭（连接收尾）。
            Poll::Ready(Result::Err(_)) => return Poll::Ready(()),
            Poll::Pending => {}
        }
        Poll::Pending
    })
    .await;
    if cancelled {
        ReadWake_::Cancelled
    } else if hint_bits != 0u64 {
        ReadWake_::Hint(hint_bits)
    } else if let Option::Some(attach) = taken_attach {
        ReadWake_::Attach(attach)
    } else if let Option::Some(woke) = woke {
        woke
    } else {
        // 传输与取消都没就绪（不该发生）：按收尾处理。
        ReadWake_::Cancelled
    }
}

/// 把 `bytes` 全部写进接收环（环满时 park，等应用消费）。
pub(super) async fn write_into_ring_<W, K>(
    writer: &mut W,
    mut bytes: &[u8],
    cancel: K,
) -> Result<(), W::Err>
where
    W: TrBuffWrite<u8>,
    K: TrCancellationToken,
{
    while !bytes.is_empty() {
        let demand = Demand::at_least(1usize);
        let mut outcome = writer
            .write_async(&demand)
            .may_cancel_with(cancel.child_token())
            .await;
        let put = match outcome.as_mut().pick_left() {
            Option::Some(segm) => segm.as_segm_mut().clone_items_from_buff(bytes),
            Option::None => {
                return Result::Err(
                    outcome
                        .pick_right()
                        .expect("IO 结果必须要么是段、要么是错误"),
                );
            }
        };
        if put == 0usize {
            return Result::Err(outcome.pick_right().expect("段为空时应当已经给出错误"));
        }
        bytes = &bytes[put..];
    }
    Result::Ok(())
}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// 复用循环
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
