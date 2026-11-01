//! 复用循环的 telegraph 发送侧：长度队列、端点排空与数据报成帧。

use abs_cancel::TrCancellationToken;

use crate::{
    connection::{
        Dock, FrameHeader, FrameKind, MuxError, TrConnCfg,
        frame_::{K_MAX_FRAME_HEADER, encode_header_},
    },
    metrics::{FrameDir, TrMetricsSink},
    x_deps::abs_cancel,
};

use super::{
    frame_io_::ring_space_,
    local_table_::{TgWriteEntry_, TgWriteTable_},
    mux_data_::write_frame_to_stage_,
    shared_::MuxShared_,
    util_::race_cancel_,
};

/// 摘掉「应用已丢弃发送半边」的 telegraph 表项（位化后的 `TgTxClosed`）。
///
/// `Sender::drop` 只置 `TgRec_::tx_closed_` + raise `HINT_W_TG_TX_CLOSED`；身份本身
/// 何时释放由两个半边的守卫共同决定，与这里无关。环里那些字节已没有提交者，
/// 长度队列一并清掉。
pub(super) fn scan_tg_tx_closed_<C>(table: &mut TgWriteTable_<crate::connection::ConnAllocCfg_<C>>)
where
    C: TrConnCfg,
{
    // 有界循环：每次摘一个，次数不超过初始表长（B+ 树没有 `retain`）。
    let mut remaining = table.len();
    while remaining > 0usize {
        remaining -= 1usize;
        let Some(local) = table
            .iter()
            .find(|(_, entry)| entry.owner_.is_tx_closed_())
            .map(|(local, _)| *local)
        else {
            return;
        };
        let Some(entry) = table.remove(local) else {
            return;
        };
        entry.owner_.out_().clear_();
    }
}

/// 把一条 telegraph 发送端点里**已提交**的报文逐条送出。
///
/// # 循环推进的判据
///
/// 报文长度由身份节点内联的发送队列给出（FIFO）。每次尝试发送**一条**：
///
/// - 环里暂时没有足够字节（应用写了但还没提交完）或没有长度记录 → 本轮结束；
/// - 传输写半边满 → 由 [`write_frame_to_stage_`] 返回「未写出」，本轮结束、数据留在
///   发送环里，等主循环下一轮（写环腾出空间后会再来）。
///
/// 返回本轮成功送出的**报文条数**（仅供诊断 / 测试断言）。
pub(super) async fn flush_tg_endpoint_<C, K>(
    tx_stage: &mut C::ConnTx,
    entry: &mut TgWriteEntry_<crate::connection::ConnAllocCfg_<C>>,
    local: Dock,
    shared: &MuxShared_<C>,
    cancel: &K,
) -> Result<usize, MuxError>
where
    C: TrConnCfg,
    K: TrCancellationToken,
{
    let mut sent = 0usize;
    // 用 `loop` 而非 `while let`：循环体里有多条**带副作用的提前退出**（丢弃一条卡住
    // 的长度记录后继续下一条），`while let` 的绑定会把「队头长度」钉在整个循环体上，
    // 而循环体里恰恰要把它出队改写。
    #[allow(clippy::while_let_loop)]
    loop {
        // 队列空 → 没有待发报文。
        let Some(_) = entry.owner_.out_().peek_len_() else {
            break;
        };
        match race_cancel_(
            cancel,
            send_one_datagram_::<C>(tx_stage, entry, local, shared),
        )
        .await
        {
            Option::None => return Result::Err(MuxError::Cancelled),
            // 送出了一条：继续试下一条。
            Option::Some(Result::Ok(true)) => {
                sent += 1usize;
                continue;
            }
            // 写环满 / 环里数据还没到齐：留到下一轮。
            Option::Some(Result::Ok(false)) => break,
            // 环被关闭（发送半边已丢弃 / 连接收尾）：丢弃这一条长度记录，继续下一条
            // ——否则它会永远卡在队头，后面的报文一条也发不出去。
            Option::Some(Result::Err(_)) => {
                let _ = entry.owner_.out_().try_pop_();
                continue;
            }
        }
    }
    Result::Ok(sent)
}

/// 尝试送出一条数据报。
///
/// 返回 `Ok(true)` 表示**确实送出了一条**（长度记录已出队、帧已进传输写半边）；
/// `Ok(false)` 表示本轮条件不满足（写环没空间，或环里还没有那么长的数据），调用方应
/// 留到下一轮重试——**长度记录不会出队**。
///
/// # Errors
///
/// 发送环已关闭（应用丢了发送半边）或读段失败时返回错误，调用方据此丢弃该长度记录。
pub(super) async fn send_one_datagram_<C>(
    tx_stage: &mut C::ConnTx,
    entry: &mut TgWriteEntry_<crate::connection::ConnAllocCfg_<C>>,
    local: Dock,
    shared: &MuxShared_<C>,
) -> Result<bool, MuxError>
where
    C: TrConnCfg,
{
    // 1. 队头条目 `(目的地址, 长度)`（不出队）。目的地址是**逐次发送**的实参。
    let Some((remote, payload_len)) = entry.owner_.out_().peek_len_() else {
        return Result::Ok(false);
    };
    // 2. 传输写半边至少要放得下帧头——否则不必去动发送环（避免「读了却写不出去」时
    //    还要处理回退）。
    let Some(space) = ring_space_(tx_stage) else {
        return Result::Ok(false);
    };
    if space < K_MAX_FRAME_HEADER {
        return Result::Ok(false);
    }
    // 3. 应用写进环里的数据够不够这一条？不够说明它还没提交完（或已经丢弃发送半
    //    边）——两者都按「本轮不推进」处理，由下一轮或 `TgTxClosed` 收尾。注意这里
    //    只做**非阻塞**探测：数据报是尽力交付，主循环不能被一条尚未提交完的报文挡住。
    let available = entry.reader_.ring_state().data_size();
    if available < payload_len {
        return Result::Ok(false);
    }
    // 4. 编码并写出。目的地址来自本次发送的实参（见 `peek_len_` 取出的队头条目）。
    let header = FrameHeader::new_(
        FrameKind::Datagram,
        0u8,
        local,
        remote,
        payload_len,
        Option::None,
    );
    let (head, head_len) = encode_header_(&header)?;
    if head_len + payload_len > shared.max_packet_size_ {
        // 单条报文超过协商的帧总长上限。这是**应用侧**的问题（它提交了一条连一帧都装
        // 不下的数据报），**不是连接级故障**：整条丢弃并把长度记录出队，让后面的报文
        // 照常发出——绝不能把整条连接判失败，也不能把它留在队头（那会把后面的报文全
        // 堵死）。
        //
        // 丢弃与接收侧的「装不下就整条丢弃」同源（都是尽力交付），因此计入同一个计数器。
        let _ = entry.owner_.out_().try_pop_();
        shared.metrics_().on_datagram_dropped(
            local,
            remote,
            u32::try_from(payload_len).unwrap_or(u32::MAX),
        );
        return Result::Ok(false);
    }
    write_frame_to_stage_::<C>(tx_stage, entry, &head[..head_len], payload_len).await?;
    // 整帧已进传输写半边：长度记录出队（这一步之后才允许下一轮读下一条）。
    let popped = entry.owner_.out_().try_pop_();
    debug_assert_eq!(
        popped,
        Option::Some((remote, payload_len)),
        "队头条目不应在发送期间变化"
    );
    // 上报：帧总长与 channel 侧同口径（帧头 + 载荷）。数据报**不**上报子流级别的
    // 统计（它没有身份寿命可言）。
    shared.metrics_().on_frame(
        FrameDir::Send,
        local,
        remote,
        FrameKind::Datagram,
        u32::try_from(head_len + payload_len).unwrap_or(u32::MAX),
    );
    shared.metrics_().on_transport_bytes(
        FrameDir::Send,
        u64::try_from(head_len + payload_len).unwrap_or(u64::MAX),
    );
    Result::Ok(true)
}
