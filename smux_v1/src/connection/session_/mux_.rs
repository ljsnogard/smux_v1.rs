//! 复用循环：公共 chunk 优先搬运、本地表扫描、`park` 与收尾守卫。

use core::{future::poll_fn, ptr::NonNull, task::Poll};

use abs_buff::TrBuffTryFlush;
use abs_cancel::TrCancellationToken;

use crate::{
    connection::{
        MuxError, TrConnCfg,
        public_channel_::{ChunkReadState_, PublicChunkIo_},
        ring_::BufferedRx,
        signal_::{
            HINT_W_FIN_ENQUEUED, HINT_W_PEER_CLOSED, HINT_W_RELEASED, HINT_W_RESET_ENQUEUED,
            HINT_W_TG_TX_CLOSED, WriteAttach_,
        },
    },
    time::TrTime,
    x_deps::{abs_buff, abs_cancel},
};

use super::{
    frame_io_::enqueue_frame_,
    local_table_::{
        TgWriteTable_, WriteTable_, handle_write_attach_, scan_fin_enqueued_, scan_peer_closed_,
        scan_reset_enqueued_, scan_write_released_,
    },
    mux_data_::drain_once_,
    mux_tg_::{flush_tg_endpoint_, scan_tg_tx_closed_},
    shared_::{K_MAX_DATA_CHUNK, MuxShared_, lock_or_exit_},
    util_::{fail_mux_loop_, race_cancel_, report_conn_failed_},
};

/// 复用循环**本地状态**的收尾守卫（表 + 事件队列各一个）。
///
/// # 为什么必须显式 `close()`
///
/// 应用侧的写者可能正 park 在「发送环满」上、读者正 park 在「接收环为空」上；而
/// `buffex` 的环半部 **drop 不置关闭位、也不唤醒对端**，事件通道的消费者又正是这个
/// 正在退出的循环。于是「循环退出 ⇒ 表被丢掉」**不产生任何唤醒**：应用那半部永远
/// 睡下去——这就是「连接级失败之后应用无期限挂起」的机制。`close()` 是唯一既置关闭位
/// 又唤醒对端的方式，而这一半的持有者只有本循环。
///
/// # 为什么分成「表」与「事件队列」两个守卫
///
/// 半部有两拨：一拨已经进了本地表，另一拨（`Attach`）还**躺在事件队列里**——连接完全
/// 可能在子流刚建好、循环还没取走那条事件时就结束，此时应用已经拿着另一半。两拨都要
/// 收尾，漏掉后者同样是永久挂起。
///
/// # 为什么用 `Deref` 转发而不是把守卫穿进主体
///
/// 主体逐字不动（`table.get_mut(..)` / `events.try_take_event_()` 经自动解引用照常
/// 工作），收尾因此与「表 / 队列的生命周期」绑定：**任何**退出路径（正常返回、取消、
/// 连接级失败，乃至 panic 展开）都会跑到，不必在每个 `return` 前手写一遍。
pub(super) struct MuxLocalGuard_<'a, C>
where
    C: TrConnCfg,
{
    /// 本地写表：dock 对 → 该子流发送环的**读端**（本循环持有）。
    table: WriteTable_<crate::connection::ConnAllocCfg_<C>>,

    /// 连接共享面（上报关闭与读时钟用）。
    shared: &'a MuxShared_<C>,
}

impl<C> core::ops::Deref for MuxLocalGuard_<'_, C>
where
    C: TrConnCfg,
{
    type Target = WriteTable_<crate::connection::ConnAllocCfg_<C>>;

    fn deref(&self) -> &Self::Target {
        &self.table
    }
}

impl<C> core::ops::DerefMut for MuxLocalGuard_<'_, C>
where
    C: TrConnCfg,
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.table
    }
}

impl<C> Drop for MuxLocalGuard_<'_, C>
where
    C: TrConnCfg,
{
    fn drop(&mut self) {
        let failed = self.shared.reg_.fail_kind_();
        let now_millis = self.shared.conn_clock_().now_millis_();
        for (pair, entry) in self.table.iter_mut() {
            // 显式关掉消费端：把「发送环已满而 park 的写者」唤醒（拿到 `Closing`）。
            entry.reader_.close();
            if failed.is_some() {
                report_conn_failed_::<C>(self.shared, &entry.owner_, pair.0, pair.1, now_millis);
            }
        }
        // `Attach` inbox 里还没取走的环半部：显式关闭，别让应用侧永久 park。
        while let Option::Some(attach) = self.shared.reg_.take_write_attach_() {
            match attach {
                WriteAttach_::Channel { mut reader_, .. }
                | WriteAttach_::Telegraph { mut reader_, .. } => reader_.close(),
                // 零 payload 的信号：没有环半部要关。
                WriteAttach_::TgReady { .. } => {}
            }
        }
        self.shared.reg_.close_write_inbox_();
    }
}

/// 传输写半边的**收尾守卫**：本循环退出时封口（[`TrBuffTryFlush::close`]）。
///
/// # 为什么用守卫、而不是在循环末尾手写一次
///
/// 复用循环有多条退出路径（取消、事件轮询被取消、处理事件/发数据/finalize 出错……），
/// 它们分散在几十行里，靠"记得每处都补一次 close"极不可靠。守卫把这件事绑到**作用域
/// 退出**这一个事件上：无论走哪条路径，都会封口一次。
///
/// # 为什么用同步的 `close` 而不是 await `flush_async` + `close_async`
///
/// [`TrBuffTryFlush::close`] 的契约是"**不再有新数据**并唤醒下游排空方"；而"把已提交
/// 的字节真正交给设备"由**下游**自己完成（全被动环由调用方的排空泵、自驱动适配器由
/// 后台泵、按需适配器由下一次 `flush_async`）。因此退出时**不需要**在本循环里 await：
///
/// * 本循环已经把所有帧提交进传输；
/// * 封口之后，"排空 + 设备 `shutdown` + 等泵退出"是下游的事，由调用方或适配器承担。
///
/// 这也让守卫能安全地只依赖同步入口——`Drop` 里不能 await。
///
/// # 为什么不直接持有 `&'a mut W`
///
/// 循环体在几十处把 `&mut tx_stage` 交给辅助函数；守卫若长期借用它，那些调用点全部
/// 变成"二次可变借用"。守卫因此只存一个指向该栈上局部量的指针——它**只为 `Drop` 而
/// 存在**：构造后不再被读、写或移动，声明点在 `tx_stage` 之后，因此 `tx_stage` 一定活
/// 得比守卫久。
pub(crate) struct TxSealGuard_<W>
where
    W: TrBuffTryFlush<u8>,
{
    /// 指向被守卫的传输写半边（借用关系见类型文档末节的说明）。
    tx_: NonNull<W>,
}

impl<W> Drop for TxSealGuard_<W>
where
    W: TrBuffTryFlush<u8>,
{
    fn drop(&mut self) {
        // SAFETY: 构造点只由 `NonNull::from(&mut tx_stage)` 得到，其声明点在该局部量
        // 之后，且本守卫是栈上值：它一定在 `tx_stage` 之前析构。守卫只存指针、不派生
        // 任何别名；`Drop` 期间循环体对 `tx_stage` 的借用都已结束，因此这里 `as_mut`
        // 不会与任何活跃借用重叠。
        let tx = unsafe { self.tx_.as_mut() };
        // 先尽量把已提交的数据推出去（对全被动环与按需适配器都可能立刻有进展），
        // 再封口。两者的失败都不改变"必须封口"这件事，因此忽略返回值——
        // 真正的设备错误会经由传输自己的错误槽（`take_error`）交给调用方。
        let _ = tx.try_flush();
        let _ = tx.close();
    }
}

/// 复用循环：按对端发送窗口调度各子流的发送环，把成帧写进**传输的写半边**。
///
/// 本循环是这条写半边**唯一**的写入者，因此成帧的「头 + 载荷」两段之间不会被别的帧
/// 插进来；字节到网络的最后一跳由调用方那一侧负责（入向 / 出向泵或适配器的自驱动泵）。
/// 循环退出时经 [`TxSealGuard_`] 封口，从而唤醒那一侧的排空者。
/// 把公共 TX ring 里的 chunk 搬进 `ConnTx`（控制帧优先）。
///
/// 只搬 payload（`[u16 len]` 前缀已被消费），不重新组帧、不解析帧头。一旦开始一个
/// chunk 就把它搬完；TX ring 暂时读空时 park 在 `tx_ready` 上，等 `main_loop` 写入。
/// 返回 `true` 表示本轮至少搬了一段（供诊断）。
pub(super) async fn flush_public_chunks_<C, K>(
    tx_stage: &mut C::ConnTx,
    shared: &MuxShared_<C>,
    public_tx_r: &mut BufferedRx,
    state: &mut ChunkReadState_,
    scratch: &mut [u8],
    cancel: &K,
) -> Result<bool, MuxError>
where
    C: TrConnCfg,
    K: TrCancellationToken,
{
    let mut moved = false;
    loop {
        if !state.has_chunk_() {
            match PublicChunkIo_::try_read_prefix_(public_tx_r, state, shared.max_packet_size_()) {
                Result::Ok(Option::Some(_len)) => {}
                Result::Ok(Option::None) => {
                    if moved {
                        shared.public_().notify_tx_space_();
                    }
                    return Result::Ok(moved);
                }
                Result::Err(_) => return Result::Err(MuxError::MalformedFrame),
            }
        }
        while state.left_() > 0 {
            let n = PublicChunkIo_::try_read_payload_into_(public_tx_r, state, scratch)
                .map_err(|_| MuxError::MalformedFrame)?;
            if n == 0 {
                if moved {
                    shared.public_().notify_tx_space_();
                }
                let wait_chunk = poll_fn(|cx| shared.public_().poll_tx_ready_(cx));
                if race_cancel_(cancel, wait_chunk).await.is_none() {
                    return Result::Err(MuxError::Cancelled);
                }
                continue;
            }
            enqueue_frame_(tx_stage, &scratch[..n], cancel.child_token()).await?;
            moved = true;
        }
        shared.public_().notify_tx_space_();
    }
}

pub(crate) async fn mux_loop_async_<C, K>(
    tx_stage: C::ConnTx,
    shared: MuxShared_<C>,
    mut public_tx_r: BufferedRx,
    cancel: K,
) where
    C: TrConnCfg,
    C::Rt: TrTime + Clone,
    K: TrCancellationToken,
{
    // 本循环直接持有**传输的写半边**（不再有连接级写环、也没有外侧写泵）。
    //
    // # 退出时怎么收尾：封口交给守卫，不依赖 drop
    //
    // 旧版这里只能"随作用域 drop"，于是对**调用方驱动的全被动环**（`buffex::ring` 的
    // `RingWriter`）不会唤醒 park 在环读端上的排空方——那一半的关闭权只属于持有者，
    // 而 `TrBuffWrite` 里没有"封口"这一步。
    //
    // 现在写半边必须实现 `TrBuffFlush`（见 `TrConnCfg::ConnTx`），本循环据此在退出时
    // **显式封口**：
    //
    // * 全被动环：置 `PRODUCER_CLOSED` 并唤醒调用方的排空泵（它负责把尾巴交给设备）；
    // * 按需适配器（`try_new`）：同样置位，唤醒/放行下游；
    // * 自驱动适配器（`try_new_local` / compio）：置位后其后台泵把剩余数据送完并
    //   `shutdown` 设备。
    //
    // 守卫在作用域**最后**析构，因此"封口"晚于本循环自己那些表 / 事件队列的关闭，
    // 不会与它们的唤醒语义打架。
    let mut tx_stage = tx_stage;

    // 传输写半边的收尾守卫（见 `TxSealGuard_` 的类型文档）。
    #[allow(dead_code, reason = "只为其 Drop 而存在；构造后不再被读写")]
    let _tx_seal: TxSealGuard_<C::ConnTx> = TxSealGuard_ {
        tx_: NonNull::from(&mut tx_stage),
    };

    // 表与事件队列都包进收尾守卫：**退出时显式关闭本循环持有的那些半部**。
    // 为什么必须这样做、以及为什么分成两个守卫，见 `MuxLocalGuard_` 的类型文档。
    let mut table: MuxLocalGuard_<'_, C> = MuxLocalGuard_ {
        table: crate::connection::dock_index_::DockBPlusTree_::new_in(lock_or_exit_!(
            shared.reg_.local_table_alloc_(cancel.child_token())
        ))
        .expect("2 KiB B+ 树足以容纳一个 channel 表项"),
        shared: &shared,
    };
    // telegraph 的本地写表**单独持有**（不进 `MuxLocalGuard_`）：主循环在同一轮里既要
    // 把 channel 表借给 `handle_write_event_`，又要排空 telegraph——两者若同处一个结构体
    // 就会在 `&mut table.table` 与 `&mut table.tg_table_` 上撞借用检查。它自己的收尾
    // 很简单（关掉每个发送环读端即可），因此不值得为对称再加一个守卫类型。
    let mut tg_table: TgWriteTable_<crate::connection::ConnAllocCfg_<C>> =
        crate::connection::dock_index_::DockBPlusTree_::new_in(lock_or_exit_!(
            shared.reg_.local_table_alloc_(cancel.child_token())
        ))
        .expect("2 KiB B+ 树足以容纳一个 telegraph 端点");
    // 公共 TX 的 chunked 读进度。
    let mut public_read = ChunkReadState_::new_();
    // park 期间被合并提示唤醒时，把掩码带到这里、在下一轮顶部统一处理。
    let mut pending_hints = 0u64;
    // 环段字节的搬出暂存：`clone_items_from_buff` / `move_items_to_buff` 需要一个
    // 连续切片作为中间落点（段本身不能作为 `write_all_async_` 的源）。走连接级临时
    // 对象域，整条连接只分配一次。
    let scratch_retain = lock_or_exit_!(crate::connection::alloc_zeroed_bytes_async_(
        shared.reg_.scope_(),
        K_MAX_DATA_CHUNK,
    ));
    let mut scratch = scratch_retain
        .try_owning()
        .expect("刚分配的搬出暂存应当可独占");

    loop {
        #[cfg(feature = "test-loop-probe")]
        crate::connection::loop_probe_::mux_tick_();
        if cancel.is_cancelled() {
            return;
        }

        // 0.05 `Attach` inbox：环半部从这里进入本地写表（不再走事件通道）。
        while let Option::Some(attach) = shared.reg_().take_write_attach_() {
            handle_write_attach_::<C, _>(
                attach,
                &mut *table,
                &mut tg_table,
                &mut tx_stage,
                &shared,
                &cancel,
            )
            .await;
        }

        // 3. 公共子流优先：先把 TX ring 里的 chunk 搬进传输，再发用户数据。
        match race_cancel_(
            &cancel,
            flush_public_chunks_::<C, _>(
                &mut tx_stage,
                &shared,
                &mut public_tx_r,
                &mut public_read,
                &mut scratch[..],
                &cancel,
            ),
        )
        .await
        {
            Option::None => return,
            Option::Some(Result::Ok(_)) => {}
            Option::Some(Result::Err(err)) => {
                fail_mux_loop_(&shared, &cancel, Option::None, &err).await;
                return;
            }
        }

        // 0.1 合并提示：应用侧封口 / 关接收只置位 + 唤醒，不再逐条投事件。
        //
        // 位是**唤醒**而不是事实：事实在各子流状态字里，这里按表扫一次即可补齐。
        let mut hints = shared.reg_().take_write_hint_();
        hints |= core::mem::take(&mut pending_hints);
        // main 已把 `CLOSE(FIN)` 放进公共 TX ring；只有把它刷空之后才能确认上线。
        // 这里不 park：若还有未刷出的 chunk / 半个前缀，下一轮或 `HINT_W_FIN_ENQUEUED`
        // 再次到达时再确认。
        if hints & HINT_W_FIN_ENQUEUED != 0u64 {
            if public_tx_r.ring_state().data_size() == 0usize {
                scan_fin_enqueued_::<C>(&shared, &mut *table);
            } else {
                // FIN 还在公共 TX 里：留到下一轮刷空之后再确认。
                pending_hints |= HINT_W_FIN_ENQUEUED;
            }
        }
        if hints & HINT_W_RESET_ENQUEUED != 0u64 {
            if public_tx_r.ring_state().data_size() == 0usize {
                scan_reset_enqueued_::<C>(&shared, &mut *table);
            } else {
                pending_hints |= HINT_W_RESET_ENQUEUED;
            }
        }
        if hints & HINT_W_PEER_CLOSED != 0u64 {
            scan_peer_closed_::<C>(&shared, &mut *table);
        }
        if hints & HINT_W_RELEASED != 0u64 {
            scan_write_released_::<C>(&mut *table);
        }
        // 应用丢了 telegraph 的发送半边：摘掉本地 tg 表项（位化替代 `TgTxClosed` 事件）。
        if hints & HINT_W_TG_TX_CLOSED != 0u64 {
            scan_tg_tx_closed_::<C>(&mut tg_table);
        }

        // 2. 尽量把各子流的数据发出去（一次一段），直到没有可发的。
        loop {
            match race_cancel_(
                &cancel,
                drain_once_::<C, _>(
                    &mut tx_stage,
                    &shared,
                    &mut *table,
                    &mut scratch[..],
                    &cancel,
                ),
            )
            .await
            {
                Option::None => return,
                Option::Some(Result::Ok(true)) => continue,
                Option::Some(Result::Ok(false)) => break,
                Option::Some(Result::Err(err)) => {
                    fail_mux_loop_(&shared, &cancel, Option::None, &err).await;
                    return;
                }
            }
        }

        // 2.7. telegraph：把各端点已提交的报文送出去。
        //
        //     必须在**主循环**里做、而不是只在 `TgReady` 事件处理里做：`TgReady` 只是
        //     「可能有新提交」的提醒（每次提交一条），而「写环刚腾出空间」这条进展不会
        //     再产生任何事件——只靠事件驱动的实现在写环满过一次之后就再也发不出去了。
        //     这里每轮把所有端点的队列排空（每条一次尝试，失败即留到下一轮），与第 2 步
        //     对 channel 的处理同构。
        for (local, entry) in tg_table.iter_mut() {
            match race_cancel_(
                &cancel,
                flush_tg_endpoint_::<C, _>(&mut tx_stage, entry, *local, &shared, &cancel),
            )
            .await
            {
                Option::None => return,
                Option::Some(Result::Ok(_)) => {}
                Option::Some(Result::Err(err)) => {
                    fail_mux_loop_(&shared, &cancel, Option::None, &err).await;
                    return;
                }
            }
        }

        // 3. 没有可发数据：park 在四类唤醒源上——取消令牌、写侧合并提示、公共 TX 的
        //    chunk、`Attach` inbox。
        //
        //    **必须同时轮询这些源**：只 park 在某一源上会让别处刚发生的进展排在睡着的
        //    循环后面（多子流下就是死锁）；尤其「公共 TX 腾出空间」这类进展**不会**
        //    再额外产生事件，不在这里轮询就会丢唤醒。
        //
        //    这里**不再 park 在子流发送环上**：R13 把 `TxReady` 位化后写循环本来就全表
        //    扫描，而「环可读」不等于「有进展可能」（环已关闭且空时读 future 也会立刻
        //    完成）；把它当唤醒条件只会带回空转风险。历史上的 `last_ready_has_segment_`
        //    判据因此随恒为 `None` 的 `last_ready` 一起删除。
        // `Attach` 通道的接收 future：跨 poll 持有。
        let mut attach_fut = core::pin::pin!(core::future::IntoFuture::into_future(
            shared.reg_().recv_write_attach_async_()
        ));
        let mut hint_ready = false;
        let mut public_ready = false;
        let mut taken_attach: Option<WriteAttach_<crate::connection::ConnAllocCfg_<C>>> =
            Option::None;
        let alive = {
            let cancel_fut = cancel.child_token().cancellation();
            let mut cancel_fut = core::pin::pin!(cancel_fut);
            poll_fn(|cx| {
                // 取消令牌优先：连接已收尾，直接退出。
                if core::future::Future::poll(cancel_fut.as_mut(), cx).is_ready() {
                    return Poll::Ready(false);
                }
                // 合并提示：应用侧封口 / 关接收只置位并唤醒，这里把掩码带回顶部处理。
                if let Poll::Ready(bits) = shared.reg_().poll_write_hint_(cx) {
                    pending_hints |= bits;
                    hint_ready = true;
                    return Poll::Ready(true);
                }
                // 公共 TX 有新的 chunk：回顶部优先搬运。
                if shared.public_().poll_tx_ready_(cx).is_ready() {
                    public_ready = true;
                    return Poll::Ready(true);
                }
                // `Attach` 通道：有新的环半部要交给本地写表。
                match core::future::Future::poll(attach_fut.as_mut(), cx) {
                    Poll::Ready(Result::Ok(attach)) => {
                        taken_attach = Option::Some(attach);
                        return Poll::Ready(true);
                    }
                    // 写侧通道关闭（连接收尾）。
                    Poll::Ready(Result::Err(_)) => return Poll::Ready(false),
                    Poll::Pending => {}
                }
                Poll::Pending
            })
            .await
        };
        if !alive {
            return;
        }
        if let Option::Some(attach) = taken_attach {
            handle_write_attach_::<C, _>(
                attach,
                &mut *table,
                &mut tg_table,
                &mut tx_stage,
                &shared,
                &cancel,
            )
            .await;
            continue;
        }
        if hint_ready {
            // 收尾事实变了（排空 / 发 FIN / RESET）：回顶部按位扫描。
            continue;
        }
        if public_ready {
            // 公共 TX 有新 chunk：回顶部优先搬运。
            continue;
        }
        continue;
    }
}
