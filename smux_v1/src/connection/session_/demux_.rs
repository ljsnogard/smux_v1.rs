//! 解复用循环：读帧、初审、写公共 RX、`Attach` 分发与唤醒；`TgReadGuard_` 也在这里。

use core::future::poll_fn;

use abs_buff::error::{TrTaggedError, WriteErrTag};
use abs_cancel::TrCancellationToken;

use crate::{
    connection::{
        FrameKind, MuxError, TrConnCfg, flags,
        frame_::{K_MAX_FRAME_HEADER, encode_header_},
        frame_parser_,
        public_channel_::{ChunkWriteErr_, PublicChunkIo_},
        ring_::BufferedTx,
        signal_::{
            HINT_R_RELEASE, HINT_R_RX_CONSUMED, HINT_W_PEER_CLOSED, HINT_W_TX_READY, ReadAttach_,
        },
    },
    flow_ctrl::Credit,
    metrics::{FrameDir, TrMetricsSink},
    time::TrTime,
    wire_io_::ReadCursor,
    x_deps::{abs_buff, abs_cancel},
};

use super::{
    demux_read_::{ReadWake_, park_read_wake_, rescan_read_, window_report_of_, write_into_ring_},
    frame_io_::{RxState_, map_read_cursor_err_, rx_state_},
    local_table_::{ReadTable_, TgReadTable_, handle_read_attach_, scan_released_},
    shared_::{MuxShared_, lock_or_exit_},
    util_::{fail_mux_loop_, race_cancel_, report_conn_failed_},
};

/// 把一条控制帧的「原始头 + 载荷」写进公共 RX ring（背压时 park 等空间）。
///
/// 写之前 `demux_loop` 已经做完帧级 / 路由级合法性初审；这里只负责搬运。返回
/// `WouldBlock` 时停在 RX 空间上，**不丢帧**（设计稿 §5.8）。
pub(super) async fn write_public_frame_async_<C, K>(
    public_rx_w: &mut BufferedTx,
    shared: &MuxShared_<C>,
    head: &[u8],
    payload: &[u8],
    cancel: &K,
) -> Result<(), MuxError>
where
    C: TrConnCfg,
    K: TrCancellationToken,
{
    loop {
        match PublicChunkIo_::try_write_chunk_(
            public_rx_w,
            head,
            payload,
            shared.max_packet_size_(),
        ) {
            Result::Ok(()) => {
                shared.public_().notify_rx_ready_();
                return Result::Ok(());
            }
            Result::Err(ChunkWriteErr_::WouldBlock) => {}
            Result::Err(_) => return Result::Err(MuxError::FrameTooLarge),
        }
        let wait_space = poll_fn(|cx| shared.public_().poll_rx_space_(cx));
        if race_cancel_(cancel, wait_space).await.is_none() {
            return Result::Err(MuxError::Cancelled);
        }
    }
}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// 解复用循环
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

/// 解复用循环**本地状态**的收尾守卫（规则与理由见 [`MuxLocalGuard_`]）。
///
/// 与复用侧对称、但**关的是另一半**：本循环持有每条子流**接收环的写端**，因此退出时
/// 要 `close()` 它——把「接收环为空而 park 的读者」唤醒（拿到 EOF 语义的 `Closing`）。
pub(super) struct DemuxLocalGuard_<'a, C>
where
    C: TrConnCfg,
{
    /// 本地读表：dock 对 → 该子流接收环的**写端**（本循环持有）。
    table: ReadTable_<crate::connection::ConnAllocCfg_<C>>,

    /// 连接共享面（上报关闭与读时钟用）。
    shared: &'a MuxShared_<C>,
}

impl<C> core::ops::Deref for DemuxLocalGuard_<'_, C>
where
    C: TrConnCfg,
{
    type Target = ReadTable_<crate::connection::ConnAllocCfg_<C>>;

    fn deref(&self) -> &Self::Target {
        &self.table
    }
}

impl<C> core::ops::DerefMut for DemuxLocalGuard_<'_, C>
where
    C: TrConnCfg,
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.table
    }
}

impl<C> Drop for DemuxLocalGuard_<'_, C>
where
    C: TrConnCfg,
{
    fn drop(&mut self) {
        let failed = self.shared.reg_.fail_kind_();
        let now_millis = self.shared.conn_clock_().now_millis_();
        for (pair, entry) in self.table.iter_mut() {
            // 显式关掉生产端：把「接收环为空而 park 的读者」唤醒（拿到 `Closing`）。
            entry.writer_.close();
            if failed.is_some() {
                report_conn_failed_::<C>(self.shared, &entry.owner_, pair.0, pair.1, now_millis);
            }
        }
        // `Attach` inbox 里还没取走的环半部：显式关闭，别让应用侧永久 park。
        while let Option::Some(attach) = self.shared.reg_.take_read_attach_() {
            match attach {
                ReadAttach_::Channel { mut writer_, .. }
                | ReadAttach_::Telegraph { mut writer_, .. } => writer_.close(),
            }
        }
        self.shared.reg_.close_read_inbox_();
    }
}

/// 解复用循环：从传输读半边解析帧、投递载荷、推进建流状态机。
pub(crate) async fn demux_loop_async_<C, K>(
    mut rx_stage: C::ConnRx,
    shared: MuxShared_<C>,
    mut public_rx_w: BufferedTx,
    cancel: K,
) where
    C: TrConnCfg,
    C::Rt: TrTime + Clone,
    K: TrCancellationToken,
{
    // 表与事件队列都包进收尾守卫（与复用侧对称；理由见 `MuxLocalGuard_`）。
    let mut table: DemuxLocalGuard_<'_, C> = DemuxLocalGuard_ {
        table: crate::connection::dock_index_::DockBPlusTree_::new_in(lock_or_exit_!(
            shared.reg_.local_table_alloc_(cancel.child_token())
        ))
        .expect("2 KiB B+ 树足以容纳一个 channel 表项"),
        shared: &shared,
    };
    // telegraph 的读表与 channel 分开持有：主循环在自查「哪些端点已被释放」时既要读
    // 它、又要（用同一个分配器）建一个临时集合，同处一个结构体会撞借用检查。
    let alloc = lock_or_exit_!(shared.reg_.local_table_alloc_(cancel.child_token()));
    // 包进守卫：循环退出时**显式关闭**每个接收环写端（`buffex` 的环半部被 drop 不会
    // 置位关闭标记，少了这一步应用侧正 park 的 `recv_async` 永远醒不过来）。
    let mut tg_table: TgReadGuard_<'_, C> = TgReadGuard_ {
        table: crate::connection::dock_index_::DockBPlusTree_::new_in(alloc.clone())
            .expect("2 KiB B+ 树足以容纳一个 telegraph 端点"),
        _use_shared_: core::marker::PhantomData,
    };
    // 自查用的临时集合：复用同一个（每轮先 `clear`）。用本地表分配器的 2 KiB B 树，
    // 不再落到全局 `BTreeSet`；它随循环退出一并归还。
    let mut tg_live = crate::connection::dock_index_::DockSet_::new_in(alloc.clone())
        .expect("2 KiB B 树足以容纳存活的 telegraph dock");
    // 「已消失」的 dock 列表：复用同一个 `Vec`，分配器同样取本地表分配器。
    let mut gone = alloc::vec::Vec::new_in(alloc);

    // 控制帧的暂存：只用于「帧头解析完成、载荷已经整块在环上」之后的取载荷。
    // 连接级环本身已经是暂存，因此这里只需要容纳**一帧**的最大载荷。
    let payload_retain = lock_or_exit_!(crate::connection::alloc_zeroed_bytes_async_(
        shared.reg_.scope_(),
        shared.max_packet_size_,
    ));
    let mut payload = payload_retain
        .try_owning()
        .expect("刚分配的载荷暂存应当可独占");

    loop {
        if cancel.is_cancelled() {
            return;
        }
        #[cfg(feature = "test-loop-probe")]
        crate::connection::loop_probe_::demux_tick_();
        // 0.05 合并提示：身份释放只置位 + 唤醒，读循环按 `RELEASED` 位摘表。
        let read_hints = shared.reg_().take_read_hint_();
        if read_hints & HINT_R_RELEASE != 0u64 {
            scan_released_::<C>(&mut *table);
        }
        // 应用刚消费过数据：按全表重算接收水位（`RxConsumed` 位化之后）。
        // `rescan_read_` 的「先清位、再读积压」顺序保证不丢。
        if read_hints & HINT_R_RX_CONSUMED != 0u64
            && let Result::Err(err) =
                rescan_read_::<C>(&shared, &mut *table, shared.conn_clock_().now_millis_()).await
        {
            fail_mux_loop_(&shared, &cancel, Option::None, &err).await;
            return;
        }
        // 0.06 `Attach` inbox：接收环写端从这里进入本地读表。
        //
        // 先前的 `RxConsumed` 事件可能比 `Attach` 更早被取出（两者不再共享同一条 FIFO）；
        // 那时表里还没有这一对，事件被丢弃、但 `RX_CONSUMED` 位仍留着。因此每插入一条
        // 新的读表项，就按位重算一次水位，把那条通知补回来。
        let mut attached_new = false;
        while let Option::Some(attach) = shared.reg_().take_read_attach_() {
            handle_read_attach_::<C>(attach, &mut *table, &mut *tg_table);
            attached_new = true;
        }
        if attached_new
            && let Result::Err(err) =
                rescan_read_::<C>(&shared, &mut *table, shared.conn_clock_().now_millis_()).await
        {
            fail_mux_loop_(&shared, &cancel, Option::None, &err).await;
            return;
        }

        // 0.1 telegraph 自查：本地表里哪些端点**已经不在身份表里**了（tx / rx 两个半边
        //     都被丢弃 ⇒ 身份已被上面那步释放）。这些端点的接收环写端必须关掉，否则
        //     应用侧正 park 的 `recv_async` 永远醒不过来。
        //
        //     自查本身**非阻塞**：拿不到注册表锁就跳过这一轮（下一轮再来），绝不能为它
        //     阻塞中心循环。回调里只做一件事——把在册的 dock 记进 `tg_live`。
        if shared.reg_.for_each_live_telegraph_(|local_dock| {
            tg_live.insert(local_dock, ());
        }) {
            gone.clear();
            for local_dock in tg_table.keys() {
                if !tg_live.contains_key(local_dock) {
                    gone.push(*local_dock);
                }
            }
            for local_dock in gone.iter().copied() {
                if let Option::Some(mut entry) = tg_table.remove(local_dock) {
                    entry.close_();
                }
            }
            gone.clear();
            tg_live.clear();
        }
        // 2. 问一次**传输读半边**有没有字节。本版没有再建连接级读环：字节直接从
        //    传输半边读进来（park 在这里，**同时与读事件通道、取消令牌竞争**）。
        let readable = match rx_state_(&rx_stage) {
            RxState_::Readable(count) => count,
            // 生产端已封口且已排空：字节流到此为止，本循环退出（对侧会随之收尾）。
            RxState_::Eof => return,
            // 查询不可用：当作「此刻没读到东西」，由下面的 park 让 `read_async` 给出
            // 结论（它要么给出段，要么给出 EOF / 错误）。
            RxState_::Unknown => 0,
        };
        if readable == 0 {
            // 此刻没字节：park 到「传输来了字节」/「有读事件」/「取消」三者之一。
            //
            // **必须同时等读事件通道**：`RxConsumed` 是「应用刚消费、去核对水位」的
            // 唯一触发点，而它到来时传输完全可能长期为空（对端正等额度、没有新帧）。
            // 只 park 在传输上会让这条通知一直排在一条睡着的循环后面——应用读空了环却
            // 没人补发额度，两端互等（本仓库流控验收用例的死锁形态）。
            match park_read_wake_::<C, _>(&cancel, &mut rx_stage, &shared).await {
                // 传输里有字节：回顶部交给正式的帧头解析。
                ReadWake_::Bytes => {}
                // 生产端封口且已排空：与上面的 `RxState_::Eof` 同义。
                ReadWake_::Eof => return,
                // 真读错误（非「封口」）：连接不可用。
                ReadWake_::Fatal => {
                    fail_mux_loop_(
                        &shared,
                        &cancel,
                        Option::None,
                        &MuxError::Transport { write: false },
                    )
                    .await;
                    return;
                }
                // `Attach` inbox 里来了一条新的接收环写端：插读表（telegraph 变体进
                // 另一张表）。
                ReadWake_::Attach(attach) => {
                    handle_read_attach_::<C>(attach, &mut table.table, &mut *tg_table);
                }
                ReadWake_::Hint(bits) => {
                    let mut rest = bits;
                    if rest & HINT_R_RELEASE != 0u64 {
                        rest &= !HINT_R_RELEASE;
                        scan_released_::<C>(&mut table.table);
                    }
                    if rest != 0u64 {
                        shared.reg_().raise_read_hint_(rest);
                    }
                }
                ReadWake_::Cancelled => return,
            }
            continue;
        }

        // 3. 读到帧头。解复用一律走 `frame_parser_` 的 **sans-IO 逐字节状态机**：
        //    它每次只向传输索要 `Demand::exactly(1)`，于是 `min_len == 1` 不可能超过任何
        //    合法环容量，旧入口「按字段索要 `width` 字节」在容量 < width 时拿到终态
        //    `Unsatisfiable` 而整条连接失败的路径**从构造上消失**。
        //    帧头可能消耗传输的读指针，因此必须与第 2 步合起来看：传输里此刻至少有一帧的
        //    **前若干字节**，帧头解析不会因为「此刻没字节」而永久 park——喂数据的一方
        //    （调用方的入向泵 / 对端连接）会继续填充。
        let (header, head_len) = match race_cancel_(
            &cancel,
            frame_parser_::read_header_async_::<_, _>(&mut rx_stage, cancel.child_token()),
        )
        .await
        {
            Option::None => return,
            Option::Some(Result::Ok(parsed)) => parsed,
            Option::Some(Result::Err(err)) => {
                fail_mux_loop_(&shared, &cancel, Option::None, &err).await;
                return;
            }
        };

        // 4. 载荷长度校验（与写侧同一条上限：帧总长 ≤ `max_packet_size`）。
        let len = header.payload_len();
        if len + K_MAX_FRAME_HEADER > shared.max_packet_size_ || len > payload.len() {
            fail_mux_loop_(
                &shared,
                &cancel,
                Option::Some(header.kind()),
                &MuxError::FrameTooLarge,
            )
            .await;
            return;
        }
        if len > 0 {
            let mut cursor = ReadCursor::new_(&mut rx_stage);
            match race_cancel_(
                &cancel,
                cursor.read_async_(&mut payload[..len], cancel.child_token()),
            )
            .await
            {
                Option::None => return,
                Option::Some(Result::Ok(())) => {}
                Option::Some(Result::Err(err)) => {
                    fail_mux_loop_(
                        &shared,
                        &cancel,
                        Option::Some(header.kind()),
                        &map_read_cursor_err_::<_>(err),
                    )
                    .await;
                    return;
                }
            }
        }
        let bytes = &payload[..len];

        // 帧种类在这里取一次：下面的上报与派发都用它。
        let kind = header.kind();

        // 5. 派发。帧头里的 `LocalDock` 是发送方的本端 dock，因此**本端的
        //    local_dock 是帧头的 `RemoteDock`**（镜像语义，见模块文档 §4.2）。
        let local = header.remote_dock();
        let remote = header.local_dock();
        let pair = (local, remote);

        // 帧已经完整到手（帧头解析 + 载荷读取 + 长度校验都过了），上报「收到一个帧」。
        //
        // **口径与写侧逐字一致**：`head_len` 是逐字节状态机自己数出的**帧头长度**
        // （[`frame_parser_::FrameHeaderParser::consumed_len_`]），加上载荷长度就是该帧的
        // **线上总长**。因此两侧的 `on_frame` 字节数可以直接相加比对——端到端用例正是
        // 用「发送帧字节之和 == 接收帧字节之和」把这条口径钉住的。
        shared.metrics_().on_frame(
            FrameDir::Recv,
            local,
            remote,
            kind,
            u32::try_from(head_len + len).unwrap_or(u32::MAX),
        );
        // 传输字节口径见 [`TrMetricsSink::on_transport_bytes`]：旧版由外侧读泵按
        // 「线上原始字节」上报（含零头与被丢弃的帧），本版没有泵，改由解复用循环按
        // 「已成型并搬运的帧字节」上报——与上面 `on_frame` 的口径一致。
        shared.metrics_().on_transport_bytes(
            FrameDir::Recv,
            u64::try_from(head_len + len).unwrap_or(u64::MAX),
        );

        // 每次活动就地读一次连接级时钟（vDSO 读，相对本帧的解析 / 环操作可忽略）：
        // 两个时钟必须记下**活动发生的时刻**，而不是计时循环扫描的时刻。
        let now_millis = shared.conn_clock_().now_millis_();
        match kind {
            FrameKind::Data => {
                let amount = Credit::try_from(len).unwrap_or(Credit::MAX);
                let Some((_, entry)) = table.get_mut(pair) else {
                    // 本地表里没有这条子流，分两种情况：
                    //
                    // - 该 dock 对刚被拆掉（**宽限态**）：这是拆流竞态里对端在收到
                    //   我们 `CLOSE` 之前发出的**在途帧**，静默丢弃即可——两个方向
                    //   是各自有序的独立字节流，「按序」并不能阻止它晚于本地拆流到达；
                    // - 真正的未知子流：协议违例，终止连接。
                    if lock_or_exit_!(shared.reg_.is_wait_close_(
                        local,
                        remote,
                        shared.conn_clock_().now_millis_(),
                        cancel.child_token()
                    )) {
                        continue;
                    }
                    fail_mux_loop_(
                        &shared,
                        &cancel,
                        Option::Some(kind),
                        &MuxError::MalformedFrame,
                    )
                    .await;
                    return;
                };
                if entry.owner_.is_app_rx_closed_() {
                    // 应用已丢弃接收半边：本端（已经或即将）向对端发 `RESET`，此刻到达的
                    // 数据都是「它在收到我们 `RESET` 之前发出的在途帧」。写进一条消费端
                    // 已关闭的环只会得到 `ProducerError::Closing`，并把整条连接判成传输
                    // 错误（旧实现的实测形态）；因此这里**静默丢弃**。
                    continue;
                }
                if entry.owner_.is_aborted_() {
                    // 计时循环已判定空闲超时并认领了中止：身份正在（或已经）转入宽限态。
                    // 这条判定不能省——从「认领中止」到「登记处落成宽限态」之间有一段
                    // 会被本循环插进来的窗口，此间到达的在途数据必须静默丢弃，而不是掉进
                    // 「未知子流」分支把整条连接判成协议违例。
                    continue;
                }
                entry.owner_.mark_data_(now_millis);
                let counted = entry.owner_.recv_on_data_(amount);
                if let Result::Err(err) = counted {
                    fail_mux_loop_(
                        &shared,
                        &cancel,
                        Option::Some(kind),
                        &MuxError::FlowCtrl(err),
                    )
                    .await;
                    return;
                }
                // **数据到达本身就是一次水位变化**（尤其是「刚好归零」），必须在这次
                // 派发里当场判定并通告。
                //
                // 不能只靠应用消费触发的核对（`HINT_R_RX_CONSUMED`）：那条路径要等
                // 应用真的读一次才走，而「只收不消费」的那一段正是对端最需要知道水位已经
                // 变小的时刻。更要紧的是时序——应用完全可能在本循环反应过来之前就把刚到的
                // 字节读走，那时窗口已经不是 0 了，归零判据再也不会成立，而**已通告快照
                // 仍是旧值**（上面的 `on_data` 不推进它），对端会一直按「还有一整个窗口」
                // 行动。判据全在 `RecvWindow::should_report_with_` 里（窗口归零落在
                // **临界区**，临界区每变必报）。
                //
                // R9 起本循环**只判定、不取快照、不组帧**：`should_report` 是消费式
                // 的，取走就必须保证帧写得出去，而写环归 `main_loop`。这里只 raise
                // 合并提示，main 在确认公共 TX 有空间后再取快照并组 `WINDOW_UPDATE`。
                if entry.owner_.window_report_due_() {
                    shared
                        .control_()
                        .raise_main_hint_(crate::connection::HINT_MAIN_WINDOW_DIRTY);
                }
                let wrote = race_cancel_(
                    &cancel,
                    write_into_ring_(&mut entry.writer_, bytes, cancel.child_token()),
                )
                .await;
                match wrote {
                    Option::None => return,
                    Option::Some(Result::Ok(())) => {}
                    // 消费端在检查之后、写进环之前被应用丢掉：这批字节已经没人要了，
                    // 静默丢弃即可（**不能**按传输错误终止整条连接）。
                    Option::Some(Result::Err(err)) if err.err_tag() == WriteErrTag::Closing => {}
                    Option::Some(Result::Err(_err)) => {
                        fail_mux_loop_(
                            &shared,
                            &cancel,
                            Option::Some(kind),
                            &MuxError::Transport { write: true },
                        )
                        .await;
                        return;
                    }
                }
            }
            FrameKind::Open => {
                if window_report_of_(&header).is_none() {
                    fail_mux_loop_(
                        &shared,
                        &cancel,
                        Option::Some(kind),
                        &MuxError::MalformedFrame,
                    )
                    .await;
                    return;
                }
                let (head, head_len) = match encode_header_(&header) {
                    Result::Ok(value) => value,
                    Result::Err(err) => {
                        fail_mux_loop_(&shared, &cancel, Option::Some(kind), &err).await;
                        return;
                    }
                };
                if let Result::Err(err) = write_public_frame_async_::<C, _>(
                    &mut public_rx_w,
                    &shared,
                    &head[..head_len],
                    bytes,
                    &cancel,
                )
                .await
                {
                    fail_mux_loop_(&shared, &cancel, Option::Some(kind), &err).await;
                    return;
                }
            }
            FrameKind::Accept | FrameKind::Reject => {
                if table.get(pair).is_some() {
                    let (head, head_len) = match encode_header_(&header) {
                        Result::Ok(value) => value,
                        Result::Err(err) => {
                            fail_mux_loop_(&shared, &cancel, Option::Some(kind), &err).await;
                            return;
                        }
                    };
                    if let Result::Err(err) = write_public_frame_async_::<C, _>(
                        &mut public_rx_w,
                        &shared,
                        &head[..head_len],
                        bytes,
                        &cancel,
                    )
                    .await
                    {
                        fail_mux_loop_(&shared, &cancel, Option::Some(kind), &err).await;
                        return;
                    }
                }
            }
            FrameKind::Close => {
                let reset = header.flags() & flags::K_RESET != 0;
                if let Option::Some((_, entry)) = table.get_mut(pair) {
                    if reset {
                        // 对端宣告**不再接收**：收尾的是**本端的发送方向**，与本端
                        // 接收方向无关。因此**不**关接收环写端——那会把对端仍在途、
                        // 且已经写进本端接收环的数据一起丢掉（旧实现正是如此，实测
                        // 少一个窗口，见 `dev-notes/flow-ctrl…` §3.3）。
                        entry.owner_.set_peer_reset_();
                        // 发送方向到此不必再等：等待中的 `flush_async` / `close_async`
                        // 应当醒来读到「对端不再接收」这个终局。
                        entry.owner_.notify_tx_progress_();
                        // 上报「对端发来 RESET」。
                        shared.metrics_().on_reset(local, remote, true);
                    } else {
                        // 对端宣告**不再发送**：关掉接收环写端，应用读完已缓存数据
                        // 之后读到 EOF。
                        entry.writer_.close();
                        entry.owner_.set_peer_fin_();
                        // 接收方向到此自然收尾：`close_async` 的等待者由此醒来。
                        entry.owner_.notify_rx_progress_();
                    }
                    entry.owner_.mark_data_(now_millis);
                    // 拆流记账（移除写侧表项、释放身份）交给复用循环的统一入口。
                    // `PEER_FIN` / `PEER_RESET` 位已经置上，这里只负责唤醒写循环。
                    shared.reg_().raise_write_hint_(HINT_W_PEER_CLOSED);
                }
                // 公共子流：把整帧交给 `main_loop` 做协议状态推进。P2 期间上面的
                // 本地动作仍在（两处都是幂等置位）；P4 删除本循环的直接处理。
                let (head, head_len) = match encode_header_(&header) {
                    Result::Ok(value) => value,
                    Result::Err(err) => {
                        fail_mux_loop_(&shared, &cancel, Option::Some(kind), &err).await;
                        return;
                    }
                };
                if let Result::Err(err) = write_public_frame_async_::<C, _>(
                    &mut public_rx_w,
                    &shared,
                    &head[..head_len],
                    bytes,
                    &cancel,
                )
                .await
                {
                    fail_mux_loop_(&shared, &cancel, Option::Some(kind), &err).await;
                    return;
                }
            }
            FrameKind::WindowUpdate | FrameKind::Pulse => {
                if let Option::Some((_, entry)) = table.get(pair)
                    && let Option::Some(report) = window_report_of_(&header)
                {
                    let owner = entry.owner_.clone();
                    let _ = owner.send_on_report_(report);
                    // `WINDOW_UPDATE` 是**数据路径**上的活动（对端应用消费了数据），
                    // 因此也刷新保活职责时钟；`PULSE` 只是保活本身，**只**刷新存活
                    // 时钟——这正是两端都不会被对方的保活「劝退」的原因。
                    if header.kind() == FrameKind::Pulse {
                        owner.touch_(now_millis);
                    } else {
                        owner.mark_data_(now_millis);
                    }
                    // **必须叫醒复用循环**：这条子流的额度刚刚（可能）变大，而写循环
                    // 完全可能正 park 在事件通道上——它上一次尝试发送时额度为 0，
                    // 于是「环里有数据但发不出去」，此后应用不再写入（环是满的），
                    // 也就不会再产生 `TxReady`。少了这次唤醒，`WINDOW_UPDATE` 会被
                    // 白收：发送方永远停在 0，接收方空着额度（本仓库流控验收用例撞到
                    // 的第三个死锁）。
                    //
                    // 这里**不能用「已入队」去重位**：它可能正被应用上一次写入置着
                    // （额度为 0 时写循环根本没机会清它），于是这次唤醒会被静默丢掉。
                    shared.reg_().raise_write_hint_(HINT_W_TX_READY);
                } else if let Option::Some(owner) = lock_or_exit_!(shared.reg_.channel_owner_(
                    local,
                    remote,
                    cancel.child_token()
                )) {
                    // 建流窗口：身份已在册（`reserve_inbound_`），而应用还没走到最终
                    // 裁决，读侧表项尚未安装。此刻到达的 `PULSE` / `WINDOW_UPDATE`
                    // 承载的正是对端的活动信号，不能因为「本地表还没这一项」就吞掉——
                    // 否则本端会在对端明明活着的时候判自己空闲超时。
                    //
                    // 这与「建流尚未完成的子流也要被超时控制」不冲突：超时判据仍是
                    // **本端自己的**（自身份登记起算，不进协议），这里只是把对端的
                    // 活动如实记账。
                    if header.kind() == FrameKind::Pulse {
                        owner.touch_(now_millis);
                    } else {
                        owner.mark_data_(now_millis);
                    }
                }
            }
            FrameKind::Datagram => {
                // 数据报（telegraph）：**地址**语义，按帧头的 `RemoteDock`（= 本端
                // local_dock）找到那条端点的接收上下文，把**整帧**（帧头 + 载荷）
                // 交给它——由它自行拆出远端地址与载荷，并自行处置「装不下就整条丢弃」。
                //
                // 两条要点：
                //
                // 1. **不查宽限态、不报协议违例**：数据报没有身份可言，发往一个本端
                //    没有 telegraph 的 dock 只是「没人收」（对端可能按 `wildcard` 发
                //    过来），**整个帧直接丢弃**即可——连丢弃计数都不记，因为无人可
                //    归因；
                // 2. 中心循环**不认识数据报的内部结构**：帧头不被拆解，帧头与载荷
                //    一起投出去（见 `telegraph::recv_ctx_` 的模块文档）。
                let Some(ctx) = tg_table.table.get_mut(local).map(|(_, value)| value) else {
                    continue;
                };
                ctx.deliver_frame_(&header, bytes, shared.metrics_());
            }
        }
    }
}

/// 解复用循环所持 telegraph 读表的**收尾守卫**。
///
/// 退出时**必须显式 `close()`** 每个接收环写端：`buffex` 的环半部被 drop 不会置位
/// 关闭标记，少了这一步，应用侧正 park 的 `recv_async` 永远醒不过来（与
/// [`DemuxLocalGuard_`] 对 channel 接收环的处理同一条理由）。
pub(super) struct TgReadGuard_<'a, C>
where
    C: TrConnCfg,
{
    /// telegraph 读表：`local_dock` → 接收环写端（本循环持有）。
    table: TgReadTable_<crate::connection::ConnAllocCfg_<C>>,

    /// 生命周期占位：守卫借在解复用循环的 `shared` 上（与 `table` 同寿命）。
    _use_shared_: core::marker::PhantomData<&'a MuxShared_<C>>,
}

impl<C> core::ops::Deref for TgReadGuard_<'_, C>
where
    C: TrConnCfg,
{
    type Target = TgReadTable_<crate::connection::ConnAllocCfg_<C>>;

    fn deref(&self) -> &Self::Target {
        &self.table
    }
}

impl<C> core::ops::DerefMut for TgReadGuard_<'_, C>
where
    C: TrConnCfg,
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.table
    }
}

impl<C> Drop for TgReadGuard_<'_, C>
where
    C: TrConnCfg,
{
    fn drop(&mut self) {
        for entry in self.table.values_mut() {
            entry.close_();
        }
    }
}
