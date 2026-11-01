//! 复用循环的数据面：成帧写入传输、单条子流/全表排空。

use core::{mem::MaybeUninit, ops::Bound};

use abs_buff::{Demand, TrBuffTryWrite, buffer::TrBuffSegmMut};
use abs_cancel::TrCancellationToken;

use crate::{
    connection::{
        Dock, FrameHeader, FrameKind, MuxError, TrConnCfg,
        frame_::{K_MAX_FRAME_HEADER, encode_header_},
    },
    flow_ctrl::Credit,
    metrics::{FrameDir, TrMetricsSink},
    time::TrTime,
    x_deps::{abs_buff, abs_cancel},
};

use super::{
    frame_io_::{enqueue_frame_parts_, ring_space_},
    local_table_::{TgWriteEntry_, WriteTable_, has_writable_},
    shared_::{K_MAX_DATA_CHUNK, MuxShared_},
};

/// 把**帧头 + 载荷**写进传输写半边，载荷直接从 telegraph 发送环里搬出。
///
/// # 为什么要求「整帧放得下」才动手
///
/// 帧一旦写出**帧头**就无法回退；而载荷可能分多段。若中途写环满，写环里就会留下一条
/// 「头已写、载荷不全」的残帧，对端解析必然错位。因此这里要求传输写半边当前至少有
/// `head_len + payload_len` 的可写空间才动手——**否则整帧推迟到下一轮**（调用方把长度
/// 记录留在队列里，不消费发送环）。
///
/// 这条要求在实践中很容易满足：telegraph 的单条载荷上限是 `max_packet_size` 量级
/// （默认 4 KiB），而传输写半边容量是 [`K_STAGE_RING_CAPACITY`]（64 KiB）。它是数据报
/// 「尽力交付、不背流控」的直接推论：宁愿推迟一条，也不在写环里留残帧。
///
/// # 零拷贝
///
/// 载荷从发送环的段直接搬进写环的段（`move_items_to_segm`），中间不经过任何暂存。
///
/// # Errors
///
/// 写环放不下整帧 / 源环被关闭 / 段借用失败时返回错误；调用方据此把该条留到下一轮。
pub(super) async fn write_frame_to_stage_<C>(
    tx_stage: &mut C::ConnTx,
    entry: &mut TgWriteEntry_<crate::connection::ConnAllocCfg_<C>>,
    head: &[u8],
    payload_len: usize,
) -> Result<(), MuxError>
where
    C: TrConnCfg,
{
    let total = head.len() + payload_len;
    if ring_space_(tx_stage).unwrap_or(0usize) < total {
        // 写环暂时放不下整帧：整帧推迟（长度记录留在队列里，发送环也不动）。
        return Result::Err(MuxError::Closed);
    }

    // **先把载荷段借到手，再往写环里写第一个字节**：帧头一旦写出就无法回退，因此一切
    // 可能失败的借段都必须发生在写之前。借出的段在函数末尾 drop，按**实际搬出量**提交
    // 消费；提前返回时消费量为 0，发送环不受影响。
    let read_demand = Demand::at_least(payload_len);
    let mut read_outcome = entry.reader_.try_read(&read_demand);
    let mut payload_segm = if payload_len == 0usize {
        Option::None
    } else {
        match read_outcome.as_mut().pick_left() {
            Option::Some(segm) => Option::Some(segm),
            Option::None => return Result::Err(MuxError::Closed),
        }
    };

    // 写环的**总**空间已判够，且本循环是它唯一的写入者：下面两个段级循环因此一定能推
    // 到底。跨**物理片**由循环承担——这正是「整帧原子性」的落点：`try_write` 每次只给
    // 当前那一片，写满就换下一片，绝不会把「头已写、载荷没跟上」的残帧留在环里。
    let mut head_left = head;
    while !head_left.is_empty() {
        let demand = Demand::at_least(1usize);
        let mut outcome = tx_stage.try_write(&demand);
        let writable = match outcome.as_mut().pick_left() {
            Option::Some(segm) => segm,
            Option::None => return Result::Err(MuxError::Closed),
        };
        let put = writable.as_segm_mut().clone_items_from_buff(head_left);
        if put == 0usize {
            return Result::Err(MuxError::Closed);
        }
        head_left = &head_left[put..];
    }

    let mut left = payload_len;
    while left > 0usize {
        let demand = Demand::at_least(1usize);
        let mut outcome = tx_stage.try_write(&demand);
        let writable = match outcome.as_mut().pick_left() {
            Option::Some(segm) => segm,
            Option::None => return Result::Err(MuxError::Closed),
        };
        let moved = {
            let mut dst = writable.as_segm_mut();
            let mut src = match payload_segm.as_mut() {
                Option::Some(segm) => segm.as_segm_ref(),
                Option::None => return Result::Err(MuxError::Closed),
            };
            // **按本条剩余长度精确裁剪源段**：`move_items_to_segm` 是整段搬运，不裁剪
            // 会把后续报文的载荷一起吃进这一帧（旧实现撞过的坑）。
            let ask = Demand::exactly(left);
            let Option::Some(mut piece) = src.take_segm_ref(&ask) else {
                return Result::Err(MuxError::Closed);
            };
            dst.move_items_from_segm(&mut piece)
        };
        if moved == 0usize {
            return Result::Err(MuxError::Closed);
        }
        left -= core::cmp::min(left, moved);
    }

    // 写段与读段在这里 drop：各自提交指针、唤醒对端。
    Result::Ok(())
}

// 注册表身份的释放（`claim_release_` 的一次性 CAS + `release_channel_`）已整体搬到
// `main_loop`：`connection::main_loop::flush_releasable_`。写循环只做本地表动作、
// 置位，并 raise `HINT_MAIN_RELEASE`。这样 `claim_release_` 与「最后一条活跃子流
// 收尾」都落在身份表唯一写者手里，复用循环不再取注册表写锁。

/// 轮转一遍本地表，最多写出一段数据；返回是否有进展。
#[allow(clippy::too_many_arguments)]
pub(super) async fn drain_once_<C, K>(
    tx_stage: &mut C::ConnTx,
    shared: &MuxShared_<C>,
    table: &mut WriteTable_<crate::connection::ConnAllocCfg_<C>>,
    scratch: &mut [u8],
    cancel: &K,
) -> Result<bool, MuxError>
where
    C: TrConnCfg,
    C::Rt: TrTime + Clone,
    K: TrCancellationToken,
{
    // 逐个 dock 对轮转。这里**不能**先把键收集成 `Vec`：那会在每次轮转时引入一次
    // 堆分配，而热路径上只该有环与网络的内存动作。改为每轮用「上一个键之后的第一个
    // 键」推进（`range` 的 O(log n)，整轮 O(n log n)），既不分配，也不会在
    // `await` 期间持有对表的借用。
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
            return Result::Ok(false);
        };
        cursor = Option::Some(pair);
        if drain_one_::<C, _>(tx_stage, shared, table, scratch, pair, cancel).await? {
            return Result::Ok(true);
        }
        // 发送方向已封口、还没进入 FIN 流程：确认环是否已经排空。
        // 只有「明确排空」才置 `TX_DRAINED` 并叫醒 main 组 `CLOSE(FIN)`；环里还有
        // 数据 / 不可判定时留给下一轮。main 写进公共 TX ring 后由
        // `scan_fin_enqueued_` 在刷空时确认上线。
        let eligible = table
            .get(pair)
            .map(|(_, entry)| {
                entry.owner_.is_app_tx_closed_()
                    && !entry.owner_.is_local_fin_sent_()
                    && !entry.owner_.is_fin_enqueued_()
                    && !entry.owner_.is_tx_drained_()
            })
            .unwrap_or(false);
        if eligible && matches!(has_writable_::<C>(table, pair), Option::Some(false)) {
            let first = table
                .get(pair)
                .map(|(_, entry)| entry.owner_.set_tx_drained_())
                .unwrap_or(false);
            if first {
                shared
                    .control_()
                    .raise_main_hint_(crate::connection::HINT_MAIN_TX_DRAINED);
            }
            return Result::Ok(false);
        }
    }
}

/// 尝试为 `pair` 写出一段数据；返回是否写出。
#[allow(clippy::too_many_arguments)]
pub(super) async fn drain_one_<C, K>(
    tx_stage: &mut C::ConnTx,
    shared: &MuxShared_<C>,
    table: &mut WriteTable_<crate::connection::ConnAllocCfg_<C>>,
    scratch: &mut [u8],
    pair: (Dock, Dock),
    cancel: &K,
) -> Result<bool, MuxError>
where
    C: TrConnCfg,
    C::Rt: TrTime + Clone,
    K: TrCancellationToken,
{
    let token = cancel.child_token();
    let Some((_, entry)) = table.get_mut(pair) else {
        return Result::Ok(false);
    };
    // 本端 `FIN` 已发出：这条子流不再发送数据，表项保留只为等对端 `CLOSE`
    // （见 `scan_peer_closed_`）。热点排空循环直接跳过它，避免每轮对已关闭的环
    // 做一次 `try_read`。
    if entry.fin_sent_ {
        return Result::Ok(false);
    }

    // 清「已入队」位：此后新的写入会重新入队（顺序不可反，见 dev-notes §11.4）。
    let owner = entry.owner_.clone();
    // 去重位在锁外：同步清位，不取锁。
    owner.clear_tx_queued_();

    let available = owner.send_available_();
    if available == 0 {
        return Result::Ok(false);
    }
    // **每轮最多搬「一个帧装得下的量」**：帧总长必须 ≤ 协商的 `max_packet_size`，而帧头
    // 最长 `K_MAX_FRAME_HEADER`。这一收敛是必须的，不是优化：应用完全可能一口气把发送环
    // 写满（一次积压远超一个帧），下面的帧总长检查只是**兜底**，不该成为「应用一次写多了」
    // 的出口——少了这一收敛，一次正常的大写入会被判 `FrameTooLarge`、整条连接失败，而
    // 连接级失败不会唤醒在册子流，应用侧看到的是**挂死**（0 CPU）。
    // 回归用例：`tests/inmem_mux.rs` 的 `mux_frame_cap_burst_dual_`；
    // 因果与真机现场见 `dev-notes/frame-cap-20261007-0540.md`。
    let payload_cap = shared.max_packet_size_.saturating_sub(K_MAX_FRAME_HEADER);
    if payload_cap == 0usize {
        // 协商出的帧总长连一个帧头都装不下：这是真正的非法协商，按协议错误处理。
        return Result::Err(MuxError::FrameTooLarge);
    }
    let want = core::cmp::min(available as usize, K_MAX_DATA_CHUNK.min(payload_cap));

    // 借一段数据。**必须用非阻塞的 `try_read`**：`read_async` 在空环上会 park，
    // 而这里一旦 park 就再也看不到事件通道里的事件（多子流下直接死锁）。等待新数据
    // 是循环顶部「park 在最近通知的环上、同时与事件通道竞争」那一步的职责。
    let demand = Demand::no_more_than(want);
    let mut outcome = entry.reader_.try_read(&demand);
    let segm = match outcome.as_mut().pick_left() {
        Option::Some(segm) => segm,
        Option::None => {
            return Result::Ok(false);
        }
    };
    let take = segm.least_count();
    if take == 0 {
        return Result::Ok(false);
    }
    if take > scratch.len() {
        // `no_more_than(want)` 保证段长不超过暂存（`want <= K_MAX_DATA_CHUNK`）；
        // 走到这里说明上游的 demand 语义变了。
        return Result::Err(MuxError::MalformedFrame);
    }

    let header = FrameHeader::new_(FrameKind::Data, 0u8, pair.0, pair.1, take, Option::None);

    // 帧总长必须先满足协商上限（头是自描述字段，取上界）。
    if take + K_MAX_FRAME_HEADER > shared.max_packet_size_ {
        return Result::Err(MuxError::FrameTooLarge);
    }
    // 传输写半边还活着吗？对端（读端）消失说明它已经不可用。
    if ring_space_(tx_stage).is_none() {
        return Result::Err(MuxError::Transport { write: true });
    }

    // 把段的字节**搬出**到循环暂存里：`move_items_to_buff` 会推进段的已消费量，
    // 段的 drop 才会把消费提交回环。**不能用 `iter_slices()` 只读不消费**——那样
    // 环的读指针不前进，同一段数据会被反复取出、反复上线。
    //
    // **必须搬整个逻辑读段，不能先 `as_segm_ref()` 再搬一个子段。**
    // `take`（= `least_count()`）是**逻辑**长度：读指针靠近环末端时，这一段在物理上
    // 是**两段**（环尾 + 环首）。`as_segm_ref()` 只给出「当前物理段」，于是只搬一次
    // 会少搬 `take − 首段长` 个字节，下面的一致性检查便会把一条**正常**的数据帧判成
    // `MalformedFrame`，进而 `fail_mux_loop_` 终止整条连接——发送方此后一帧也不再
    // 搬出（本仓库流控验收用例的「第二轮窗口归零后停摆」正是死在这里）。
    // `ReclSliceRef::move_items_to_buff` 会自己按物理段循环，直到搬够 `take`。
    let moved = {
        let dst = &mut scratch[..take];
        // SAFETY: `MaybeUninit<u8>` 与 `u8` 布局相同（同尺寸、同对齐、无 niche）；
        // `dst` 是本循环独占的可写区间，`move_items_to_buff` 只写入其中已初始化的
        // 前缀并返回写入长度，因此既不会读到未初始化内存，也不会越界。
        let uninit = unsafe {
            core::slice::from_raw_parts_mut(dst.as_mut_ptr() as *mut MaybeUninit<u8>, dst.len())
        };
        unsafe { segm.move_items_to_buff(uninit) }
    };
    if moved != take {
        // 段长度与搬出量应当一致；不一致说明上游语义变了。
        return Result::Err(MuxError::MalformedFrame);
    }

    // 帧头进**栈上定长缓冲**（零分配），并用它的**实际长度**做帧总长判定。
    let (head, head_len) = encode_header_(&header)?;
    if head_len + moved > shared.max_packet_size_ {
        return Result::Err(MuxError::FrameTooLarge);
    }

    // 预扣窗口（`take <= available`，因此必定足额）。
    let granted = owner.send_reserve_(take as Credit);
    if granted < take as Credit {
        owner.send_refund_(granted);
        return Result::Ok(false);
    }

    // **两段**写进传输写半边：先帧头（栈上缓冲）、再载荷（`scratch`）。写环是单生产者，
    // 因此两段之间不会被别的帧插进来。空间在上面已经判过（保守预判 + 实际头长），
    // 因此这里**不会再**出现「字节已离开子流环、帧却没写出去」的状态。
    if let Result::Err(err) = enqueue_frame_parts_::<_, _>(
        tx_stage,
        &head[..head_len],
        &scratch[..moved],
        token.child_token(),
    )
    .await
    {
        // 写环已结束：把预扣的窗口退回去，再按连接级失败处理。
        owner.send_refund_(take as Credit);
        return Result::Err(err);
    }
    owner.mark_data_(shared.conn_clock_().now_millis_());
    // 这一段已交给传输：发送环可能刚好被取空，等待中的 `flush_async` 由此醒来复检
    // 「环已排空」。每次取走都通知是有意的——排空是**渐进**发生的，没有别的时刻能
    // 精确覆盖「最后一段离开环」那一瞬。
    owner.notify_tx_progress_();
    // 上报「写出一个数据帧」：帧总长 = 帧头的**实际**长度 + 载荷长度。计数点必须在
    // `enqueue_frame_parts_` **之后**——它把头与载荷分两次入环，在那之前计数会把一个
    // 帧算成半个（见该函数的说明）。
    shared.metrics_().on_frame(
        FrameDir::Send,
        pair.0,
        pair.1,
        FrameKind::Data,
        u32::try_from(head_len + moved).unwrap_or(u32::MAX),
    );
    shared.metrics_().on_transport_bytes(
        FrameDir::Send,
        u64::try_from(head_len + moved).unwrap_or(u64::MAX),
    );
    Result::Ok(true)
}
