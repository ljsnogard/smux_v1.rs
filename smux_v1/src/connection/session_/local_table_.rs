//! 循环的本地表：表项类型、`Attach` 入口与各类扫描。

use core::ops::Bound;

use abs_buff::Demand;
use abs_cancel::TrCancellationToken;
use buffex::ring::ConsumerError;

use crate::{
    connection::{
        Dock, TrConnAllocCfg, TrConnCfg,
        owner_::{ChannelOwner_, TgOwner_},
        ring_::{BufferedRx, BufferedTx},
        signal_::{HINT_W_PEER_CLOSED, ReadAttach_, WriteAttach_},
        telegraph::TgRecvCtx_,
    },
    time::TrTime,
    x_deps::{abs_buff, abs_cancel},
};

use super::{mux_tg_::flush_tg_endpoint_, shared_::MuxShared_, util_::race_cancel_};

/// 解复用循环本地持有的一条子流：共享状态 + 会话侧**接收环写端**。
///
/// `local_dock` / `remote_dock` 不再是字段：dock 对已经是所在表的键。
pub(super) struct ReadEntry_<C>
where
    C: TrConnAllocCfg + 'static,
{
    pub(super) owner_: ChannelOwner_<C::ChanOwnerAlloc>,
    pub(super) writer_: BufferedTx,
}

/// 复用循环本地持有的一条子流：共享状态 + 会话侧**发送环读端**。
pub(super) struct WriteEntry_<C>
where
    C: TrConnAllocCfg + 'static,
{
    pub(super) owner_: ChannelOwner_<C::ChanOwnerAlloc>,
    pub(super) reader_: BufferedRx,

    /// 「对端 `FIN` 已经推进过拆流记账」：状态位是持久的，没有这一格，写循环每轮
    /// 扫描都会对同一条子流重复 raise 一次 `HINT_MAIN_RELEASE`（多一次无谓的注册表
    /// 扫描）。
    pub(super) peer_fin_handled_: bool,

    /// 本端 `CLOSE(FIN)` 已经发出：此后这条子流不再发送数据，但表项要**留到身份
    /// 释放**——对端随后的 `FIN` / `RESET` 仍需要经本表扫描推进释放。
    pub(super) fin_sent_: bool,
}

/// 解复用循环的本地表：dock 对 → 该子流的接收环写端与共享状态。
///
/// 用 `BTreeMap` 而不是手写单链表：链表的 `find` / `remove` 是 O(n)，且每次
/// 增删都要自己用分配器构造 / 释放节点；`BTreeMap` 直接以调用方注入的分配器
/// （`allocator_api` 的 `new_in`）承担这些分配，查找降到 O(log n)。
pub(super) type ReadTable_<C> = crate::connection::dock_index_::DockBPlusTree_<
    <C as TrConnAllocCfg>::LocalTableAlloc,
    (Dock, Dock),
    ReadEntry_<C>,
>;

/// 复用循环的本地表：dock 对 → 该子流的发送环读端与共享状态。
pub(super) type WriteTable_<C> = crate::connection::dock_index_::DockBPlusTree_<
    <C as TrConnAllocCfg>::LocalTableAlloc,
    (Dock, Dock),
    WriteEntry_<C>,
>;

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// telegraph 的两个本地表
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

/// telegraph 的读表项：解复用循环持有的**接收上下文**（接收环写端 + 身份句柄）。
///
/// 它把「帧头 + 载荷」整帧投进接收环，并由**接收侧**自行拆出远端地址与载荷——中心
/// 循环因此完全不理解 `DATAGRAM` 的字段语义（见 [`TgRecvCtx_`] 的模块文档）。
///
/// 复用循环本地持有的一条 telegraph：发送环的**读端** + 身份句柄。
///
/// 身份句柄是必需的：**已提交报文长度**记在身份节点内联的队列里（`TgLenQueue_`），
/// 循环按 FIFO 取长度才知道环里哪一段是一条报文（环本身只是字节流）。
pub(super) struct TgWriteEntry_<C>
where
    C: TrConnAllocCfg + 'static,
{
    /// 该端点的身份节点句柄（含发送长度队列）。
    pub(super) owner_: TgOwner_<C::ChanOwnerAlloc>,

    /// 会话侧发送环读端（应用写、本循环读）。
    pub(super) reader_: BufferedRx,
}

/// 解复用循环的 telegraph 表：`local_dock` → 接收上下文。
///
/// 键只有 `local_dock`：telegraph **独占**该 dock（协议不允许它与 channel / listener
/// 共用），对端地址在数据报里是**地址**而不是**身份**，因此没有第二个键维度。
///
/// 表里**有**条目就是「有接收方」：没有条目时到达的 `DATAGRAM` 整帧丢弃，连丢弃计数
/// 都不记（无人可归因）。
pub(super) type TgReadTable_<C> = crate::connection::dock_index_::DockBPlusTree_<
    <C as TrConnAllocCfg>::LocalTableAlloc,
    Dock,
    TgRecvCtx_<C>,
>;

/// 复用循环的 telegraph 表：`local_dock` → 发送环读端与身份句柄。
pub(super) type TgWriteTable_<C> = crate::connection::dock_index_::DockBPlusTree_<
    <C as TrConnAllocCfg>::LocalTableAlloc,
    Dock,
    TgWriteEntry_<C>,
>;

// 半关闭的 `FIN` 不再维护独立的 `PendingFin_` 集合：
// `drain_once_` 每轮扫描本地写表时，对 `APP_TX_CLOSED && !TX_DRAINED` 的条目在
// 发送环排空后置 `TX_DRAINED` 并叫醒 main；main 把 `CLOSE(FIN)` 写进公共 TX ring，
// 由 `scan_fin_enqueued_` 在刷空后确认上线。既不会饿死后面的条目，也省掉一整棵
// 本地索引树。

/// 处理写循环收到的一条 `Attach`：插入本地写表，并补查那些可能早于 `Attach` 到达的
/// 一次性事实（应用封口 / 关接收、对端 `CLOSE`）。
pub(super) async fn handle_write_attach_<C, K>(
    attach: WriteAttach_<crate::connection::ConnAllocCfg_<C>>,
    table: &mut WriteTable_<crate::connection::ConnAllocCfg_<C>>,
    tg_table: &mut TgWriteTable_<crate::connection::ConnAllocCfg_<C>>,
    tx_stage: &mut C::ConnTx,
    shared: &MuxShared_<C>,
    cancel: &K,
) where
    C: TrConnCfg,
    C::Rt: TrTime + Clone,
    K: TrCancellationToken,
{
    match attach {
        WriteAttach_::Channel {
            pair_,
            owner_,
            reader_,
        } => {
            let rx_closed = owner_.is_app_rx_closed_();
            let peer_closed = owner_.is_peer_fin_() || owner_.is_peer_reset_();
            table.insert(
                pair_,
                WriteEntry_ {
                    owner_,
                    reader_,
                    peer_fin_handled_: false,
                    fin_sent_: false,
                },
            );
            if rx_closed {
                shared
                    .control_()
                    .raise_main_hint_(crate::connection::HINT_MAIN_RESET);
            }
            if peer_closed {
                shared.reg_().raise_write_hint_(HINT_W_PEER_CLOSED);
            }
        }
        // telegraph 的 `Attach`：键只有 `local_dock`，进另一张本地表。
        WriteAttach_::Telegraph {
            local_dock_,
            owner_,
            reader_,
        } => {
            tg_table.insert(local_dock_, TgWriteEntry_ { owner_, reader_ });
        }
        // 「某条 telegraph 有已提交报文」：当场排空该端点（与事件通道时的语义一致）。
        WriteAttach_::TgReady { local_dock_ } => {
            if let Option::Some(entry) = tg_table.get_mut(local_dock_).map(|(_, v)| v) {
                let _ = race_cancel_(
                    cancel,
                    flush_tg_endpoint_::<C, _>(tx_stage, entry, local_dock_, shared, cancel),
                )
                .await;
            }
        }
    }
}

/// 处理读循环收到的一条 `Attach`：插入本地读表；身份已经释放时直接关掉写端。
pub(super) fn handle_read_attach_<C>(
    attach: ReadAttach_<crate::connection::ConnAllocCfg_<C>>,
    table: &mut ReadTable_<crate::connection::ConnAllocCfg_<C>>,
    tg_table: &mut TgReadTable_<crate::connection::ConnAllocCfg_<C>>,
) where
    C: TrConnCfg,
{
    match attach {
        ReadAttach_::Channel {
            pair_,
            owner_,
            writer_,
        } => {
            if owner_.is_released_() {
                let mut writer_ = writer_;
                writer_.close();
                return;
            }
            table.insert(pair_, ReadEntry_ { owner_, writer_ });
        }
        // telegraph 的 `Attach`：键只有 `local_dock`，进另一张本地表。
        ReadAttach_::Telegraph {
            local_dock_,
            owner_,
            writer_,
        } => {
            tg_table.insert(local_dock_, TgRecvCtx_::new_(local_dock_, owner_, writer_));
        }
    }
}

/// 推进「对端已发来 `CLOSE`」的拆流记账。
///
/// `PEER_FIN` / `PEER_RESET` 位由 `main_loop` 在处理帧面时置上；这里只负责本地表动作：
///
/// - `RESET`：对端不再接收，本端发送方向作废 —— 摘写表项、放弃「欠一条 `FIN`」的
///   登记，再通知 main 有可释放的完成子流；
/// - `FIN`：对端不再发送，发送方向仍可继续；只通知一次（由表项的 `peer_fin_handled_`
///   去重，状态位本身是持久的）。
///
/// **注册表身份不在这里释放**：释放判据（两个方向都收尾的一次性 CAS）由 `main_loop`
/// 在 [`HINT_MAIN_RELEASE`] 上落实（T1）。本函数只置位 / 摘本地表 / raise 提示。
pub(super) fn scan_peer_closed_<C>(
    shared: &MuxShared_<C>,
    table: &mut WriteTable_<crate::connection::ConnAllocCfg_<C>>,
) where
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
            return;
        };
        cursor = Option::Some(pair);
        let Option::Some((_, entry)) = table.get(pair) else {
            continue;
        };
        let owner = entry.owner_.clone();
        if owner.is_peer_reset_() {
            table.remove(pair);
            raise_release_hint_::<C>(shared);
            continue;
        }
        if !owner.is_peer_fin_() {
            continue;
        }
        let Option::Some((_, entry)) = table.get_mut(pair) else {
            continue;
        };
        if entry.peer_fin_handled_ {
            continue;
        }
        entry.peer_fin_handled_ = true;
        raise_release_hint_::<C>(shared);
        if table
            .get(pair)
            .map(|(_, entry)| entry.owner_.is_released_())
            .unwrap_or(false)
        {
            table.remove(pair);
        }
    }
}

/// 通知 main「可能有子流的两个方向都已在协议层收尾，可以释放身份」。
///
/// 提示是**合并位**：重复 raise 只多一次扫描，是否真的释放由
/// [`ChannelState_::claim_release_`] 的一次性 CAS 判定。
///
/// [`ChannelState_::claim_release_`]: crate::connection::owner_::ChannelState_::claim_release_
pub(super) fn raise_release_hint_<C>(shared: &MuxShared_<C>)
where
    C: TrConnCfg,
{
    shared
        .control_()
        .raise_main_hint_(crate::connection::HINT_MAIN_RELEASE);
}

/// 收尾写表里**身份已释放 / 已中止**的条目（`HINT_W_RELEASED`）。
///
/// 替换原先的「一次性 abort 事件」：计时循环拆流时只置 owner 的 `RELEASED` 位
/// 与合并提示，写循环在这里按表扫描做本地动作——**显式关闭发送环消费端**（应用
/// 立刻看到发送方向已关闭）、摘写表项。
///
/// 与对端 `RESET` 那条路径的区别（`scan_peer_closed_`）：后者**不**关消费端，因为
/// 协议只要求允许放弃已提交字节，而既有验收（`mux_recv_dropped_dual_`）钉住了
/// 「对端丢弃接收半边之前写入的字节仍然写得完」。本扫描只处理本端主动拆流
/// （或身份已被 `main_loop` 释放）。
///
/// 这里不 raise [`HINT_MAIN_RELEASE`]：`RELEASED` 位为真意味着释放**已经发生**
/// （`claim_release_` 成功或计时循环的强制拆流），没有待认领的身份。
pub(super) fn scan_write_released_<C>(table: &mut WriteTable_<crate::connection::ConnAllocCfg_<C>>)
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
            return;
        };
        cursor = Option::Some(pair);
        let Option::Some((_, entry)) = table.get(pair) else {
            continue;
        };
        if !entry.owner_.is_released_() {
            continue;
        }
        if let Option::Some((_, entry)) = table.get_mut(pair) {
            entry.reader_.close();
            entry.owner_.notify_both_progress_();
        }
        table.remove(pair);
    }
}

/// 确认 `main_loop` 已写进公共 TX ring 的 `CLOSE(FIN)` 真正上线。
///
/// 调用点保证公共 TX ring 已经刷空；此时 `FIN_ENQUEUED` 的条目可以置
/// `LOCAL_FIN_SENT`、关发送环消费端并唤醒 `close_async` 等待者。身份是否可释放交给
/// `main_loop`（[`HINT_MAIN_RELEASE`]）：`LOCAL_FIN_SENT` 只是发送方向的收尾事实，
/// 接收方向是否也已收尾记在 owner 的状态字里，由 `claim_release_` 的一次性 CAS 判定。
pub(super) fn scan_fin_enqueued_<C>(
    shared: &MuxShared_<C>,
    table: &mut WriteTable_<crate::connection::ConnAllocCfg_<C>>,
) where
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
            return;
        };
        cursor = Option::Some(pair);
        let Option::Some((_, entry)) = table.get(pair) else {
            continue;
        };
        if !entry.owner_.is_fin_enqueued_() || entry.owner_.is_local_fin_sent_() {
            continue;
        }
        let owner = entry.owner_.clone();
        owner.set_local_fin_sent_();
        #[cfg(feature = "test-loop-probe")]
        crate::connection::loop_probe_::local_fin_sent_();
        // FIN 已上线；此前因「等 FIN」被跳过的 RESET 现在可以发了。
        shared
            .control_()
            .raise_main_hint_(crate::connection::HINT_MAIN_RESET);
        if let Option::Some((_, entry)) = table.get_mut(pair) {
            entry.reader_.close();
            entry.fin_sent_ = true;
            entry.owner_.notify_tx_progress_();
        }
        raise_release_hint_::<C>(shared);
        if table
            .get(pair)
            .map(|(_, entry)| entry.owner_.is_released_())
            .unwrap_or(false)
        {
            table.remove(pair);
        }
    }
}

/// 确认 main 写进公共 TX ring 的 `CLOSE(RESET)` 已经上线。
///
/// 调用点保证公共 TX ring 已刷空；此时 `RESET_ENQUEUED` 的条目可以置
/// `LOCAL_RESET_FLUSHED`、唤醒 `ChannelRx::close_async` 等待者，并把「可能可释放」
/// 交给 `main_loop`。`RESET` 必须先于释放：注册表的释放扫描会跳过「应用已关读但
/// `RESET` 尚未上线」的子流（见 `ChannelRegistry_::release_done_channels_`）。
pub(super) fn scan_reset_enqueued_<C>(
    shared: &MuxShared_<C>,
    table: &mut WriteTable_<crate::connection::ConnAllocCfg_<C>>,
) where
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
            return;
        };
        cursor = Option::Some(pair);
        let Option::Some((_, entry)) = table.get(pair) else {
            continue;
        };
        if !entry.owner_.is_reset_enqueued_() || entry.owner_.is_local_reset_flushed_() {
            continue;
        }
        let owner = entry.owner_.clone();
        owner.set_local_reset_flushed_();
        owner.notify_rx_progress_();
        raise_release_hint_::<C>(shared);
        if table
            .get(pair)
            .map(|(_, entry)| entry.owner_.is_released_())
            .unwrap_or(false)
        {
            table.remove(pair);
        }
    }
}

/// 摘掉「身份已释放」的读表项并关闭接收环写端。
///
/// `buffex` 的环半部被 drop **不会**置位关闭标记：少了显式 `close()`，应用侧正 park
/// 的读永远拿不到 EOF。释放事实是 `RELEASED` 位（`claim_release_` 的一次性 CAS），
/// 提示位只负责把读循环叫醒。
pub(super) fn scan_released_<C>(table: &mut ReadTable_<crate::connection::ConnAllocCfg_<C>>)
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
            return;
        };
        cursor = Option::Some(pair);
        let released = table
            .get(pair)
            .map(|(_, entry)| entry.owner_.is_released_())
            .unwrap_or(false);
        if released && let Option::Some(mut entry) = table.remove(pair) {
            entry.writer_.close();
        }
    }
}

/// 非阻塞地问一句：这条子流的发送环里**还有数据没发出去**吗？
///
/// - `Some(true)`：还有（`drain_one_` 此刻取不出来，多半是额度不够或环被写者占住）；
/// - `Some(false)`：已排空，可以发 `FIN`；
/// - `None`：**不可判定**（环给不出段也给不出「空」的错误，例如写者正持有段）。
///
/// 与 [`drain_one_`] 同一条纪律：这里**只能用 `try_read`**——`read_async` 会在空环上
/// park，而一 park 就再也看不到事件通道里的事件（多子流下直接死锁）。
pub(super) fn has_writable_<C>(
    table: &mut WriteTable_<crate::connection::ConnAllocCfg_<C>>,
    pair: (Dock, Dock),
) -> Option<bool>
where
    C: TrConnCfg,
{
    let entry = table.get_mut(pair).map(|(_, entry)| entry)?;
    let demand = Demand::at_least(1usize);
    let mut outcome = entry.reader_.try_read(&demand);
    match outcome.as_mut().pick_left() {
        // 借到了段：里面确实还有数据。段在这里被丢弃（**不消费**）——`drain_one_`
        // 才是唯一有权搬出字节的地方。
        Option::Some(_segm) => Option::Some(true),
        Option::None => match outcome.pick_right() {
            // 环空（`Drained`）或已被写者关闭（`Closing`）：都不会再有新数据。
            Option::Some(err) => {
                if matches!(err, ConsumerError::Drained(_) | ConsumerError::Closing) {
                    Option::Some(false)
                } else {
                    // `Stuffed` / `Unsatisfiable` / `Cancelled` 都不足以断言「已排空」。
                    Option::None
                }
            }
            Option::None => Option::None,
        },
    }
}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// 小工具
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
