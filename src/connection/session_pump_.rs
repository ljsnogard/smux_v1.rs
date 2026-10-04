//! 连接内部**贴传输**的两个泵循环。
//!
//! 本模块是经 `abs_art` 的本地作用域 spawn 出来的**外侧**两个 `'static` 任务的全部
//! 实现。内侧（贴子流）的两个循环见 [`session_`](super::session_)。
//!
//! # 职责
//!
//! ```text
//! 读泵：C::ConnRx --读--> 连接读环（写端）
//! 写泵：连接写环（读端）--写--> C::ConnTx
//! ```
//!
//! 这两个循环只做**字节搬运**：不解析帧、不认识 dock、不碰身份表。把「字节流」与
//! 「帧 / 子流」两类关注点分开之后：
//!
//! - 内侧循环永远不会 park 在传输上（它只 park 在连接级环上），因此事件通道
//!   不会因为对端不读 / 不写而被饿死；
//! - 「对端不读时写阻塞」这个必然会发生的状态，被限制在写泵这一个任务里；
//! - 网络系统调用次数与帧数解耦：一次读把环填上一截、一次写把环上借出的段交出去。
//!
//! # park 与退出
//!
//! 两个循环的每一处 park（传输读、传输写、环满、环空）都经
//! [`race_cancel_`](super::session_::race_cancel_) 与取消令牌竞争，因此与传输实现
//! 的 park 语义无关：连接被丢弃 ⇒
//! [`MuxCore::drop`](super::mux_connection::core_::MuxCore) 触发取消 ⇒ 两个泵在下一个
//! await 点退出并释放传输半边。
//!
//! 循环**不持有** [`MuxCore`](super::mux_connection::core_) 的强引用，理由见
//! [`session_`](super::session_) 模块文档（否则核心永远不析构，四个任务与整个连接
//! 状态永久泄漏）。
//!
//! # 半关闭的表达
//!
//! 传输读端读到 EOF / 出错时，读泵**不再往连接读环写任何字节**并退出：读环写端随
//! 任务结束而 drop，读环进入「生产端关闭」态，于是内侧解复用循环把环里剩余字节解析
//! 完之后读到 EOF，正常收尾。写泵相反：它读的连接写环被封口（内侧复用循环结束）且
//! 已排空、或传输写失败时退出。

use core::{
    alloc::AllocatorClone,
    borrow::BorrowMut,
    mem::MaybeUninit,
};

use abs_buff::{
    Demand, TrBuffRead, TrBuffWrite,
    buffer::{TrBuffSegmMut, TrBuffSegmRef, TrBuffSegmView},
    x_deps::abs_cancel,
};
use abs_cancel::TrCancellationToken;
use buffex::x_deps::abs_buff;

use crate::{
    connection::{
        MuxError, TrConnCfg,
        ring_::{BufferedRx, BufferedTx},
        session_::{ByteLoopShared_, Took_, race_cancel_, took_},
    },
};


/// 连接读环当前的**可写**空间，以及它是否已经封口（读端消失）。
fn stage_space_<B, A>(writer: &BufferedTx<B, A>) -> (usize, bool)
where
    B: BorrowMut<[MaybeUninit<u8>]> + 'static,
    A: AllocatorClone + Send + Sync,
{
    match writer.producer_state() {
        Option::Some((free, closed)) => (free, closed),
        // 只有切片之类的桩端才会返回 `None`；环端不会。
        Option::None => (0usize, true),
    }
}

/// 连接写环当前的**可读**字节数，以及它是否已经封口（写端消失）。
fn stage_readable_<B, A>(reader: &BufferedRx<B, A>) -> (usize, bool)
where
    B: BorrowMut<[MaybeUninit<u8>]> + 'static,
    A: AllocatorClone + Send + Sync,
{
    // 从**读端**看「有多少可读」用 `consumer_state`；第二项的含义是
    // 「生产端（内侧复用循环）是否已经关闭」。
    match reader.consumer_state() {
        Option::Some((size, producer_closed)) => (size, producer_closed),
        Option::None => (0usize, true),
    }
}

/// 泵循环的连接级失败处理：**取消导致的收尾不算失败**（与内侧两个循环同义）。
pub(crate) async fn fail_pump_<A, K>(shared: &ByteLoopShared_<A>, cancel: &K, err: &MuxError)
where
    A: AllocatorClone + Send + Sync,
    K: TrCancellationToken,
{
    if !cancel.is_cancelled() {
        let _ = shared.reg_.mark_failed_(err, cancel.child_token()).await;
    }
}

/// 读泵：`transport Rx → 连接读环`。
///
/// `shared` 只用于在连接级失败时打标记；本循环不认识任何子流。
pub(crate) async fn rx_pump_loop_async_<C, K>(
    mut rx: C::ConnRx,
    _shared: ByteLoopShared_<C::Alloc>,
    mut stage: BufferedTx<C::StageBuff, C::Alloc>,
    cancel: K,
) where
    C: TrConnCfg,
    K: TrCancellationToken,
{
    loop {
        if cancel.is_cancelled() {
            return;
        }

        let (space, closed) = stage_space_(&stage);
        if closed {
            // 内侧解复用循环已经结束（读端消失）：没有消费者，停止搬运。
            return;
        }
        if space == 0 {
            // 环满：park 到「腾出空间」或「取消」（段不消费即释放，只为等唤醒）。
            match race_cancel_(&cancel, stage.write_async(&Demand::at_least(1usize))).await {
                Option::None => return,
                Option::Some(outcome) => match took_(outcome) {
                    Took_::Segm(segm) => drop(segm),
                    Took_::Failed(_) | Took_::Nothing => return,
                },
            }
            continue;
        }

        // 借一段网络数据（`no_more_than(space)`：不会要超过环内空闲的量），
        // 再把它**段到段**搬进环。两个段都在本次迭代结束时 drop，各自提交自己的
        // 消费量，因此「读进来多少」与「消费掉多少」始终一致。
        let read_demand = Demand::no_more_than(space);
        let waiting = race_cancel_(&cancel, rx.read_async(&read_demand)).await;
        // 被取消：正常收尾，不记失败。
        let Option::Some(outcome) = waiting else {
            return;
        };
        let mut source = match took_(outcome) {
            Took_::Segm(segm) => segm,
            // 传输读失败 / 对端关闭：读环不再有新字节。本循环退出，读环写端
            // 随任务结束而 drop ⇒ 内侧解复用循环读到 EOF 后收尾。
            Took_::Failed(_) | Took_::Nothing => return,
        };
        let wanted = source.least_count();
        if wanted == 0 {
            continue;
        }
        let fill_demand = Demand::at_least(1usize);
        let filling = race_cancel_(&cancel, stage.write_async(&fill_demand)).await;
        let Option::Some(outcome) = filling else {
            return;
        };
        let mut dst = match took_(outcome) {
            Took_::Segm(segm) => segm,
            Took_::Failed(_) | Took_::Nothing => return,
        };
        let moved = {
            let mut dst_child = dst.as_segm_mut();
            let mut src_child = source.as_segm_ref();
            dst_child.move_items_from_segm(&mut src_child)
        };
        if moved == 0 {
            // 两段都不接受写入：极小概率的边界（环在两次判断之间被填满）。
            // 回到顶部重新轮询空间。
            continue;
        }
    }
}

/// 写泵：`连接写环 → transport Tx`。
pub(crate) async fn tx_pump_loop_async_<C, K>(
    mut tx: C::ConnTx,
    shared: ByteLoopShared_<C::Alloc>,
    mut stage: BufferedRx<C::StageBuff, C::Alloc>,
    cancel: K,
) where
    C: TrConnCfg,
    K: TrCancellationToken,
{
    loop {
        if cancel.is_cancelled() {
            return;
        }

        let (readable, closed) = stage_readable_(&stage);
        if readable == 0 {
            if closed {
                // 写环封口且已排空：外侧字节流到此为止（内侧复用循环已结束）。
                return;
            }
            // 写环空：park 到「有字节」或「取消」（段不消费即释放，只为等唤醒）。
            let demand = Demand::at_least(1usize);
            match race_cancel_(&cancel, stage.read_async(&demand)).await {
                Option::None => return,
                Option::Some(outcome) => match took_(outcome) {
                    Took_::Segm(segm) => drop(segm),
                    Took_::Failed(_) | Took_::Nothing => return,
                },
            }
            continue;
        }

        // 借一段环上字节（非阻塞：上面已确认环非空），把它搬进传输端的可写段。
        //
        // 两段各自持有自己的 reclaim：`dst`（传输端）在 drop 时按**实际搬入量**
        // 提交一次真正的写；`source`（环段）在 drop 时提交消费，读指针因此恰好
        // 前进「已经写上网」的字节数。这就是「写多少、消费多少」的来源。
        let moved = {
            let borrow_demand = Demand::at_least(1usize);
            let borrowed = stage.try_read(&borrow_demand);
            let Some(mut source) = borrowed.pick_left() else {
                continue;
            };
            let wanted = source.least_count();
            if wanted == 0 {
                continue;
            }
            let dst_demand = Demand::at_least(wanted);
            let mut dst = match race_cancel_(&cancel, tx.write_async(&dst_demand)).await {
                Option::None => return,
                Option::Some(outcome) => match took_(outcome) {
                    Took_::Segm(segm) => segm,
                    // 传输写失败：连接已不可用。标记后退出；内侧复用循环会在下一次
                    // 往写环写时拿到「读端消失」，从而收尾。
                    Took_::Failed(_) | Took_::Nothing => {
                        fail_pump_(&shared, &cancel, &MuxError::Transport { write: true }).await;
                        return;
                    }
                },
            };
            let mut dst_child = dst.as_segm_mut();
            let mut src_child = source.as_segm_ref();
            dst_child.move_items_from_segm(&mut src_child)
        };
        if moved == 0 {
            // 传输端拒收（关闭态）：连接已不可用。
            fail_pump_(&shared, &cancel, &MuxError::Transport { write: true }).await;
            return;
        }
    }
}
