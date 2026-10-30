//! 帧与字节的搬运原语：环状态、成帧入环、传输状态探测。

use core::mem::MaybeUninit;

use abs_buff::{
    Demand, TrBuffWrite,
    buffer::{TrBuffSegmMut, TrConsumerState, TrProducerState},
};
use abs_cancel::TrCancellationToken;
use anylr::SomeOf;

use crate::{
    connection::MuxError,
    wire_io_::CursorError,
    x_deps::{abs_buff, abs_cancel, anylr},
};

use super::util_::race_cancel_;

/// 传输**读半边**（`C::ConnRx`）此刻的状态。
///
/// 旧版这里问的是「传输读半边」——那是连接自己建的帧暂存环；本版没有再建环，问的
/// 就是**传输本身**。要求传输实现 [`TrConsumerState`] 正是为了让内侧循环在
/// **不 park** 的前提下回答「有没有字节、是不是已经到头了」。
pub(crate) enum RxState_ {
    /// 还有这么多字节可读（`0` 表示此刻没有，但链路还活着）。
    Readable(usize),

    /// 生产端已封口且已排空：字节流到此为止（干净 EOF）。
    Eof,

    /// 查询不可用（传输没给出状态）：交给 `read_async` 自己去回答。
    Unknown,
}

/// 问一次传输读半边：可读多少 / 是否已经到头。
pub(crate) fn rx_state_<R>(reader: &R) -> RxState_
where
    R: TrConsumerState,
{
    match reader.consumer_state() {
        // 生产端关闭 + 无剩余数据：字节流到此为止。注意「封口但还有数据」必须先读干净，
        // 因此它落到下面的 `Readable(count)` 分支。
        Option::Some((0usize, true)) => RxState_::Eof,
        Option::Some((count, _)) => RxState_::Readable(count),
        // 默认实现返回 `None`（传输没实现这个查询）。**不能**当成 EOF——那会让连接
        // 静默地「干净退出」；交给下面的 park 由 `read_async` 给出真正的结论。
        Option::None => RxState_::Unknown,
    }
}

/// 一次「借段 / 等唤醒」的结果分类。
///
/// `abs_buff` 的等待结果类型是 `SomeOf<段, 错误>`——它可以**同时**给出左与右
/// （`Both`），因此不能像 `Result` 那样做模式匹配。这里统一收敛成一个三态枚举，
/// 让每个 park 点的处理都是一行。
pub(crate) enum Took_<T, E> {
    /// 拿到段（`Both` 也按拿到段处理：错误信息可以从段自身的关闭态推断）。
    Segm(T),
    /// 只有错误。
    Failed(E),
    /// 既没有段也没有错误：关闭态 / 无数据，回到循环顶部重新判断。
    Nothing,
}

/// 把 `SomeOf<段, 错误>` 收敛成 [`Took_`]。
pub(crate) fn took_<T, E>(outcome: SomeOf<T, E>) -> Took_<T, E> {
    if outcome.contains_left() {
        Took_::Segm(outcome.pick_left().expect("已确认有左值"))
    } else if outcome.contains_right() {
        Took_::Failed(outcome.pick_right().expect("已确认有右值"))
    } else {
        Took_::Nothing
    }
}

/// 传输写半边上还剩多少可写空间；`None` 表示**查询不可用或对端已消失**。
pub(super) fn ring_space_<W>(writer: &W) -> Option<usize>
where
    W: TrProducerState,
{
    writer.producer_state().map(|(free, _closed)| free)
}

/// 把一帧（已编码）**分块**写进传输的写半边，直到全部写完。
///
/// # 为什么不能要求「整帧一次写」
///
/// 传输半边的容量由**调用方**决定（它背后是环，也可能是「设备 + 环」适配器），而
/// 「等它装得下整帧」在容量互相钳制时可能永远不成立（实测：环里剩 1987 字节、下一帧
/// 要 2010 字节，而排空方此刻正等别的方向腾空间）。因此这里按它当前能给的段**用多少
/// 写多少**：每片都是分段接口给出的连续区，写完由段的 drop 提交写指针。
///
/// 分段推进与「单生产者」这两点合起来保证了**不会写出残帧**：本循环是这条写半边
/// 唯一的写入者，因此一帧的片与片之间不会被别的帧插进来。
///
/// # Errors
///
/// 写半边不可用（对端消失 / 适配器报错）→ [`MuxError::Transport`]（方向为写）；
/// 被取消 → [`MuxError::Cancelled`]。
pub(crate) async fn enqueue_frame_<W, K>(
    tx_stage: &mut W,
    frame: &[u8],
    cancel: K,
) -> Result<(), MuxError>
where
    W: TrBuffWrite<u8>,
    K: TrCancellationToken,
{
    let mut offset = 0usize;
    // 片长上限取环容量量级即可：真正的片长由 `try_write` 按当前空闲空间给出。
    let demand = Demand::at_least(1usize);
    while offset < frame.len() {
        let rest = frame.len() - offset;
        // 把 `try_write` 的借用限制在自己的块里：借出的段在块结束时 drop（提交写
        // 指针、唤醒下游排空方），随后才能重新可变借用 `tx_stage` 去 park。
        let written = {
            let mut outcome = tx_stage.try_write(&demand);
            match outcome.as_mut().pick_left() {
                Option::Some(segment) => {
                    // `move_items_from_buff` 会**推进段的已消费量**，段的 drop 才会把
                    // 写入提交回环（推进写指针、唤醒下游排空方）。直接对 `iter_slices_mut`
                    // 的切片做逐元素赋值**不会**推进偏移，提交量会是 0——写环永远空着。
                    let mut child = segment.as_segm_mut();
                    let limit = core::cmp::min(rest, child.least_count());
                    if limit == 0 {
                        0usize
                    } else {
                        // SAFETY: `u8` 与 `MaybeUninit<u8>` 布局相同、对齐相同（均为 1）；
                        // 源切片来自本函数独占的 `frame`，目标段由 `try_write` 借出。
                        let src = unsafe {
                            core::slice::from_raw_parts(
                                frame[offset..offset + limit].as_ptr() as *const MaybeUninit<u8>,
                                limit,
                            )
                        };
                        // SAFETY: 源与目标都是 `MaybeUninit<u8>`，`u8` 无 drop 资源；
                        // `src` 指向本函数独占的 `frame`，`limit` 已按段可容量取小。
                        unsafe { child.move_items_from_buff(src) }
                    }
                }
                // 写环已满：下面 park 等空间，然后重试同一片。
                Option::None => 0usize,
            }
        };
        if written > 0 {
            offset += written;
            continue;
        }
        match race_cancel_(&cancel, tx_stage.write_async(&demand)).await {
            Option::None => return Result::Err(MuxError::Cancelled),
            Option::Some(outcome) => match took_(outcome) {
                // 段不消费即释放：只为唤醒。
                Took_::Segm(segm) => drop(segm),
                Took_::Failed(_) | Took_::Nothing => {
                    return Result::Err(MuxError::Transport { write: true });
                }
            },
        }
    }
    Result::Ok(())
}

/// 把**帧头 + 载荷**分两段写进传输写半边（数据面与控制面共用的入环入口）。
///
/// # 为什么可以分两段
///
/// 传输写半边是**单生产者**（只有复用循环写它），因此先写头、再写载荷不会被别的帧
/// 插进来；两段各自都是「按环当前能给的段分块推进、环满即 park」，与单段写入的语义
/// 完全一致。
///
/// 这样每帧不必先拼出一个连续 `Vec`：帧头来自栈上定长缓冲（[`frame_::encode_header_`]），
/// 载荷来自 `scratch`（注入分配器）或控制帧自己的载荷。**每帧少一次全局分配，数据面
/// 少一跳拷贝**（见 `dev-notes/audit-heap-alloc-20261004-1122.md` §3.1 #1 与 §5.1）。
///
/// # Errors
///
/// 与 [`enqueue_frame_`] 相同。
pub(super) async fn enqueue_frame_parts_<W, K>(
    tx_stage: &mut W,
    head: &[u8],
    payload: &[u8],
    cancel: K,
) -> Result<(), MuxError>
where
    W: TrBuffWrite<u8>,
    K: TrCancellationToken,
{
    enqueue_frame_::<_, _>(tx_stage, head, cancel.child_token()).await?;
    enqueue_frame_::<_, _>(tx_stage, payload, cancel.child_token()).await
}

/// 把**读侧**游标错误映射为连接错误。
pub(super) fn map_read_cursor_err_<E>(err: CursorError<E, ()>) -> MuxError {
    match err {
        CursorError::Read(_) => MuxError::Transport { write: false },
        CursorError::Write(()) => MuxError::Transport { write: true },
        CursorError::PeerClosed => MuxError::PeerClosed,
    }
}
