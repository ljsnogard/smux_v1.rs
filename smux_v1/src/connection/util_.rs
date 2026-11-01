//! 连接层的控制面小工具。

use core::mem::MaybeUninit;

use abs_buff::{
    Demand, TrBuffRead,
    buffer::TrBuffSegmRef,
};
use abs_cancel::{TrCancellationToken, TrMayCancel};

use crate::x_deps::{abs_buff, abs_cancel};

/// 把 `src` 当前可用的字节读进调用方给的切片，返回实际写入长度。
///
/// 作为「开场消息 / 拒绝理由 / welcome」的冷路径，本函数**不再自己分配**：调用方
/// 用连接级 Scope 的临时缓冲（容量 = `pending_msg_capacity + 1`）传入，读满即停；
/// 随后由调用方判断「是否超过上限」。读取过程中遇到的任何错误（`Drained` /
/// `Closing`）都按「消息到此为止」处理——这三处载荷在现行 `abs_smux` API 下没有明确
/// 的长度语义（见 dev-notes §2.11）。
pub(crate) async fn read_available_into_slice_<M, K>(
    src: &mut M,
    dst: &mut [u8],
    cancel: K,
) -> usize
where
    M: TrBuffRead<u8>,
    K: TrCancellationToken,
{
    let mut filled = 0usize;
    while filled < dst.len() {
        let demand = Demand::at_least(1usize);
        let mut outcome = src
            .read_async(&demand)
            .may_cancel_with(cancel.child_token())
            .await;
        let put = match outcome.as_mut().pick_left() {
            Option::Some(segm) => {
                let mut child = segm.as_segm_ref();
                let want = core::cmp::min(child.least_count(), dst.len() - filled);
                // SAFETY: `MaybeUninit<u8>` 与 `u8` 布局相同（同尺寸、同对齐、无
                // niche）；`dst[filled..filled + want]` 是本地独占的可写区间，
                // `move_items_to_buff` 只写入其中已初始化的前缀并返回写入长度，
                // 因此既不会读到未初始化内存，也不会越界。
                let target = unsafe {
                    core::slice::from_raw_parts_mut(
                        dst.as_mut_ptr().add(filled) as *mut MaybeUninit<u8>,
                        want,
                    )
                };
                let moved = unsafe { child.move_items_to_buff(target) };
                filled += moved;
                moved
            }
            Option::None => break,
        };
        if put == 0usize {
            break;
        }
    }
    filled
}
