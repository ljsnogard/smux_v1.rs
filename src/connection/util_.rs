//! 连接层的控制面小工具。

use core::mem::MaybeUninit;

use buffex::x_deps::abs_buff;
use abs_buff::{
    Demand, TrBuffRead,
    buffer::TrBuffSegmRef,
    x_deps::abs_cancel,
};
use abs_cancel::{TrCancellationToken, TrMayCancel};

/// 把 `src` 当前可用的字节读进 `Vec`（最多 `limit` 字节）。
///
/// 作为「开场消息 / 拒绝理由」的冷路径，这里允许一次堆分配；读取过程中遇到的任何
/// 错误（`Drained` / `Closing`）都按「消息到此为止」处理——这三处载荷在现行
/// `abs_smux` API 下没有明确的长度语义（见 dev-notes §2.11）。
pub(crate) async fn read_available_into_vec_<M, K>(src: &mut M, limit: usize, cancel: K) -> Vec<u8>
where
    M: TrBuffRead<u8>,
    K: TrCancellationToken,
{
    let mut out: Vec<u8> = Vec::new();
    while out.len() < limit {
        let demand = Demand::at_least(1usize);
        let mut outcome = src
            .read_async(&demand)
            .may_cancel_with(cancel.child_token())
            .await;
        let put = match outcome.as_mut().pick_left() {
            Option::Some(segm) => {
                let mut child = segm.as_segm_ref();
                let want = core::cmp::min(child.least_count(), limit - out.len());
                let base = out.len();
                out.resize(base + want, 0u8);
                // SAFETY: `MaybeUninit<u8>` 与 `u8` 布局相同（同尺寸、同对齐、无
                // niche）；`out[base..base + want]` 是本地独占的可写区间，
                // `move_items_to_buff` 只写入其中已初始化的前缀并返回写入长度，
                // 因此既不会读到未初始化内存，也不会越界。
                let dst = unsafe {
                    core::slice::from_raw_parts_mut(
                        out.as_mut_ptr().add(base) as *mut MaybeUninit<u8>,
                        want,
                    )
                };
                let moved = unsafe { child.move_items_to_buff(dst) };
                out.truncate(base + moved);
                moved
            }
            Option::None => break,
        };
        if put == 0usize {
            break;
        }
    }
    out
}
