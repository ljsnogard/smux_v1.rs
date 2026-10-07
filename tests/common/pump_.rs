//! 传输侧两条**调用方驱动**的泵，以及它们搬运的**全被动**环。
//!
//! 为什么不把 socket 设备直接接成 `TrBuffRead` / `TrBuffWrite`：三处实测阻塞点
//! （compio 半边 `!Send`、`TrBuffWrite` 没有 flush 钩子、buffex 主动输出泵只做一次
//! 非阻塞 poll），记录见 `dev-notes/` 的连接层文档。

use core::mem::MaybeUninit;
use buffex::{
    ring::Ring,
    x_deps::{
        abs_buff::{
            Demand,
            TrBuffRead,
            TrBuffWrite,
            buffer::{TrBuffSegmMut, TrBuffSegmRef},
            io::{TrInput, TrOutput},
        },
    },
};
use mm_ptr::{Shared, x_deps::abs_mm::CoreAlloc};

use crate::common::SmokeBuff;

/// 单条**全被动**环，返回 `(写端, 读端)`。
///
/// 读端交给 smux 当 `Rx`（由 [`pump_input_`] 写入），或写端交给 smux 当 `Tx`
/// （由 [`pump_output_`] 排空）。环不接任何设备，搬运完全由调用方的泵负责——原因
/// 见模块文档「为什么必须由调用方驱动泵」。
pub fn make_passive_ring_(
    capacity: usize,
) -> (
    smux_v1::connection::BufferedTx<SmokeBuff, CoreAlloc>,
    smux_v1::connection::BufferedRx<SmokeBuff, CoreAlloc>,
) {
    let buffer = SmokeBuff::try_new(CoreAlloc, capacity)
        .expect("测试的传输环内存应当分配成功");
    let ring = Ring::try_new(buffer).expect("环容量应当落在 buffex 允许的区间内");
    let shared = Shared::new(ring, CoreAlloc);
    // SAFETY: 这条环只被刚建出的 `Shared` 独占，且没有对应的 `Weak`（不存在升级
    // 路径），因此两个半部各持一个强引用是安全的；与 `Ring::split_unchecked` 文档
    // 要求的两条调用方保证一致。
    unsafe { Ring::split_unchecked(shared) }
}


/// 单次从 socket 搬进环的分块上限（字节）。
///
/// 设备读一返回（≥ 1 字节）就立刻提交进环，因此该值只影响单次搬运量，不影响
/// 首字节时延。
const K_PUMP_CHUNK: usize = 4096;


/// 入向泵：`TrInput`（socket 读设备）→ 全被动环。
///
/// 该环的消费端交给 smux 当 `Rx`。循环：从设备读一段（返回 0 即 EOF，退出并 drop
/// 环写端，让 smux 的 `Rx` 看到关闭）→ 把这段写进环 → drop 段提交（`advance_write`
/// 唤醒 smux 读侧）。
pub(super) async fn pump_input_<I, W>(mut input: I, mut ring_tx: W)
where
    I: TrInput<u8>,
    W: TrBuffWrite<u8>,
{
    let mut chunk: Vec<MaybeUninit<u8>> =
        (0..K_PUMP_CHUNK).map(|_| MaybeUninit::uninit()).collect();
    loop {
        // 1. 从 socket 读一段（至少 1 字节，或 EOF）。
        let read = input.read_async(&mut chunk).await;
        let n = match read.pick_left() {
            Some(n) => n,
            None => panic!("入向泵：socket 读设备报错"),
        };
        if n == 0usize {
            // EOF：退出并 drop `ring_tx`，让对端读到环关闭。
            return;
        }
        // SAFETY: 设备只把已初始化的字节写进 `chunk[..n]`；`MaybeUninit<u8>` 与
        // `u8` 布局相同、对齐相同（均为 1），按已初始化字节读取是健全的。
        let bytes: &[u8] =
            unsafe { core::slice::from_raw_parts(chunk.as_ptr() as *const u8, n) };

        // 2. 把 `bytes` 全部搬进环（环可能空间不足，故分段写入）。
        let mut off = 0usize;
        while off < bytes.len() {
            let demand = Demand::at_least(1);
            let mut outcome = ring_tx.write_async(&demand).await;
            let put;
            {
                let segm = outcome.as_mut().pick_left();
                match segm {
                    Some(segm) => {
                        put = segm.as_segm_mut().clone_items_from_buff(&bytes[off..]);
                    }
                    None => panic!("入向泵：环写端不可用"),
                }
            }
            if put == 0usize {
                panic!("入向泵：环写段为空");
            }
            off += put;
            // `outcome` 在此 drop：提交写入 → advance_write → 唤醒 smux 读侧。
        }
    }
}


/// 出向泵：全被动环 → `TrOutput`（socket 写设备）。
///
/// 该环的生产端交给 smux 当 `Tx`。循环：从环读一段（`least_count` 即当前可读量）
/// → 拷进本地缓冲 → **写完 socket 之后**才 drop 段提交消费（保证「先上网、后释放
/// 缓冲」，与 `TrBuffRead` 的消费语义一致）。
pub(super) async fn pump_output_<R, O>(mut ring_rx: R, mut output: O)
where
    R: TrBuffRead<u8>,
    O: TrOutput<u8>,
{
    loop {
        let demand = Demand::at_least(1);
        let mut outcome = ring_rx.read_async(&demand).await;
        {
            let segm = outcome.as_mut().pick_left();
            match segm {
                Some(segm) => {
                    let mut child = segm.as_segm_ref();
                    let n = child.least_count();
                    let mut dst: Vec<MaybeUninit<u8>> =
                        (0..n).map(|_| MaybeUninit::uninit()).collect();
                    // SAFETY: `dst` 是本函数独占的可写切片；`move_items_to_buff`
                    // 只会写入其中已初始化的前缀（返回值给出长度）。
                    let moved = unsafe { child.move_items_to_buff(&mut dst) };
                    if moved == 0usize {
                        panic!("出向泵：环读段为空");
                    }
                    // 把 `dst[..moved]` 全部写给设备（允许部分写，循环到写完）。
                    // 此时这些字节仍被环段占用，写失败不会丢数据。
                    let mut off = 0usize;
                    while off < moved {
                        let w = output.write_async(&dst[off..moved]).await;
                        match w.pick_left() {
                            Some(0usize) => panic!("出向泵：socket 写设备返回 0"),
                            Some(k) => off += k,
                            None => panic!("出向泵：socket 写设备报错"),
                        }
                    }
                    drop(child);
                }
                None => {
                    // 环被关闭（smux 侧 `Tx` 被 drop）或读错误：结束泵。
                    return;
                }
            }
        }
        // `outcome` 在此 drop：提交消费 → advance_read → 唤醒 smux 写侧。
    }
}
