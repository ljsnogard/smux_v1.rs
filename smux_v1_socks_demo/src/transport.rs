//! 传输侧的**缓冲供给**与段级读写工具。
//!
//! # 这一层现在只负责什么
//!
//! `MuxConnection` 只吃 `abs_buff` 两条半边（`TrBuffRead` / `TrBuffWrite`），
//! 「socket ↔ 环」的搬运**不在本 crate**：三个运行时的 socket 各自由
//! `buffex_tokio_adapt` / `buffex_compio_adapt` / `buffex_smol_adapt` 的
//! `BuffRead` / `BuffWrite` 负责（它们本身就是「设备 + 环」的适配器）。
//!
//! 本模块因此只剩两件事：
//!
//! 1. **缓冲供给**——新版 `smux_v1` 把「交多大、从哪来」全部交给调用方：
//!    连接级帧暂存由 [`make_stage_buffs`] 提供，子流环内存由 [`RingPrepare`]
//!    在 `accept_async` 当场提供；
//! 2. **子流半边的段级工具**——[`write_all`] / [`read_exact`] / [`expect_eof`]。
//!
//! # 为什么不再有手写的传输泵
//!
//! 本 demo 曾经自己写两条「调用方驱动的泵」（socket 半边 → 环、环 → socket 半边）。
//! 那是把适配器 crate 该做的事抄了一遍：它掩盖了适配器**无法自行驱动写环**的缺口
//! （`BuffWrite` 的「环 → 设备」只在调用方调用它的 `write_async` 时才推进一步），
//! 于是手写泵一旦去掉，写环里的字节就再也没人搬。现在搬运完全由适配器负责，
//! 本 crate 一行设备 IO 都不写。

use core::mem::MaybeUninit;

use smux_v1::connection::{K_STAGE_RING_CAPACITY, MuxChanBuffOwnedBy};
use smux_v1::x_deps::abs_buff::{
    Demand, TrBuffRead, TrBuffWrite,
    buffer::{TrBuffSegmMut, TrBuffSegmRef, TrBuffSegmView},
    error::{ReadErrTag, TrTaggedError},
};
use smux_v1::x_deps::{
    abs_smux::chan::{RingBuffAlloc, TrPrepareRing},
    mm_ptr::{Owned, x_deps::abs_mm::CoreAlloc},
};

/// 环存储类型（元素 `u8` + 平台无关分配器）。
pub type Buff = Owned<[MaybeUninit<u8>], CoreAlloc>;

/// 交给 `MuxConnection::new` 的连接级帧暂存缓冲（视图 + 拥有者）。
pub type StageBuff = MuxChanBuffOwnedBy<Buff>;

/// 造一对连接级帧暂存缓冲，容量取 `smux_v1` 的既定常量。
pub fn make_stage_buffs() -> (StageBuff, StageBuff) {
    (
        MuxChanBuffOwnedBy::new(Owned::new_uninit_slice(K_STAGE_RING_CAPACITY, CoreAlloc)),
        MuxChanBuffOwnedBy::new(Owned::new_uninit_slice(K_STAGE_RING_CAPACITY, CoreAlloc)),
    )
}

/// `TrPrepareRing` 的最小实现：`accept_async` 当场按容量现造两块子流环内存。
///
/// 上游把「交多大、从哪来」全交给调用方（`smux_v1` 的配置里不再有缓冲类型），
/// 本类型就是本 demo 的回答：容量取 `--ring-cap`（同时是子流环与传输环的容量）。
pub struct RingPrepare {
    /// 每块环内存的容量（元素个数）。
    cap_: usize,
}

impl RingPrepare {
    /// 按容量构造。
    pub const fn new(capacity: usize) -> Self {
        RingPrepare { cap_: capacity }
    }
}

impl TrPrepareRing<Buff, u8> for RingPrepare {
    fn prepare(self) -> RingBuffAlloc<Buff, u8> {
        RingBuffAlloc::new(
            Owned::new_uninit_slice(self.cap_, CoreAlloc),
            Owned::new_uninit_slice(self.cap_, CoreAlloc),
        )
    }
}

/// 把 `bytes` 全量写进一条子流半边。
///
/// # Errors
///
/// 发送方向被关闭或出错时返回该错误；调用方拿到的 `offset` 语义由底层错误自带
/// （本 demo 只区分「整段写完」与「失败」）。
pub async fn write_all<W>(tx: &mut W, bytes: &[u8]) -> Result<(), Box<dyn std::error::Error>>
where
    W: TrBuffWrite<u8>,
    W::Err: std::error::Error + 'static,
{
    let mut offset = 0usize;
    while offset < bytes.len() {
        let rest = bytes.len() - offset;
        let demand = Demand::exactly(rest);
        let mut outcome = tx.write_async(&demand).await;
        let put;
        {
            match outcome.as_mut().pick_left() {
                Option::Some(segm) => {
                    put = segm.as_segm_mut().clone_items_from_buff(&bytes[offset..]);
                }
                Option::None => {
                    let err = outcome
                        .pick_right()
                        .expect("IO 结果必须要么是段、要么是错误");
                    return Err(Box::new(err));
                }
            }
        }
        if put == 0usize {
            return Err(std::io::Error::new(
                std::io::ErrorKind::WriteZero,
                "子流发送方向已关闭，仍有字节没写完",
            )
            .into());
        }
        offset += put;
    }
    Ok(())
}

/// 从一条子流半边读满 `out`。
///
/// 与 `write_all` 对称：段可能比请求更长，只搬走需要的前缀，剩余字节在段回收时归还。
/// 与 `smux_v1` 测试里那份宽松版本不同，这里**读不满就是错误**——本 demo 要拿「字节数
/// 对不对」当判据，不能把短读悄悄吞掉。
///
/// # Errors
///
/// 读侧报错（含对端提前 `FIN` 导致的 `Closing`）或段为空却仍要不到数据时返回错误。
pub async fn read_exact<R>(rx: &mut R, out: &mut [u8]) -> Result<(), Box<dyn std::error::Error>>
where
    R: TrBuffRead<u8>,
    R::Err: std::error::Error + 'static,
{
    let mut offset = 0usize;
    while offset < out.len() {
        let rest = out.len() - offset;
        let demand = Demand::exactly(rest);
        let mut outcome = rx.read_async(&demand).await;
        let got;
        {
            match outcome.as_mut().pick_left() {
                Option::Some(segm) => {
                    let mut child = segm.as_segm_ref();
                    let limit = core::cmp::min(rest, child.least_count());
                    let dst = &mut out[offset..offset + limit];
                    // SAFETY: `MaybeUninit<u8>` 与 `u8` 布局相同、对齐相同（都是 1），
                    // 且 `dst` 是本函数独占的可写切片；`move_items_to_buff` 只会写入
                    // 其中已初始化的前缀（返回值给出长度）。
                    let uninit = unsafe {
                        core::slice::from_raw_parts_mut(
                            dst.as_mut_ptr() as *mut MaybeUninit<u8>,
                            dst.len(),
                        )
                    };
                    got = unsafe { child.move_items_to_buff(uninit) };
                }
                Option::None => {
                    let err = outcome
                        .pick_right()
                        .expect("IO 结果必须要么是段、要么是错误");
                    return Err(Box::new(err));
                }
            }
        }
        if got == 0usize {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                format!("子流在收齐之前结束：已收 {offset} / {} 字节", out.len()),
            )
            .into());
        }
        offset += got;
    }
    Ok(())
}

/// 等在接收半边读到 EOF（对端 `FIN` 生效）。
///
/// # Errors
///
/// 读到了数据（对端在自己半关闭之后又发了东西），或底层给出的终止原因不是
/// `Closing` 时返回错误。
pub async fn expect_eof<R>(rx: &mut R) -> Result<(), Box<dyn std::error::Error>>
where
    R: TrBuffRead<u8>,
{
    let demand = Demand::exactly(1usize);
    let mut outcome = rx.read_async(&demand).await;
    match outcome.as_mut().pick_left() {
        Option::Some(segm) => {
            if segm.least_count() > 0usize {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "半关闭之后仍然读到了数据",
                )
                .into());
            }
            Ok(())
        }
        Option::None => {
            let err = outcome
                .pick_right()
                .expect("IO 结果必须要么是段、要么是错误");
            let tag: ReadErrTag = err.err_tag();
            if tag == ReadErrTag::Closing {
                Ok(())
            } else {
                Err(format!("半关闭之后应当读到 Closing，实际是 {tag:?}").into())
            }
        }
    }
}
