//! 传输侧的**缓冲供给**与段级读写工具（两条被测路径共用）。
//!
//! # 这一层负责什么
//!
//! 1. **子流环缓冲供给**：`smux_v1` 把「子流环交多大、从哪来」交给调用方，由
//!    [`RingPrepare`] 在 `accept_async` 当场提供（raw 路径不用它，它有整个 socket 可用）；
//! 2. **段级字节工具**：[`write_all`] / [`read_exact`] / [`read_frame`]。
//!    它们只依赖 `abs_buff` 的两条半边 trait，因此 raw 路径（对着适配器的 `Tx` / `Rx`）
//!    与 smux 路径（对着 `ChannelTx` / `ChannelRx`）用的是**同一份代码**。
//!
//! # 为什么两条路径都用适配器
//!
//! raw 路径本可以直接读写 socket 的 `AsyncRead` / `AsyncWrite`。本 bench 刻意不这么做：
//! 那样一来「raw 与 smux」的差别会同时包含「环形缓冲 vs 直连设备」与「真 socket vs
//! 复用子流」两项，测出来的差值无法归因。让两边都经 `buffex_*_adapt` 的环，
//! **唯一的差别就只剩传输形态**——这正是本 bench 想回答的问题。
//!
//! # 与服务端收流的关系
//!
//! 接收侧按 [`read_frame`] 收：先读 12 字节头部，再按声明的长度读满本体并现算 CRC-32。
//! 因此「服务端读完了某个客户端的全部字节」这件事是**可观测的**——本 bench 的墙钟
//! 终点正是它（见 [`crate::report::CaseResult::wall_secs`] 的文档）。

use core::mem::MaybeUninit;

use smux_v1::x_deps::abs_buff::{
    Demand, TrBuffRead, TrBuffWrite,
    buffer::{TrBuffSegmMut, TrBuffSegmRef, TrBuffSegmView},
    error::{ReadErrTag, TrTaggedError},
};
use smux_v1::x_deps::{
    abs_smux::chan::{RingBuffAlloc, TrPrepareRing},
    mm_ptr::{Owned, x_deps::abs_mm::CoreAlloc},
};

use crate::{
    payload::DigestAccum,
    proto::{K_HEADER_LEN, check_crc, check_len, decode_header},
};

/// 环存储类型（元素 `u8` + 平台无关分配器）。
pub type Buff = Owned<[MaybeUninit<u8>], CoreAlloc>;

/// `TrPrepareRing` 的最小实现：`accept_async` 当场按容量现造两块子流环内存。
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

/// 把 `bytes` 全量写进一条发送半边。
///
/// # Errors
///
/// 发送方向被关闭或出错时返回该错误；已经写出去的部分**不会回滚**。
pub async fn write_all<W>(tx: &mut W, bytes: &[u8]) -> Result<(), Box<dyn std::error::Error>>
where
    W: TrBuffWrite<u8>,
    W::Err: std::error::Error + 'static,
{
    let mut offset = 0usize;
    while offset < bytes.len() {
        let rest = bytes.len() - offset;
        // 下限只用 1：环容量再小也满足（与 `smux_v1` 的 `wire_io_` 同款说明）。
        let demand = Demand::at_least(1usize);
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
                format!("发送方向已关闭，仍有 {rest} 字节没写完（共 {}）", bytes.len()),
            )
            .into());
        }
        offset += put;
    }
    Ok(())
}

/// 从一条接收半边读满 `out`。
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
                format!("收齐之前结束：已收 {offset} / {} 字节", out.len()),
            )
            .into());
        }
        offset += got;
    }
    Ok(())
}

/// 收一条完整帧并**核对**头部声明的长度与校验和。
///
/// 这是本 bench 每一格的正确性判据：raw 与 smux 两条路径都必须在收满之后自证
/// 「收的东西与发的东西逐字节一致」。
///
/// # Errors
///
/// 读失败、长度不一致或校验和不一致时返回错误。
pub async fn read_frame_checked<R>(
    rx: &mut R,
    chunk: usize,
) -> Result<u64, Box<dyn std::error::Error>>
where
    R: TrBuffRead<u8>,
    R::Err: std::error::Error + 'static,
{
    let mut header = [0u8; K_HEADER_LEN];
    read_exact(rx, &mut header).await?;
    let (declared_len, declared_crc) = decode_header(&header);

    let mut acc = DigestAccum::new();
    let mut buf = vec![0u8; chunk.max(1usize)];
    let mut got = 0u64;
    while got < declared_len {
        let want = core::cmp::min(declared_len - got, buf.len() as u64) as usize;
        read_exact(rx, &mut buf[..want]).await?;
        acc.update(&buf[..want]);
        got += want as u64;
    }
    check_len(declared_len, got)?;
    check_crc(declared_crc, acc.finish())?;
    Ok(got)
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
