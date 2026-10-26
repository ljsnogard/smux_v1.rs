//! 公共报文子流的 **chunked 读写原语**：`[u16 大端长度][完整 smux 帧]`。
//!
//! 公共子流内部就是两条普通 ring（与用户子流同一种 `BufferedTx` / `BufferedRx`）；
//! 本模块只提供「按 chunk 写 / 按 chunk 读」的同步原语，不持有 buffer、不新增
//! buffer 类型，也不依赖 `abs_mptp`。
//!
//! # 协议
//!
//! ```text
//! public-ring = *chunk
//! chunk       = chunk-size(2 字节大端 u16) chunk-data
//! chunk-data  = 一个完整 smux 帧（帧头 + 载荷）
//! ```
//!
//! - 每个 chunk 承载**一个控制帧**；`chunk-size = 帧总长`，必须同时满足
//!   `<= K_MAX_CHUNK_PAYLOAD` 与 `<= max_packet_size`。
//! - 公共子流是持续的、**没有终止块**：`0x0000` 不产生；读到 0 按协议错误处理。
//!   结束由 ring 半部关闭表示。
//!
//! # 半 chunk
//!
//! 写侧动笔前先确认 ring 总空闲 `>= 2 + len`，再同步写长度与内容；总空闲已判够，
//! 因此正常路径不会在中途 `WouldBlock`。连接收尾时写者可能停在「前缀已写、内容未写」，
//! 此时两端都在退出、ring 随后被 drop，不需要恢复。
//!
//! # 为什么前缀要单独读
//!
//! ring 是字节流，`try_read` 借出的段可能只覆盖前缀的一部分（并发写入的中间态、
//! 或跨回绕）。[`ChunkReadState_`] 因此刻意保存「已读到的前缀字节数」，让调用方
//! 可以在任意一次 poll 里只推进一部分而**不丢字节**。

// 过渡：P0 阶段只落原语、不接线；接入 main_loop / 数据循环后删除本行。
#![allow(dead_code)]

use core::mem::MaybeUninit;

use abs_buff::{
    Demand, TrBuffRead, TrBuffWrite,
    buffer::{TrBuffSegmMut, TrBuffSegmRef, TrProducerState},
    error::{ReadErrTag, TrTaggedError, WriteErrTag},
};

use crate::x_deps::abs_buff;

/// 单个 chunk 的长度上限（长度前缀是 `u16`）。
pub(crate) const K_MAX_CHUNK_PAYLOAD: usize = u16::MAX as usize;

/// chunk 写入失败的原因。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ChunkWriteErr_ {
    /// 帧过长（超过 `max_packet_size` 或 `u16` 上限），永远写不进去。
    TooLarge,

    /// ring 当前空闲不足 `2 + len`：调用方保留请求 / 帧，等空间后重试。
    WouldBlock,

    /// ring 的消费端已关闭（连接正在收尾）。
    Closed,
}

/// chunk 读取失败的原因。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ChunkReadErr_ {
    /// 长度为 0：公共子流没有终止块，这是协议错误。
    ZeroLen,

    /// 声明长度超过上限：协议错误。
    TooLarge,

    /// ring 的生产端已关闭（连接正在收尾）。
    Closed,

    /// 其他底层读错误（映射为连接级失败）。
    Other,
}

/// 一次 chunk 读取的进度。
///
/// 前缀 `[u16 大端长度]` 可能跨多次 poll 才凑齐，`prefix_len_` 记录已读到的字节数。
/// 前缀解析完成后 `has_chunk_` 为真、`left_` 是尚未读出的内容字节数；内容读完后两者
/// 一起复位，下一次 [`PublicChunkIo_::try_read_prefix_`] 才会读下一个长度。
///
/// `allow_zero_` 区分两种协议：公共子流的 chunk **不允许** 0 长度（协议错误）；
/// 建流期 message ring 用 `[0x0000]` 表示空 message，因此允许。
pub(crate) struct ChunkReadState_ {
    /// 已读到的长度前缀字节。
    prefix_: [u8; 2usize],

    /// 已读到的前缀字节数。
    prefix_len_: usize,

    /// 当前 chunk 内容还剩多少字节未读。
    left_: usize,

    /// 前缀已解析、内容尚未读完。
    has_chunk_: bool,

    /// 是否允许 `0x0000`（建流期 message ring）。
    allow_zero_: bool,
}

impl ChunkReadState_ {
    /// 公共子流的空状态：0 长度按协议错误处理。
    pub(crate) fn new_() -> Self {
        ChunkReadState_ {
            prefix_: [0u8; 2usize],
            prefix_len_: 0usize,
            left_: 0usize,
            has_chunk_: false,
            allow_zero_: false,
        }
    }

    /// 建流期 message ring 的空状态：`0x0000` 表示空 message。
    pub(crate) fn new_message_() -> Self {
        ChunkReadState_ {
            allow_zero_: true,
            ..ChunkReadState_::new_()
        }
    }

    /// 是否处于「两个 chunk 之间」（前缀未开始、内容已读完）。
    pub(crate) fn is_idle_(&self) -> bool {
        self.prefix_len_ == 0usize && !self.has_chunk_
    }

    /// 当前 chunk 尚未读取的内容字节数（0 表示还在读前缀或空 message）。
    pub(crate) fn left_(&self) -> usize {
        self.left_
    }

    /// 是否已经解析出长度、但内容还没读完。
    pub(crate) fn has_chunk_(&self) -> bool {
        self.has_chunk_
    }

    /// 已读到但尚未凑齐的长度前缀字节数（诊断 / 测试用）。
    #[cfg(test)]
    pub(crate) fn prefix_len_(&self) -> usize {
        self.prefix_len_
    }
}

/// 公共报文子流的 chunked 读写入口（零大小的关联函数集合）。
pub(crate) struct PublicChunkIo_;

impl PublicChunkIo_ {
    /// 公共子流每条 ring 的推荐容量：`2 * (2 + max_packet_size)`。
    ///
    /// 下界 `2 + max_packet_size` 保证「一个最大控制帧的 chunk 能完整写进空 ring」；
    /// 取两倍是为了让「一个 chunk 正在被读、一个 chunk 正在被写」可以同时存在。
    pub(crate) fn ring_capacity_(max_packet_size: usize) -> usize {
        let one_chunk = 2usize.saturating_add(max_packet_size);
        one_chunk.saturating_mul(2usize)
    }

    /// 把「2 字节大端长度 + header + payload」写进 ring。
    ///
    /// `max_chunk_len` 是该连接允许的帧总长上限（通常是 `max_packet_size`）。返回
    /// [`ChunkWriteErr_::WouldBlock`] 表示 ring 当前空闲不足 `2 + len`；调用方保留
    /// 请求 / 帧，park 等空间后重试。**不会写半 chunk**（总空闲不足时一个字节都不写）。
    pub(crate) fn try_write_chunk_<W>(
        ring: &mut W,
        header: &[u8],
        payload: &[u8],
        max_chunk_len: usize,
    ) -> Result<(), ChunkWriteErr_>
    where
        W: TrBuffWrite<u8> + TrProducerState,
    {
        let len = header.len().saturating_add(payload.len());
        if len == 0usize || len > K_MAX_CHUNK_PAYLOAD || len > max_chunk_len {
            return Result::Err(ChunkWriteErr_::TooLarge);
        }
        // 1. 先看总空闲，不足就整体推迟——绝不写半 chunk。
        let free = ring
            .producer_state()
            .map(|(free, _closed)| free)
            .unwrap_or(0usize);
        if free < 2usize + len {
            return Result::Err(ChunkWriteErr_::WouldBlock);
        }
        // 2. 同步写：写端独占，总空闲已够，循环必然完成。
        let prefix = (len as u16).to_be_bytes();
        write_all_sync_(ring, &prefix)?;
        write_all_sync_(ring, header)?;
        write_all_sync_(ring, payload)?;
        Result::Ok(())
    }

    /// 把「2 字节大端长度 + body」写进 ring（建流期 message ring 用；允许空 body）。
    ///
    /// 与 [`Self::try_write_chunk_`] 共用同一套「先判总空闲、再同步写」的规则；
    /// 差别只有两处：允许 `body` 为空（写 `0x0000`），且没有单独的 header 段。
    pub(crate) fn try_write_message_<W>(
        ring: &mut W,
        body: &[u8],
        max_len: usize,
    ) -> Result<(), ChunkWriteErr_>
    where
        W: TrBuffWrite<u8> + TrProducerState,
    {
        let len = body.len();
        if len > K_MAX_CHUNK_PAYLOAD || len > max_len {
            return Result::Err(ChunkWriteErr_::TooLarge);
        }
        let free = ring
            .producer_state()
            .map(|(free, _closed)| free)
            .unwrap_or(0usize);
        if free < 2usize + len {
            return Result::Err(ChunkWriteErr_::WouldBlock);
        }
        write_all_sync_(ring, &(len as u16).to_be_bytes())?;
        write_all_sync_(ring, body)?;
        Result::Ok(())
    }

    /// 尝试读完长度前缀。
    ///
    /// - `Ok(Some(len))`：解析出一个完整 chunk（`len` 是内容长度；`allow_zero_` 下可为 0）；
    /// - `Ok(None)`：前缀还没凑齐，调用方保留状态、下一轮继续；
    /// - `Err(_)`：长度为 0（公共子流）/ 超长 / 环关闭等。
    pub(crate) fn try_read_prefix_<R>(
        ring: &mut R,
        state: &mut ChunkReadState_,
        max_len: usize,
    ) -> Result<Option<usize>, ChunkReadErr_>
    where
        R: TrBuffRead<u8>,
    {
        if state.has_chunk_ {
            // 内容还没读完：前缀已经解析过了。
            return Result::Ok(Option::Some(state.left_));
        }
        while state.prefix_len_ < 2usize {
            let demand = Demand::at_least(1usize);
            let mut outcome = ring.try_read(&demand);
            match outcome.as_mut().pick_left() {
                Option::Some(segment) => {
                    let mut child = segment.as_segm_ref();
                    let want = 2usize - state.prefix_len_;
                    let limit = core::cmp::min(want, child.least_count());
                    if limit == 0usize {
                        break;
                    }
                    let start = state.prefix_len_;
                    let dst = &mut state.prefix_[start..start + limit];
                    // SAFETY: `MaybeUninit<u8>` 与 `u8` 布局相同（同尺寸、同对齐、
                    // 无 niche）；`dst` 是 `state` 本地独占的可写切片，
                    // `move_items_to_buff` 只写入其中已初始化的前缀并返回写入长度。
                    let uninit = unsafe {
                        core::slice::from_raw_parts_mut(
                            dst.as_mut_ptr() as *mut MaybeUninit<u8>,
                            dst.len(),
                        )
                    };
                    let moved = unsafe { child.move_items_to_buff(uninit) };
                    if moved == 0usize {
                        break;
                    }
                    state.prefix_len_ += moved;
                }
                Option::None => {
                    return match outcome.pick_right() {
                        Option::Some(err) => match err.err_tag() {
                            ReadErrTag::Closing => Result::Err(ChunkReadErr_::Closed),
                            // 空 / 暂时不可读：前缀留在 state 里，下一轮继续。
                            ReadErrTag::Drained => Result::Ok(Option::None),
                            _ => Result::Err(ChunkReadErr_::Other),
                        },
                        Option::None => Result::Ok(Option::None),
                    };
                }
            }
        }
        if state.prefix_len_ < 2usize {
            return Result::Ok(Option::None);
        }
        let len = u16::from_be_bytes(state.prefix_) as usize;
        state.prefix_len_ = 0usize;
        state.prefix_ = [0u8; 2usize];
        if len == 0usize {
            if !state.allow_zero_ {
                return Result::Err(ChunkReadErr_::ZeroLen);
            }
            // 空 message：不进入「读内容」状态，调用方看到 `Some(0)` 即完成。
            return Result::Ok(Option::Some(0usize));
        }
        if len > K_MAX_CHUNK_PAYLOAD || len > max_len {
            return Result::Err(ChunkReadErr_::TooLarge);
        }
        state.left_ = len;
        state.has_chunk_ = true;
        Result::Ok(Option::Some(len))
    }

    /// 把当前 chunk 的内容读进 `dst`（最多 `state.left_()` 字节），返回本次读到的字节数。
    ///
    /// 环暂时为空时返回 `Ok(0)` 或部分字节，调用方下一轮继续。读满当前 chunk 后
    /// `state` 回到 idle，下一次 [`PublicChunkIo_::try_read_prefix_`] 才会读下一个长度。
    pub(crate) fn try_read_payload_into_<R>(
        ring: &mut R,
        state: &mut ChunkReadState_,
        dst: &mut [u8],
    ) -> Result<usize, ChunkReadErr_>
    where
        R: TrBuffRead<u8>,
    {
        if !state.has_chunk_ || state.left_ == 0usize || dst.is_empty() {
            return Result::Ok(0usize);
        }
        let want = core::cmp::min(dst.len(), state.left_);
        let mut offset = 0usize;
        while offset < want {
            let demand = Demand::at_least(1usize);
            let mut outcome = ring.try_read(&demand);
            match outcome.as_mut().pick_left() {
                Option::Some(segment) => {
                    let mut child = segment.as_segm_ref();
                    let limit = core::cmp::min(want - offset, child.least_count());
                    if limit == 0usize {
                        break;
                    }
                    let dst_part = &mut dst[offset..offset + limit];
                    // SAFETY: 同 `try_read_prefix_` 的论证；目标切片只在本函数内使用。
                    let uninit = unsafe {
                        core::slice::from_raw_parts_mut(
                            dst_part.as_mut_ptr() as *mut MaybeUninit<u8>,
                            dst_part.len(),
                        )
                    };
                    let moved = unsafe { child.move_items_to_buff(uninit) };
                    if moved == 0usize {
                        break;
                    }
                    offset += moved;
                }
                Option::None => {
                    return match outcome.pick_right() {
                        Option::Some(err) => match err.err_tag() {
                            ReadErrTag::Closing => Result::Err(ChunkReadErr_::Closed),
                            // 环暂时为空：交出已读到的部分。
                            ReadErrTag::Drained => {
                                state.left_ -= offset;
                                Result::Ok(offset)
                            }
                            _ => Result::Err(ChunkReadErr_::Other),
                        },
                        Option::None => {
                            state.left_ -= offset;
                            Result::Ok(offset)
                        }
                    };
                }
            }
        }
        state.left_ -= offset;
        if state.left_ == 0usize {
            state.has_chunk_ = false;
        }
        Result::Ok(offset)
    }
}

/// 同步写全部字节；调用方必须已经确认总空闲足够。
///
/// 单写者（公共 TX 只有 `main_loop`、公共 RX 只有 `demux_loop`），因此这里不需要
/// 与别的写者交错。
fn write_all_sync_<W>(ring: &mut W, bytes: &[u8]) -> Result<(), ChunkWriteErr_>
where
    W: TrBuffWrite<u8>,
{
    if bytes.is_empty() {
        return Result::Ok(());
    }
    let mut offset = 0usize;
    let demand = Demand::at_least(1usize);
    while offset < bytes.len() {
        let rest = bytes.len() - offset;
        let written = {
            let mut outcome = ring.try_write(&demand);
            match outcome.as_mut().pick_left() {
                Option::Some(segment) => {
                    let mut child = segment.as_segm_mut();
                    let limit = core::cmp::min(rest, child.least_count());
                    if limit == 0usize {
                        0usize
                    } else {
                        // SAFETY: `u8` 与 `MaybeUninit<u8>` 布局相同、对齐相同（均为 1）；
                        // 源切片来自本函数独占的 `bytes`，目标段由 `try_write` 借出，
                        // `limit` 已按段可容量取小。
                        let src = unsafe {
                            core::slice::from_raw_parts(
                                bytes[offset..offset + limit].as_ptr() as *const MaybeUninit<u8>,
                                limit,
                            )
                        };
                        // SAFETY: 源与目标都是 `u8` 的位拷贝，`src` 的生命周期覆盖本次调用。
                        unsafe { child.move_items_from_buff(src) }
                    }
                }
                Option::None => {
                    return match outcome.pick_right() {
                        Option::Some(err) if err.err_tag() == WriteErrTag::Stuffed => {
                            Result::Err(ChunkWriteErr_::WouldBlock)
                        }
                        _ => Result::Err(ChunkWriteErr_::Closed),
                    };
                }
            }
        };
        if written == 0usize {
            return Result::Err(ChunkWriteErr_::WouldBlock);
        }
        offset += written;
    }
    Result::Ok(())
}

#[cfg(test)]
mod tests_ {
    use mm_ptr::{Owned, x_deps::abs_mm::CoreAlloc};

    use super::*;
    use crate::connection::ring_::{BufferedRx, BufferedTx, new_buffered_channel};

    /// 建一对容量为 `cap` 的普通环（与公共子流用同一种 ring）。
    fn make_ring_(cap: usize) -> (BufferedTx, BufferedRx) {
        new_buffered_channel(Owned::new_uninit_slice(cap, CoreAlloc)).expect("测试环的容量应当合法")
    }

    /// 绕过 chunk 协议，直接往环里写原始字节（用于构造非法前缀的测试输入）。
    fn write_raw_bytes_(tx: &mut BufferedTx, bytes: &[u8]) {
        let demand = Demand::exactly(bytes.len());
        let outcome = tx.try_write(&demand);
        let mut segm = outcome.pick_left().expect("环应当可写");
        // SAFETY: `u8` 与 `MaybeUninit<u8>` 布局相同（同尺寸、同对齐、无 niche），
        // `bytes` 在本次调用期间由本函数独占。
        let src = unsafe {
            core::slice::from_raw_parts(bytes.as_ptr() as *const MaybeUninit<u8>, bytes.len())
        };
        // SAFETY: 段只在本函数内独占使用，写入量由段容量与源长度共同限定。
        unsafe { segm.as_segm_mut().move_items_from_buff(src) };
    }

    /// 测试一个 chunk 的长度前缀 + 内容能被原样读回。
    /// - 手段：容量 64 的环，写 `header=[0xAA]`、`payload=[1,2,3,4]`；读前缀，再读 5 字节。
    /// - 判断：前缀返回 `Some(5)` 且 `left_()==5`；读回内容等于 `[0xAA,1,2,3,4]`；
    ///   读完后 `is_idle_()` 为真。
    ///
    /// Test goal: a chunk round-trips through the length prefix and payload reader.
    /// Method: write `[0xAA] + [1,2,3,4]` into a 64-byte ring; read prefix then 5 bytes.
    /// Assertion: prefix yields `Some(5)` with `left_()==5`; bytes match; state is idle.
    #[test]
    fn chunk_round_trip_() {
        let (mut tx, mut rx) = make_ring_(64usize);
        let mut state = ChunkReadState_::new_();

        PublicChunkIo_::try_write_chunk_(&mut tx, &[0xAAu8], &[1u8, 2u8, 3u8, 4u8], 64usize)
            .expect("空环应当写得下");
        assert_eq!(
            PublicChunkIo_::try_read_prefix_(&mut rx, &mut state, 64usize).expect("读前缀"),
            Option::Some(5usize)
        );
        assert_eq!(state.left_(), 5usize);

        let mut out = [0u8; 5usize];
        let n =
            PublicChunkIo_::try_read_payload_into_(&mut rx, &mut state, &mut out).expect("读内容");
        assert_eq!(n, 5usize);
        assert_eq!(out, [0xAAu8, 1u8, 2u8, 3u8, 4u8]);
        assert!(state.is_idle_());
    }

    /// 测试前缀跨多次 poll 到达时不丢字节。
    /// - 手段：只往环里写 1 字节前缀，读一次（应 `Ok(None)`）；再写剩下的前缀，
    ///   读一次（应 `Some(3)`）。
    /// - 判断：第一次 `prefix_len_()==1`；第二次解析出正确长度。
    ///
    /// Test goal: a length prefix split across polls is not lost.
    /// Method: write one prefix byte, poll (expect `Ok(None)`); write the second
    /// byte, poll again.
    /// Assertion: first poll leaves `prefix_len_()==1`; second parses the length.
    #[test]
    fn chunk_prefix_can_arrive_in_parts_() {
        let (mut tx, mut rx) = make_ring_(32usize);
        let mut state = ChunkReadState_::new_();

        write_raw_bytes_(&mut tx, &[0x00u8]);
        assert!(
            PublicChunkIo_::try_read_prefix_(&mut rx, &mut state, 32usize)
                .expect("读前缀")
                .is_none()
        );
        assert_eq!(state.prefix_len_(), 1usize);

        write_raw_bytes_(&mut tx, &[0x03u8]);
        assert_eq!(
            PublicChunkIo_::try_read_prefix_(&mut rx, &mut state, 32usize).expect("读前缀"),
            Option::Some(3usize)
        );
        assert_eq!(state.left_(), 3usize);
    }

    /// 测试「空闲不足」时整体推迟、一个字节都不写。
    /// - 手段：容量 4 的环，尝试写 `len=4` 的 chunk（需要 `2+4=6` 字节）。
    /// - 判断：返回 `WouldBlock`；随后读前缀得到 `Ok(None)`（环仍空）。
    ///
    /// Test goal: a chunk that does not fit is deferred as a whole (no half chunk).
    /// Method: on a 4-byte ring try to write a chunk whose total length is 4
    /// (needs `2+4=6` bytes).
    /// Assertion: `WouldBlock`; a following prefix read returns `Ok(None)`.
    #[test]
    fn chunk_write_defers_when_no_space_() {
        let (mut tx, mut rx) = make_ring_(4usize);
        let mut state = ChunkReadState_::new_();

        let err = PublicChunkIo_::try_write_chunk_(&mut tx, &[1u8, 2u8], &[3u8, 4u8], 64usize)
            .expect_err("总空闲不足 2+4");
        assert_eq!(err, ChunkWriteErr_::WouldBlock);
        assert!(
            PublicChunkIo_::try_read_prefix_(&mut rx, &mut state, 64usize)
                .expect("读前缀")
                .is_none()
        );
        assert!(state.is_idle_());
    }

    /// 测试超长帧在写侧被拒绝。
    /// - 手段：`max_chunk_len=8`，写 9 字节的 chunk。
    /// - 判断：返回 `TooLarge`，且环里没有写入任何字节。
    ///
    /// Test goal: an oversized chunk is rejected before writing anything.
    /// Method: write a 9-byte chunk with `max_chunk_len=8`.
    /// Assertion: `TooLarge`; the ring stays empty.
    #[test]
    fn chunk_write_rejects_too_large_() {
        let (mut tx, mut rx) = make_ring_(64usize);
        let mut state = ChunkReadState_::new_();

        let err = PublicChunkIo_::try_write_chunk_(&mut tx, &[1u8], &[2u8; 8usize], 8usize)
            .expect_err("帧总长 9 超过上限 8");
        assert_eq!(err, ChunkWriteErr_::TooLarge);
        assert!(
            PublicChunkIo_::try_read_prefix_(&mut rx, &mut state, 64usize)
                .expect("读前缀")
                .is_none()
        );
        assert!(state.is_idle_());
    }

    /// 测试零长度前缀按协议错误处理。
    /// - 手段：手工往环里写 `0x0000`，再读前缀。
    /// - 判断：返回 `ZeroLen`。
    ///
    /// Test goal: a zero-length prefix is a protocol error.
    /// Method: hand-write `0x0000` into the ring and read the prefix.
    /// Assertion: `ZeroLen`.
    #[test]
    fn chunk_read_rejects_zero_len_() {
        let (mut tx, mut rx) = make_ring_(8usize);
        let mut state = ChunkReadState_::new_();

        write_raw_bytes_(&mut tx, &[0x00u8, 0x00u8]);
        assert_eq!(
            PublicChunkIo_::try_read_prefix_(&mut rx, &mut state, 64usize).expect_err("0 长度非法"),
            ChunkReadErr_::ZeroLen
        );
    }

    /// 测试 ring 回绕后连续多个 chunk 仍按 FIFO 读回。
    /// - 手段：容量 12 的环，写两个 `len=3` 的 chunk（共 10 字节），读走第一个；
    ///   再写第三个 chunk，读回第二、第三个。
    /// - 判断：读回的载荷依次为 1 / 2 / 3 号标记。
    ///
    /// Test goal: multiple chunks survive ring wraparound in FIFO order.
    /// Method: on a 12-byte ring write two 3-byte chunks (10 bytes total), read the
    /// first; write a third; read the second and third.
    /// Assertion: payload markers come back in order 1, 2, 3.
    #[test]
    fn chunk_ring_wraps_and_keeps_fifo_() {
        let (mut tx, mut rx) = make_ring_(12usize);
        let mut state = ChunkReadState_::new_();

        let read_one_ = |rx: &mut BufferedRx, state: &mut ChunkReadState_| -> u8 {
            assert!(
                PublicChunkIo_::try_read_prefix_(rx, state, 64usize)
                    .expect("读前缀")
                    .is_some()
            );
            let mut out = [0u8; 3usize];
            let n = PublicChunkIo_::try_read_payload_into_(rx, state, &mut out).expect("读内容");
            assert_eq!(n, 3usize);
            out[0]
        };

        PublicChunkIo_::try_write_chunk_(&mut tx, &[1u8], &[1u8, 1u8], 64usize).expect("写 1");
        PublicChunkIo_::try_write_chunk_(&mut tx, &[2u8], &[2u8, 2u8], 64usize).expect("写 2");
        assert_eq!(read_one_(&mut rx, &mut state), 1u8);

        PublicChunkIo_::try_write_chunk_(&mut tx, &[3u8], &[3u8, 3u8], 64usize).expect("写 3");
        assert_eq!(read_one_(&mut rx, &mut state), 2u8);
        assert_eq!(read_one_(&mut rx, &mut state), 3u8);
        assert!(state.is_idle_());
    }

    /// 测试推荐容量下界：至少容得下一个最大帧的 chunk。
    /// - 手段：对若干 `max_packet_size` 调用 `ring_capacity_`。
    /// - 判断：结果 `>= 2 + max_packet_size`，且默认 4 KiB 时为 8 KiB + 4。
    ///
    /// Test goal: the recommended ring capacity always fits one maximal chunk.
    /// Method: call `ring_capacity_` for several `max_packet_size` values.
    /// Assertion: result is `>= 2 + max_packet_size`; 4 KiB maps to 8 KiB + 4.
    #[test]
    fn public_ring_capacity_has_one_chunk_lower_bound_() {
        for max in [1usize, 64usize, 4096usize, 65535usize] {
            assert!(PublicChunkIo_::ring_capacity_(max) >= 2usize + max);
        }
        assert_eq!(PublicChunkIo_::ring_capacity_(4096usize), 8196usize);
    }
}
