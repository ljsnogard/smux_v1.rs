//! 握手帧的编解码：直接针对 [`TrBuffRead`] / [`TrBuffWrite`] 工作。
//!
//! 本模块把 [`crate::handshake`] 模块文档 §4、§6 的线格式实现为两个操作：
//!
//! - [`read_frame_`]：从字节流中读出一个完整握手帧；
//! - [`write_frame_`]：把一个握手帧写进字节流。
//!
//! 帧没有长度字段（见模块文档 §4），因此解析是**顺序推进**的：读到的每个
//! 字节先记进校验覆盖区，读到校验尾后再按它声明的算法一次性算 CRC。这里不
//! 存在「缓冲不足就返回 NeedMore」的中间状态，也不需要调用方提供长度已知的
//! 整帧缓冲区；写侧同样先在定长缓冲中成形再交给 [`TrBuffWrite`]。
//!
//! 单个条目的 `header` 与 `value` 宽度全部取自 [`NegotiationKey`] 与
//! [`NegotiationValType`]，本模块不重复描述任何「键 ↔ 字节数」的对应关系；
//! 校验尾只是 `key == Checksum` 的一个普通条目，其算法与校验码长度同样由
//! `val_type` 决定（见 [`HandshakeChecksum`]）。

use core::time::Duration;

use abs_buff::{
    Demand, TrBuffRead, TrBuffTryRead, TrBuffTryWrite, TrBuffWrite, buffer::{TrBuffSegmMut, TrBuffSegmRef}, x_deps::abs_cancel::{TrCancellationToken, TrMayCancel},
};

use crate::handshake::{
    K_ACCEPT_MAGIC, K_CONFRM_MAGIC, K_INVITE_MAGIC, K_REJECT_MAGIC, MagicField,
    opts::{BasicOpts, NegotiationBasicEntry, NegotiationKey, NegotiationValType},
};

/// 单个握手帧允许的最大字节数。
///
/// 最坏情况为 `4 + 4 * (1 + 8) + (1 + 4) = 45` 字节（全部基础项都用 `BeU64`
/// 编码、校验尾为 CRC-32），取 64 留出余量。
pub const K_MAX_HANDSHAKE_FRAME: usize = 64;

/// 基础项的键数量。
const K_BASIC_KEY_COUNT: usize = 4;

const HANDSHAKE_CRC16: crc::Crc<u16> = crc::Crc::<u16>::new(&crc::CRC_16_XMODEM);

const HANDSHAKE_CRC24: crc::Crc<u32> = crc::Crc::<u32>::new(&crc::CRC_24_BLE);

const HANDSHAKE_CRC32: crc::Crc<u32> = crc::Crc::<u32>::new(&crc::CRC_32_ISO_HDLC);

/// 握手帧校验尾使用的校验算法。
///
/// 校验尾是 `key == [`NegotiationKey::Checksum`]` 的条目；按模块文档 §6.2，
/// 其头字节的高半字节同时给出算法与校验码宽度，因此本类型与
/// [`NegotiationValType`] 一一对应：
///
/// | 枚举 | `val_type` | 头字节 | 算法 | `crc` 长度 |
/// | --- | --- | --- | --- | --- |
/// | [`HandshakeChecksum::Crc16`] | `BeU16` | `0x1C` | CRC-16/XMODEM | 2 |
/// | [`HandshakeChecksum::Crc24`] | `BeU24` | `0x2C` | CRC-24/BLE | 3 |
/// | [`HandshakeChecksum::Crc32`] | `BeU32` | `0x3C` | CRC-32/ISO-HDLC | 4 |
///
/// 校验范围见模块文档 §4：从 `magic` 首字节起，到校验码本身之前（含校验尾
/// 头字节）为止。
///
/// # Examples
///
/// ```
/// use smux_v1::handshake::HandshakeChecksum;
/// use smux_v1::handshake::opts::NegotiationValType;
///
/// // 帧中声明的 `val_type` 决定校验算法与校验码长度。
/// let algo = HandshakeChecksum::try_new(NegotiationValType::BeU24).unwrap();
/// assert_eq!(algo.checksum_len(), 3usize);
/// assert_eq!(u8::from(algo.header()), u8::from(NegotiationValType::BeU24));
///
/// // 只有 16 / 24 / 32 位校验码是合法校验尾。
/// assert!(HandshakeChecksum::try_new(NegotiationValType::BeU64).is_none());
/// ```
pub enum HandshakeChecksum {
    /// CRC-16/XMODEM，校验码 2 字节。
    Crc16(crc::Crc<u16>),

    /// CRC-24/BLE，校验码 3 字节。
    Crc24(crc::Crc<u32>),

    /// CRC-32/ISO-HDLC，校验码 4 字节。
    Crc32(crc::Crc<u32>),
}

impl Clone for HandshakeChecksum {
    fn clone(&self) -> Self {
        match self {
            HandshakeChecksum::Crc16(c) => HandshakeChecksum::Crc16(c.clone()),
            HandshakeChecksum::Crc24(c) => HandshakeChecksum::Crc24(c.clone()),
            HandshakeChecksum::Crc32(c) => HandshakeChecksum::Crc32(c.clone()),
        }
    }
}

impl core::fmt::Debug for HandshakeChecksum {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        let name = match self {
            HandshakeChecksum::Crc16(_) => "Crc16",
            HandshakeChecksum::Crc24(_) => "Crc24",
            HandshakeChecksum::Crc32(_) => "Crc32",
        };
        write!(
            f,
            "HandshakeChecksum::{}({} 字节)",
            name,
            self.checksum_len()
        )
    }
}

impl HandshakeChecksum {
    /// 把校验尾头字节声明的 [`NegotiationValType`] 映射为校验算法。
    ///
    /// `BeU8` 与 `BeU64` 在 v1 中不是合法的校验尾类型，返回 `None`。
    pub const fn try_new(val_type: NegotiationValType) -> Option<Self> {
        match val_type {
            NegotiationValType::BeU16 => Option::Some(HandshakeChecksum::Crc16(HANDSHAKE_CRC16)),
            NegotiationValType::BeU24 => Option::Some(HandshakeChecksum::Crc24(HANDSHAKE_CRC24)),
            NegotiationValType::BeU32 => Option::Some(HandshakeChecksum::Crc32(HANDSHAKE_CRC32)),
            _ => Option::None,
        }
    }

    /// 本算法写在校验尾头字节中的 `val_type`。
    pub const fn header(&self) -> NegotiationValType {
        match self {
            HandshakeChecksum::Crc16(_) => NegotiationValType::BeU16,
            HandshakeChecksum::Crc24(_) => NegotiationValType::BeU24,
            HandshakeChecksum::Crc32(_) => NegotiationValType::BeU32,
        }
    }

    /// 校验码的字节数。
    ///
    /// 直接委托 [`NegotiationValType::value_len`]，避免在长度上再维护一份
    /// 对应关系。
    pub const fn checksum_len(&self) -> usize {
        self.header().value_len()
    }

    /// 校验尾的完整头字节（`key == Checksum` 与 `val_type` 的组合）。
    const fn header_byte_(&self) -> u8 {
        compose_header_(NegotiationKey::Checksum, self.header())
    }

    /// 计算 `coverage` 的校验码。
    fn checksum_(&self, coverage: &[u8]) -> u32 {
        match self {
            HandshakeChecksum::Crc16(c) => c.checksum(coverage) as u32,
            HandshakeChecksum::Crc24(c) => c.checksum(coverage),
            HandshakeChecksum::Crc32(c) => c.checksum(coverage),
        }
    }
}

/// 校验覆盖区的字节，即 `magic` 与校验尾之前的全部条目。
///
/// v1 单帧不超过 [`K_MAX_HANDSHAKE_FRAME`]，故用定长数组承载，不需要堆分配；
/// 这也让「读到校验尾之后才知道算法」的两段式校验成为可能。
struct FrameBytes {
    bytes_: [u8; K_MAX_HANDSHAKE_FRAME],
    len_: usize,
}

impl FrameBytes {
    const fn new_() -> Self {
        FrameBytes {
            bytes_: [0u8; K_MAX_HANDSHAKE_FRAME],
            len_: 0usize,
        }
    }

    /// 记入 `bytes`；超出缓冲上限返回 `false`。
    fn push_(&mut self, bytes: &[u8]) -> bool {
        let end = self.len_ + bytes.len();
        if end > K_MAX_HANDSHAKE_FRAME {
            return false;
        }
        self.bytes_[self.len_..end].copy_from_slice(bytes);
        self.len_ = end;
        true
    }

    /// 已记入的校验覆盖区。
    fn coverage_(&self) -> &[u8] {
        &self.bytes_[..self.len_]
    }
}

/// 一个已解析的握手帧。
///
/// `entries[k]` 为 `Some` 表示键为 `k`（`0x00..=0x03`）的基础项在帧中出现过；
/// `None` 表示未出现。校验尾不在此结构中出现。
#[derive(Debug, Clone)]
pub struct ParsedFrame {
    /// 帧首 4 字节 magic。
    pub magic: MagicField,

    /// 按基础键下标索引的条目。
    pub entries: [Option<NegotiationBasicEntry>; K_BASIC_KEY_COUNT],
}

/// 握手帧编解码失败。
///
/// 读写两侧的底层错误都原样携带，由
/// [`HandshakeError`](super::error::HandshakeError) 统一呈现给调用方。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WireError<RE, WE> {
    /// 底层读错误。
    Read(RE),

    /// 底层写错误。
    Write(WE),

    /// 帧首 magic 不是 smux v1 握手的四种之一。
    InvalidMagic,

    /// 未知/保留键，或校验尾的 `val_type` 不是 16/24/32 位校验码。
    UnsupportedOption,

    /// 条目区结构非法：重复键、基础项取值为 0、数值超出 `usize`。
    MalformedBody,

    /// 累计帧长超过 `max_size`。
    FrameTooLarge,

    /// CRC 不匹配。
    ChecksumErr,

    /// 帧未读完对端就已经关闭。
    PeerClosed,
}

impl<RE, WE> core::fmt::Display for WireError<RE, WE> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        let text = match self {
            WireError::Read(_) => "握手帧读取失败",
            WireError::Write(_) => "握手帧写入失败",
            WireError::InvalidMagic => "握手帧 magic 不匹配",
            WireError::UnsupportedOption => "握手帧包含不支持的键或校验类型",
            WireError::MalformedBody => "握手帧条目区结构非法",
            WireError::FrameTooLarge => "握手帧超过长度上限",
            WireError::ChecksumErr => "握手帧校验失败",
            WireError::PeerClosed => "对端在握手帧中途关闭连接",
        };
        f.write_str(text)
    }
}

impl<RE, WE> core::error::Error for WireError<RE, WE>
where
    RE: core::error::Error,
    WE: core::error::Error,
{}

/// 读侧游标：累计已读帧长，并对 `max_size` 施加限制。
///
/// 底层缓冲可能一次只交付部分字节，因此这里以**块**为单位重试：每次
/// [`TrBuffTryRead::try_read`] 借出的段在被回收时会把已消费量记回缓冲，
/// 下一次重试继续往后读，直到满足目标长度或对端关闭。
struct FrameCursor<'f, R> {
    buff_: &'f mut R,
    max_: usize,
    consumed_: usize,
}

impl<'f, R> FrameCursor<'f, R>
where
    R: TrBuffRead<u8> + TrBuffTryRead<u8>,
{
    fn new_(buff: &'f mut R, max_size: usize) -> Self {
        FrameCursor {
            buff_: buff,
            max_: max_size,
            consumed_: 0usize,
        }
    }

    /// 分多次读满 `out`；若累计帧长会越过 `max_size` 则返回
    /// [`WireError::FrameTooLarge`]。
    ///
    /// [`TrBuffTryRead::try_read`] 借出的段可能比请求的更长，这里只取需要的
    /// 部分，多出的字节留在底层缓冲里。
    async fn read_async_<K>(
        &mut self,
        out: &mut [u8],
        cancel: &mut K,
    ) -> Result<(), WireError<R::Err, ()>>
    where
        K: TrCancellationToken + Clone,
    {
        if out.is_empty() {
            return Result::Ok(());
        }
        let mut offset = 0usize;
        while offset < out.len() {
            if cancel.is_cancelled() {
                return Result::Err(WireError::PeerClosed);
            }
            let rest = out.len() - offset;
            if self.consumed_.saturating_add(out.len()) > self.max_ {
                return Result::Err(WireError::FrameTooLarge);
            }
            let demand = Demand::exactly(rest);
            let got;
            {
                // 段只提供只读视图；消费量由 `move_items_to_buff` 提交，
                // 段回收时游标才会前进（见 abs_buff 的消费语义）。
                let mut read_res = self
                    .buff_
                    .read_async(&demand)
                    .may_cancel_with(cancel)
                    .await;
                let segm: Option<&mut R::SegmRef<'_>> = read_res.as_mut().pick_left();
                match segm {
                    Option::Some(segm) => {
                        let mut child = segm.as_segm_ref();
                        // 只搬运请求的长度：段可能比请求的更长。
                        let limit = core::cmp::min(rest, child.least_count());
                        let dst = &mut out[offset..offset + limit];
                        // SAFETY: `MaybeUninit<u8>` 与 `u8` 布局相同，且
                        // `dst` 是本地独占的可写切片；`move_items_to_buff`
                        // 只会写入其中已初始化的前缀（返回值给出长度）。
                        let uninit = unsafe {
                            core::slice::from_raw_parts_mut(
                                dst.as_mut_ptr() as *mut core::mem::MaybeUninit<u8>,
                                dst.len(),
                            )
                        };
                        got = unsafe { child.move_items_to_buff(uninit) };
                    }
                    Option::None => {
                        return Result::Err(match read_res.pick_right() {
                            Option::Some(err) => WireError::Read(err),
                            Option::None => WireError::PeerClosed,
                        });
                    }
                }
            }
            if got == 0 {
                return Result::Err(WireError::PeerClosed);
            }
            offset += got;
        }
        self.consumed_ += out.len();
        Result::Ok(())
    }
}

/// 顺势读取一个握手帧并校验其校验尾。
///
/// 帧使用哪种校验算法由**校验尾自己的头字节**声明，因此本函数不预设算法：
/// 读到校验尾后再按声明的算法计算 CRC（见 [`FrameBytes`]）。校验码本身不计入
/// 校验范围（模块文档 §4）。
///
/// # Errors
///
/// 任意字段读取失败、累计帧长超过 `max_size`、未知键、重复键、取值为 0、
/// magic 不匹配或 CRC 校验失败时返回 [`WireError`]。
pub(super) async fn read_frame_<R, K>(
    buff: &mut R,
    max_size: usize,
    cancel: &mut K,
) -> Result<ParsedFrame, WireError<R::Err, ()>>
where
    R: TrBuffRead<u8> + TrBuffTryRead<u8>,
    K: TrCancellationToken + Clone,
{
    let mut cursor = FrameCursor::new_(buff, max_size);
    let mut magic = [0u8; 4];
    if let Result::Err(err) = cursor.read_async_(&mut magic, cancel).await {
        return Result::Err(err);
    }
    if magic != K_INVITE_MAGIC
        && magic != K_ACCEPT_MAGIC
        && magic != K_CONFRM_MAGIC
        && magic != K_REJECT_MAGIC
    {
        return Result::Err(WireError::InvalidMagic);
    }

    let mut coverage = FrameBytes::new_();
    coverage.push_(&magic);
    let mut entries: [Option<NegotiationBasicEntry>; K_BASIC_KEY_COUNT] =
        core::array::from_fn(|_| Option::None);
    let mut seen = 0u8;
    loop {
        let mut header = [0u8; 1];
        if let Result::Err(err) = cursor.read_async_(&mut header, cancel).await {
            return Result::Err(err);
        }
        if !coverage.push_(&header) {
            return Result::Err(WireError::FrameTooLarge);
        }

        let key = NegotiationKey::try_from(header[0]).map_err(|_| WireError::UnsupportedOption)?;
        let vl_type =
            NegotiationValType::try_from(header[0]).map_err(|_| WireError::UnsupportedOption)?;

        if matches!(key, NegotiationKey::Checksum) {
            let Option::Some(algo) = HandshakeChecksum::try_new(vl_type) else {
                return Result::Err(WireError::UnsupportedOption);
            };
            let width = algo.checksum_len();
            let mut bytes = [0u8; 4];
            if let Result::Err(err) = cursor.read_async_(&mut bytes[..width], cancel).await {
                return Result::Err(err);
            }
            let expect = decode_checksum_(width, &bytes[..width]);
            if algo.checksum_(coverage.coverage_()) != expect {
                return Result::Err(WireError::ChecksumErr);
            }
            return Result::Ok(ParsedFrame { magic, entries });
        }

        let Result::Ok(basic_key) = basic_key_(key) else {
            return Result::Err(WireError::UnsupportedOption);
        };
        let width = vl_type.value_len();
        let mut bytes = [0u8; 8];
        if let Result::Err(err) = cursor.read_async_(&mut bytes[..width], cancel).await {
            return Result::Err(err);
        }
        if !coverage.push_(&bytes[..width]) {
            return Result::Err(WireError::FrameTooLarge);
        }
        let value = decode_value_(width, &bytes[..width]).map_err(|_| WireError::MalformedBody)?;
        if value == 0 {
            return Result::Err(WireError::MalformedBody);
        }
        let bit = 1u8 << basic_key;
        if seen & bit != 0 {
            return Result::Err(WireError::MalformedBody);
        }
        seen |= bit;
        entries[basic_key as usize] = Option::Some(NegotiationBasicEntry {
            opts_key: header[0],
            val_data: value,
        });
    }
}

/// 把一组基础项写成一个完整握手帧。
///
/// `values[k]` 为 `None` 时跳过键 `k`；每个条目使用能容纳该值的最小
/// `val_type` 宽度（模块文档 §6）。`checksum` 指定本帧使用的校验算法，帧会在
/// 自己的校验尾头字节中声明它。
///
/// # Errors
///
/// 底层写入失败或对端提前关闭时返回 [`WireError::Write`]。
pub(super) async fn write_frame_<W, K>(
    buff: &mut W,
    magic: MagicField,
    values: &[Option<usize>; K_BASIC_KEY_COUNT],
    checksum: HandshakeChecksum,
    cancel: &mut K,
) -> Result<(), WireError<(), W::Err>>
where
    W: TrBuffWrite<u8> + TrBuffTryWrite<u8>,
    K: TrCancellationToken,
{
    // 先在定长缓冲里成形：magic + 各条目 + 校验尾头字节即校验覆盖区，其长度
    // 最坏为 K_MAX_HANDSHAKE_FRAME（见该常量的文档）。
    let mut coverage = [0u8; K_MAX_HANDSHAKE_FRAME];
    let mut len = 0usize;
    coverage[len..len + 4].copy_from_slice(&magic);
    len += 4;
    for (key, value) in values.iter().enumerate() {
        let Option::Some(value) = value else {
            continue;
        };
        let Ok(basic_key) = NegotiationKey::try_from(key as u8) else {
            return Result::Err(WireError::UnsupportedOption);
        };
        let (vl_type, entry) = encode_entry_(basic_key, *value);
        let width = 1usize + vl_type.value_len();
        coverage[len..len + width].copy_from_slice(&entry[..width]);
        len += width;
    }
    coverage[len] = checksum.header_byte_();
    len += 1;

    // 先算出校验码并留出尾部位置，再一次性把整帧写进字节流。
    let value = checksum.checksum_(&coverage[..len]);
    let (bytes, checksum_len) = checksum_bytes_(checksum, value);
    coverage[len..len + checksum_len].copy_from_slice(&bytes[..checksum_len]);
    len += checksum_len;
    write_all_(&mut *buff, &coverage[..len], cancel).await
}

/// 写出 `bytes` 的全部内容。
///
/// 与读侧对称：借出的段可能比请求的更短，也可能更长；本函数只写入需要的前
/// 缀，未用完的容量在段被回收时归还，因此可以安全地分多次写完。
async fn write_all_<W, K>(
    buff: &mut W,
    bytes: &[u8],
    cancel: &mut K,
) -> Result<(), WireError<(), W::Err>>
where
    W: TrBuffWrite<u8> + TrBuffTryWrite<u8>,
    K: TrCancellationToken,
{
    let mut offset = 0usize;
    while offset < bytes.len() {
        if cancel.is_cancelled() {
            return Result::Err(WireError::PeerClosed);
        }
        let rest = bytes.len() - offset;
        let demand = Demand::exactly(rest);
        let put;
        {
            let mut write_res = buff.try_write(&demand);
            let segm: Option<&mut W::SegmMut<'_>> = write_res.as_mut().pick_left();
            match segm {
                Option::Some(segm) => {
                    put = segm.as_segm_mut().clone_items_from_buff(&bytes[offset..]);
                }
                Option::None => {
                    return Result::Err(match write_res.pick_right() {
                        Option::Some(err) => WireError::Write(err),
                        Option::None => WireError::PeerClosed,
                    });
                }
            }
        }
        if put == 0 {
            return Result::Err(WireError::PeerClosed);
        }
        offset += put;
    }
    Result::Ok(())
}

/// 组装条目头字节：`header = (val_type << 4) | key`。
const fn compose_header_(key: NegotiationKey, vl_type: NegotiationValType) -> u8 {
    (key as u8) | (vl_type as u8)
}

/// 把基础键映射为下标；`Checksum` 与保留键不在这里处理。
fn basic_key_(key: NegotiationKey) -> Result<u8, NegotiationKey> {
    match key {
        NegotiationKey::MaxPacketSize => Result::Ok(0u8),
        NegotiationKey::MaxChannelCount => Result::Ok(1u8),
        NegotiationKey::MaxDockChanCount => Result::Ok(2u8),
        NegotiationKey::MaxChannelTimeout => Result::Ok(3u8),
        other => Result::Err(other),
    }
}

/// 把校验码的数值写成 `checksum_len()` 字节大端序。
fn checksum_bytes_(checksum: HandshakeChecksum, value: u32) -> ([u8; 4], usize) {
    let all = value.to_be_bytes();
    let len = checksum.checksum_len();
    let mut out = [0u8; 4];
    out[..len].copy_from_slice(&all[4 - len..]);
    (out, len)
}

/// 把大端 `width` 字节的校验码解码为数值。
///
/// CRC-24 只有 3 字节，这里统一按 `u32` 承载。
fn decode_checksum_(width: usize, bytes: &[u8]) -> u32 {
    debug_assert_eq!(width, bytes.len());
    let mut value = 0u32;
    for &b in bytes {
        value = (value << 8) | b as u32;
    }
    value
}

/// 把 `width` 字节大端无符号数解码为 `usize`；超出 `usize` 表示范围返回错误。
fn decode_value_(width: usize, bytes: &[u8]) -> Result<usize, WireError<(), ()>> {
    debug_assert_eq!(width, bytes.len());
    let mut value = 0u64;
    for &b in bytes {
        value = (value << 8) | b as u64;
    }
    usize::try_from(value).map_err(|_| WireError::MalformedBody)
}

/// 编码一个条目：`header` 字节 + 大端 `value`，使用能容纳该值的最小宽度。
///
/// 返回 `(val_type, 整个条目的字节)`；有效长度为
/// `1 + val_type.value_len()`。
fn encode_entry_(key: NegotiationKey, value: usize) -> (NegotiationValType, [u8; 9]) {
    let (vl_type, width) = if value <= u8::MAX as usize {
        (NegotiationValType::BeU8, 1usize)
    } else if value <= u16::MAX as usize {
        (NegotiationValType::BeU16, 2usize)
    } else if value <= 0x00FF_FFFFusize {
        (NegotiationValType::BeU24, 3usize)
    } else if value <= u32::MAX as usize {
        (NegotiationValType::BeU32, 4usize)
    } else {
        (NegotiationValType::BeU64, 8usize)
    };
    let mut out = [0u8; 9];
    out[0] = compose_header_(key, vl_type);
    let all = (value as u64).to_be_bytes();
    out[1..1 + width].copy_from_slice(&all[8 - width..]);
    (vl_type, out)
}

/// 把基础键的可选取值集合补全为 [`BasicOpts`]；缺位项使用协议缺省值。
pub(super) fn values_to_basic_(values: &[Option<usize>; K_BASIC_KEY_COUNT]) -> BasicOpts {
    BasicOpts {
        max_packet_size: values[0].unwrap_or(BasicOpts::DEFAULT.max_packet_size),
        max_channel_count: values[1].unwrap_or(BasicOpts::DEFAULT.max_channel_count),
        max_dock_chan_count: values[2].unwrap_or(BasicOpts::DEFAULT.max_dock_chan_count),
        max_channel_timeout: Duration::from_secs(values[3].unwrap_or(30usize) as u64),
    }
}

/// 把 [`BasicOpts`] 展开为 4 个全部存在的基础键取值。
pub(super) fn basic_to_values_(opts: &BasicOpts) -> [Option<usize>; K_BASIC_KEY_COUNT] {
    [
        Option::Some(opts.max_packet_size),
        Option::Some(opts.max_channel_count),
        Option::Some(opts.max_dock_chan_count),
        Option::Some(opts.max_channel_timeout.as_secs() as usize),
    ]
}

/// 判断 4 个基础键是否全部出现。
pub(super) fn is_complete_(values: &[Option<usize>; K_BASIC_KEY_COUNT]) -> bool {
    values.iter().all(Option::is_some)
}

/// 等待方补全规则（模块文档 §7.2）：以 `local` 为基础，用 `INVITE` 中已提及的
/// 项覆盖对应位置。
pub(super) fn complete_invite_(
    local: &BasicOpts,
    entries: &[Option<NegotiationBasicEntry>; K_BASIC_KEY_COUNT],
) -> [Option<usize>; K_BASIC_KEY_COUNT] {
    let mut values = basic_to_values_(local);
    for (k, entry) in entries.iter().enumerate() {
        if let Option::Some(entry) = entry {
            values[k] = Option::Some(entry.val_data);
        }
    }
    values
}

/// 把解析出的条目转换为按键下标排列的取值；`ACCEPT` 必须补全全部 4 项，
/// 否则返回 `None`。
pub(super) fn complete_values_(
    entries: &[Option<NegotiationBasicEntry>; K_BASIC_KEY_COUNT],
) -> Option<[Option<usize>; K_BASIC_KEY_COUNT]> {
    let mut values: [Option<usize>; K_BASIC_KEY_COUNT] = [Option::None; K_BASIC_KEY_COUNT];
    for (k, entry) in entries.iter().enumerate() {
        values[k] = entry.as_ref().map(|e| e.val_data);
    }
    if is_complete_(&values) {
        Option::Some(values)
    } else {
        Option::None
    }
}

/// 校验 `CONFIRM` 的条目是否恰好等于 `expected`（模块文档 §7.4）。
pub(super) fn confirm_matches_(
    entries: &[Option<NegotiationBasicEntry>; K_BASIC_KEY_COUNT],
    expected: &[Option<usize>; K_BASIC_KEY_COUNT],
) -> bool {
    let Option::Some(values) = complete_values_(entries) else {
        return false;
    };
    values.iter().zip(expected.iter()).all(|(a, b)| a == b)
}

#[cfg(test)]
mod tests_ {
    use super::*;
    use abs_buff::x_deps::abs_cancel::NonCancellableToken;
    use core::future::Future;
    use core::pin::pin;
    use core::task::{Context, Poll, Waker};

    /// 把一帧写进 `buf`，返回写入的字节数。
    fn write_into_buf_(
        buf: &mut [u8],
        magic: MagicField,
        values: &[Option<usize>; K_BASIC_KEY_COUNT],
        checksum: HandshakeChecksum,
    ) -> usize {
        let mut cursor: &mut [u8] = buf;
        let mut cancel = NonCancellableToken::new();
        block_on_(write_frame_(
            &mut cursor,
            magic,
            values,
            checksum,
            &mut cancel,
        ))
        .unwrap();
        K_MAX_HANDSHAKE_FRAME - cursor.len()
    }

    /// 在当前线程把 future 跑到完成。
    ///
    /// 本 crate 是 `no_std`，测试中不引入异步运行时；编解码用到的
    /// `TrBuffRead` / `TrBuffWrite` 实现（字节切片）都是立即就绪的。
    fn block_on_<F: Future>(fut: F) -> F::Output {
        let mut fut = pin!(fut);
        let waker = Waker::noop();
        let mut ctx = Context::from_waker(waker);
        loop {
            match fut.as_mut().poll(&mut ctx) {
                Poll::Ready(out) => return out,
                Poll::Pending => continue,
            }
        }
    }

    /// 测试字节切片上的读写是按「引用自身前进」的方式消费的。
    /// - 手段：用 `write_frame_` 往 `&mut &mut [u8]` 写一帧，再用
    ///   `read_frame_` 从 `&mut &[u8]` 读回。
    /// - 判断：读出的 magic 与四个基础项数值与写入一致，且读侧指针前进了
    ///   整帧长度。
    #[test]
    fn write_then_read_roundtrip_crc16() {
        let values = [
            Option::Some(4096usize),
            Option::Some(1usize << 28),
            Option::Some(64usize),
            Option::Some(30usize),
        ];
        let mut buf = [0u8; K_MAX_HANDSHAKE_FRAME];
        let len = write_into_buf_(
            &mut buf,
            K_INVITE_MAGIC,
            &values,
            HandshakeChecksum::Crc16(HANDSHAKE_CRC16),
        );
        let mut probe: &[u8] = &buf[..len];
        let mut cancel = NonCancellableToken::new();
        let frame = block_on_(read_frame_(&mut probe, 64usize, &mut cancel)).unwrap();
        assert_eq!(frame.magic, K_INVITE_MAGIC);
        assert_eq!(frame.entries[0].as_ref().unwrap().val_data, 4096usize);
        assert_eq!(frame.entries[1].as_ref().unwrap().val_data, 1usize << 28);
        assert_eq!(frame.entries[2].as_ref().unwrap().val_data, 64usize);
        assert_eq!(frame.entries[3].as_ref().unwrap().val_data, 30usize);
        // 4096 用 BeU16、1<<28 用 BeU32、64 用 BeU8、30 用 BeU8，再加
        // magic 与 1 + 2 字节的 CRC-16 校验尾。
        assert_eq!(len, 4 + 3 + 5 + 2 + 2 + 3);
        // 读侧正好前进整帧长度。
        assert_eq!(probe.len(), 0usize);
    }

    /// 测试三种校验算法都能被写侧声明、被读侧识别。
    /// - 手段：分别用 CRC-16/CRC-24/CRC-32 写帧，再读回。
    /// - 判断：三次读取都成功，且校验尾长度分别为 2/3/4。
    #[test]
    fn roundtrip_supports_crc16_crc24_crc32() {
        let values = [
            Option::Some(7usize),
            Option::None,
            Option::None,
            Option::None,
        ];
        let cases = [
            (
                HandshakeChecksum::Crc16(HANDSHAKE_CRC16),
                NegotiationValType::BeU16,
                2usize,
            ),
            (
                HandshakeChecksum::Crc24(HANDSHAKE_CRC24),
                NegotiationValType::BeU24,
                3usize,
            ),
            (
                HandshakeChecksum::Crc32(HANDSHAKE_CRC32),
                NegotiationValType::BeU32,
                4usize,
            ),
        ];
        for (checksum, vl_type, crc_len) in cases {
            assert_eq!(u8::from(checksum.header()), u8::from(vl_type));
            assert_eq!(checksum.checksum_len(), crc_len);
            let mut buf = [0u8; K_MAX_HANDSHAKE_FRAME];
            let total = write_into_buf_(&mut buf, K_ACCEPT_MAGIC, &values, checksum);
            let mut probe: &[u8] = &buf[..total];
            let mut cancel = NonCancellableToken::new();
            let frame = block_on_(read_frame_(&mut probe, 64usize, &mut cancel)).unwrap();
            assert_eq!(frame.entries[0].as_ref().unwrap().val_data, 7usize);
            assert_eq!(total, 4 + 2 + 1 + crc_len);
        }
    }

    /// 测试 CRC-24 校验码被篡改时会被识别。
    /// - 手段：用 CRC-24 写帧后翻转校验码最后一个字节。
    /// - 判断：解析返回 `ChecksumErr`。
    #[test]
    fn crc24_mismatch_is_rejected() {
        let values = [
            Option::Some(7usize),
            Option::None,
            Option::None,
            Option::None,
        ];
        let mut buf = [0u8; K_MAX_HANDSHAKE_FRAME];
        let total = write_into_buf_(
            &mut buf,
            K_INVITE_MAGIC,
            &values,
            HandshakeChecksum::Crc24(HANDSHAKE_CRC24),
        );
        buf[total - 1] ^= 0xFF;
        let mut probe: &[u8] = &buf[..total];
        let mut cancel = NonCancellableToken::new();
        let res = block_on_(read_frame_(&mut probe, 64usize, &mut cancel));
        assert!(matches!(res, Result::Err(WireError::ChecksumErr)));
    }

    /// 测试 CRC-8（`BeU8`）不是合法校验尾。
    /// - 手段：手工构造 `magic + 0x0C`（`BeU8` + Checksum）。
    /// - 判断：解析返回 `UnsupportedOption`。
    #[test]
    fn crc8_trailer_is_unsupported() {
        let mut buf = [0u8; 8];
        buf[..4].copy_from_slice(&K_INVITE_MAGIC);
        buf[4] = compose_header_(NegotiationKey::Checksum, NegotiationValType::BeU8);
        let mut probe: &[u8] = &buf;
        let mut cancel = NonCancellableToken::new();
        let res = block_on_(read_frame_(&mut probe, 64usize, &mut cancel));
        assert!(matches!(res, Result::Err(WireError::UnsupportedOption)));
    }

    /// 测试累计帧长超过 `max_size` 会被拒绝。
    /// - 手段：写好一个合法帧，用小于帧长的 `max_size` 解析。
    /// - 判断：解析返回 `FrameTooLarge`。
    #[test]
    fn over_max_size_is_rejected() {
        let values = [
            Option::Some(4096usize),
            Option::None,
            Option::None,
            Option::None,
        ];
        let mut buf = [0u8; K_MAX_HANDSHAKE_FRAME];
        let total = write_into_buf_(
            &mut buf,
            K_INVITE_MAGIC,
            &values,
            HandshakeChecksum::Crc16(HANDSHAKE_CRC16),
        );
        let mut probe: &[u8] = &buf[..total];
        let mut cancel = NonCancellableToken::new();
        let res = block_on_(read_frame_(&mut probe, 5usize, &mut cancel));
        assert!(matches!(res, Result::Err(WireError::FrameTooLarge)));
    }

    /// 测试未知/保留键会被拒绝。
    /// - 手段：构造头字节键为 `0x05`（保留段）的条目后追加合法校验尾。
    /// - 判断：解析返回 `UnsupportedOption`。
    #[test]
    fn reserved_key_is_rejected() {
        let mut buf = [0u8; 16];
        buf[..4].copy_from_slice(&K_INVITE_MAGIC);
        buf[4] = 0x05;
        buf[5] = 0x01;
        let coverage_len = 6usize;
        let crc = HANDSHAKE_CRC16.checksum(&buf[..coverage_len + 1]);
        buf[6] = compose_header_(NegotiationKey::Checksum, NegotiationValType::BeU16);
        buf[7..9].copy_from_slice(&crc.to_be_bytes());
        let mut probe: &[u8] = &buf[..11];
        let mut cancel = NonCancellableToken::new();
        let res = block_on_(read_frame_(&mut probe, 64usize, &mut cancel));
        assert!(matches!(res, Result::Err(WireError::UnsupportedOption)));
        assert_eq!(coverage_len, 6);
    }

    /// 测试重复键会被拒绝。
    /// - 手段：构造同一基础键 `0x00` 出现两次的条目区，再补合法校验尾。
    /// - 判断：解析返回 `MalformedBody`。
    #[test]
    fn duplicate_key_is_rejected() {
        let mut buf = [0u8; 16];
        buf[..4].copy_from_slice(&K_INVITE_MAGIC);
        buf[4] = 0x00;
        buf[5] = 0x01;
        buf[6] = 0x00;
        buf[7] = 0x02;
        let coverage_len = 8usize;
        buf[8] = compose_header_(NegotiationKey::Checksum, NegotiationValType::BeU16);
        let crc = HANDSHAKE_CRC16.checksum(&buf[..9]);
        buf[9..11].copy_from_slice(&crc.to_be_bytes());
        let mut probe: &[u8] = &buf[..11];
        let mut cancel = NonCancellableToken::new();
        let res = block_on_(read_frame_(&mut probe, 64usize, &mut cancel));
        assert!(matches!(res, Result::Err(WireError::MalformedBody)));
        assert_eq!(coverage_len, 8);
    }

    /// 测试等待方补全规则：已提及项覆盖本地值，未提及项保留本地值。
    /// - 手段：本地四项齐全，`INVITE` 只提及 `max_packet_size = 8192`。
    /// - 判断：补全结果第 0 项为 8192，其余三项等于本地值。
    #[test]
    fn complete_invite_overrides_only_mentioned() {
        let local = BasicOpts::DEFAULT;
        let mut entries: [Option<NegotiationBasicEntry>; K_BASIC_KEY_COUNT] =
            core::array::from_fn(|_| Option::None);
        entries[0] = Option::Some(NegotiationBasicEntry {
            opts_key: 0x10,
            val_data: 8192,
        });
        let values = complete_invite_(&local, &entries);
        assert_eq!(values[0], Option::Some(8192));
        assert_eq!(values[1], Option::Some(local.max_channel_count));
        assert_eq!(values[2], Option::Some(local.max_dock_chan_count));
        assert_eq!(
            values[3],
            Option::Some(local.max_channel_timeout.as_secs() as usize)
        );
    }
}
