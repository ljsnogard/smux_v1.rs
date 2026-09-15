//! 握手帧的纯编解码逻辑（不含 IO）。
//!
//! 本模块把 [`crate::handshake`] 模块文档 §4、§6 的线格式实现为可独立测试的
//! 纯函数：把 `&[u8]` 解析成 [`ParsedFrame`]，或把基础项编码成完整帧。

use core::time::Duration;

use crate::handshake::{
    K_ACCEPT_MAGIC, K_CONFRM_MAGIC, K_INVITE_MAGIC, K_REJECT_MAGIC,
    MagicField,
    opts::{BasicOpts, NegotiationBasicEntry, NegotiationValType},
};

/// 单个握手帧允许的最大字节数（内部缓冲上限）。
///
/// v1 只有 4 个基础项，最坏情况为 `4 + 4 * (1 + 8) + 1 + 4 = 45` 字节，
/// 取 64 留出余量。
pub const K_MAX_HANDSHAKE_FRAME: usize = 64;

/// 基础项的键数量。
const K_BASIC_KEY_COUNT: usize = 4;

const HANDSHAKE_CRC16: crc::Crc<u16> =
    crc::Crc::<u16>::new(&crc::CRC_16_XMODEM);

const HANDSHAKE_CRC24: crc::Crc<u32> =
    crc::Crc::<u32>::new(&crc::CRC_24_BLE);

const HANDSHAKE_CRC32: crc::Crc<u32> =
    crc::Crc::<u32>::new(&crc::CRC_32_ISO_HDLC);

pub enum HandshakeChecksum {
    Crc16(crc::Crc<u16>),
    Crc24(crc::Crc<u32>),
    Crc32(crc::Crc<u32>),
}

impl HandshakeChecksum {
    pub fn try_new(val_type: NegotiationValType) -> Option<Self> {
        match val_type {
            NegotiationValType::BeU16 =>
                Option::Some(HandshakeChecksum::Crc16(HANDSHAKE_CRC16)),
            NegotiationValType::BeU24 =>
                Option::Some(HandshakeChecksum::Crc24(HANDSHAKE_CRC24)),
            NegotiationValType::BeU32 =>
                Option::Some(HandshakeChecksum::Crc32(HANDSHAKE_CRC32)),
            _ => Option::None,
        }
    }

    pub const fn header(&self) -> NegotiationValType {
        match self {
            HandshakeChecksum::Crc16(_) => NegotiationValType::BeU16,
            HandshakeChecksum::Crc24(_) => NegotiationValType::BeU24,
            HandshakeChecksum::Crc32(_) => NegotiationValType::BeU32,
        }
    }

    pub const fn checksum_len(&self) -> usize {
        match self {
            HandshakeChecksum::Crc16(_) => 2usize,
            HandshakeChecksum::Crc24(_) => 3usize,
            HandshakeChecksum::Crc32(_) => 4usize,
        }
    }
}

impl core::fmt::Debug for HandshakeChecksum {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        todo!()
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

/// 帧解析失败。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ParseFrameError {
    /// 需要至少这么多字节才能继续解析。
    NeedMore(usize),

    /// 帧首 magic 不是 smux v1 握手的四种之一。
    InvalidMagic,

    /// 未知/保留键，或校验尾头不是 `0x1C` / `0x2C`。
    UnsupportedOption,

    /// 条目区结构非法：重复键、基础项取值为 0、数值超出 `usize`。
    MalformedBody,

    /// 帧长超过 `max_size`。
    FrameTooLarge,

    /// CRC 不匹配。
    ChecksumErr,
}

/// 把 4 个基础项的可选取值编码成一个完整握手帧。
///
/// `values[k]` 为 `None` 时跳过键 `k`。编码使用最小宽度（见模块文档 §6），
/// 校验尾固定为 CRC-16/XMODEM。返回写入 `out` 的字节数；缓冲不足返回 `None`。
pub(super) fn build_frame_(
    magic: MagicField,
    values: &[Option<usize>; K_BASIC_KEY_COUNT],
    checksum: HandshakeChecksum,
    out: &mut [u8],
) -> Option<usize> {
    if out.len() < 4 {
        return Option::None;
    }
    out[..4].copy_from_slice(&magic);
    let mut pos = 4usize;
    for (key, value) in values.iter().enumerate() {
        let Option::Some(value) = value else {
            continue;
        };
        let written = encode_basic_entry_(key as u8, *value, &mut out[pos..])?;
        pos += written;
    }
    // 校验尾头 + 2 字节 CRC。
    if out.len() < pos + 3 {
        return Option::None;
    }
    out[pos] = K_CHECKSUM_HEADER_CRC16;
    pos += 1;
    let crc = HANDSHAKE_CRC16.checksum(&out[..pos]);
    out[pos..pos + 2].copy_from_slice(&crc.to_be_bytes());
    Option::Some(pos + 2)
}

/// 解析一个完整或部分握手帧。
///
/// 返回 `(解析结果, 本帧占用的字节数)`。缓冲不足以解析下一个字段时返回
/// [`ParseFrameError::NeedMore`]，其值为继续解析所需的最小字节数。
pub(super) fn parse_frame_(
    buf: &[u8],
    max_size: usize,
) -> Result<(ParsedFrame, usize), ParseFrameError> {
    if buf.len() < 4 {
        return Result::Err(ParseFrameError::NeedMore(4));
    }
    if max_size < 4 {
        return Result::Err(ParseFrameError::FrameTooLarge);
    }
    let mut magic = [0u8; 4];
    magic.copy_from_slice(&buf[..4]);
    if magic != K_INVITE_MAGIC
        && magic != K_ACCEPT_MAGIC
        && magic != K_CONFRM_MAGIC
        && magic != K_REJECT_MAGIC
    {
        return Result::Err(ParseFrameError::InvalidMagic);
    }
    let mut pos = 4usize;
    let mut entries: [Option<NegotiationBasicEntry>; K_BASIC_KEY_COUNT] =
        [Option::None, Option::None, Option::None, Option::None];
    let mut seen = 0u8;
    loop {
        if buf.len() < pos + 1 {
            return Result::Err(ParseFrameError::NeedMore(pos + 1));
        }
        if pos + 1 > max_size {
            return Result::Err(ParseFrameError::FrameTooLarge);
        }
        let header = buf[pos];
        pos += 1;
        let key = header & 0x0F;
        let val_type = header & 0x30;
        let width = match val_type {
            0x00 => 1usize,
            0x10 => 2,
            0x20 => 4,
            _ => 8,
        };
        if key == 0x0C {
            let crc_len = if header == K_CHECKSUM_HEADER_CRC16 {
                2usize
            } else if header == K_CHECKSUM_HEADER_CRC32 {
                4
            } else {
                return Result::Err(ParseFrameError::UnsupportedOption);
            };
            if buf.len() < pos + crc_len {
                return Result::Err(ParseFrameError::NeedMore(pos + crc_len));
            }
            if pos + crc_len > max_size {
                return Result::Err(ParseFrameError::FrameTooLarge);
            }
            let is_ok = if crc_len == 2 {
                let crc = HANDSHAKE_CRC16.checksum(&buf[..pos]);
                let mut expect = [0u8; 2];
                expect.copy_from_slice(&buf[pos..pos + 2]);
                crc.to_be_bytes() == expect
            } else {
                let crc = HANDSHAKE_CRC32.checksum(&buf[..pos]);
                let mut expect = [0u8; 4];
                expect.copy_from_slice(&buf[pos..pos + 4]);
                crc.to_be_bytes() == expect
            };
            if !is_ok {
                return Result::Err(ParseFrameError::ChecksumErr);
            }
            return Result::Ok((ParsedFrame { magic, entries }, pos + crc_len));
        }
        if key as usize >= K_BASIC_KEY_COUNT {
            return Result::Err(ParseFrameError::UnsupportedOption);
        }
        if buf.len() < pos + width {
            return Result::Err(ParseFrameError::NeedMore(pos + width));
        }
        if pos + width > max_size {
            return Result::Err(ParseFrameError::FrameTooLarge);
        }
        let value = decode_value_(width, &buf[pos..pos + width])
            .ok_or(ParseFrameError::MalformedBody)?;
        pos += width;
        if value == 0 {
            return Result::Err(ParseFrameError::MalformedBody);
        }
        let bit = 1u8 << key;
        if seen & bit != 0 {
            return Result::Err(ParseFrameError::MalformedBody);
        }
        seen |= bit;
        entries[key as usize] = Option::Some(NegotiationBasicEntry {
            opts_key: header,
            val_data: value,
        });
    }
}

/// 把基础键的可选取值集合补全为 [`BasicOpts`]；缺位项使用协议缺省值。
pub(super) fn values_to_basic_(
    values: &[Option<usize>; K_BASIC_KEY_COUNT],
) -> BasicOpts {
    BasicOpts {
        max_packet_size: values[0]
            .unwrap_or(BasicOpts::DEFAULT.max_packet_size),
        max_channel_count: values[1]
            .unwrap_or(BasicOpts::DEFAULT.max_channel_count),
        max_dock_chan_count: values[2]
            .unwrap_or(BasicOpts::DEFAULT.max_dock_chan_count),
        max_channel_timeout: Duration::from_secs(
            values[3].unwrap_or(30usize) as u64
        ),
    }
}

/// 把 [`BasicOpts`] 展开为 4 个全部存在的基础键取值。
pub(super) fn basic_to_values_(
    opts: &BasicOpts,
) -> [Option<usize>; K_BASIC_KEY_COUNT] {
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
    let mut values: [Option<usize>; K_BASIC_KEY_COUNT] =
        [Option::None, Option::None, Option::None, Option::None];
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

/// 编码一个基础条目，返回写入字节数；使用能容纳该值的最小宽度。
///
/// `key` 只取低 4 位；数值按大端写入。
fn encode_basic_entry_(key: u8, value: usize, out: &mut [u8]) -> Option<usize> {
    let (val_type, width) = if value <= u8::MAX as usize {
        (0x00u8, 1usize)
    } else if value <= u16::MAX as usize {
        (0x10, 2)
    } else if value <= u32::MAX as usize {
        (0x20, 4)
    } else {
        (0x30, 8)
    };
    if out.len() < 1 + width {
        return Option::None;
    }
    out[0] = val_type | (key & 0x0F);
    let bytes = (value as u64).to_be_bytes();
    out[1..1 + width].copy_from_slice(&bytes[8 - width..]);
    Option::Some(1 + width)
}

/// 把 `width` 字节大端无符号数解码为 `usize`；超出 `usize` 表示范围返回
/// `None`。
fn decode_value_(width: usize, bytes: &[u8]) -> Option<usize> {
    debug_assert_eq!(width, bytes.len());
    let mut value: u64 = 0;
    for &b in bytes {
        value = (value << 8) | b as u64;
    }
    usize::try_from(value).ok()
}

#[cfg(test)]
mod tests_ {
    use super::*;

    /// 测试最小宽度编码：不同数值区间应选择 1/2/4/8 字节宽度。
    /// - 手段：分别编码 0x12、0x1234、0x12345678、0x1_0000_0000。
    /// - 判断：写出的头字节高半字节依次为 0x0、0x1、0x2、0x3，且总长度为
    ///   1 + 宽度。
    #[test]
    fn encode_basic_entry_picks_minimal_width() {
        let mut out = [0u8; 16];
        let n = encode_basic_entry_(0x00, 0x12, &mut out).unwrap();
        assert_eq!(n, 2);
        assert_eq!(out[0], 0x00);
        assert_eq!(&out[1..2], &[0x12]);

        let n = encode_basic_entry_(0x01, 0x1234, &mut out).unwrap();
        assert_eq!(n, 3);
        assert_eq!(out[0], 0x11);
        assert_eq!(&out[1..3], &[0x12, 0x34]);

        let n = encode_basic_entry_(0x02, 0x1234_5678, &mut out).unwrap();
        assert_eq!(n, 5);
        assert_eq!(out[0], 0x22);
        assert_eq!(&out[1..5], &[0x12, 0x34, 0x56, 0x78]);

        #[cfg(target_pointer_width = "64")]
        {
            let n = encode_basic_entry_(0x03, 0x1_0000_0000, &mut out).unwrap();
            assert_eq!(n, 9);
            assert_eq!(out[0], 0x33);
            assert_eq!(
                &out[1..9],
                &[0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00]
            );
        }
    }

    /// 测试完整帧的构建与解析往返。
    /// - 手段：用 `build_frame_` 构造一个含全部 4 个基础项的 INVITE，再解析。
    /// - 判断：magic 一致、4 个条目数值与输入一致、消耗字节数等于帧长。
    #[test]
    fn build_and_parse_frame_roundtrip() {
        let values = [
            Some(4096usize),
            Some(1usize << 28),
            Some(64usize),
            Some(30usize),
        ];
        let mut buf = [0u8; K_MAX_HANDSHAKE_FRAME];
        let len = build_frame_(K_INVITE_MAGIC, &values, &mut buf).unwrap();
        assert_eq!(len, 4 + 3 + 5 + 2 + 2 + 3);
        let (frame, consumed) = parse_frame_(&buf[..len], 64).unwrap();
        assert_eq!(consumed, len);
        assert_eq!(frame.magic, K_INVITE_MAGIC);
        assert_eq!(frame.entries[0].as_ref().unwrap().val_data, 4096);
        assert_eq!(frame.entries[1].as_ref().unwrap().val_data, 1usize << 28);
        assert_eq!(frame.entries[2].as_ref().unwrap().val_data, 64);
        assert_eq!(frame.entries[3].as_ref().unwrap().val_data, 30);
    }

    /// 测试 CRC 校验失败会被识别。
    /// - 手段：构造一个合法帧后翻转 CRC 字段的最后一个字节。
    /// - 判断：解析返回 `ChecksumErr`。
    #[test]
    fn parse_frame_detects_bad_crc() {
        let values = [Some(4096usize), None, None, None];
        let mut buf = [0u8; K_MAX_HANDSHAKE_FRAME];
        let len = build_frame_(K_INVITE_MAGIC, &values, &mut buf).unwrap();
        buf[len - 1] ^= 0xFF;
        assert!(matches!(
            parse_frame_(&buf[..len], 64),
            Err(ParseFrameError::ChecksumErr)
        ));
    }

    /// 测试 CRC-32 校验尾也能被解析。
    /// - 手段：手工构造 `magic + 基础条目 + 0x2C + CRC-32`。
    /// - 判断：解析成功且条目数值正确。
    #[test]
    fn parse_frame_accepts_crc32_trailer() {
        let mut buf = [0u8; K_MAX_HANDSHAKE_FRAME];
        buf[..4].copy_from_slice(&K_ACCEPT_MAGIC);
        let n = encode_basic_entry_(0x00, 4096, &mut buf[4..]).unwrap();
        let pos = 4 + n;
        buf[pos] = K_CHECKSUM_HEADER_CRC32;
        let crc = HANDSHAKE_CRC32.checksum(&buf[..pos + 1]);
        buf[pos + 1..pos + 5].copy_from_slice(&crc.to_be_bytes());
        let total = pos + 5;
        let (frame, consumed) = parse_frame_(&buf[..total], 64).unwrap();
        assert_eq!(consumed, total);
        assert_eq!(frame.entries[0].as_ref().unwrap().val_data, 4096);
    }

    /// 测试未知/保留键会被拒绝。
    /// - 手段：构造头字节键为 `0x05`（保留段）的条目后追加校验尾。
    /// - 判断：解析返回 `UnsupportedOption`，即使 CRC 正确。
    #[test]
    fn parse_frame_rejects_reserved_key() {
        let mut buf = [0u8; K_MAX_HANDSHAKE_FRAME];
        buf[..4].copy_from_slice(&K_INVITE_MAGIC);
        buf[4] = 0x05; // 保留键 0x05，宽度 1
        buf[5] = 0x01;
        let pos = 6;
        buf[pos] = K_CHECKSUM_HEADER_CRC16;
        let crc = HANDSHAKE_CRC16.checksum(&buf[..pos + 1]);
        buf[pos + 1..pos + 3].copy_from_slice(&crc.to_be_bytes());
        assert!(matches!(
            parse_frame_(&buf[..pos + 3], 64),
            Err(ParseFrameError::UnsupportedOption)
        ));
    }

    /// 测试重复键会被拒绝。
    /// - 手段：构造同一基础键 `0x00` 出现两次的条目区。
    /// - 判断：解析返回 `MalformedBody`。
    #[test]
    fn parse_frame_rejects_duplicate_key() {
        let mut buf = [0u8; K_MAX_HANDSHAKE_FRAME];
        buf[..4].copy_from_slice(&K_INVITE_MAGIC);
        buf[4] = 0x00;
        buf[5] = 0x01;
        buf[6] = 0x00;
        buf[7] = 0x02;
        let pos = 8;
        buf[pos] = K_CHECKSUM_HEADER_CRC16;
        let crc = HANDSHAKE_CRC16.checksum(&buf[..pos + 1]);
        buf[pos + 1..pos + 3].copy_from_slice(&crc.to_be_bytes());
        assert!(matches!(
            parse_frame_(&buf[..pos + 3], 64),
            Err(ParseFrameError::MalformedBody)
        ));
    }

    /// 测试基础项取值为 0 会被拒绝。
    /// - 手段：基础键 `0x00` 的值为 0。
    /// - 判断：解析返回 `MalformedBody`。
    #[test]
    fn parse_frame_rejects_zero_value() {
        let values = [Some(0usize), None, None, None];
        let mut buf = [0u8; K_MAX_HANDSHAKE_FRAME];
        let len = build_frame_(K_INVITE_MAGIC, &values, &mut buf).unwrap();
        assert!(matches!(
            parse_frame_(&buf[..len], 64),
            Err(ParseFrameError::MalformedBody)
        ));
    }

    /// 测试不完整帧会请求更多字节。
    /// - 手段：只提供 magic 与一个条目的头字节。
    /// - 判断：返回 `NeedMore`，且其值大于当前缓冲长度。
    #[test]
    fn parse_frame_needs_more_bytes() {
        let values = [Some(4096usize), None, None, None];
        let mut buf = [0u8; K_MAX_HANDSHAKE_FRAME];
        let _ = build_frame_(K_INVITE_MAGIC, &values, &mut buf).unwrap();
        let short = &buf[..6];
        match parse_frame_(short, 64) {
            Err(ParseFrameError::NeedMore(need)) => assert!(need > short.len()),
            other => panic!("expected NeedMore, got {:?}", other),
        }
    }

    /// 测试帧长超过 `max_size` 会被拒绝。
    /// - 手段：构造合法帧但以小于帧长的 `max_size` 解析。
    /// - 判断：解析返回 `FrameTooLarge`。
    #[test]
    fn parse_frame_rejects_over_max_size() {
        let values = [Some(4096usize), None, None, None];
        let mut buf = [0u8; K_MAX_HANDSHAKE_FRAME];
        let len = build_frame_(K_INVITE_MAGIC, &values, &mut buf).unwrap();
        assert!(matches!(
            parse_frame_(&buf[..len], 5),
            Err(ParseFrameError::FrameTooLarge)
        ));
    }

    /// 测试等待方补全规则：已提及项覆盖本地值，未提及项保留本地值。
    /// - 手段：本地四项齐全，INVITE 只提及 `max_packet_size = 8192`。
    /// - 判断：补全结果的第 0 项为 8192，其余三项等于本地值。
    #[test]
    fn complete_invite_overrides_only_mentioned() {
        let local = BasicOpts::DEFAULT;
        let mut entries: [Option<NegotiationBasicEntry>; K_BASIC_KEY_COUNT] =
            [None, None, None, None];
        entries[0] = Some(NegotiationBasicEntry {
            opts_key: 0x10,
            val_data: 8192,
        });
        let values = complete_invite_(&local, &entries);
        assert_eq!(values[0], Some(8192));
        assert_eq!(values[1], Some(local.max_channel_count));
        assert_eq!(values[2], Some(local.max_dock_chan_count));
        assert_eq!(
            values[3],
            Some(local.max_channel_timeout.as_secs() as usize)
        );
    }

    /// 测试 `complete_values_` 要求四项齐全。
    /// - 手段：只提供 3 项基础条目。
    /// - 判断：返回 `None`；补全 4 项后返回 `Some`。
    #[test]
    fn complete_values_requires_all_keys() {
        let mut entries: [Option<NegotiationBasicEntry>; K_BASIC_KEY_COUNT] =
            [None, None, None, None];
        entries[0] = Some(NegotiationBasicEntry {
            opts_key: 0x00,
            val_data: 1,
        });
        entries[1] = Some(NegotiationBasicEntry {
            opts_key: 0x01,
            val_data: 1,
        });
        entries[2] = Some(NegotiationBasicEntry {
            opts_key: 0x02,
            val_data: 1,
        });
        assert!(complete_values_(&entries).is_none());
        entries[3] = Some(NegotiationBasicEntry {
            opts_key: 0x03,
            val_data: 1,
        });
        assert!(complete_values_(&entries).is_some());
    }

    /// 测试 `confirm_matches_` 能识别数值不一致。
    /// - 手段：`expected` 与条目仅在 `max_packet_size` 上不同。
    /// - 判断：返回 `false`；改为一致后返回 `true`。
    #[test]
    fn confirm_matches_detects_mismatch() {
        let expected = [Some(4096usize), Some(1), Some(1), Some(30)];
        let mut entries: [Option<NegotiationBasicEntry>; K_BASIC_KEY_COUNT] =
            [None, None, None, None];
        for (k, v) in expected.iter().enumerate() {
            entries[k] = Some(NegotiationBasicEntry {
                opts_key: k as u8,
                val_data: v.unwrap(),
            });
        }
        entries[0] = Some(NegotiationBasicEntry {
            opts_key: 0x00,
            val_data: 2048,
        });
        assert!(!confirm_matches_(&entries, &expected));
        entries[0] = Some(NegotiationBasicEntry {
            opts_key: 0x00,
            val_data: 4096,
        });
        assert!(confirm_matches_(&entries, &expected));
    }

    /// 测试一次完整的四消息交互（字节层面）。
    /// - 手段：按发起方立场构造 INVITE；解析后按 §7.2 补全并构造 ACCEPT；
    ///   再解析 ACCEPT、构造 CONFIRM；最后校验 CONFIRM 并构造空 CONFRM。
    /// - 判断：每一步解析出的数值与预期一致，补全与提交校验均通过。
    #[test]
    fn full_four_message_flow() {
        let mut buf = [0u8; K_MAX_HANDSHAKE_FRAME];
        let local = BasicOpts::DEFAULT;

        // INVITE：只声明 max_packet_size。
        let invite_values = [Some(8192usize), None, None, None];
        let len =
            build_frame_(K_INVITE_MAGIC, &invite_values, &mut buf).unwrap();
        let (invite, _) = parse_frame_(&buf[..len], 64).unwrap();
        assert_eq!(invite.magic, K_INVITE_MAGIC);

        // 等待方补全并回 ACCEPT。
        let values = complete_invite_(&local, &invite.entries);
        assert_eq!(values[0], Some(8192));
        let len = build_frame_(K_ACCEPT_MAGIC, &values, &mut buf).unwrap();
        let (accept, _) = parse_frame_(&buf[..len], 64).unwrap();
        let accepted = complete_values_(&accept.entries).unwrap();
        assert_eq!(accepted, values);

        // 发起方回 CONFIRM。
        let len = build_frame_(K_CONFRM_MAGIC, &accepted, &mut buf).unwrap();
        let (confirm, _) = parse_frame_(&buf[..len], 64).unwrap();
        assert!(confirm_matches_(&confirm.entries, &values));

        // 等待方回空 CONFRM。
        let empty = [None, None, None, None];
        let len = build_frame_(K_CONFRM_MAGIC, &empty, &mut buf).unwrap();
        let (done, _) = parse_frame_(&buf[..len], 64).unwrap();
        assert!(done.entries.iter().all(Option::is_none));
    }
}
