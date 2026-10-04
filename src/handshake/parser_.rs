//! 握手帧的 **sans-IO** 逐字节解析状态机。
//!
//! 与 [`crate::connection::frame_parser_`] 同构：解析算法本身**不碰 IO**，只按字节
//! 推进并给出结论；「还需要更多输入」时由调用方去读。异步驱动在
//! [`FrameReader`](super::codec_::FrameReader) 里，它每次只索要 1 个字节。
//!
//! # 为什么必须逐字节
//!
//! 握手的读缓冲是**调用方注入**的环（`ConnRx`），容量可以小到 `buffex::ring` 的下限
//! 1 字节。任何「一次索要 `width` 字节」的解析都会在 `width > 容量` 时拿到**终态**的
//! `Unsatisfiable`，整条连接随之失败。状态机每次只消费 1 个字节，
//! `min_len == 1` 不超过任何合法容量，这条失败路径因此从构造上消失。
//!
//! # 状态划分
//!
//! ```text
//! Magic(0..4) --第 4 字节--> Alg --1 字节--> EntryHeader
//! EntryHeader --条目头字节--> EntryValue(width) --收满--> EntryHeader
//! EntryHeader --校验头字节--> ChecksumValue(crc_len) --收满且 crc 相符--> Done
//! ```
//!
//! `magic` 与算法预告都必须计入 `crc`，但算法要到第 5 个字节才知道，因此状态机先
//! 把 `magic` 存在定长数组里，读到算法预告时再一次性建好校验状态并补喂这两个字段。
//!
//! # 内存与帧长无关
//!
//! 值缓冲是定长的 8 字节（基础项最宽为 `BeU64`），条目逐个串行解析，任意时刻只有
//! 一个字段「在途」；5 个基础键各有一个定长槽位。因此内存占用是 O(1)。

use crate::handshake::{
    MagicField,
    codec_::{
        CrcDigest, HandshakeChecksum, basic_key_, decode_checksum_, decode_value_,
    },
    opts::{K_BASIC_KEY_COUNT, NegotiationBasicEntry, NegotiationKey, NegotiationValType},
};

/// `magic` 的字节数。
const K_MAGIC_LEN: usize = 4;

/// 单条目值缓冲上限：基础项最宽是 `BeU64`。
const K_VALUE_BUF_LEN: usize = 8;

/// sans-IO 状态机产出的**协议层**失败原因。
///
/// 它**不含**底层 IO 错误：状态机不接触 IO，`Read` / `Write` / `PeerClosed` 由驱动方
/// （[`FrameReader`](super::codec_::FrameReader)）在读字节时补齐。这样状态机可以完全
/// 脱离 IO 单元测试，也不必在类型里挂一个它永远产不出的泛型参数。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ProtoError_ {
    /// 未知 / 保留键，或算法预告 / 校验头的 `val_type` 不是 16/24/32 位校验码。
    UnsupportedOption,

    /// 条目区结构非法：重复键、基础项取值为 0、数值超出 `usize`、
    /// 校验头与算法预告不一致。
    MalformedBody,

    /// CRC 不匹配。
    ChecksumErr,
}

impl ProtoError_ {
    /// 把协议层原因搬到携带底层错误类型 `RE` 的 [`WireError`](super::codec_::WireError)
    /// 上；两个枚举的协议层变体一一对应。
    pub(crate) fn into_wire_<RE>(self) -> super::codec_::WireError<RE, ()> {
        use super::codec_::WireError;
        match self {
            ProtoError_::UnsupportedOption => WireError::UnsupportedOption,
            ProtoError_::MalformedBody => WireError::MalformedBody,
            ProtoError_::ChecksumErr => WireError::ChecksumErr,
        }
    }
}

/// 一次 [`HandshakeParser::consume_byte_`] 的结果。
#[derive(Debug, Clone)]
pub(crate) enum Consume_ {
    /// 还需要更多输入。
    Pending,

    /// 又完成一条基础条目：可**立即**交给协商方判断，不必等整帧。
    Entry(NegotiationBasicEntry),

    /// 整帧完成：校验头已读到、`crc` 比对通过。
    Done,

    /// 协议层失败。
    Failed(ProtoError_),
}

/// 解析所处的位置。
#[derive(Debug, Clone, Copy)]
enum State_ {
    /// 正在收 `magic` 的第 `got` 个字节。
    Magic { got: usize },

    /// 等算法预告字节。
    Alg,

    /// 等条目头字节（或校验头字节）。
    EntryHeader,

    /// 正在收某条基础项的值字节。
    EntryValue {
        /// 条目头字节 `(val_type << 4) | key`，原样存进 [`NegotiationBasicEntry`]。
        header: u8,
        /// [`basic_key_`] 给出的槽位下标。
        idx: u8,
        /// 本条目的值宽度。
        width: usize,
        /// 已收字节数。
        got: usize,
    },

    /// 正在收校验码字节。
    ChecksumValue { width: usize, got: usize },

    /// 整帧完成。
    Done,

    /// 已失败；重复喂入返回同一个结论。
    Failed(ProtoError_),
}

/// 握手帧的 sans-IO 逐字节解析状态机。
pub(crate) struct HandshakeParser {
    /// 当前位置。
    state_: State_,

    /// 帧首 4 字节；建校验状态时要补喂。
    magic_: MagicField,

    /// 算法预告字节（与帧尾校验头逐位相同）。
    alg_hdr_: u8,

    /// 校验码字节数（2/3/4），由算法预告决定。
    checksum_len_: usize,

    /// 增量校验状态；读到算法预告之前为 `None`。
    crc_: Option<CrcDigest>,

    /// 5 个基础键的定长槽位。
    basics_: [Option<NegotiationBasicEntry>; K_BASIC_KEY_COUNT],

    /// 已出现的基础键位图，用于重复键检测。
    seen_: u8,

    /// 当前字段的值缓冲（定长，最宽 `BeU64`）。
    value_: [u8; K_VALUE_BUF_LEN],
}

impl HandshakeParser {
    /// 建立一台停在帧首的状态机。
    pub(crate) fn new_() -> Self {
        HandshakeParser {
            state_: State_::Magic { got: 0 },
            magic_: [0u8; K_MAGIC_LEN],
            alg_hdr_: 0u8,
            checksum_len_: 0usize,
            crc_: Option::None,
            basics_: core::array::from_fn(|_| Option::None),
            seen_: 0u8,
            value_: [0u8; K_VALUE_BUF_LEN],
        }
    }

    /// `magic` 与算法预告是否都已收到（即帧首 5 字节已消费）。
    pub(crate) fn is_started_(&self) -> bool {
        !matches!(self.state_, State_::Magic { .. } | State_::Alg)
    }

    /// 整帧是否已完成。
    pub(crate) fn is_done_(&self) -> bool {
        matches!(self.state_, State_::Done)
    }

    /// 帧首 4 字节 `magic`（`is_started_` 之后才有意义）。
    pub(crate) fn magic_(&self) -> MagicField {
        self.magic_
    }

    /// 已解析的基础项槽位；下标即基础键 `0x00..=0x04`。
    pub(crate) fn basics_(&self) -> &[Option<NegotiationBasicEntry>; K_BASIC_KEY_COUNT] {
        &self.basics_
    }

    /// 已出结论时给出结论；还在中途返回 `None`。
    ///
    /// 供「先按需喂字节、再统一取结果」的驱动收尾——已出结论之后驱动**不该再去读**，
    /// 否则会白白 park 在已经没有后续输入的地方。
    pub(crate) fn finish_(&self) -> Option<Result<(), ProtoError_>> {
        match self.state_ {
            State_::Done => Option::Some(Result::Ok(())),
            State_::Failed(err) => Option::Some(Result::Err(err)),
            _ => Option::None,
        }
    }

    /// 推进 1 个字节。
    ///
    /// **「还需要更多输入时必定消费这 1 个字节」**——包括「这 1 字节正好让帧完成」
    /// （`Done`）或「正好判定非法」（`Failed`）两种情形；出了结论之后的重复喂入一律
    /// 返回既成结论、不再消费。这条不变量让喂字节的循环不必回退或缓存，因此在字节流
    /// 上不会丢字节。
    pub(crate) fn consume_byte_(&mut self, byte: u8) -> Consume_ {
        match self.state_ {
            // 已出结论：返回既成结论，不再消费。
            State_::Done => return Consume_::Done,
            State_::Failed(err) => return Consume_::Failed(err),
            _ => {}
        }

        match self.state_ {
            State_::Done | State_::Failed(_) => unreachable!("上面已经拦下终态"),

            State_::Magic { got } => {
                self.magic_[got] = byte;
                self.state_ = if got + 1 == K_MAGIC_LEN {
                    State_::Alg
                } else {
                    State_::Magic { got: got + 1 }
                };
                Consume_::Pending
            }

            State_::Alg => {
                // 算法预告非法必须立即终止：既无法确定 `crc` 长度，也无法定位帧尾。
                let Option::Some(checksum) = HandshakeChecksum::try_header(byte) else {
                    return self.fail_(ProtoError_::UnsupportedOption);
                };
                self.alg_hdr_ = byte;
                self.checksum_len_ = checksum.checksum_len();
                // 校验覆盖 `magic` 与算法预告本身；`magic` 此刻才补喂。
                let mut crc = CrcDigest::new_(&checksum);
                crc.update_(&self.magic_);
                crc.update_(&[byte]);
                self.crc_ = Option::Some(crc);
                self.state_ = State_::EntryHeader;
                Consume_::Pending
            }

            State_::EntryHeader => self.consume_entry_header_(byte),

            State_::EntryValue {
                header,
                idx,
                width,
                got,
            } => {
                self.crc_update_(&[byte]);
                self.value_[got] = byte;
                if got + 1 < width {
                    self.state_ = State_::EntryValue {
                        header,
                        idx,
                        width,
                        got: got + 1,
                    };
                    return Consume_::Pending;
                }
                let Result::Ok(value) = decode_value_(width, &self.value_[..width]) else {
                    return self.fail_(ProtoError_::MalformedBody);
                };
                // 基础项取值为 0 在 v1 中非法（0 表示「未提供」，应当省略该键）。
                if value == 0 {
                    return self.fail_(ProtoError_::MalformedBody);
                }
                let entry = NegotiationBasicEntry {
                    opts_key: header,
                    val_data: value,
                };
                self.basics_[idx as usize] = Option::Some(entry.clone());
                self.state_ = State_::EntryHeader;
                Consume_::Entry(entry)
            }

            State_::ChecksumValue { width, got } => {
                // 校验码本身**不**计入 `crc`（校验覆盖区到校验头为止）。
                self.value_[got] = byte;
                if got + 1 < width {
                    self.state_ = State_::ChecksumValue {
                        width,
                        got: got + 1,
                    };
                    return Consume_::Pending;
                }
                let expect = decode_checksum_(width, &self.value_[..width]);
                // `EntryHeader` 走到这里之前必然已经建好校验状态。
                let computed = match self.crc_.as_ref() {
                    Option::Some(crc) => crc.finalize_(),
                    Option::None => 0u32,
                };
                // 校验一票否决：算出的值与帧尾声明的值不等即整帧失败。
                if computed != expect {
                    return self.fail_(ProtoError_::ChecksumErr);
                }
                self.state_ = State_::Done;
                Consume_::Done
            }
        }
    }

    /// 消费一个**条目头字节**：可能是基础项的头，也可能是校验头（帧尾）。
    fn consume_entry_header_(&mut self, byte: u8) -> Consume_ {
        self.crc_update_(&[byte]);

        let Result::Ok(key) = NegotiationKey::try_from(byte) else {
            return self.fail_(ProtoError_::UnsupportedOption);
        };

        match key {
            NegotiationKey::Checksum => {
                // 先确认取值合法（是三种校验码之一），再确认与算法预告一致。
                let Option::Some(_) = HandshakeChecksum::try_header(byte) else {
                    return self.fail_(ProtoError_::UnsupportedOption);
                };
                if byte != self.alg_hdr_ {
                    return self.fail_(ProtoError_::MalformedBody);
                }
                self.state_ = State_::ChecksumValue {
                    width: self.checksum_len_,
                    got: 0,
                };
                Consume_::Pending
            }

            // 扩展条目在 v1 中是保留键；即便将来启用，也只会走「先校验声明长度、
            // 再准备等长内部缓冲」的路径，不会预分配整帧。
            NegotiationKey::ExtMsg => self.fail_(ProtoError_::UnsupportedOption),

            basic => {
                let Result::Ok(val_type) = NegotiationValType::try_from(byte) else {
                    return self.fail_(ProtoError_::UnsupportedOption);
                };
                let Result::Ok(idx) = basic_key_(basic) else {
                    return self.fail_(ProtoError_::UnsupportedOption);
                };
                let bit = 1u8 << idx;
                if self.seen_ & bit != 0 {
                    return self.fail_(ProtoError_::MalformedBody);
                }
                self.seen_ |= bit;
                self.state_ = State_::EntryValue {
                    header: byte,
                    idx,
                    width: val_type.value_len(),
                    got: 0,
                };
                Consume_::Pending
            }
        }
    }

    /// 把字节计入增量校验（状态未建立时静默跳过）。
    fn crc_update_(&mut self, bytes: &[u8]) {
        if let Option::Some(crc) = self.crc_.as_mut() {
            crc.update_(bytes);
        }
    }

    /// 记下失败结论并终止解析。
    fn fail_(&mut self, err: ProtoError_) -> Consume_ {
        self.state_ = State_::Failed(err);
        Consume_::Failed(err)
    }
}

#[cfg(test)]
mod tests_ {
    use super::*;
    use crate::handshake::{K_ACCEPT_MAGIC, K_INVITE_MAGIC, codec_::write_frame_, opts::NegotiationKey};

    /// 一次喂字节的产出：`(条目列表, 最终结论)`。
    type FeedOutcome_ = (Vec<(u8, usize)>, Option<Result<(), ProtoError_>>);

    /// 把一台状态机喂完整帧，返回「产出过的条目」与最终结论。
    ///
    /// 这是 sans-IO 的价值所在：**完全不接触 IO**，直接把字节喂进去。
    fn feed_frame_(bytes: &[u8]) -> FeedOutcome_ {
        let mut parser = HandshakeParser::new_();
        let mut entries = Vec::new();
        for &byte in bytes {
            if parser.finish_().is_some() {
                // 出结论之后不再喂（真实驱动也是这么停的）。
                break;
            }
            match parser.consume_byte_(byte) {
                Consume_::Pending => {}
                Consume_::Entry(entry) => entries.push((entry.opts_key, entry.val_data)),
                Consume_::Done => {}
                Consume_::Failed(_) => {}
            }
        }
        (entries, parser.finish_())
    }

    /// 造一帧的字节（借用生产侧的 [`write_frame_`]），返回实际长度。
    async fn frame_bytes_(
        buf: &mut [u8],
        magic: MagicField,
        values: &[Option<usize>; K_BASIC_KEY_COUNT],
    ) -> usize {
        use buffex::x_deps::abs_cancel::NonCancellableToken;
        let checksum = crate::handshake::codec_::K_DEFAULT_CHECKSUM;
        let capacity = buf.len();
        // 本函数只在单元测试里跑：这里 `block_on` 一个立即完成的写。
        let mut cursor: &mut [u8] = buf;
        let cancel = NonCancellableToken::new();
        write_frame_(&mut cursor, magic, values, &checksum, cancel)
            .await
            .expect("写帧应当成功");
        capacity - cursor.len()
    }

    /// 测试 sans-IO 状态机在**逐字节**喂入下能解析完整帧。
    /// - 手段：用 `write_frame_` 造一帧，再按**每 1 个字节**调用 `consume_byte_`；
    ///   整个过程中不接触任何 `TrBuffRead`。
    /// - 判断：产出一条基础条目（键与取值正确）、最终结论为 `Ok(())`，
    ///   且 `magic` 与手工构造的期望一致。
    #[test]
    fn sans_io_parser_decodes_byte_by_byte() {
        let bytes = [
            K_INVITE_MAGIC[0],
            K_INVITE_MAGIC[1],
            K_INVITE_MAGIC[2],
            K_INVITE_MAGIC[3],
            0x1Cu8, // 算法预告：CRC-16（val_type = BeU16，key = Checksum）
            0x00u8, // 条目头：BeU8 | MaxPacketSize（val_type 0x00、key 0x00）
            0x07u8, // 值：7
            0x1Cu8, // 校验头
        ];
        let mut parser = HandshakeParser::new_();
        for &b in &bytes {
            let _ = parser.consume_byte_(b);
        }
        // 手写序列的 crc 尚未补上，因此还没结束。
        assert!(parser.is_started_(), "帧首 5 字节之后应当已进入条目区");
        assert_eq!(parser.magic_(), K_INVITE_MAGIC);
        assert_eq!(
            parser.basics_()[0].as_ref().map(|e| e.val_data),
            Option::Some(7usize)
        );
        assert!(parser.finish_().is_none(), "crc 还没喂完，不应出结论");
    }

    /// 测试条目在**校验未完成之前**就能交给调用方（流式协商的前提）。
    /// - 手段：喂到「magic + 算法预告 + 一条基础条目」，此时 crc 尚未读完。
    /// - 判断：已经能取到该条目，且状态机尚未给出终局结论。
    #[test]
    fn entry_is_available_before_frame_completes() {
        let mut parser = HandshakeParser::new_();
        let mut got = Option::None;
        for &b in &[
            K_ACCEPT_MAGIC[0],
            K_ACCEPT_MAGIC[1],
            K_ACCEPT_MAGIC[2],
            K_ACCEPT_MAGIC[3],
            0x1Cu8,
            0x01u8, // BeU8 | MaxChannelCount
            0x03u8,
        ] {
            if let Consume_::Entry(entry) = parser.consume_byte_(b) {
                got = Option::Some(entry);
            }
        }
        assert_eq!(got.map(|e| e.val_data), Option::Some(3usize));
        assert!(parser.is_started_());
        assert!(!parser.is_done_());
        assert!(parser.finish_().is_none());
    }

    /// 测试非法算法预告立即失败，且**消费**了那 1 个字节。
    /// - 手段：喂 4 字节 magic 后喂一个不是 16/24/32 位校验码的字节。
    /// - 判断：结论为 `UnsupportedOption`；重复喂入返回同一结论。
    #[test]
    fn bad_alg_header_fails_immediately() {
        let mut parser = HandshakeParser::new_();
        for &b in &K_INVITE_MAGIC {
            assert!(matches!(parser.consume_byte_(b), Consume_::Pending));
        }
        let out = parser.consume_byte_(0x0Cu8); // CRC-8：v1 不合法
        assert!(matches!(out, Consume_::Failed(ProtoError_::UnsupportedOption)));
        assert_eq!(
            parser.finish_(),
            Option::Some(Result::Err(ProtoError_::UnsupportedOption))
        );
        // 出结论之后重复喂入不再消费、不再改判。
        assert!(matches!(
            parser.consume_byte_(0x00u8),
            Consume_::Failed(ProtoError_::UnsupportedOption)
        ));
    }

    /// 测试重复基础键被拒绝。
    /// - 手段：喂两条 `MaxPacketSize`（头字节 `0x00`）条目。
    /// - 判断：第二条的头字节处即判 `MalformedBody`。
    #[test]
    fn duplicate_basic_key_is_rejected() {
        let mut parser = HandshakeParser::new_();
        let mut last = Consume_::Pending;
        for &b in &[
            K_INVITE_MAGIC[0],
            K_INVITE_MAGIC[1],
            K_INVITE_MAGIC[2],
            K_INVITE_MAGIC[3],
            0x1Cu8,
            0x00u8,
            0x01u8,
            0x00u8, // 重复键
        ] {
            last = parser.consume_byte_(b);
            if let Consume_::Failed(_) = last {
                break;
            }
        }
        assert!(matches!(last, Consume_::Failed(ProtoError_::MalformedBody)));
    }

    /// 测试基础项取值为 0 被拒绝。
    /// - 手段：喂一条值为 0 的 `MaxPacketSize`。
    /// - 判断：值字节收满时判 `MalformedBody`。
    #[test]
    fn zero_valued_basic_entry_is_rejected() {
        let mut parser = HandshakeParser::new_();
        let mut last = Consume_::Pending;
        for &b in &[
            K_INVITE_MAGIC[0],
            K_INVITE_MAGIC[1],
            K_INVITE_MAGIC[2],
            K_INVITE_MAGIC[3],
            0x1Cu8,
            0x00u8,
            0x00u8,
        ] {
            last = parser.consume_byte_(b);
        }
        assert!(matches!(last, Consume_::Failed(ProtoError_::MalformedBody)));
    }

    /// 测试 `crc` 不匹配被识别（一票否决）。
    /// - 手段：造一帧后翻转校验码的最后一个字节。
    /// - 判断：收满校验码时判 `ChecksumErr`。
    #[test]
    fn checksum_mismatch_is_rejected() {
        let mut buf = [0u8; 64];
        let values = [
            Option::Some(7usize),
            Option::None,
            Option::None,
            Option::None,
            Option::None,
        ];
        // 这里需要一个异步写来造帧；用最小的 futures 执行器驱动它。
        let total = futures::executor::block_on(frame_bytes_(&mut buf, K_INVITE_MAGIC, &values));
        // 翻转最后一个 crc 字节。
        buf[total - 1] ^= 0xFF;
        let mut parser = HandshakeParser::new_();
        let mut last = Consume_::Pending;
        for &b in &buf[..total] {
            last = parser.consume_byte_(b);
            if let Consume_::Failed(_) = last {
                break;
            }
        }
        assert!(matches!(last, Consume_::Failed(ProtoError_::ChecksumErr)));
    }

    /// 测试完整帧在逐字节喂入下产出正确的条目集与终局结论。
    /// - 手段：`write_frame_` 造帧（含 CRC-16），逐字节喂给状态机。
    /// - 判断：条目集合与写入值一致，终局为 `Ok(())`。
    #[test]
    fn full_frame_reaches_done() {
        let mut buf = [0u8; 64];
        let values = [
            Option::Some(7usize),
            Option::Some(3usize),
            Option::None,
            Option::None,
            Option::None,
        ];
        let total = futures::executor::block_on(frame_bytes_(&mut buf, K_INVITE_MAGIC, &values));
        let (entries, end) = feed_frame_(&buf[..total]);
        assert_eq!(end, Option::Some(Result::Ok(())));
        assert_eq!(
            entries,
            vec![(0x00u8, 7usize), (0x01u8, 3usize)],
            "条目应按线上顺序产出：头字节 = (val_type << 4) | key，取值宽度取最小合法宽度"
        );
    }

    /// 测试扩展键（v1 保留）被拒绝。
    /// - 手段：喂一条 `ExtMsg`（`key == 0x0E`）的头字节。
    /// - 判断：判 `UnsupportedOption`。
    #[test]
    fn reserved_ext_key_is_rejected() {
        let mut parser = HandshakeParser::new_();
        let mut last = Consume_::Pending;
        for &b in &[
            K_INVITE_MAGIC[0],
            K_INVITE_MAGIC[1],
            K_INVITE_MAGIC[2],
            K_INVITE_MAGIC[3],
            0x1Cu8,
            u8::from(NegotiationValType::BeU8) << 4 | (NegotiationKey::ExtMsg as u8),
        ] {
            last = parser.consume_byte_(b);
        }
        assert!(matches!(last, Consume_::Failed(ProtoError_::UnsupportedOption)));
    }
}
