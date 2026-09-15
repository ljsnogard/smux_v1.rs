//! 握手协商项的类型定义与线格式映射。
//!
//! 本模块只描述「协商项如何编码进帧、如何从帧还原」，不涉及握手流程本身
//! ——流程见 [`crate::handshake`] 模块文档（§4 帧格式、§6 条目编码、§7 协商
//! 语义）。
//!
//! # 与线格式的对应关系
//!
//! 帧 = `magic(4B)` + `算法预告(1B)` + `条目区` + `校验头(1B)` + `crc(2/3/4B)`
//! （见模块文档 §4）。条目区由若干
//! **条目（Entry）**顺序拼接而成，每个条目的第一个字节是
//! `header = (val_type << 4) | key`：
//!
//! - 低半字节 `key` 对应 [`NegotiationKey`]；
//! - 高半字节 `val_type` 对应 [`NegotiationValType`]，决定后续 `value` 的
//!   字节数（1/2/3/4/8，大端）。
//!
//! 因此每个条目的解析必然先读 1 字节 `header`，再按 `val_type` 决定 `value`
//! 宽度；[`NegotiationBasicEntry::opts_key`] 保存的就是这个**完整 header
//! 字节**，而不是仅低半字节的裸 key。
//!
//! [`NegotiationKey::Checksum`]（`0x0C`）的头字节编码在帧中出现**三处**，其中
//! 前两处是同一个字节（算法预告与校验头），合法取值只有 `0x1C`（CRC-16/XMODEM，
//! 2 字节）、`0x2C`（CRC-24/BLE，3 字节）与 `0x3C`（CRC-32/ISO-HDLC，4 字节）。
//! 校验头是条目区的**定界符**，条目区没有长度字段。
//!
//! # 编码约定
//!
//! - 所有多字节整数均为大端无符号数。
//! - 发送方必须使用能容纳数值的**最小** `val_type` 宽度；接收方必须接受任意
//!   足够宽的编码，并把它还原为 `usize`。
//! - 除基础键（`0x00..=0x03`）与校验键（`0x0C`）外的键在 v1 中一律非法；接收
//!   方遇到保留键或未识别的键时必须拒绝该帧（见模块文档 §6.3）。

use core::{borrow::Borrow, time::Duration};

/// 一次成功握手产出的协商结果。
///
/// 目前只包含四项基础协商项的最终取值。
#[derive(Debug, Clone)]
pub struct HandshakeOpts {
    /// 协商后的四项基础配置。
    pub basic_opts: BasicOpts,
    // 扩展条目暂缓实现：恢复时在此加回扩展条目存放区与有效数量字段，见
    // dev-notes.md（仓库根目录）。
}

/// 基础协商结果，即四项基础协商项的最终取值。
///
/// 各字段在网络上的单位、合法范围与协商流程见 [`crate::handshake`] 模块文档
/// §7.1。
///
/// # Examples
///
/// 直接使用协议缺省值：
///
/// ```
/// use smux_v1::handshake::opts::BasicOpts;
///
/// assert_eq!(BasicOpts::default().max_packet_size, 4096usize);
/// ```
#[derive(Debug, Clone)]
pub struct BasicOpts {
    /// 分片传输过程中最大报文大小（含头部）
    pub max_packet_size: usize,

    /// 单个连接上最大同时活动的 channel 数量
    pub max_channel_count: usize,

    /// 单个 dock 能容纳的最大同时活动的 channel 数量
    pub max_dock_chan_count: usize,

    /// 一个 Channel 在无任何数据交流后的最长存活时间。
    ///
    /// # Discussion
    /// 不断地发送心跳报文（ACK）可以无限地延长 channel 存活时间，直到有一端主动关闭。
    pub max_channel_timeout: Duration,
}

impl BasicOpts {
    /// 协议缺省协商值。
    ///
    /// 当某一方在 `INVITE` / `ACCEPT` 中未显式给出某项时，以本常量为本地缺省
    /// 值参与补全（见 [`crate::handshake`] 模块文档 §7.1、§7.2）。
    ///
    /// **注意**：`max_channel_count` 与 `max_dock_chan_count` 取
    /// `1usize << 28`，可在 32 位 `usize` 上表示。但线格式允许 `BeU64`，而
    /// 本结构以 `usize` 承载，32 位平台上超过 `usize::MAX` 的取值必须在解码
    /// 时报错，不得截断。
    pub const DEFAULT: BasicOpts = BasicOpts {
        max_packet_size: 4096usize,
        max_channel_count: 1usize << 28,
        max_dock_chan_count: 1usize << 28,
        max_channel_timeout: Duration::from_secs(30u64),
    };

    /// 从「基础条目」迭代器还原 [`BasicOpts`]，未出现的项保留
    /// [`BasicOpts::DEFAULT`]。
    ///
    /// 每个条目的 [`NegotiationBasicEntry::opts_key`] 同时携带 `key` 与
    /// `val_type`；本函数只按低半字节的 `key` 分派，因此调用方可以传入任意
    /// 合法宽度的条目。校验尾（`Checksum`）在成帧层就被消费，不会传到这里。
    ///
    /// # Errors
    ///
    /// 当某个条目的低半字节不是本版本认识的基础键时返回该条目。按模块文档
    /// §6.4，未知与保留键必须导致握手失败。
    pub fn from_entries<I>(entries_iter: I) -> Result<Self, NegotiationBasicEntry>
    where
        I: Iterator<Item: Borrow<NegotiationBasicEntry>>,
    {
        let mut x = BasicOpts::DEFAULT;
        for entry in entries_iter {
            let Result::Ok(key) = NegotiationKey::try_from(entry.borrow().opts_key) else {
                return Result::Err(entry.borrow().clone());
            };
            let entry = entry.borrow();
            match key {
                NegotiationKey::MaxPacketSize => x.max_packet_size = entry.val_data,
                NegotiationKey::MaxChannelCount => x.max_channel_count = entry.val_data,
                NegotiationKey::MaxDockChanCount => x.max_dock_chan_count = entry.val_data,
                NegotiationKey::MaxChannelTimeout => {
                    x.max_channel_timeout = Duration::from_secs(entry.val_data as u64)
                }
                _ => (),
            }
        }
        Result::Ok(x)
    }
}

impl Default for BasicOpts {
    fn default() -> Self {
        BasicOpts::DEFAULT
    }
}

impl<'a> core::iter::IntoIterator for &'a BasicOpts {
    type Item = NegotiationEntry<'a>;
    type IntoIter = core::array::IntoIter<NegotiationEntry<'a>, K_BASIC_KEY_COUNT>;

    fn into_iter(self) -> Self::IntoIter {
        const KEYS: [NegotiationKey; K_BASIC_KEY_COUNT] = [
            NegotiationKey::MaxPacketSize,
            NegotiationKey::MaxChannelCount,
            NegotiationKey::MaxDockChanCount,
            NegotiationKey::MaxChannelTimeout,
        ];
        let items: [NegotiationEntry<'a>; K_BASIC_KEY_COUNT] = core::array::from_fn(|i| {
            let value = match KEYS[i] {
                NegotiationKey::MaxPacketSize => self.max_packet_size,
                NegotiationKey::MaxChannelCount => self.max_channel_count,
                NegotiationKey::MaxDockChanCount => self.max_dock_chan_count,
                NegotiationKey::MaxChannelTimeout => self.max_channel_timeout.as_secs() as usize,
                _ => 0usize,
            };
            NegotiationEntry::Basic(NegotiationBasicEntry {
                opts_key: u8::from(KEYS[i]) | u8::from(min_val_type_(value)),
                val_data: value,
            })
        });
        items.into_iter()
    }
}

/// 基础项个数，等于 [`NegotiationKey`] 中基础键的数量。
pub(crate) const K_BASIC_KEY_COUNT: usize = 4;

/// 取能容纳 `value` 的最小 [`NegotiationValType`]（规范化编码，见 §6）。
pub(crate) const fn min_val_type_(value: usize) -> NegotiationValType {
    if value <= u8::MAX as usize {
        NegotiationValType::BeU8
    } else if value <= u16::MAX as usize {
        NegotiationValType::BeU16
    } else if value <= 0x00FF_FFFFusize {
        NegotiationValType::BeU24
    } else if value <= u32::MAX as usize {
        NegotiationValType::BeU32
    } else {
        NegotiationValType::BeU64
    }
}

/// 协商项的种类（名称），占用条目 `header` 字节的**低四位**。
///
/// `NegotiationKey` 与 [`NegotiationValType`] 共用同一个 `header` 字节：
/// `header = (val_type << 4) | key`。解析时用 [`NegotiationKey::try_from`] 取
/// 低半字节；组装时用 `u8::from(key)` 与 `u8::from(val_type)` 做按位或。
///
/// 各键的线格式含义见 [`crate::handshake`] 模块文档 §6、§7。
#[derive(Debug, Clone, Copy)]
#[repr(u8)]
pub enum NegotiationKey {
    /// 单个报文最大字节数（含头部），`value` 为定长无符号整数。
    MaxPacketSize = 0x00,

    /// 单连接最大同时活动 channel 数。
    MaxChannelCount = 0x01,

    /// 单 dock 最大同时活动 channel 数。
    MaxDockChanCount = 0x02,

    /// channel 无数据后的最长存活秒数。
    MaxChannelTimeout = 0x03,

    /// 校验键。
    ///
    /// 该键**不是协商项**。它的头字节编码（高半字节 `val_type` 给出算法与
    /// `crc` 长度，低半字节固定为本键 `0x0C`）在帧中出现**三处**，其中前两处
    /// 是同一个头字节的两种角色：
    ///
    /// - `magic` 之后的 **1 字节算法预告**：让接收方能增量起步算校验，见 §4.2；
    /// - 帧尾的 **1 字节校验头**：**定界符**，宣告条目区结束、`crc` 开始，
    ///   见 §4.3 与 §6.2；
    /// - 帧尾紧随其后的 **`crc` 值**：裸的大端数值，长度由算法预告决定。
    ///
    /// 合法算法只有 `0x1C`（CRC-16/XMODEM，2 字节）、`0x2C`（CRC-24/BLE，
    /// 3 字节）与 `0x3C`（CRC-32/ISO-HDLC，4 字节）；其它取值非法。校验头
    /// **必须与算法预告一致**，否则整帧按非法处理。
    Checksum = 0x0C,

    /// 扩展条目暂缓实现：0x0E 暂按保留键处理，见 dev-notes.md（仓库根目录）。
    /// 扩展消息，`value` 为「长度字段 + 负载」。
    ExtMsg = 0x0E,
}

impl NegotiationKey {
    /// 低四位掩码：从 `header` 取出键。
    pub(crate) const MASK: u8 = 0x0F;
}

impl TryFrom<u8> for NegotiationKey {
    type Error = u8;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value & NegotiationKey::MASK {
            0x00 => Result::Ok(NegotiationKey::MaxPacketSize),
            0x01 => Result::Ok(NegotiationKey::MaxChannelCount),
            0x02 => Result::Ok(NegotiationKey::MaxDockChanCount),
            0x03 => Result::Ok(NegotiationKey::MaxChannelTimeout),
            0x0C => Result::Ok(NegotiationKey::Checksum),
            0x0E => Result::Ok(NegotiationKey::ExtMsg),
            _ => Result::Err(value),
        }
    }
}

impl From<NegotiationKey> for u8 {
    fn from(v: NegotiationKey) -> Self {
        (v as u8) & NegotiationKey::MASK
    }
}

impl core::ops::BitOr<NegotiationValType> for NegotiationKey {
    type Output = u8;

    fn bitor(self, rhs: NegotiationValType) -> Self::Output {
        let key: u8 = self.into();
        let typ: u8 = rhs.into();
        key | typ
    }
}

/// 协商项的值类型，占用条目 `header` 字节的**高四位**。
///
/// `NegotiationKey` 与 `NegotiationValType` 共用同一个 `header` 字节：
/// `header = (val_type << 4) | key`。除 [`NegotiationKey::Checksum`] 外，本类型
/// 只决定 `value` 字段的字节数；对校验键 `Checksum` 而言，它同时表示校验算法：
/// 只允许 `BeU16`（CRC-16/XMODEM，2 字节）、`BeU24`（CRC-24/BLE，3 字节）与
/// `BeU32`（CRC-32/ISO-HDLC，4 字节），见 [`crate::handshake`] 模块文档 §6.2。
///
/// 编码规则：
///
/// - 发送方必须使用能容纳数值的**最小**宽度；
/// - 接收方必须接受任意足够宽的编码；
/// - `header` 的 bit 6、bit 7 保留，发送方置 0，接收方按 `0x30` 掩码忽略。
#[derive(Debug, Clone, Copy)]
#[repr(u8)]
pub enum NegotiationValType {
    /// `value` 为 1 字节无符号整数；校验尾语义下表示 CRC-8（非法）。
    BeU8 = 0x00,

    /// `value` 为 2 字节大端无符号整数；校验尾语义下表示 CRC-16/XMODEM。
    BeU16 = 0x10,

    /// `value` 为 3 字节大端无符号证书；校验尾语义下表示 CRC-24/BLE
    BeU24 = 0x20,

    /// `value` 为 4 字节大端无符号整数；校验尾语义下表示 CRC-32/ISO-HDLC。
    BeU32 = 0x30,

    /// `value` 为 8 字节大端无符号整数；校验尾语义下表示 CRC-64（非法）。
    BeU64 = 0x40,
}

impl NegotiationValType {
    const MASK: u8 = 0x70;

    /// 在传输中占用的字节长度
    pub const fn value_len(&self) -> usize {
        match self {
            NegotiationValType::BeU8 => 1usize,
            NegotiationValType::BeU16 => 2usize,
            NegotiationValType::BeU24 => 3usize,
            NegotiationValType::BeU32 => 4usize,
            NegotiationValType::BeU64 => 8usize,
        }
    }
}

impl TryFrom<u8> for NegotiationValType {
    type Error = u8;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value & NegotiationValType::MASK {
            0x00 => Result::Ok(NegotiationValType::BeU8),
            0x10 => Result::Ok(NegotiationValType::BeU16),
            0x20 => Result::Ok(NegotiationValType::BeU24),
            0x30 => Result::Ok(NegotiationValType::BeU32),
            0x40 => Result::Ok(NegotiationValType::BeU64),
            _ => Result::Err(value),
        }
    }
}

impl From<NegotiationValType> for u8 {
    fn from(v: NegotiationValType) -> Self {
        (v as u8) & NegotiationValType::MASK
    }
}

/// 一个已解析的协商条目。
///
/// 帧的条目区由若干条目顺序拼接而成（见模块文档 §6），本枚举是它们在 Rust 侧
/// 的表示：
///
/// - [`NegotiationEntry::Basic`]：4 个基础键之一，值是定长整数；
/// - [`NegotiationEntry::Extended`]：扩展条目。**v1 中扩展条目是保留键**，读侧
///   遇到即 `UnsupportedOption`，因此该变体目前只用于表示「将来的形状」，
///   见仓库根目录 `dev-notes.md`。
#[derive(Debug)]
pub enum NegotiationEntry<'a> {
    /// 基础条目（`key ∈ 0x00..=0x03`）。
    Basic(NegotiationBasicEntry),

    /// 扩展条目（`key == 0x0E`）；v1 中尚未启用。
    Extended(NegotiationExtEntry<'a>),
}

/// 基础协商事项的键值对，对应线格式中的一个定长条目。
///
/// ```text
/// +------------------+-------------------------------+
/// | opts_key (1 B)   | value (W B, BE)               |
/// +------------------+-------------------------------+
/// opts_key = (val_type << 4) | key
/// W        = 1/2/4/8（由 val_type 决定）
/// ```
#[derive(Clone, Debug)]
pub struct NegotiationBasicEntry {
    /// 条目的完整 `header` 字节：高四位为 [`NegotiationValType`]，低四位为
    /// [`NegotiationKey`]。**不是**裸 `key`。
    pub opts_key: u8,

    /// 已解码的大端无符号数值。解码方必须接受非规范宽度并把它扩展为
    /// `usize`；编码方必须选择能容纳该数值的最小宽度。
    pub val_data: usize,
}

/// 扩展协商事项，对应线格式中的一个「长度 + 负载」条目。
/// 只用于握手协商时，作为构造条目的参考。
///
/// ```text
/// +---------------+------------------+------------------+
/// | len_type (1B) | length (W B, BE) | msg_data (L B)   |
/// +---------------+------------------+------------------+
/// len_type = (len_val_type << 4) | 0x0E
/// W        = 1/2/4/8（由 len_type 的高半字节决定）
/// L        = len_data
/// ```
///
/// 负载 `msg_data` 对握手内核不透明：内核只负责长度合法、CRC 正确，并在
/// `ACCEPT` / `CONFIRM` 中逐字节原样回显。
#[derive(Debug)]
pub struct NegotiationExtEntry<'a> {
    header_: u8,
    data_len_: usize,

    /// 负载首字节的原始指针；配合 `_mark_lt_` 表达「借用 `'a`」。
    ///
    /// 扩展条目在 v1 中是保留键（见 `dev-notes.md`），因此本字段目前不会被
    /// 读取，属于已定义但尚未启用的读取路径。
    #[allow(dead_code)]
    ext_data_: *const u8,
    _mark_lt_: core::marker::PhantomData<&'a [u8]>,
}

impl<'a> NegotiationExtEntry<'a> {
    /// 扩展条目在 v1 中是保留键，读取路径尚未启用；保留构造入口供后续实现。
    #[allow(dead_code)]
    pub(crate) const fn new(header: u8, data_len: usize, ext_data: *const u8) -> Self {
        NegotiationExtEntry {
            header_: header,
            data_len_: data_len,
            ext_data_: ext_data,
            _mark_lt_: core::marker::PhantomData,
        }
    }

    /// 扩展条目的完整 `header` 字节（低半字节固定为 `0x0E`）。
    pub const fn header(&self) -> u8 {
        self.header_
    }

    /// 负载的声明长度（字节数）。
    pub const fn data_len(&self) -> usize {
        self.data_len_
    }

    /// 扩展负载字节。
    ///
    /// # Panics
    ///
    /// 扩展条目在 v1 中是保留键，负载读取路径尚未启用，因此本方法目前总是
    /// panic。恢复扩展条目时会把定长内部缓冲接上（见仓库根目录
    /// `dev-notes.md` 的待办）。
    pub fn ext_data(&self) -> &[u8] {
        todo!("扩展条目在 v1 中尚未启用")
    }
}

#[cfg(test)]
mod tests_ {}
