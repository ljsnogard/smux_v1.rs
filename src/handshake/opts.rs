//! 握手协商项的类型定义与线格式映射。
//!
//! 本模块只描述「协商项如何编码进帧、如何从帧还原」，不涉及握手流程本身
//! ——流程见 [`crate::handshake`] 模块文档（§4 帧格式、§6 条目编码、§7 协商
//! 语义）。
//!
//! # 与线格式的对应关系
//!
//! 帧 = `magic(4B)` + `条目区` + `校验尾`（见模块文档 §4）。条目区由若干
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
//! [`NegotiationKey::Checksum`]（`0x0C`）的 `value` 布局特殊：它是**校验尾**，
//! 只有 `0x1C`（CRC-16/XMODEM，2 字节）与 `0x2C`（CRC-32/ISO-HDLC，4 字节）
//! 两种合法形式，且必须是帧的最后一个条目。
//!
//! # 编码约定
//!
//! - 所有多字节整数均为大端无符号数。
//! - 发送方必须使用能容纳数值的**最小** `val_type` 宽度；接收方必须接受任意
//!   足够宽的编码，并把它还原为 `usize`。
//! - 除基础键（`0x00..=0x03`）与校验尾（`0x0C`）外的键一律非法；接收方遇到
//!   保留键或未识别的键时必须拒绝该帧（见模块文档 §6.4）。

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

    /// 帧尾校验标记。
    ///
    /// 该键**不是协商项**，而是帧的定界符：以它为键的条目是帧的最后一个条目，
    /// 且必须恰好出现一次。头字节只允许 `0x1C`（`BeU16` + CRC-16/XMODEM，
    /// `value` 2 字节）与 `0x2C`（`BeU32` + CRC-32/ISO-HDLC，`value` 4 字节）；
    /// 其它取值非法。详见模块文档 §6.2。
    Checksum = 0x0C,
    // 扩展条目暂缓实现：0x0E 暂按保留键处理，见 dev-notes.md（仓库根目录）。
    // /// 扩展消息，`value` 为「长度字段 + 负载」。
    // ExtMsg            = 0x0E,
}

impl NegotiationKey {
    const MASK: u8 = 0x0F;
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
            // 扩展条目暂缓实现。
            // 0x0E => Result::Ok(NegotiationKey::ExtMsg),
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
/// 只决定 `value` 字段的字节数；对校验尾 `Checksum` 而言，它同时表示校验算法：
/// 只允许 `BeU16`（CRC-16/XMODEM，2 字节）与 `BeU32`（CRC-32/ISO-HDLC，
/// 4 字节），见 [`crate::handshake`] 模块文档 §6.2。
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

/// 基础协商事项的键值对，对应线格式中的一个定长条目。
///
/// ```text
/// +------------------+-------------------------------+
/// | opts_key (1 B)   | value (W B, BE)               |
/// +------------------+-------------------------------+
/// opts_key = (val_type << 4) | key
/// W        = 1/2/4/8（由 val_type 决定）
/// ```
#[derive(Debug, Clone)]
pub struct NegotiationBasicEntry {
    /// 条目的完整 `header` 字节：高四位为 [`NegotiationValType`]，低四位为
    /// [`NegotiationKey`]。**不是**裸 `key`。
    pub opts_key: u8,

    /// 已解码的大端无符号数值。解码方必须接受非规范宽度并把它扩展为
    /// `usize`；编码方必须选择能容纳该数值的最小宽度。
    pub val_data: usize,
}

// 扩展条目暂缓实现：以下类型保留备查，待扩展条目设计定稿后再恢复。
// 相关讨论见 dev-notes.md（仓库根目录）。
//
// /// 扩展协商事项，对应线格式中的一个「长度 + 负载」条目。
// ///
// /// ```text
// /// +---------------+------------------+------------------+
// /// | len_type (1B) | length (W B, BE) | msg_data (L B)   |
// /// +---------------+------------------+------------------+
// /// len_type = (len_val_type << 4) | 0x0E
// /// W        = 1/2/4/8（由 len_type 的高半字节决定）
// /// L        = len_data
// /// ```
// ///
// /// 负载 `msg_data` 对握手内核不透明：内核只负责长度合法、CRC 正确，并在
// /// `ACCEPT` / `CONFIRM` 中逐字节原样回显。
// #[derive(Debug, Clone)]
// pub struct NegotiationExtEntry<'a> {
//     /// 长度字段所在条目的完整 `header` 字节：低半字节固定为
//     /// `NegotiationKey::ExtMsg`（`0x0E`），高半字节给出长度字段宽度。
//     pub len_type: u8,
//
//     /// 负载字节数 `L`，即 `msg_data.len()`。
//     pub len_data: usize,
//
//     /// 不透明的扩展负载，通常零拷贝借用读缓冲。
//     pub msg_data: &'a [u8],
// }

#[cfg(test)]
mod tests_ {}
