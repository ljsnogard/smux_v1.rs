//! 复用帧的线格式（sans-IO）。
//!
//! 本模块只描述「字段如何编码进帧、如何从帧还原」，不涉及解复用流程本身——流程
//! 见 [`crate::connection`] 模块文档 §3。
//!
//! # 自描述字段字节
//!
//! 复用帧沿用握手协议（[`crate::handshake::opts`]）的编码策略：每个头字段的第一
//! 个字节同时表达「字段标识」与「值宽度」：
//!
//! ```text
//! header = (val_type << 4) | field_id
//! ```
//!
//! - 低 4 位 `field_id`（[`FieldId`]）：**这个字段是什么**。`LocalDock` 与
//!   `RemoteDock` 是两个不同取值，因此同一个字节就同时说明了「宽度」与「这是哪个
//!   dock」，不需要额外的类型字节；
//! - 高 3 位 `val_type`（复用
//!   [`NegotiationValType`](crate::handshake::opts::NegotiationValType)）：值是
//!   几个字节（1/2/3/4/8，大端）；
//! - bit 7 保留：发送方置 0，接收方按 [`K_VAL_TYPE_MASK`] 掩码忽略。
//!
//! 编码约定与握手一致：**发送方取能容纳数值的最小宽度**，接收方接受任意足够宽的
//! 编码并还原为 `usize`。
//!
//! # 帧形状
//!
//! ```text
//! +-------------------------------+-------------------------------+
//! | 头字段序列（每个自描述）        | 载荷（PayloadLen 字节）        |
//! +-------------------------------+-------------------------------+
//! ```
//!
//! - `Kind` 与 `PayloadLen` 必需；
//! - channel 帧还需 `LocalDock` 与 `RemoteDock`；
//! - `WindowUpdate` 只出现在 `WINDOW_UPDATE` 帧上；
//! - 头字段顺序不承载语义，接收方必须能处理任意合法顺序（与握手 §3 一致）；
//! - 帧总长（头 + 载荷）不得超过协商出的 `max_packet_size`。

// 本模块目前是**骨架**：类型、签名与文档已定稿，方法体统一为 `todo!()`。
// 「字段未被读取」「函数未被调用」属于预期内的过渡状态；实现落地后必须移除
// 本行的 `allow`（见 `dev-notes/` 的待办）。
#![allow(dead_code, unused_variables)]

use abs_buff::{
    TrBuffRead, TrBuffWrite,
    x_deps::abs_cancel,
};
use abs_cancel::TrCancellationToken;

use crate::connection::{Dock, MuxError};
use crate::flow_ctrl::Credit;
use crate::handshake::opts::NegotiationValType as FieldValType;

/// `field_id` 的掩码（低 4 位）。
pub const K_FIELD_ID_MASK: u8 = 0x0F;

/// `val_type` 的掩码（高 3 位，bit 7 保留）。
pub const K_VAL_TYPE_MASK: u8 = 0x70;

/// 头字段标识，占自描述字段字节的低 4 位。
///
/// 与握手的 [`NegotiationKey`](crate::handshake::opts::NegotiationKey) 同构，但
/// 属于复用阶段、取值空间独立。`0x07..=0x0F` 在本版本中保留，接收方遇到即
/// [`MuxError::UnsupportedField`]。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum FieldId {
    /// 帧种类，值见 [`FrameKind`]。必需。
    Kind = 0x00,

    /// 帧标志位，位定义见 [`flags`]。
    Flags = 0x01,

    /// 本端子流端点 dock。channel / datagram 帧必需。
    LocalDock = 0x02,

    /// 对端子流端点 dock。channel / datagram 帧必需。
    RemoteDock = 0x03,

    /// 载荷字节数。必需；`0` 表示无载荷。
    PayloadLen = 0x04,

    /// 接收窗口增量，只在 `WINDOW_UPDATE` 帧上出现，类型见
    /// [`crate::flow_ctrl::WindowUpdate`]。
    WindowUpdate = 0x05,

    /// 拒绝原因码，只在 `REJECT` 帧上出现。
    ReasonCode = 0x06,
}

impl FieldId {
    /// 从自描述字段字节取出 `field_id`。
    pub const fn from_header_(header: u8) -> Option<Self> {
        panic!("按低 4 位匹配 FieldId")
    }

    /// 本标识对应的低 4 位取值。
    pub const fn as_u8(&self) -> u8 {
        (*self as u8) & K_FIELD_ID_MASK
    }

    /// 该字段是否允许用给定的宽度编码。
    ///
    /// dock 字段只允许 `BeU8` / `BeU16` / `BeU32` 三种宽度（模块文档 §4）；
    /// 其它字段当前一律只允许 `BeU16` / `BeU32`（载荷长度可达 `max_packet_size`，
    /// 其余字段的值域很小）。
    pub const fn accepts_val_type_(&self, val_type: FieldValType) -> bool {
        panic!("按字段校验宽度")
    }
}

/// 复用帧的种类，即 `Kind` 字段的取值。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum FrameKind {
    /// 发起方请求建立子流，载荷为随帧附带的「开场消息」。
    Open = 0x01,

    /// 等待方同意建立子流，载荷为欢迎信息。
    Accept = 0x02,

    /// 任一方拒绝建立子流，载荷可携带理由。
    Reject = 0x03,

    /// 关闭某个方向（配合 `flags` 的 `FIN` / `RESET`）。
    Close = 0x04,

    /// 子流数据。
    Data = 0x05,

    /// 数据报（telegraph）载荷，无需建流。
    Datagram = 0x06,

    /// 接收窗口更新。
    WindowUpdate = 0x07,

    /// 保活探测。
    Ping = 0x08,

    /// 保活应答。
    Pong = 0x09,
}

impl TryFrom<u8> for FrameKind {
    type Error = u8;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        todo!("按取值匹配 FrameKind")
    }
}

impl From<FrameKind> for u8 {
    fn from(value: FrameKind) -> Self {
        value as u8
    }
}

/// 帧标志位定义（`Flags` 字段的位）。
pub mod flags {
    /// 本方向不再发送数据（半关闭）。
    pub const K_FIN: u8 = 0b0000_0001;

    /// 立即终止子流，丢弃未交付数据。
    pub const K_RESET: u8 = 0b0000_0010;

    /// 应答标记（`ACCEPT` / `PONG` 等）。
    pub const K_ACK: u8 = 0b0000_0100;

    /// 本帧不含载荷，仅承载控制信息。
    pub const K_NO_PAYLOAD: u8 = 0b0000_1000;
}

/// 一个已解析的帧头。
///
/// 只保留解复用与流控需要的字段；未知 / 保留字段由解析层直接拒绝，因此这里没有
/// 「扩展字段」存放区——将来扩展时新增字段标识与对应槽位。
#[derive(Debug, Clone, Copy)]
pub struct FrameHeader {
    kind_: FrameKind,
    flags_: u8,
    local_dock_: Dock,
    remote_dock_: Dock,
    payload_len_: usize,
    window_update_: Option<Credit>,
}

impl FrameHeader {
    /// 帧种类。
    pub const fn kind(&self) -> FrameKind {
        self.kind_
    }

    /// 原始标志位；用 [`flags`] 的常量做位测试。
    pub const fn flags(&self) -> u8 {
        self.flags_
    }

    /// 本端 dock。
    pub const fn local_dock(&self) -> Dock {
        self.local_dock_
    }

    /// 对端 dock。
    pub const fn remote_dock(&self) -> Dock {
        self.remote_dock_
    }

    /// 载荷字节数。
    pub const fn payload_len(&self) -> usize {
        self.payload_len_
    }

    /// 接收窗口增量（仅 `WINDOW_UPDATE` 帧为 `Some`）。
    pub const fn window_update(&self) -> Option<Credit> {
        self.window_update_
    }

    /// 是否带 `FIN` 标志。
    pub const fn is_fin(&self) -> bool {
        self.flags_ & flags::K_FIN != 0
    }

    /// 是否带 `RESET` 标志。
    pub const fn is_reset(&self) -> bool {
        self.flags_ & flags::K_RESET != 0
    }
}

/// 组装自描述字段字节：`header = (val_type << 4) | field_id`。
pub(crate) const fn compose_field_header_(id: FieldId, val_type: FieldValType) -> u8 {
    panic!("按位或出字段字节")
}

/// 取能容纳 `value` 的最小宽度（规范化编码）。
///
/// 与握手侧 [`min_val_type_`](crate::handshake::opts) 同一策略；dock 字段另由
/// [`FieldId::accepts_val_type_`] 限制到 1 / 2 / 4 字节。
pub(crate) const fn min_field_val_type_(value: usize) -> FieldValType {
    panic!("按数值取最小宽度")
}

/// 从网络读半边解析出一个帧头。
///
/// 头字段逐个解析到 `Kind` / `PayloadLen` 齐全为止；随后由调用方按
/// [`FrameHeader::payload_len`] 读取载荷。任一步失败即整帧失败（不保留部分状态）。
///
/// # Errors
///
/// - 未知 / 保留字段标识或非法宽度 → [`MuxError::UnsupportedField`]；
/// - 缺少必需字段、字段重复、`Kind` 与字段组合不符 → [`MuxError::MalformedFrame`]；
/// - 底层读失败 / 对端关闭 → [`MuxError::Rx`] / [`MuxError::PeerClosed`]。
pub(crate) async fn read_header_async_<R, C>(
    rx: &mut R,
    cancel: C,
) -> Result<FrameHeader, MuxError<R::Err, ()>>
where
    R: TrBuffRead<u8>,
    C: TrCancellationToken,
{
    todo!("逐字段解析帧头")
}

/// 把一个帧头写成自描述字段序列。
///
/// 只写头，不写载荷；调用方随后把载荷字节直接拼上。
///
/// # Errors
///
/// 字段宽度越界（例如 dock 超过 `u32`）→ [`MuxError::UnsupportedField`]；
/// 底层写失败 → [`MuxError::Tx`]。
pub(crate) async fn write_header_async_<W, C>(
    tx: &mut W,
    header: &FrameHeader,
    cancel: C,
) -> Result<(), MuxError<(), W::Err>>
where
    W: TrBuffWrite<u8>,
    C: TrCancellationToken,
{
    todo!("把帧头逐字段写出")
}
