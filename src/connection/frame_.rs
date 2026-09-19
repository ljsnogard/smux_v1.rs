//! 复用帧的线格式（sans-IO）。
//!
//! 本模块只描述「字段如何编码进帧、如何从帧还原」，不涉及解复用流程本身——流程
//! 见 [`crate::connection`] 模块文档 §3。
//!
//! # 帧首字节：`kind` 与 `flags` 合并
//!
//! 帧的第一个字节是**位置绑定**的合成字节，同时表达帧种类与标志位：
//!
//! ```text
//! head = (flags << 4) | kind
//! ```
//!
//! - 低 4 位 `kind`（[`FrameKind`]）：取值 `0x01..=0x09`；`0x00` 与 `0x0A..=0x0F`
//!   保留，接收方遇到即 [`MuxError::UnsupportedField`]；
//! - 高 4 位 `flags`（[`flags`]）：恰好用满 4 位（`FIN` / `RESET` / `ACK` /
//!   `NO_PAYLOAD`）。
//!
//! 之所以合并：`kind` 与 `flags` 是**每一帧都有**的定长信息，各自单列一个自描述
//! 字段要占掉「1 个头字节 + 至少 1 个值字节」，合并后只占 1 字节。代价是它们不再
//! 是「自描述、可换序」的字段，而是固定首字节。
//!
//! # 其余字段：自描述字段字节
//!
//! 其余头字段沿用握手协议（[`crate::handshake::opts`]）的编码策略：字段的第一个
//! 字节同时表达「字段标识」与「值宽度」：
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
//! 编码并还原为 `usize`。唯一的例外是 dock 字段：它只接受 1 / 2 / 4 字节
//! （[`FieldId::accepts_val_type_`]），因为 `Dock` 的数值语义是规范的，3 字节与
//! 8 字节宽度没有对应类型。
//!
//! # 帧形状
//!
//! ```text
//! +----------+--------------------------------+----------------------+
//! | 首字节    | 自描述字段序列                  | 载荷                  |
//! | kind+flag| （以 PayloadLen 字段收尾）      | （PayloadLen 字节）   |
//! +----------+--------------------------------+----------------------+
//! ```
//!
//! - `PayloadLen` 必需，且**必须是最后一个头字段**：接收方读到它即知头结束、其
//!   后就是载荷，因此它同时充当「头字段序列的定界符」——帧头不需要额外的长度
//!   前缀。反之，若某个字段出现在 `PayloadLen` 之后，接收方会把它当作载荷的一
//!   部分（字节流上没有可回退的边界，这是本格式的固有约定）；
//! - 其余字段的顺序不承载语义，接收方必须能处理任意合法顺序（与握手 §3 一致）；
//! - `LocalDock` / `RemoteDock` 这两个字段合起来**就是**子流身份，因此帧头没有
//!   channel id 字段（理由见 [`crate::connection`] 模块文档 §4.1）；接收方靠
//!   「本地 dock + 来源 dock」定位子流。除 `PING` / `PONG` 这两个连接级帧外，
//!   所有帧都必须带这对字段（`WINDOW_UPDATE` 也要，否则写会话无法知道该更新属于
//!   哪条子流）；`PING` / `PONG` 则**不得**带，解析结果里两个 dock 都是
//!   [`Dock::unspecified`]（`0` 是保留的特殊值，不是合法子流 dock）；
//! - `WindowUpdate` 只出现在 `WINDOW_UPDATE` 帧上，且是该帧的必需字段；
//! - `ReasonCode` 只允许出现在 `REJECT` 帧上。当前实现只**校验**它（取值必须能
//!   装进 `u8`），不把它存进 [`FrameHeader`]：`abs_smux` 的
//!   `reject_async(reason)` 把拒绝理由当作**载荷**传递，因此这个数值字段目前没有
//!   消费者，发送侧也不产出它；
//! - 帧总长（头 + 载荷）不得超过协商出的 `max_packet_size`。校验由读会话完成
//!   （帧长上限来自 [`BasicOpts`](crate::handshake::opts::BasicOpts)，而本模块的
//!   编解码入口只管子结构），超限即 [`MuxError::FrameTooLarge`]。

// 本模块的实现尚未被读写会话调用（会话落地见 `dev-notes/` 的置顶计划第 5 步），
// 因此这里保留 `dead_code` 允许；**第 5 步完成后必须移除本行**。
#![allow(dead_code)]

use abs_buff::{TrBuffRead, TrBuffWrite, x_deps::abs_cancel};
use abs_cancel::TrCancellationToken;

use crate::connection::{Dock, MuxError};
use crate::flow_ctrl::Credit;
use crate::handshake::opts::NegotiationValType as FieldValType;
use crate::wire_io_::{CursorError, ReadCursor, write_all_async_};

/// `kind` 在帧首字节中的掩码（低 4 位）。
pub const K_KIND_MASK: u8 = 0x0F;

/// `flags` 在帧首字节中的掩码（高 4 位）。
pub const K_FLAGS_MASK: u8 = 0xF0;

/// `flags` 在帧首字节中的左移位数。
pub const K_FLAGS_SHIFT: u32 = 4;

/// `field_id` 的掩码（低 4 位）。
pub const K_FIELD_ID_MASK: u8 = 0x0F;

/// `val_type` 的掩码（高 3 位，bit 7 保留）。
pub const K_VAL_TYPE_MASK: u8 = 0x70;

/// 头字段标识，占自描述字段字节的低 4 位。
///
/// 与握手的 [`NegotiationKey`](crate::handshake::opts::NegotiationKey) 同构，但
/// 属于复用阶段、取值空间独立。`Kind` 与 `Flags` **不在**此列：它们合并进了帧首
/// 字节（见模块文档）。`0x05..=0x0F` 在本版本中保留，接收方遇到即
/// [`MuxError::UnsupportedField`]。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum FieldId {
    /// 本端子流端点 dock。除 `PING` / `PONG` 外的帧必需。
    LocalDock = 0x00,

    /// 对端子流端点 dock。除 `PING` / `PONG` 外的帧必需。
    RemoteDock = 0x01,

    /// 载荷字节数。必需，且必须是最后一个头字段；`0` 表示无载荷。
    PayloadLen = 0x02,

    /// 接收窗口增量，只在 `WINDOW_UPDATE` 帧上出现，类型见
    /// [`crate::flow_ctrl::WindowUpdate`]。
    WindowUpdate = 0x03,

    /// 拒绝原因码，只在 `REJECT` 帧上出现；当前只校验、不保存（见模块文档）。
    ReasonCode = 0x04,
}

impl FieldId {
    /// 从自描述字段字节取出 `field_id`；保留取值返回 `None`。
    pub const fn from_header_(header: u8) -> Option<Self> {
        match header & K_FIELD_ID_MASK {
            0x00 => Option::Some(FieldId::LocalDock),
            0x01 => Option::Some(FieldId::RemoteDock),
            0x02 => Option::Some(FieldId::PayloadLen),
            0x03 => Option::Some(FieldId::WindowUpdate),
            0x04 => Option::Some(FieldId::ReasonCode),
            _ => Option::None,
        }
    }

    /// 本标识对应的低 4 位取值。
    pub const fn as_u8(&self) -> u8 {
        (*self as u8) & K_FIELD_ID_MASK
    }

    /// 该字段是否允许用给定的宽度编码。
    ///
    /// dock 字段只允许 `BeU8` / `BeU16` / `BeU32` 三种宽度（模块文档）；其余字段
    /// 接受所有能容纳其数值的宽度——发送方仍取**最小**宽度（模块文档「编码约定
    /// 与握手一致」），宽度上限只受数值本身约束（`PayloadLen` 因此可以超过
    /// `u16`，甚至在必要时用 `BeU64`）。
    pub const fn accepts_val_type_(&self, val_type: FieldValType) -> bool {
        match self {
            FieldId::LocalDock | FieldId::RemoteDock => matches!(
                val_type,
                FieldValType::BeU8 | FieldValType::BeU16 | FieldValType::BeU32
            ),
            FieldId::PayloadLen | FieldId::WindowUpdate | FieldId::ReasonCode => true,
        }
    }
}

/// 复用帧的种类，即帧首字节低 4 位的取值。
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

    /// 保活探测（连接级，不带 dock 对）。
    Ping = 0x08,

    /// 保活应答（连接级，不带 dock 对）。
    Pong = 0x09,
}

impl FrameKind {
    /// 本种类是否携带 `LocalDock` / `RemoteDock` 字段。
    ///
    /// 只有 `PING` / `PONG` 是连接级帧，其余都是子流作用域（含
    /// `WINDOW_UPDATE`，见模块文档）。
    pub const fn carries_docks_(&self) -> bool {
        !matches!(self, FrameKind::Ping | FrameKind::Pong)
    }
}

impl TryFrom<u8> for FrameKind {
    type Error = u8;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        // 只取低 4 位：高 4 位是 `flags`，由调用方按 [`K_FLAGS_MASK`] 单独取出。
        match value & K_KIND_MASK {
            0x01 => Result::Ok(FrameKind::Open),
            0x02 => Result::Ok(FrameKind::Accept),
            0x03 => Result::Ok(FrameKind::Reject),
            0x04 => Result::Ok(FrameKind::Close),
            0x05 => Result::Ok(FrameKind::Data),
            0x06 => Result::Ok(FrameKind::Datagram),
            0x07 => Result::Ok(FrameKind::WindowUpdate),
            0x08 => Result::Ok(FrameKind::Ping),
            0x09 => Result::Ok(FrameKind::Pong),
            other => Result::Err(other),
        }
    }
}

impl From<FrameKind> for u8 {
    fn from(value: FrameKind) -> Self {
        value as u8
    }
}

/// 帧标志位定义（帧首字节的高 4 位，见模块文档）。
///
/// 4 位恰好用满，没有空闲位留给后续扩展。
pub mod flags {
    /// 本方向不再发送数据（半关闭）。
    pub const K_FIN: u8 = 0b0000_0001;

    /// 立即终止子流，丢弃未交付数据。
    pub const K_RESET: u8 = 0b0000_0010;

    /// 应答标记（`ACCEPT` / `PONG` 等）。
    pub const K_ACK: u8 = 0b0000_0100;

    /// 本帧不含载荷，仅承载控制信息。
    ///
    /// 与必需的 `PayloadLen == 0` **语义重复**：该位当前只是一个提示，接收方不
    /// 校验它与 `PayloadLen` 是否一致。保留编号而不删除，是为了将来若需要新的
    /// 标志位，能明确知道这一位可以被回收。
    pub const K_NO_PAYLOAD: u8 = 0b0000_1000;
}

/// 一个已解析的帧头。
///
/// 只保留解复用与流控需要的字段；未知 / 保留字段由解析层直接拒绝，因此这里没有
/// 「扩展字段」存放区——将来扩展时新增字段标识与对应槽位。`ReasonCode` 属于
/// 「只校验不保存」的例外，见模块文档。
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
    ///
    /// 连接级帧（[`FrameKind::Ping`] / [`FrameKind::Pong`]）不带 dock 字段，此时
    /// 返回 [`Dock::unspecified`]。
    pub const fn local_dock(&self) -> Dock {
        self.local_dock_
    }

    /// 对端 dock；连接级帧返回 [`Dock::unspecified`]，语义同
    /// [`FrameHeader::local_dock`]。
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

/// 组装帧首字节：`head = (flags << 4) | kind`。
///
/// `flags` 只有低 4 位有效，更高的位被丢弃。
pub(crate) const fn compose_frame_head_(kind: FrameKind, flags: u8) -> u8 {
    ((flags << K_FLAGS_SHIFT) & K_FLAGS_MASK) | ((kind as u8) & K_KIND_MASK)
}

/// 从帧首字节取出 `flags`（高 4 位）。
pub(crate) const fn flags_from_frame_head_(head: u8) -> u8 {
    (head & K_FLAGS_MASK) >> K_FLAGS_SHIFT
}

/// 组装自描述字段字节：`header = (val_type << 4) | field_id`。
pub(crate) const fn compose_field_header_(id: FieldId, val_type: FieldValType) -> u8 {
    (val_type as u8) | id.as_u8()
}

/// 取能容纳 `value` 的最小宽度（规范化编码）。
///
/// 与握手侧 [`min_val_type_`](crate::handshake::opts::min_val_type_) 是同一个
/// 策略；dock 字段对宽度的额外限制由 [`FieldId::accepts_val_type_`] 表达。
pub(crate) const fn min_field_val_type_(value: usize) -> FieldValType {
    crate::handshake::opts::min_val_type_(value)
}

/// 为该字段挑一个既合法、又能容纳 `value` 的最小宽度。
///
/// 返回 `None` 表示「该字段不接受任何能容纳此数值的宽度」：目前只有 dock 字段在
/// 数值超过 `u32::MAX` 时会遇到（`Dock<u32>` 本身装不下这种值，属于调用方错误）。
fn pick_val_type_(id: FieldId, value: usize) -> Option<FieldValType> {
    const CANDIDATES: [FieldValType; 5] = [
        FieldValType::BeU8,
        FieldValType::BeU16,
        FieldValType::BeU24,
        FieldValType::BeU32,
        FieldValType::BeU64,
    ];
    let min = min_field_val_type_(value);
    CANDIDATES
        .into_iter()
        .find(|val_type| (*val_type as u8) >= (min as u8) && id.accepts_val_type_(*val_type))
}

/// 把大端 `width` 字节无符号数解码为 `usize`；超出 `usize` 表示范围返回 `None`。
fn decode_value_(width: usize, bytes: &[u8]) -> Option<usize> {
    debug_assert_eq!(width, bytes.len());
    let mut value = 0u64;
    for &byte in bytes {
        value = (value << 8) | byte as u64;
    }
    usize::try_from(value).ok()
}

/// 编码一个字段：自描述头字节 + 大端值，使用能容纳该值的最小合法宽度。
///
/// 返回字段总字节数与栈缓冲（最长 1 + 8 字节）；`None` 表示该字段不接受任何能
/// 容纳 `value` 的宽度，由调用方映射为 [`MuxError::UnsupportedField`]。
fn encode_field_(id: FieldId, value: usize) -> Option<([u8; 9], usize)> {
    let val_type = pick_val_type_(id, value)?;
    let width = val_type.value_len();
    let mut out = [0u8; 9];
    out[0] = compose_field_header_(id, val_type);
    let all = (value as u64).to_be_bytes();
    out[1..1 + width].copy_from_slice(&all[8 - width..]);
    Option::Some((out, 1 + width))
}

/// 写一个字段，值以 `usize` 给出。
async fn write_field_async_<W, C>(
    tx: &mut W,
    id: FieldId,
    value: usize,
    cancel: C,
) -> Result<(), MuxError<(), W::Err>>
where
    W: TrBuffWrite<u8>,
    C: TrCancellationToken,
{
    let (bytes, len) = encode_field_(id, value).ok_or(MuxError::UnsupportedField)?;
    write_all_async_(tx, &bytes[..len], cancel)
        .await
        .map_err(map_cursor_err_)
}

/// 把共享字节游标（[`crate::wire_io_`]）的错误映射为复用帧错误。
fn map_cursor_err_<RE, WE>(err: CursorError<RE, WE>) -> MuxError<RE, WE> {
    match err {
        CursorError::Read(err) => MuxError::Rx(err),
        CursorError::Write(err) => MuxError::Tx(err),
        CursorError::PeerClosed => MuxError::PeerClosed,
    }
}

/// 从网络读半边解析出一个帧头。
///
/// 先读固定首字节（`kind` + `flags`），再逐个解析自描述字段，直到
/// `PayloadLen` 出现为止；随后由调用方按 [`FrameHeader::payload_len`] 读取载荷。
/// 任一步失败即整帧失败（不保留部分状态）。
///
/// 注意：`PayloadLen` **必须**是最后一个头字段（模块文档 §帧形状）；若发送方把它
/// 放在中间，接收方会把后续头字段当作载荷，帧随之错位——字节流上无法回退，因此
/// 这是本格式对发送方的硬约束，而不是可以补救的解析细节。
///
/// # Errors
///
/// - 未知 / 保留的 `kind` 或字段标识、非法宽度 → [`MuxError::UnsupportedField`]；
/// - 缺少必需字段、字段重复、字段与 `Kind` 的组合不符 → [`MuxError::MalformedFrame`]；
/// - 底层读失败 / 对端关闭 → [`MuxError::Rx`] / [`MuxError::PeerClosed`]。
pub(crate) async fn read_header_async_<R, C>(
    rx: &mut R,
    cancel: C,
) -> Result<FrameHeader, MuxError<R::Err, ()>>
where
    R: TrBuffRead<u8>,
    C: TrCancellationToken,
{
    let mut cursor = ReadCursor::new_(rx);

    // 1. 固定首字节：`kind` 与 `flags`。
    let head = cursor
        .read_byte_async_(cancel.child_token())
        .await
        .map_err(map_cursor_err_)?;
    let kind = FrameKind::try_from(head).map_err(|_| MuxError::UnsupportedField)?;
    let flags = flags_from_frame_head_(head);

    // 2. 自描述字段序列，直到 `PayloadLen` 收尾。
    let mut local_dock: Option<Dock> = Option::None;
    let mut remote_dock: Option<Dock> = Option::None;
    let mut window_update: Option<Credit> = Option::None;
    let mut reason_seen = false;

    let payload_len = loop {
        let header = cursor
            .read_byte_async_(cancel.child_token())
            .await
            .map_err(map_cursor_err_)?;
        let id = FieldId::from_header_(header).ok_or(MuxError::UnsupportedField)?;
        let val_type =
            FieldValType::try_from(header).map_err(|_| MuxError::UnsupportedField)?;
        if !id.accepts_val_type_(val_type) {
            return Result::Err(MuxError::UnsupportedField);
        }

        // 值宽度由自描述字节给出，最多 8 字节；先读进栈缓冲再解码。
        let width = val_type.value_len();
        let mut raw = [0u8; 8];
        cursor
            .read_async_(&mut raw[..width], cancel.child_token())
            .await
            .map_err(map_cursor_err_)?;
        let value = decode_value_(width, &raw[..width]).ok_or(MuxError::MalformedFrame)?;

        match id {
            FieldId::LocalDock => {
                if local_dock.is_some() {
                    return Result::Err(MuxError::MalformedFrame);
                }
                local_dock = Option::Some(Dock::new(
                    dock_value_(value).ok_or(MuxError::MalformedFrame)?,
                ));
            }
            FieldId::RemoteDock => {
                if remote_dock.is_some() {
                    return Result::Err(MuxError::MalformedFrame);
                }
                remote_dock = Option::Some(Dock::new(
                    dock_value_(value).ok_or(MuxError::MalformedFrame)?,
                ));
            }
            FieldId::WindowUpdate => {
                if window_update.is_some() {
                    return Result::Err(MuxError::MalformedFrame);
                }
                window_update =
                    Option::Some(Credit::try_from(value).map_err(|_| MuxError::MalformedFrame)?);
            }
            FieldId::ReasonCode => {
                if reason_seen {
                    return Result::Err(MuxError::MalformedFrame);
                }
                // 只校验取值域，不保存：拒绝理由由 `REJECT` 的载荷承载（模块文档）。
                if u8::try_from(value).is_err() {
                    return Result::Err(MuxError::MalformedFrame);
                }
                reason_seen = true;
            }
            FieldId::PayloadLen => break value,
        }
    };

    // 3. 字段与 `Kind` 的组合校验。
    let (local_dock, remote_dock) = match (kind.carries_docks_(), local_dock, remote_dock) {
        (true, Option::Some(local), Option::Some(remote)) => (local, remote),
        (true, _, _) => return Result::Err(MuxError::MalformedFrame),
        // 连接级帧：两个 dock 都不得出现，统一归一为 `unspecified`。
        (false, Option::None, Option::None) => {
            (Dock::unspecified(), Dock::unspecified())
        }
        (false, _, _) => return Result::Err(MuxError::MalformedFrame),
    };

    match kind {
        FrameKind::WindowUpdate => {
            if window_update.is_none() {
                return Result::Err(MuxError::MalformedFrame);
            }
        }
        _ => {
            if window_update.is_some() {
                return Result::Err(MuxError::MalformedFrame);
            }
        }
    }

    if reason_seen && kind != FrameKind::Reject {
        return Result::Err(MuxError::MalformedFrame);
    }

    Result::Ok(FrameHeader {
        kind_: kind,
        flags_: flags,
        local_dock_: local_dock,
        remote_dock_: remote_dock,
        payload_len_: payload_len,
        window_update_: window_update,
    })
}

/// 把 dock 字段的数值收窄为 `Dock<u32>` 的承载类型。
///
/// 超出 `u32` 的取值属于结构非法，由调用方映射为 [`MuxError::MalformedFrame`]。
fn dock_value_(value: usize) -> Option<u32> {
    u32::try_from(value).ok()
}

/// 把一个帧头写成「帧首字节 + 自描述字段序列」。
///
/// 只写头，不写载荷；调用方随后把载荷字节直接拼上。字段按固定顺序写出
/// （`LocalDock` → `RemoteDock` → `WindowUpdate` → `PayloadLen`），因此
/// `PayloadLen` 天然收尾（模块文档 §帧形状）。
///
/// # Errors
///
/// - 字段宽度越界（例如 dock 超过 `u32::MAX`）→ [`MuxError::UnsupportedField`]；
/// - `WINDOW_UPDATE` 帧缺少窗口增量，或非 `WINDOW_UPDATE` 帧带上了窗口增量
///   → [`MuxError::MalformedFrame`]（都属调用方构造了自相矛盾的帧头）；
/// - 底层写失败 → [`MuxError::Tx`]。
pub(crate) async fn write_header_async_<W, C>(
    tx: &mut W,
    header: &FrameHeader,
    cancel: C,
) -> Result<(), MuxError<(), W::Err>>
where
    W: TrBuffWrite<u8>,
    C: TrCancellationToken,
{
    let head = compose_frame_head_(header.kind_, header.flags_);
    write_all_async_(tx, &[head], cancel.child_token())
        .await
        .map_err(map_cursor_err_)?;

    if header.kind_.carries_docks_() {
        let local = usize::try_from(header.local_dock_.value())
            .map_err(|_| MuxError::UnsupportedField)?;
        write_field_async_(tx, FieldId::LocalDock, local, cancel.child_token()).await?;
        let remote = usize::try_from(header.remote_dock_.value())
            .map_err(|_| MuxError::UnsupportedField)?;
        write_field_async_(tx, FieldId::RemoteDock, remote, cancel.child_token()).await?;
    }

    match (header.kind_, header.window_update_) {
        (FrameKind::WindowUpdate, Option::Some(update)) => {
            write_field_async_(
                tx,
                FieldId::WindowUpdate,
                usize::try_from(update).map_err(|_| MuxError::UnsupportedField)?,
                cancel.child_token(),
            )
            .await?;
        }
        (FrameKind::WindowUpdate, Option::None) | (_, Option::Some(_)) => {
            return Result::Err(MuxError::MalformedFrame);
        }
        (_, Option::None) => {}
    }

    write_field_async_(tx, FieldId::PayloadLen, header.payload_len_, cancel.child_token()).await
}

#[cfg(test)]
mod tests_ {
    use abs_buff::x_deps::abs_cancel::NonCancellableToken;

    use super::*;

    /// 断言用的错误种类：把底层错误折叠掉，只保留协议层语义。
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum ErrKind {
        UnsupportedField,
        MalformedFrame,
        PeerClosed,
        Rx,
    }

    /// 取出错误的协议层种类，忽略底层读 / 写错误的细节。
    /// - 手段：对 [`MuxError`] 的变体做匹配；出现测试未覆盖的变体即 panic。
    /// - 判断：返回的 [`ErrKind`] 与被测代码产生的变体一一对应。
    fn err_kind_<RE, WE>(err: MuxError<RE, WE>) -> ErrKind {
        match err {
            MuxError::UnsupportedField => ErrKind::UnsupportedField,
            MuxError::MalformedFrame => ErrKind::MalformedFrame,
            MuxError::PeerClosed => ErrKind::PeerClosed,
            MuxError::Rx(_) => ErrKind::Rx,
            other => panic!("测试未覆盖的错误种类：{other}"),
        }
    }

    /// 构造一个只填了种类与标志的帧头；dock 为 `unspecified`、载荷长度为 0。
    /// - 手段：直接写字面量（测试与被测模块同处一个模块，可访问私有字段）。
    /// - 判断：返回的帧头即「最小可用帧头」，供各用例按需改写字段。
    fn header_(kind: FrameKind, flags: u8) -> FrameHeader {
        FrameHeader {
            kind_: kind,
            flags_: flags,
            local_dock_: Dock::unspecified(),
            remote_dock_: Dock::unspecified(),
            payload_len_: 0usize,
            window_update_: Option::None,
        }
    }

    /// 把一个帧头写进 `buf`，返回写入的字节数。
    /// - 手段：用切片实现 [`TrBuffWrite`]，写出后由切片剩余长度反推写入量。
    /// - 判断：写入成功；返回值为实际写出的帧头长度。
    async fn write_header_into_buf_(buf: &mut [u8], header: &FrameHeader) -> usize {
        let capacity = buf.len();
        let mut cursor: &mut [u8] = buf;
        write_header_async_(&mut cursor, header, NonCancellableToken::new())
            .await
            .expect("写帧头应当成功");
        capacity - cursor.len()
    }

    /// 从 `bytes` 读出一个帧头，失败时返回错误的协议层种类。
    /// - 手段：用切片实现 [`TrBuffRead`]，把 [`MuxError`] 折叠为 [`ErrKind`]。
    /// - 判断：`Ok` 为解析出的帧头；`Err` 为被测代码报出的错误种类。
    async fn read_header_from_buf_(bytes: &[u8]) -> Result<FrameHeader, ErrKind> {
        let mut probe: &[u8] = bytes;
        read_header_async_(&mut probe, NonCancellableToken::new())
            .await
            .map_err(err_kind_)
    }

    /// 读一个帧头并只取错误，便于对失败原因做断言。
    /// - 手段：复用 [`read_header_from_buf_`]，丢弃成功值。
    /// - 判断：`Some` 为被测代码报出的错误种类，`None` 表示竟然读成功了。
    async fn read_err_(bytes: &[u8]) -> Option<ErrKind> {
        read_header_from_buf_(bytes).await.err()
    }

    /// 写一个自相矛盾的帧头，返回错误的协议层种类。
    /// - 手段：在局部缓冲上调用 [`write_header_async_`]，期望它拒绝该帧头。
    /// - 判断：返回被测代码报出的错误种类。
    async fn write_header_err_(header: &FrameHeader) -> ErrKind {
        let mut buf = [0u8; 64];
        let mut cursor: &mut [u8] = &mut buf;
        let err = write_header_async_(&mut cursor, header, NonCancellableToken::new())
            .await
            .expect_err("写帧头应当失败");
        err_kind_(err)
    }

    /// 测试帧首字节把 `kind` 与 `flags` 合并成一个字节。
    /// - 手段：以 `DATA` + `FIN|ACK` 组帧首，再分别取回两个字段。
    /// - 判断：帧首为 `0x55`（flags 5 左移 4 位、kind 5）；取回的 flags 为 5、
    ///   kind 仍为 `Data`；超出 4 位的 flags 被掩掉。
    #[test]
    fn frame_head_packs_kind_and_flags() {
        let head = compose_frame_head_(FrameKind::Data, flags::K_FIN | flags::K_ACK);
        assert_eq!(head, 0x55u8);
        assert_eq!(flags_from_frame_head_(head), 5u8);
        assert_eq!(FrameKind::try_from(head), Result::Ok(FrameKind::Data));

        // flags 的高 4 位属于越界输入：只保留低 4 位。
        assert_eq!(compose_frame_head_(FrameKind::Data, 0xFFu8), 0xF5u8);
    }

    /// 测试数据帧的完整往返，并逐字节校验「最小宽度」编码。
    /// - 手段：`DATA` 帧，local=3、remote=0x0102、载荷 1024，写进缓冲后比对
    ///   全部字节，再交给读侧解析。
    /// - 判断：字节序列恰为 `15 00 03 11 01 02 12 04 00`（帧首 = `FIN << 4 | DATA`，
    ///   dock 各按 1 / 2 字节、载荷长度按 2 字节编码）；解析结果各字段与写入值
    ///   完全一致。
    #[compio::test]
    async fn data_frame_roundtrip_uses_min_width() {
        let mut header = header_(FrameKind::Data, flags::K_FIN);
        header.local_dock_ = Dock::new(3u32);
        header.remote_dock_ = Dock::new(0x0102u32);
        header.payload_len_ = 1024usize;

        let mut buf = [0u8; 64];
        let total = write_header_into_buf_(&mut buf, &header).await;

        assert_eq!(total, 9usize);
        assert_eq!(
            &buf[..total],
            &[0x15u8, 0x00, 0x03, 0x11, 0x01, 0x02, 0x12, 0x04, 0x00]
        );

        let parsed = read_header_from_buf_(&buf[..total])
            .await
            .expect("读回自己写出的帧头应当成功");
        assert_eq!(parsed.kind(), FrameKind::Data);
        assert_eq!(parsed.flags(), flags::K_FIN);
        assert!(parsed.is_fin());
        assert!(!parsed.is_reset());
        assert_eq!(parsed.local_dock(), Dock::new(3u32));
        assert_eq!(parsed.remote_dock(), Dock::new(0x0102u32));
        assert_eq!(parsed.payload_len(), 1024usize);
        assert_eq!(parsed.window_update(), Option::None);
    }

    /// 测试 dock 的宽度边界：按数值大小在 1 / 2 / 4 字节间选宽，且能原值往返。
    /// - 手段：对 `0xFF` / `0x100` / `0xFFFF` / `0x1_0000` / `u32::MAX` 各写一个
    ///   `DATAGRAM` 帧头，直接检查紧跟帧首的 `LocalDock` 头字节里的 `val_type`。
    /// - 判断：宽度分别为 `BeU8` / `BeU16` / `BeU16` / `BeU32` / `BeU32`；
    ///   解析回来的 dock 数值与写入值相等。
    #[compio::test]
    async fn dock_widths_follow_value_magnitude() {
        let cases = [
            (0x00FFu32, FieldValType::BeU8),
            (0x0100u32, FieldValType::BeU16),
            (0xFFFFu32, FieldValType::BeU16),
            (0x0001_0000u32, FieldValType::BeU32),
            (u32::MAX, FieldValType::BeU32),
        ];
        for (value, expect) in cases {
            let mut header = header_(FrameKind::Datagram, 0u8);
            header.local_dock_ = Dock::new(value);

            let mut buf = [0u8; 64];
            let total = write_header_into_buf_(&mut buf, &header).await;

            // 第 0 字节是帧首，第 1 字节是 LocalDock 的自描述头。
            assert_eq!(buf[1], (expect as u8) | FieldId::LocalDock.as_u8());
            let parsed = read_header_from_buf_(&buf[..total])
                .await
                .expect("读回 dock 应当成功");
            assert_eq!(parsed.local_dock(), Dock::new(value));
        }
    }

    /// 测试 `WINDOW_UPDATE` 帧的往返与「窗口字段必需」两条约束。
    /// - 手段：带增量 4096 的 `WINDOW_UPDATE` 帧头往返；再分别用「写侧缺增量」
    ///   「读侧缺字段」「非 `WINDOW_UPDATE` 帧带增量」三种畸形输入触发校验。
    /// - 判断：正常帧的增量与 dock 对原值返回；三种畸形输入都报
    ///   [`MuxError::MalformedFrame`]。
    #[compio::test]
    async fn window_update_frame_requires_delta_field() {
        let mut header = header_(FrameKind::WindowUpdate, 0u8);
        header.local_dock_ = Dock::new(7u32);
        header.remote_dock_ = Dock::new(9u32);
        header.window_update_ = Option::Some(4096u32);

        let mut buf = [0u8; 64];
        let total = write_header_into_buf_(&mut buf, &header).await;
        let parsed = read_header_from_buf_(&buf[..total])
            .await
            .expect("读回窗口更新帧应当成功");
        assert_eq!(parsed.window_update(), Option::Some(4096u32));
        assert_eq!(parsed.local_dock(), Dock::new(7u32));
        assert_eq!(parsed.remote_dock(), Dock::new(9u32));

        // 写侧：WINDOW_UPDATE 却没有增量。
        let mut missing = header_(FrameKind::WindowUpdate, 0u8);
        missing.local_dock_ = Dock::new(7u32);
        missing.remote_dock_ = Dock::new(9u32);
        assert_eq!(
            write_header_err_(&missing).await,
            ErrKind::MalformedFrame
        );

        // 读侧：帧首声明 WINDOW_UPDATE，但字段序列里没有 WindowUpdate。
        // 载荷长度字段（0x02 0x00）直接收尾。
        let without_field = [0x07u8, 0x00, 0x07, 0x01, 0x09, 0x02, 0x00];
        assert_eq!(
            read_err_(&without_field).await,
            Option::Some(ErrKind::MalformedFrame)
        );

        // 读侧：DATA 帧却带上了 WindowUpdate 字段（0x03 头 + 1 字节值 1）。
        let wrong_kind = [0x05u8, 0x00, 0x01, 0x01, 0x02, 0x03, 0x01, 0x02, 0x00];
        assert_eq!(
            read_err_(&wrong_kind).await,
            Option::Some(ErrKind::MalformedFrame)
        );
    }

    /// 测试连接级帧（`PING` / `PONG`）不带 dock 对。
    /// - 手段：写一个 `PING` 帧头并逐字节比对；再把「带 dock 对的 `PING`」交给
    ///   读侧。
    /// - 判断：写出恰为 `88 02 00`（帧首 = `NO_PAYLOAD << 4 | PING`，随后是载荷
    ///   长度字段与值 0）；解析结果的
    ///   两个 dock 都是 `unspecified`；带 dock 对的 `PING` 报
    ///   [`MuxError::MalformedFrame`]。
    #[compio::test]
    async fn ping_and_pong_carry_no_dock_pair() {
        let header = header_(FrameKind::Ping, flags::K_NO_PAYLOAD);
        let mut buf = [0u8; 64];
        let total = write_header_into_buf_(&mut buf, &header).await;
        assert_eq!(&buf[..total], &[0x88u8, 0x02, 0x00]);

        let parsed = read_header_from_buf_(&buf[..total])
            .await
            .expect("读回 PING 帧应当成功");
        assert_eq!(parsed.kind(), FrameKind::Ping);
        assert_eq!(parsed.local_dock(), Dock::unspecified());
        assert_eq!(parsed.remote_dock(), Dock::unspecified());

        // 读侧：PING 却带了 dock 对（LocalDock=1、RemoteDock=2）。
        let with_docks = [0x08u8, 0x00, 0x01, 0x01, 0x02, 0x02, 0x00];
        assert_eq!(
            read_err_(&with_docks).await,
            Option::Some(ErrKind::MalformedFrame)
        );
    }

    /// 测试保留的 `kind` 与保留的字段标识都被拒绝。
    /// - 手段：帧首分别取 `0x00`（保留）与 `0x0A`（保留），以及字段标识 `0x05`
    ///   （保留）。
    /// - 判断：三种输入都报 [`MuxError::UnsupportedField`]。
    #[compio::test]
    async fn reserved_kind_and_field_id_are_rejected() {
        // 保留 kind：0x00 与 0x0A..=0x0F。
        for head in [0x00u8, 0x0A, 0x0F] {
            assert_eq!(
                read_err_(&[head, 0x02, 0x00]).await,
                Option::Some(ErrKind::UnsupportedField)
            );
        }

        // 保留字段标识：DATA 帧首之后的 0x05 表示 field_id = 5。
        let reserved_field = [0x05u8, 0x05, 0x00, 0x02, 0x00];
        assert_eq!(
            read_err_(&reserved_field).await,
            Option::Some(ErrKind::UnsupportedField)
        );
    }

    /// 测试 dock 字段拒绝非 1 / 2 / 4 字节的宽度，而其它字段接受非最小宽度。
    /// - 手段：给出以 `BeU24` / `BeU64` 编码的 `LocalDock`；再给出以 `BeU24`
    ///   编码的 `PayloadLen`（数值 3，本可用 1 字节）。
    /// - 判断：dock 的两种宽度都报 [`MuxError::UnsupportedField`]；非最小的
    ///   `PayloadLen` 被接受，解析出 3。
    #[compio::test]
    async fn dock_rejects_non_byte_aligned_width() {
        // LocalDock 用 BeU24（0x20）与 BeU64（0x40）：宽度检查在读值之前发生，
        // 因此不需要提供值字节。
        for field_header in [0x20u8, 0x40] {
            assert_eq!(
                read_err_(&[0x05u8, field_header]).await,
                Option::Some(ErrKind::UnsupportedField)
            );
        }

        // PayloadLen 用 BeU24 编码数值 3：0x22 = (BeU24 << 4) | PayloadLen。
        let non_canonical = [
            0x05u8, // 帧首：DATA
            0x00, 0x01, // LocalDock = 1
            0x01, 0x02, // RemoteDock = 2
            0x22, 0x00, 0x00, 0x03, // PayloadLen = 3（3 字节编码）
        ];
        let parsed = read_header_from_buf_(&non_canonical)
            .await
            .expect("接收方应当接受任意足够宽的编码");
        assert_eq!(parsed.payload_len(), 3usize);
    }

    /// 测试字段重复与必需字段缺失都被判为结构非法。
    /// - 手段：给出「两个 `LocalDock`」与「`DATA` 帧缺 `RemoteDock`」两种字段
    ///   序列。
    /// - 判断：两种输入都报 [`MuxError::MalformedFrame`]。
    #[compio::test]
    async fn duplicate_and_missing_fields_are_rejected() {
        let duplicate_local = [
            0x05u8, // 帧首：DATA
            0x00, 0x01, // LocalDock = 1
            0x00, 0x02, // LocalDock = 2（重复）
            0x01, 0x03, // RemoteDock = 3
            0x02, 0x00, // PayloadLen = 0
        ];
        assert_eq!(
            read_err_(&duplicate_local).await,
            Option::Some(ErrKind::MalformedFrame)
        );

        let missing_remote = [
            0x05u8, // 帧首：DATA
            0x00, 0x01, // LocalDock = 1
            0x02, 0x00, // PayloadLen = 0
        ];
        assert_eq!(
            read_err_(&missing_remote).await,
            Option::Some(ErrKind::MalformedFrame)
        );
    }

    /// 测试 `ReasonCode` 只允许出现在 `REJECT` 帧上，且取值必须能装进 `u8`。
    /// - 手段：`REJECT` + `ReasonCode = 7`（合法）；`REJECT` + `ReasonCode = 0x100`
    ///   （超 `u8`）；`DATA` + `ReasonCode = 7`（种类不符）。
    /// - 判断：第一种解析成功（且不保留该字段）；后两种都报
    ///   [`MuxError::MalformedFrame`]。
    #[compio::test]
    async fn reason_code_only_on_reject() {
        let reject_with_reason = [
            0x03u8, // 帧首：REJECT
            0x00, 0x01, // LocalDock = 1
            0x01, 0x02, // RemoteDock = 2
            0x04, 0x07, // ReasonCode = 7
            0x02, 0x00, // PayloadLen = 0
        ];
        let parsed = read_header_from_buf_(&reject_with_reason)
            .await
            .expect("REJECT 帧带原因码应当合法");
        assert_eq!(parsed.kind(), FrameKind::Reject);

        let reason_too_large = [
            0x03u8, // 帧首：REJECT
            0x00, 0x01, // LocalDock = 1
            0x01, 0x02, // RemoteDock = 2
            0x14, 0x01, 0x00, // ReasonCode = 0x0100（BeU16）
            0x02, 0x00, // PayloadLen = 0
        ];
        assert_eq!(
            read_err_(&reason_too_large).await,
            Option::Some(ErrKind::MalformedFrame)
        );

        let reason_on_data = [
            0x05u8, // 帧首：DATA
            0x00, 0x01, // LocalDock = 1
            0x01, 0x02, // RemoteDock = 2
            0x04, 0x07, // ReasonCode = 7
            0x02, 0x00, // PayloadLen = 0
        ];
        assert_eq!(
            read_err_(&reason_on_data).await,
            Option::Some(ErrKind::MalformedFrame)
        );
    }

    /// 测试除 `PayloadLen` 外的字段顺序不承载语义。
    /// - 手段：`WINDOW_UPDATE` 帧按「WindowUpdate → RemoteDock → LocalDock →
    ///   PayloadLen」排列。
    /// - 判断：解析成功，各字段取值与排列顺序无关地正确。
    #[compio::test]
    async fn field_order_before_payload_len_is_free() {
        let shuffled = [
            0x07u8, // 帧首：WINDOW_UPDATE
            0x13, 0x00, 0x05, // WindowUpdate = 5（BeU16 编码）
            0x01, 0x02, // RemoteDock = 2
            0x00, 0x01, // LocalDock = 1
            0x02, 0x00, // PayloadLen = 0
        ];
        let parsed = read_header_from_buf_(&shuffled)
            .await
            .expect("任意字段顺序都应当能解析");
        assert_eq!(parsed.window_update(), Option::Some(5u32));
        assert_eq!(parsed.local_dock(), Dock::new(1u32));
        assert_eq!(parsed.remote_dock(), Dock::new(2u32));
    }

    /// 测试 `PayloadLen` 是头字段序列的定界符：它之后的字节属于载荷。
    /// - 手段：`PayloadLen = 1` 之后再放一个「看起来像字段」的字节 `0xFF`。
    /// - 判断：解析成功且 `payload_len` 为 1——说明 `0xFF` 没有被当成头字段
    ///   （否则会因保留字段标识而报 [`MuxError::UnsupportedField`]）。
    #[compio::test]
    async fn payload_len_terminates_the_header() {
        let with_payload = [
            0x05u8, // 帧首：DATA
            0x00, 0x01, // LocalDock = 1
            0x01, 0x02, // RemoteDock = 2
            0x02, 0x01, // PayloadLen = 1
            0xFF, // 载荷（不是头字段）
        ];
        let parsed = read_header_from_buf_(&with_payload)
            .await
            .expect("PayloadLen 之后的字节属于载荷");
        assert_eq!(parsed.payload_len(), 1usize);
    }

    /// 测试帧中途截断时如实上报底层读错误，而不是解析出一半成功。
    /// - 手段：给空缓冲，以及「`LocalDock` 头字节之后缺值字节」两种截断输入。
    /// - 判断：两种情况都返回错误（切片夹具把「读不够」表现为底层读错误，
    ///   因此这里断言 [`ErrKind::Rx`]）。
    #[compio::test]
    async fn truncated_header_reports_read_error() {
        assert_eq!(
            read_err_(&[]).await,
            Option::Some(ErrKind::Rx)
        );
        // 0x00 只声明了 LocalDock 的字段头，值字节缺失。
        assert_eq!(
            read_err_(&[0x05u8, 0x00]).await,
            Option::Some(ErrKind::Rx)
        );
    }
}
