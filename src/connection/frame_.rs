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
//!   「本地 dock + 来源 dock」定位子流。**本版本的每一种帧都是子流作用域**，
//!   因此这对字段在所有帧上都必需——包括保活帧 `PING` / `PONG`
//!   （保活是为了维持**某条**子流并通告它的接收窗口，见 [`FrameKind::Pulse`]）；
//! - `ReasonCode` 只允许出现在 `REJECT` 帧上。当前实现只**校验**它（取值必须能
//!   装进 `u8`），不把它存进 [`FrameHeader`]：`abs_smux` 的
//!   `reject_async(reason)` 把拒绝理由当作**载荷**传递，因此这个数值字段目前没有
//!   消费者，发送侧也不产出它；
//! - 帧总长（头 + 载荷）不得超过协商出的 `max_packet_size`。校验由中心循环的读路径完成
//!   （帧长上限来自 [`BasicOpts`](crate::handshake::opts::BasicOpts)，而本模块的
//!   编解码入口只管子结构），超限即 [`MuxError::FrameTooLarge`]。
//!
//! # 本模块现在只管**编码**与字段级公共件
//!
//! 帧头**解析**已迁到 [`crate::connection::frame_parser_`] 的 sans-IO 逐字节状态机
//! （原因：旧入口按字段索要 `width`
//! 字节，环容量小于 `width` 时会拿到终态的 `Unsatisfiable` 而整条连接失败）。
//! 本模块保留 `encode_header_`（栈上定长缓冲，写路径的唯一入口）、`FieldId` /
//! `FrameKind` / `flags` 等公共件，以及两个状态机共用的 `decode_dock_field_`；`Vec`
//! 版本的 `encode_header_into_` 只留给测试。

use abs_smux::dock::TrDock;
// 测试夹具用 `abs_buff` 的 `NonCancellableToken` 驱动解析器；非测试构建用不到。
#[cfg(test)]
use buffex::x_deps::abs_buff;

use crate::{
    connection::{Dock, MuxError},
    flow_ctrl::{Credit, RecvTotal},
    handshake::opts::NegotiationValType as FieldValType,
};

/// `kind` 在帧首字节中的掩码（低 4 位）。
pub const K_KIND_MASK: u8 = 0x0F;

/// `flags` 在帧首字节中的掩码（高 4 位）。
pub const K_FLAGS_MASK: u8 = 0xF0;

/// `flags` 在帧首字节中的左移位数。
pub const K_FLAGS_SHIFT: u32 = 4;

/// `field_id` 的掩码（低 4 位）。
pub const K_FIELD_ID_MASK: u8 = 0x0F;

/// `val_type` 的掩码（高 3 位，bit 7 保留）。
///
/// 协议常量：当前解码按 `FieldId` 直接匹配宽度，不需要掩码；保留它是为了让
/// 「bit 7 保留」这条线格式约定在代码里有单一出处（模块文档 §引用它）。
#[allow(dead_code)]
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
    /// 本端子流端点 dock。所有帧必需（本版本的帧都是子流作用域）。
    LocalDock = 0x00,

    /// 对端子流端点 dock。所有帧必需（本版本的帧都是子流作用域）。
    RemoteDock = 0x01,

    /// 载荷字节数。必需，且必须是最后一个头字段；`0` 表示无载荷。
    PayloadLen = 0x02,

    /// **接收窗口通告（窗口值）**：发送方当前的接收窗口大小（`W`，绝对值）。
    ///
    /// 与 [`FieldId::RecvTotal`] 一起，在 `OPEN` / `PULSE` / `WINDOW_UPDATE` 三种帧上
    /// **必需**，其余帧上**禁止**出现。两者必须成对出现。
    RecvWindow = 0x03,

    /// **接收窗口通告（累计已收字节数）**：本通告对应的累计已收字节数（`R`）。
    ///
    /// 发送方据此算 `可用 = W − (已发 − R)`，精确扣掉在途数据；缺了它，只凭
    /// [`FieldId::RecvWindow`] 会把在途量重复计入而越权（推导见
    /// [`crate::flow_ctrl::WindowReport`]）。
    ///
    /// 宽度**允许 2 / 4 / 8 字节三种规格**（发送方取能容纳该值的最小者）：小值编成
    /// 2 字节让控制帧保持紧凑；`R` 涨到当前规格放不下时，由发送方按
    /// [`TrFlowCtrlPolicy::recv_total_epoch`](crate::flow_ctrl::TrFlowCtrlPolicy::recv_total_epoch)
    /// 的约定**重置累计量**并发一帧带 [`flags::K_TOTAL_RESET`] 的通告（见该常量）。
    RecvTotal = 0x04,

    /// 拒绝原因码，只在 `REJECT` 帧上出现；当前只校验、不保存（见模块文档）。
    ReasonCode = 0x05,
}

impl FieldId {
    /// 从自描述字段字节取出 `field_id`；保留取值返回 `None`。
    pub const fn from_header_(header: u8) -> Option<Self> {
        match header & K_FIELD_ID_MASK {
            0x00 => Option::Some(FieldId::LocalDock),
            0x01 => Option::Some(FieldId::RemoteDock),
            0x02 => Option::Some(FieldId::PayloadLen),
            0x03 => Option::Some(FieldId::RecvWindow),
            0x04 => Option::Some(FieldId::RecvTotal),
            0x05 => Option::Some(FieldId::ReasonCode),
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
            // 累计已收量只允许 2 / 4 / 8 字节三种规格：1 字节太小（一次突发就要重置），
            // 3 字节没有对应累加类型。
            FieldId::RecvTotal => matches!(
                val_type,
                FieldValType::BeU16 | FieldValType::BeU32 | FieldValType::BeU64
            ),
            FieldId::PayloadLen | FieldId::RecvWindow | FieldId::ReasonCode => true,
        }
    }
}

/// 复用帧的种类，即帧首字节低 4 位的取值。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum FrameKind {
    /// 发起方请求建立子流，载荷为随帧附带的「开场消息」。
    ///
    /// **双方都会发送 `OPEN`**：主动方发出的 `OPEN` 带自己的开场消息；被动方收到后
    /// 也回一条 `OPEN`（载荷为空）以通告自己的接收窗口。两条 `OPEN` 都必需携带发送
    /// 方的接收窗口，这样两侧才能用**同一个状态机**处理建流（见
    /// [`crate::connection`] 模块文档 §4.2）。
    Open = 0x01,

    /// 被动方在回完 `OPEN` 之后发送，载荷为欢迎信息。
    ///
    /// 只有被动方发送 `ACCEPT`；接收窗口已经由它自己的那条 `OPEN` 通告过，因此本帧
    /// 不再需要窗口字段。
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

    /// 保活 / 接收窗口通告（子流作用域）。
    ///
    /// 该子流闲置接近 `max_channel_timeout` 时由本方发出，载荷为发送方**当前接收
    /// 窗口**。收到的一方**只**借它刷新对端窗口与该子流的活跃时间，**不立即回**
    /// ——两个方向各自按自己的空闲计时发 `PULSE`，否则两条 `PULSE` 会互相触发成
    /// 死循环。这也正是只用一种帧（而不是 `PING` / `PONG` 两种）的原因：没有
    /// 「请求 / 应答」之分，双方的处理栈完全同一份。
    Pulse = 0x08,
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
            0x08 => Result::Ok(FrameKind::Pulse),
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
/// 4 位恰好用满：`FIN` / `RESET` / `ACK` / `TOTAL_RESET`。
pub mod flags {
    /// 本方向不再发送数据（半关闭）。
    pub const K_FIN: u8 = 0b0000_0001;

    /// 立即终止子流，丢弃未交付数据。
    pub const K_RESET: u8 = 0b0000_0010;

    /// 应答标记（`ACCEPT` 等）。
    pub const K_ACK: u8 = 0b0000_0100;

    /// **窗口通告的变体**：累计已收量已经重置。
    ///
    /// 本帧携带的 [`FieldId::RecvTotal`](super::FieldId::RecvTotal) 是**重置前**的累计量（即上一个 epoch 的
    /// 总量），收到它的一方据此把本端的发送计数 rebase 过来，之后的通告里
    /// `RecvTotal` 从 `0` 重新计数。这样窄规格（2 / 4 字节）的累计量可以在快要放
    /// 不下时干净地重新开始，而不必改用更宽的字段。
    ///
    /// 只允许出现在携带窗口通告的帧上（[`FrameKind::Pulse`](super::FrameKind::Pulse)
    /// / [`FrameKind::WindowUpdate`](super::FrameKind::WindowUpdate)）；`OPEN` 时还没有
    /// epoch，带上即非法。
    pub const K_TOTAL_RESET: u8 = 0b0000_1000;
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
    recv_window_: Option<Credit>,
    recv_total_: Option<RecvTotal>,
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

    /// 本端 dock（所有帧都带，见模块文档）。
    pub const fn local_dock(&self) -> Dock {
        self.local_dock_
    }

    /// 对端 dock，语义同 [`FrameHeader::local_dock`]。
    pub const fn remote_dock(&self) -> Dock {
        self.remote_dock_
    }

    /// 载荷字节数。
    pub const fn payload_len(&self) -> usize {
        self.payload_len_
    }

    /// 接收窗口通告的窗口值 `W`（仅 `OPEN` / `PULSE` / `WINDOW_UPDATE` 帧为
    /// `Some`，且必定与 [`FrameHeader::recv_total`] 同时为 `Some`）。
    pub const fn recv_window(&self) -> Option<Credit> {
        self.recv_window_
    }

    /// 接收窗口通告对应的累计已收字节数 `R`，与 [`FrameHeader::recv_window`] 成对。
    pub const fn recv_total(&self) -> Option<RecvTotal> {
        self.recv_total_
    }

    /// 是否带 `FIN` 标志。
    pub const fn is_fin(&self) -> bool {
        self.flags_ & flags::K_FIN != 0
    }

    /// 是否带 `RESET` 标志。
    pub const fn is_reset(&self) -> bool {
        self.flags_ & flags::K_RESET != 0
    }

    /// 本帧是否为「累计已收量已重置」的窗口通告变体
    /// （见 [`flags::K_TOTAL_RESET`]）：此时
    /// [`FrameHeader::recv_total`] 携带的是**重置前**的累计量。
    pub const fn is_total_reset(&self) -> bool {
        self.flags_ & flags::K_TOTAL_RESET != 0
    }

    /// 由写路径组装一个帧头。
    ///
    /// `window` 只在 `OPEN` / `PULSE` / `WINDOW_UPDATE` 上给出（`(累计已收 R,
    /// 接收窗口 W)`），其余帧必须传 `None`；合法性由
    /// [`encode_header_`] 再次校验。
    pub(crate) const fn new_(
        kind: FrameKind,
        flags: u8,
        local_dock: Dock,
        remote_dock: Dock,
        payload_len: usize,
        window: Option<(RecvTotal, Credit)>,
    ) -> Self {
        let (recv_window_, recv_total_) = match window {
            Option::Some((total, window)) => (Option::Some(window), Option::Some(total)),
            Option::None => (Option::None, Option::None),
        };
        FrameHeader {
            kind_: kind,
            flags_: flags,
            local_dock_: local_dock,
            remote_dock_: remote_dock,
            payload_len_: payload_len,
            recv_window_,
            recv_total_,
        }
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

/// dock 字段解析失败的原因（载荷无关，便于调用点映射到自己的错误类型）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DockDecode_ {
    /// 数值超出 `u32`：结构非法。
    TooLarge,

    /// 取了保留值（`wildcard` / `unspecified`）。
    Reserved,
}

/// 把 dock 字段的数值收窄为 `Dock<u32>`，并拒绝**保留值**。
///
/// `wildcard`（全 1）与 `unspecified`（全 0）由双方共同保留，不能作为子流 dock，
/// 因此线上出现即 [`MuxError::ReservedDock`]。
fn decode_dock_(value: usize) -> Result<Dock, DockDecode_> {
    let raw = u32::try_from(value).map_err(|_| DockDecode_::TooLarge)?;
    let dock = Dock::new(raw);
    if dock.is_special() {
        return Result::Err(DockDecode_::Reserved);
    }
    Result::Ok(dock)
}

/// 把 [`DockDecode_`] 映射为帧层错误。
fn map_dock_decode_(err: DockDecode_) -> MuxError {
    match err {
        DockDecode_::TooLarge => MuxError::MalformedFrame,
        DockDecode_::Reserved => MuxError::ReservedDock,
    }
}

/// 把一个已解码的 dock 字段数值收窄为 [`Dock`]，并做与帧头解析一致的校验。
///
/// 本函数是 `frame_parser_` 的逐字节状态机与旧入口 [`read_header_async_`] 共用的
/// **唯一** dock 校验出处：两处都不再各写一份「保留值 / 超宽」判断，避免它们漂移。
///
/// # Errors
///
/// - 数值超出 `u32` → [`MuxError::MalformedFrame`]；
/// - 取到保留值（`wildcard` / `unspecified`）→ [`MuxError::ReservedDock`]。
pub(crate) fn decode_dock_field_(value: usize) -> Result<Dock, MuxError> {
    decode_dock_(value).map_err(map_dock_decode_)
}

/// 把 `DATAGRAM` 的 **remote dock** 字段收窄为 [`Dock`]，**允许**协议保留值。
///
/// 与 [`decode_dock_field_`] 的唯一差别是不拒绝 `wildcard` / `unspecified`：数据报的
/// remote dock 是**地址**而不是**身份**（见 `crate::connection` 模块文档 §4），
/// 「发往 wildcard / unspecified」是对端的策略问题，不是协议违例。local dock 仍然
/// 必须是真实值——它来自本端已绑定的 telegraph。
///
/// # Errors
///
/// 数值超出 `u32` → [`MuxError::MalformedFrame`]。
pub(crate) fn decode_dock_field_lenient_(value: usize) -> Result<Dock, MuxError> {
    let raw = u32::try_from(value).map_err(|_| MuxError::MalformedFrame)?;
    Result::Ok(Dock::new(raw))
}

/// 该帧种类是否必需携带接收窗口通告（`RecvWindow` + `RecvTotal` 两个字段）。
pub(crate) const fn requires_window_report_(kind: FrameKind) -> bool {
    matches!(
        kind,
        FrameKind::Open | FrameKind::Pulse | FrameKind::WindowUpdate
    )
}

/// 把帧头编码为「帧首字节 + 自描述字段序列」，追加到 `sink` 末尾。
///
/// 把帧头编码并追加到 `sink`（**仅测试使用**）。
///
/// 生产路径一律用 [`encode_header_`]（栈上定长缓冲、零分配）；这里保留 `Vec` 版本是
/// 因为测试更愿意比对一串字节，而测试不受分配纪律约束。
#[cfg(test)]
pub(crate) fn encode_header_into_(
    sink: &mut Vec<u8>,
    header: &FrameHeader,
) -> Result<(), MuxError> {
    let (buf, len) = encode_header_(header)?;
    sink.extend_from_slice(&buf[..len]);
    Result::Ok(())
}

/// 帧头的**最坏编码长度**（字节）。
///
/// 每个自描述字段占「1 字节头 + 大端值」，宽度取能容纳该值的最小合法者
/// （见 `encode_field_`）。本版本的帧头字段与最坏宽度：
///
/// | 字段 | 允许宽度 | 最坏字节 |
/// | --- | --- | --- |
/// | 帧首字节 | — | 1 |
/// | `LocalDock` | 1 / 2 / 4 字节 | 5 |
/// | `RemoteDock` | 1 / 2 / 4 字节 | 5 |
/// | `RecvTotal` | 2 / 4 / 8 字节 | 9 |
/// | `RecvWindow` | 任意宽度 | 9 |
/// | `PayloadLen` | 任意宽度 | 9 |
///
/// **真实**最坏组合（窗口两项只出现在 `OPEN` / `PULSE` / `WINDOW_UPDATE` 上，且与
/// `REJECT` 的 `ReasonCode` 互斥；`ReasonCode` 目前走**载荷**而不是头字段）是
/// `1 + 5 + 5 + 9 + 9 + 9 = 38` 字节；把不存在的字段组合也算上也只有 47 字节。
/// 取 64 是为了给后续加字段留余量，而不是「刚好够用」。
pub(crate) const K_MAX_FRAME_HEADER: usize = 64usize;

/// 把帧头编码进一块**栈上定长缓冲**，返回 `(缓冲, 实际长度)`。
///
/// 这是数据面唯一需要「一段连续帧头字节」的地方；用定长数组把它从堆上拿下来之后，
/// 每 DATA 帧少一次全局分配，载荷也不必再整体拷进临时 `Vec`
/// （见 `dev-notes/audit-heap-alloc-20261004-1122.md` §3.1 #1 与 §5.1）。
///
/// # Errors
///
/// 与测试用的 `encode_header_into_` 完全一致（两者共用本实现）：`TOTAL_RESET` 用在
/// `PULSE` / `WINDOW_UPDATE` 之外 → [`MuxError::MalformedFrame`]；dock 取保留值 →
/// [`MuxError::ReservedDock`]；窗口通告缺一半或多一半 → [`MuxError::MalformedFrame`]；
/// 字段宽度无法容纳 → [`MuxError::UnsupportedField`]。
pub(crate) fn encode_header_(
    header: &FrameHeader,
) -> Result<([u8; K_MAX_FRAME_HEADER], usize), MuxError> {
    let mut buf = [0u8; K_MAX_FRAME_HEADER];
    let mut cursor = 0usize;

    // `TOTAL_RESET` 只对窗口通告有意义，且 OPEN 时还没有 epoch。
    if header.flags_ & flags::K_TOTAL_RESET != 0
        && !matches!(header.kind_, FrameKind::Pulse | FrameKind::WindowUpdate)
    {
        return Result::Err(MuxError::MalformedFrame);
    }

    write_byte_(&mut buf, &mut cursor, compose_frame_head_(header.kind_, header.flags_))?;

    // channel 作用域的帧里 dock 对就是身份：两端都必须是真实 dock。
    //
    // `DATAGRAM` 是**地址**而非**身份**：它的 remote dock 允许取 `wildcard` /
    // `unspecified`（「发到 wildcard」由对端自己的策略决定收还是不收），因此这一条
    // 按帧种类放宽；local dock 仍然是真实值（它来自本端已绑定的 telegraph）。
    // 见 `crate::connection` 模块文档 §4。
    let (local_dock, remote_dock) = (header.local_dock_, header.remote_dock_);
    if local_dock.is_special() {
        return Result::Err(MuxError::ReservedDock);
    }
    if remote_dock.is_special() && header.kind_ != FrameKind::Datagram {
        return Result::Err(MuxError::ReservedDock);
    }
    let local = usize::try_from(local_dock.value()).map_err(|_| MuxError::UnsupportedField)?;
    write_field_(&mut buf, &mut cursor, FieldId::LocalDock, local)?;
    let remote = usize::try_from(remote_dock.value()).map_err(|_| MuxError::UnsupportedField)?;
    write_field_(&mut buf, &mut cursor, FieldId::RemoteDock, remote)?;

    match (
        requires_window_report_(header.kind_),
        header.recv_window_,
        header.recv_total_,
    ) {
        (true, Option::Some(window), Option::Some(total)) => {
            // 先累计字节数（`R`）后窗口值（`W`），与模块文档的字段顺序一致。
            let total = usize::try_from(total).map_err(|_| MuxError::UnsupportedField)?;
            write_field_(&mut buf, &mut cursor, FieldId::RecvTotal, total)?;
            let window = usize::try_from(window).map_err(|_| MuxError::UnsupportedField)?;
            write_field_(&mut buf, &mut cursor, FieldId::RecvWindow, window)?;
        }
        (false, Option::None, Option::None) => {}
        // 缺一个、多一个、或出现在不该出现的帧上：都是自相矛盾的帧头。
        _ => return Result::Err(MuxError::MalformedFrame),
    }

    write_field_(&mut buf, &mut cursor, FieldId::PayloadLen, header.payload_len_)?;
    Result::Ok((buf, cursor))
}

/// 往定长缓冲里写一个字节。
fn write_byte_(buf: &mut [u8], cursor: &mut usize, byte: u8) -> Result<(), MuxError> {
    let Some(slot) = buf.get_mut(*cursor) else {
        // 缓冲按最坏字段组合取值；越界只可能是「字段上界被改小」这类内部错误。
        return Result::Err(MuxError::FrameTooLarge);
    };
    *slot = byte;
    *cursor += 1usize;
    Result::Ok(())
}

/// 编码单个自描述字段并写进定长缓冲（宽度取能容纳 `value` 的最小合法宽度）。
fn write_field_(
    buf: &mut [u8],
    cursor: &mut usize,
    id: FieldId,
    value: usize,
) -> Result<(), MuxError> {
    let (bytes, len) = encode_field_(id, value).ok_or(MuxError::UnsupportedField)?;
    let end = *cursor + len;
    let Some(dst) = buf.get_mut(*cursor..end) else {
        return Result::Err(MuxError::FrameTooLarge);
    };
    dst.copy_from_slice(&bytes[..len]);
    *cursor = end;
    Result::Ok(())
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
        ReservedDock,
        PeerClosed,
        Rx,
    }

    /// 取出错误的协议层种类，忽略底层读 / 写错误的细节。
    /// - 手段：对 [`MuxError`] 的变体做匹配；出现测试未覆盖的变体即 panic。
    /// - 判断：返回的 [`ErrKind`] 与被测代码产生的变体一一对应。
    fn err_kind_(err: MuxError) -> ErrKind {
        match err {
            MuxError::UnsupportedField => ErrKind::UnsupportedField,
            MuxError::MalformedFrame => ErrKind::MalformedFrame,
            MuxError::ReservedDock => ErrKind::ReservedDock,
            MuxError::PeerClosed => ErrKind::PeerClosed,
            // 帧层把任意读半边的错误统一投影成“读方向传输失败”；测试仍按原来的
            // `Rx` 种类断言。
            MuxError::Transport { write: false } => ErrKind::Rx,
            other => panic!("测试未覆盖的错误种类：{other}"),
        }
    }

    /// 构造一个只填了种类与标志的帧头；dock 取一对**合法**取值、载荷长度为 0。
    /// - 手段：直接写字面量（测试与被测模块同处一个模块，可访问私有字段）。
    /// - 判断：返回的帧头即「最小可用帧头」，供各用例按需改写字段。
    ///   注意 dock 不能取 `unspecified` / `wildcard`——那是保留值，写侧会直接拒绝
    ///   （见 `reserved_docks_are_rejected_both_ways`）。
    fn header_(kind: FrameKind, flags: u8) -> FrameHeader {
        FrameHeader {
            kind_: kind,
            flags_: flags,
            local_dock_: Dock::new(1u32),
            remote_dock_: Dock::new(2u32),
            payload_len_: 0usize,
            recv_window_: Option::None,
            recv_total_: Option::None,
        }
    }

    /// 把一个帧头编码进 `buf`，返回写出的字节数。
    /// - 手段：调用 [`encode_header_into_`] 得到一个临时 `Vec`，再拷进 `buf`。
    /// - 判断：编码成功；返回值为实际写出的帧头长度。
    fn write_header_into_buf_(buf: &mut [u8], header: &FrameHeader) -> usize {
        let mut bytes = Vec::new();
        encode_header_into_(&mut bytes, header).expect("编码帧头应当成功");
        assert!(bytes.len() <= buf.len(), "测试缓冲应当装得下帧头");
        buf[..bytes.len()].copy_from_slice(&bytes);
        bytes.len()
    }

    /// 从 `bytes` 读出一个帧头，失败时返回错误的协议层种类。
    /// - 手段：用切片实现 [`TrBuffRead`]，交给**连接实际使用的**逐字节状态机
    ///   （[`crate::connection::frame_parser_::read_header_async_`]），把 [`MuxError`]
    ///   折叠为 [`ErrKind`]。
    /// - 判断：`Ok` 为解析出的帧头；`Err` 为被测代码报出的错误种类。
    ///
    /// 本模块原先自带一个「按字段索要 `width` 字节」的解析器；切换解复用路径后它已删除，
    /// 整套用例（字段组合、重复字段、保留 dock、`ReasonCode` 位置、乱序字段、逐字节边界）
    /// 直接压在实际使用的状态机上。
    async fn read_header_from_buf_(bytes: &[u8]) -> Result<FrameHeader, ErrKind> {
        let mut probe: &[u8] = bytes;
        crate::connection::frame_parser_::read_header_async_::<_, _>(
            &mut probe,
            NonCancellableToken::new(),
        )
        .await
        // 第二个分量是帧头长度（`frame_parser_` 自己数的已消费字节数）；本辅助函数
        // 只关心帧头本身。
        .map(|(header, _head_len)| header)
        .map_err(err_kind_)
    }

    /// 读一个帧头并只取错误，便于对失败原因做断言。
    /// - 手段：复用 [`read_header_from_buf_`]，丢弃成功值。
    /// - 判断：`Some` 为被测代码报出的错误种类，`None` 表示竟然读成功了。
    async fn read_err_(bytes: &[u8]) -> Option<ErrKind> {
        read_header_from_buf_(bytes).await.err()
    }

    /// 写一个自相矛盾的帧头，返回错误的协议层种类。
    /// - 手段：调用 [`encode_header_into_`]，期望它拒绝该帧头。
    /// - 判断：返回被测代码报出的错误种类。
    fn write_header_err_(header: &FrameHeader) -> ErrKind {
        let mut bytes = Vec::new();
        let err = encode_header_into_(&mut bytes, header).expect_err("编码帧头应当失败");
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
    async fn data_frame_roundtrip_uses_min_width() {
        let mut header = header_(FrameKind::Data, flags::K_FIN);
        header.local_dock_ = Dock::new(3u32);
        header.remote_dock_ = Dock::new(0x0102u32);
        header.payload_len_ = 1024usize;

        let mut buf = [0u8; 64];
        let total = write_header_into_buf_(&mut buf, &header);

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
        assert_eq!(parsed.recv_window(), Option::None);
    }
    dual_runtime_test_!(data_frame_roundtrip_uses_min_width);

    /// 测试 dock 的宽度边界：按数值大小在 1 / 2 / 4 字节间选宽，且能原值往返。
    /// - 手段：对 `0xFF` / `0x100` / `0xFFFF` / `0x1_0000` / `0xFFFF_FFFE` 各写一个
    ///   `DATAGRAM` 帧头，直接检查紧跟帧首的 `LocalDock` 头字节里的 `val_type`。
    /// - 判断：宽度分别为 `BeU8` / `BeU16` / `BeU16` / `BeU32` / `BeU32`；
    ///   解析回来的 dock 数值与写入值相等。
    ///   （`u32::MAX` 是保留的 `wildcard`，故用 `u32::MAX - 1` 覆盖 4 字节宽度；
    ///   保留值的拒绝见 `reserved_docks_are_rejected_both_ways`。）
    async fn dock_widths_follow_value_magnitude() {
        let cases = [
            (0x00FFu32, FieldValType::BeU8),
            (0x0100u32, FieldValType::BeU16),
            (0xFFFFu32, FieldValType::BeU16),
            (0x0001_0000u32, FieldValType::BeU32),
            (0xFFFF_FFFEu32, FieldValType::BeU32),
        ];
        for (value, expect) in cases {
            let mut header = header_(FrameKind::Datagram, 0u8);
            header.local_dock_ = Dock::new(value);

            let mut buf = [0u8; 64];
            let total = write_header_into_buf_(&mut buf, &header);

            // 第 0 字节是帧首，第 1 字节是 LocalDock 的自描述头。
            assert_eq!(buf[1], (expect as u8) | FieldId::LocalDock.as_u8());
            let parsed = read_header_from_buf_(&buf[..total])
                .await
                .expect("读回 dock 应当成功");
            assert_eq!(parsed.local_dock(), Dock::new(value));
        }
    }
    dual_runtime_test_!(dock_widths_follow_value_magnitude);

    /// 测试 `WINDOW_UPDATE` 帧的往返与「窗口通告两字段必需」两条约束。
    /// - 手段：带增量 4096 的 `WINDOW_UPDATE` 帧头往返；再分别用「写侧缺增量」
    ///   「读侧缺字段」「非 `WINDOW_UPDATE` 帧带增量」三种畸形输入触发校验。
    /// - 判断：正常帧的增量与 dock 对原值返回；三种畸形输入都报
    ///   [`MuxError::MalformedFrame`]。
    async fn window_update_frame_requires_window_report() {
        let mut header = header_(FrameKind::WindowUpdate, 0u8);
        header.local_dock_ = Dock::new(7u32);
        header.remote_dock_ = Dock::new(9u32);
        header.recv_window_ = Option::Some(4096u32);
        header.recv_total_ = Option::Some(8192u64);

        let mut buf = [0u8; 64];
        let total = write_header_into_buf_(&mut buf, &header);
        let parsed = read_header_from_buf_(&buf[..total])
            .await
            .expect("读回窗口更新帧应当成功");
        assert_eq!(parsed.recv_window(), Option::Some(4096u32));
        assert_eq!(parsed.recv_total(), Option::Some(8192u64));
        assert_eq!(parsed.local_dock(), Dock::new(7u32));
        assert_eq!(parsed.remote_dock(), Dock::new(9u32));

        // 写侧：WINDOW_UPDATE 却没有窗口通告。
        let mut missing = header_(FrameKind::WindowUpdate, 0u8);
        missing.local_dock_ = Dock::new(7u32);
        missing.remote_dock_ = Dock::new(9u32);
        assert_eq!(
            write_header_err_(&missing),
            ErrKind::MalformedFrame
        );

        // 读侧：帧首声明 WINDOW_UPDATE，但字段序列里完全没有窗口通告。
        // 载荷长度字段（0x02 0x00）直接收尾。
        let without_field = [0x07u8, 0x00, 0x07, 0x01, 0x09, 0x02, 0x00];
        assert_eq!(
            read_err_(&without_field).await,
            Option::Some(ErrKind::MalformedFrame)
        );

        // 读侧：DATA 帧却带上了窗口通告（0x14 = BeU16 | RecvTotal，值为 1）。
        let wrong_kind = [
            0x05u8, 0x00, 0x01, 0x01, 0x02, 0x14, 0x00, 0x01, 0x02, 0x00,
        ];
        assert_eq!(
            read_err_(&wrong_kind).await,
            Option::Some(ErrKind::MalformedFrame)
        );
    }
    dual_runtime_test_!(window_update_frame_requires_window_report);

    /// 测试保活帧 `PULSE` 是子流作用域，且必须携带接收窗口通告。
    /// - 手段：写一个 dock 对为 `(1, 2)`、接收窗口为 4096 的 `PULSE` 帧头并逐字节
    ///   比对；再把「缺 dock 对」与「缺窗口字段」两种 `PULSE` 交给读侧。
    /// - 判断：写出恰为 `88 00 01 01 02 02 12 10 00`（帧首 = `NO_PAYLOAD << 4 |
    ///   PULSE`，随后 `LocalDock=1`、`RemoteDock=2`、`RecvWindow=4096`（`BeU16`）、
    ///   `PayloadLen=0`）；解析结果与写入一致；两种畸形输入都报
    ///   [`MuxError::MalformedFrame`]。
    async fn pulse_is_substream_scoped_and_carries_window() {
        let mut header = header_(FrameKind::Pulse, 0u8);
        header.local_dock_ = Dock::new(1u32);
        header.remote_dock_ = Dock::new(2u32);
        header.recv_window_ = Option::Some(4096u32);
        header.recv_total_ = Option::Some(0u64);

        let mut buf = [0u8; 64];
        let total = write_header_into_buf_(&mut buf, &header);
        assert_eq!(
            &buf[..total],
            &[0x08u8, 0x00, 0x01, 0x01, 0x02, 0x14, 0x00, 0x00, 0x13, 0x10, 0x00, 0x02, 0x00]
        );

        let parsed = read_header_from_buf_(&buf[..total])
            .await
            .expect("读回 PULSE 帧应当成功");
        assert_eq!(parsed.kind(), FrameKind::Pulse);
        assert_eq!(parsed.local_dock(), Dock::new(1u32));
        assert_eq!(parsed.remote_dock(), Dock::new(2u32));
        assert_eq!(parsed.recv_window(), Option::Some(4096u32));
        assert_eq!(parsed.recv_total(), Option::Some(0u64));

        // 读侧：缺 dock 对 → 结构非法。
        let without_docks = [0x08u8, 0x12, 0x10, 0x00, 0x02, 0x00];
        assert_eq!(
            read_err_(&without_docks).await,
            Option::Some(ErrKind::MalformedFrame)
        );

        // 读侧：完全缺窗口通告（dock 对齐全，直接以 PayloadLen 收尾）。
        let without_report = [0x08u8, 0x00, 0x01, 0x01, 0x02, 0x02, 0x00];
        assert_eq!(
            read_err_(&without_report).await,
            Option::Some(ErrKind::MalformedFrame)
        );

        // 读侧：只带窗口值、缺累计已收字节数。
        let without_total = [
            0x08u8, 0x00, 0x01, 0x01, 0x02, 0x13, 0x10, 0x00, 0x02, 0x00,
        ];
        assert_eq!(
            read_err_(&without_total).await,
            Option::Some(ErrKind::MalformedFrame)
        );

        // 写侧：PULSE 却没有窗口通告。
        let mut missing = header_(FrameKind::Pulse, 0u8);
        missing.local_dock_ = Dock::new(1u32);
        missing.remote_dock_ = Dock::new(2u32);
        assert_eq!(
            write_header_err_(&missing),
            ErrKind::MalformedFrame
        );
    }
    dual_runtime_test_!(pulse_is_substream_scoped_and_carries_window);

    /// 测试建流用的 `OPEN` 必须携带接收窗口通告，而 `ACCEPT` 不得携带。
    /// - 手段：写一个带窗口的 `OPEN` 并读回；再分别构造「无窗口的 `OPEN`」与
    ///   「带窗口的 `ACCEPT`」。
    /// - 判断：正常 `OPEN` 往返后 dock 对与窗口都一致；两种自相矛盾的帧头都报
    ///   [`MuxError::MalformedFrame`]（`ACCEPT` 的接收窗口已由被动方自己的
    ///   `OPEN` 通告过）。
    async fn open_carries_window_but_accept_does_not() {
        let mut open = header_(FrameKind::Open, 0u8);
        open.local_dock_ = Dock::new(3u32);
        open.remote_dock_ = Dock::new(7u32);
        open.recv_window_ = Option::Some(2048u32);
        open.recv_total_ = Option::Some(7u64);
        open.payload_len_ = 4usize;

        let mut buf = [0u8; 64];
        let total = write_header_into_buf_(&mut buf, &open);
        let parsed = read_header_from_buf_(&buf[..total])
            .await
            .expect("读回 OPEN 帧应当成功");
        assert_eq!(parsed.kind(), FrameKind::Open);
        assert_eq!(parsed.recv_window(), Option::Some(2048u32));
        assert_eq!(parsed.recv_total(), Option::Some(7u64));
        assert_eq!(parsed.payload_len(), 4usize);

        // 写侧：OPEN 缺窗口。
        let mut open_no_window = header_(FrameKind::Open, 0u8);
        open_no_window.local_dock_ = Dock::new(3u32);
        open_no_window.remote_dock_ = Dock::new(7u32);
        assert_eq!(
            write_header_err_(&open_no_window),
            ErrKind::MalformedFrame
        );

        // 写侧：ACCEPT 带窗口（多余）。
        let mut accept_with_window = header_(FrameKind::Accept, 0u8);
        accept_with_window.local_dock_ = Dock::new(7u32);
        accept_with_window.remote_dock_ = Dock::new(3u32);
        accept_with_window.recv_window_ = Option::Some(2048u32);
        accept_with_window.recv_total_ = Option::Some(0u64);
        assert_eq!(
            write_header_err_(&accept_with_window),
            ErrKind::MalformedFrame
        );
    }
    dual_runtime_test_!(open_carries_window_but_accept_does_not);

    /// 测试 `wildcard` 与 `unspecified` 两个保留 dock 不能作为子流 dock。
    /// - 手段：在 `DATA` 帧里分别把 `LocalDock` 写成 `0`（`unspecified`）与
    ///   `u32::MAX`（`wildcard`）；再用写侧构造同样取保留值的帧头。
    /// - 判断：读侧的两种输入都报 [`MuxError::ReservedDock`]；写侧也拒绝。
    async fn reserved_docks_are_rejected_both_ways() {
        // unspecified = 0：0x00 是 LocalDock 的字段头，值为 1 字节 0x00。
        let unspecified = [0x05u8, 0x00, 0x00, 0x01, 0x02, 0x02, 0x00];
        assert_eq!(
            read_err_(&unspecified).await,
            Option::Some(ErrKind::ReservedDock)
        );

        // wildcard = u32::MAX：LocalDock 用 4 字节编码（0x30 | 0x00）。
        let wildcard = [
            0x05u8, 0x30, 0xFF, 0xFF, 0xFF, 0xFF, 0x01, 0x02, 0x02, 0x00,
        ];
        assert_eq!(
            read_err_(&wildcard).await,
            Option::Some(ErrKind::ReservedDock)
        );

        // 写侧：dock 取保留值同样拒绝。
        let mut header = header_(FrameKind::Data, 0u8);
        header.local_dock_ = Dock::unspecified();
        header.remote_dock_ = Dock::new(2u32);
        assert_eq!(write_header_err_(&header), ErrKind::ReservedDock);

        header.local_dock_ = Dock::new(1u32);
        header.remote_dock_ = Dock::wildcard();
        assert_eq!(write_header_err_(&header), ErrKind::ReservedDock);
    }
    dual_runtime_test_!(reserved_docks_are_rejected_both_ways);

    /// 测试 `RecvTotal` 只接受 2 / 4 / 8 字节三种规格，并按值取最小者。
    /// - 手段：`PULSE` 帧里把 `RecvTotal` 分别写成 0（`BeU16`）、70000（`BeU32`）、
    ///   `u32::MAX + 1`（`BeU64`），检查该字段的自描述头字节；再单独喂一个用
    ///   `BeU8` 编码 `RecvTotal` 的帧。
    /// - 判断：三种值分别编成 `0x14` / `0x34` / `0x44`；`BeU8` 编码报
    ///   [`MuxError::UnsupportedField`]。
    async fn recv_total_accepts_two_four_eight_byte_widths() {
        let cases = [
            (0u64, 0x14u8),
            (70_000u64, 0x34u8),
            (u32::MAX as u64 + 1u64, 0x44u8),
        ];
        for (value, expect_header) in cases {
            let mut header = header_(FrameKind::Pulse, 0u8);
            header.recv_window_ = Option::Some(64u32);
            header.recv_total_ = Option::Some(value);

            let mut buf = [0u8; 64];
            let total = write_header_into_buf_(&mut buf, &header);
            // 第 0 字节是帧首，其后依次是 LocalDock / RemoteDock，然后才是 RecvTotal。
            assert_eq!(buf[5], expect_header, "RecvTotal 的宽度规格不对");
            let parsed = read_header_from_buf_(&buf[..total])
                .await
                .expect("读回 PULSE 应当成功");
            assert_eq!(parsed.recv_total(), Option::Some(value));
        }

        // `BeU8` 编码的 RecvTotal（0x04）非法：累计量的最小规格是 2 字节。
        let one_byte_total = [0x08u8, 0x00, 0x01, 0x01, 0x02, 0x04, 0x00, 0x13, 0x00, 0x40, 0x02, 0x00];
        assert_eq!(
            read_err_(&one_byte_total).await,
            Option::Some(ErrKind::UnsupportedField)
        );
    }
    dual_runtime_test_!(recv_total_accepts_two_four_eight_byte_widths);

    /// 测试「累计量已重置」变体：`TOTAL_RESET` 标记往返，且只允许出现在保活 /
    /// 窗口更新帧上。
    /// - 手段：写一个带 `K_TOTAL_RESET` 的 `WINDOW_UPDATE` 帧并读回；再把同一标记
    ///   放到 `OPEN` 与 `DATA` 帧上。
    /// - 判断：读回的 `is_total_reset()` 为真且累计量是重置前的值；两种非法组合都报
    ///   [`MuxError::MalformedFrame`]。
    async fn total_reset_flag_is_a_window_report_variant() {
        let mut header = header_(FrameKind::WindowUpdate, flags::K_TOTAL_RESET);
        header.local_dock_ = Dock::new(4u32);
        header.remote_dock_ = Dock::new(6u32);
        header.recv_window_ = Option::Some(1024u32);
        header.recv_total_ = Option::Some(65_536u64);

        let mut buf = [0u8; 64];
        let total = write_header_into_buf_(&mut buf, &header);
        let parsed = read_header_from_buf_(&buf[..total])
            .await
            .expect("读回重置变体应当成功");
        assert!(parsed.is_total_reset());
        assert_eq!(parsed.recv_total(), Option::Some(65_536u64));

        // OPEN 时还没有 epoch：带上重置标记非法。
        let mut open = header_(FrameKind::Open, flags::K_TOTAL_RESET);
        open.recv_window_ = Option::Some(1024u32);
        open.recv_total_ = Option::Some(0u64);
        assert_eq!(write_header_err_(&open), ErrKind::MalformedFrame);

        // DATA 帧本来就不带窗口通告，更不该有重置标记。
        let on_data = [
            0x85u8, // 帧首：TOTAL_RESET(8) << 4 | DATA(5)
            0x00, 0x01, 0x01, 0x02, 0x02, 0x00,
        ];
        assert_eq!(
            read_err_(&on_data).await,
            Option::Some(ErrKind::MalformedFrame)
        );
    }
    dual_runtime_test_!(total_reset_flag_is_a_window_report_variant);

    /// 测试保留的 `kind` 与保留的字段标识都被拒绝。
    /// - 手段：帧首分别取 `0x00`（保留）与 `0x0A`（保留），以及字段标识 `0x06`
    ///   （保留）。
    /// - 判断：三种输入都报 [`MuxError::UnsupportedField`]。
    async fn reserved_kind_and_field_id_are_rejected() {
        // 保留 kind：0x00 与 0x0A..=0x0F。
        for head in [0x00u8, 0x0A, 0x0F] {
            assert_eq!(
                read_err_(&[head, 0x02, 0x00]).await,
                Option::Some(ErrKind::UnsupportedField)
            );
        }

        // 保留字段标识：DATA 帧首之后的 0x06 表示 field_id = 6（未分配）。
        let reserved_field = [0x05u8, 0x06, 0x00, 0x02, 0x00];
        assert_eq!(
            read_err_(&reserved_field).await,
            Option::Some(ErrKind::UnsupportedField)
        );
    }
    dual_runtime_test_!(reserved_kind_and_field_id_are_rejected);

    /// 测试 dock 字段拒绝非 1 / 2 / 4 字节的宽度，而其它字段接受非最小宽度。
    /// - 手段：给出以 `BeU24` / `BeU64` 编码的 `LocalDock`；再给出以 `BeU24`
    ///   编码的 `PayloadLen`（数值 3，本可用 1 字节）。
    /// - 判断：dock 的两种宽度都报 [`MuxError::UnsupportedField`]；非最小的
    ///   `PayloadLen` 被接受，解析出 3。
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
    dual_runtime_test_!(dock_rejects_non_byte_aligned_width);

    /// 测试字段重复与必需字段缺失都被判为结构非法。
    /// - 手段：给出「两个 `LocalDock`」与「`DATA` 帧缺 `RemoteDock`」两种字段
    ///   序列。
    /// - 判断：两种输入都报 [`MuxError::MalformedFrame`]。
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
    dual_runtime_test_!(duplicate_and_missing_fields_are_rejected);

    /// 测试 `ReasonCode` 只允许出现在 `REJECT` 帧上，且取值必须能装进 `u8`。
    /// - 手段：`REJECT` + `ReasonCode = 7`（合法）；`REJECT` + `ReasonCode = 0x100`
    ///   （超 `u8`）；`DATA` + `ReasonCode = 7`（种类不符）。
    /// - 判断：第一种解析成功（且不保留该字段）；后两种都报
    ///   [`MuxError::MalformedFrame`]。
    async fn reason_code_only_on_reject() {
        let reject_with_reason = [
            0x03u8, // 帧首：REJECT
            0x00, 0x01, // LocalDock = 1
            0x01, 0x02, // RemoteDock = 2
            0x05, 0x07, // ReasonCode = 7
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
            0x15, 0x01, 0x00, // ReasonCode = 0x0100（BeU16）
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
            0x05, 0x07, // ReasonCode = 7
            0x02, 0x00, // PayloadLen = 0
        ];
        assert_eq!(
            read_err_(&reason_on_data).await,
            Option::Some(ErrKind::MalformedFrame)
        );
    }
    dual_runtime_test_!(reason_code_only_on_reject);

    /// 测试除 `PayloadLen` 外的字段顺序不承载语义。
    /// - 手段：`WINDOW_UPDATE` 帧按「WindowUpdate → RemoteDock → LocalDock →
    ///   PayloadLen」排列。
    /// - 判断：解析成功，各字段取值与排列顺序无关地正确。
    async fn field_order_before_payload_len_is_free() {
        let shuffled = [
            0x07u8, // 帧首：WINDOW_UPDATE
            0x13, 0x00, 0x05, // RecvWindow = 5（BeU16 编码）
            0x01, 0x02, // RemoteDock = 2
            0x00, 0x01, // LocalDock = 1
            0x14, 0x00, 0x09, // RecvTotal = 9（BeU16 编码）
            0x02, 0x00, // PayloadLen = 0
        ];
        let parsed = read_header_from_buf_(&shuffled)
            .await
            .expect("任意字段顺序都应当能解析");
        assert_eq!(parsed.recv_window(), Option::Some(5u32));
        assert_eq!(parsed.local_dock(), Dock::new(1u32));
        assert_eq!(parsed.remote_dock(), Dock::new(2u32));
    }
    dual_runtime_test_!(field_order_before_payload_len_is_free);

    /// 测试 `PayloadLen` 是头字段序列的定界符：它之后的字节属于载荷。
    /// - 手段：`PayloadLen = 1` 之后再放一个「看起来像字段」的字节 `0xFF`。
    /// - 判断：解析成功且 `payload_len` 为 1——说明 `0xFF` 没有被当成头字段
    ///   （否则会因保留字段标识而报 [`MuxError::UnsupportedField`]）。
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
    dual_runtime_test_!(payload_len_terminates_the_header);

    /// 测试帧中途截断时如实上报错误，而不是解析出一半成功。
    /// - 手段：给空缓冲，以及「`LocalDock` 头字节之后缺值字节」两种截断输入。
    /// - 判断：两种情况都**必须报错**（不允许把半截头当成成功）。具体种类是
    ///   [`ErrKind::PeerClosed`]：字节流在帧头读完之前就结束了，逐字节状态机把它
    ///   归为「对端关闭」——这是切到新解析器后的**有意细化**，旧实现把所有读侧错误
    ///   一律折成读方向传输错误（[`ErrKind::Rx`]）。真正的传输故障仍映射到 `Rx`
    ///   （见 `err_kind_`），只是切片夹具表现不出那种情形。
    async fn truncated_header_is_not_a_partial_success() {
        assert_eq!(
            read_err_(&[]).await,
            Option::Some(ErrKind::PeerClosed)
        );
        // 0x00 只声明了 LocalDock 的字段头，值字节缺失。
        assert_eq!(
            read_err_(&[0x05u8, 0x00]).await,
            Option::Some(ErrKind::PeerClosed)
        );
    }
    dual_runtime_test_!(truncated_header_is_not_a_partial_success);
}
