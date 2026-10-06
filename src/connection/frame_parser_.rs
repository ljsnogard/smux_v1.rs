//! 帧头的**逐字节**解析器（sans-IO 状态机）。
//!
//! 本模块是 [`super::frame_`] 里 [`read_header_async_`](super::frame_::read_header_async_)
//! 的替代实现，动机只有一条：**旧入口按字段索要字节，索要量一旦超过环容量就不可满足**
//! （见本模块末尾「为什么不能按字段索要」）。
//!
//! # 形状：定长内置缓冲 + 逐字节推进
//!
//! [`FrameHeaderParser`] 自带一个 `[u8; 9]` 的定长数组，随调用方逐字节喂入产生状态变化，
//! 直到帧头解析完成。它**不接触任何 IO**：喂字节是同步的，因此可以在没有异步上下文、
//! 没有分配器的场合使用（单元测试、`no_std` 环境、任何运行时）。
//!
//! ## 9 字节为什么够
//!
//! 字段是**逐个串行**解析的：任意时刻只有一个字段「在途」，而一个字段在线上最多是
//! `1` 字节自描述头 + `8` 字节大端值（[`FieldValType::BeU64`]）。因此内置缓冲只需
//! 覆盖「一个头字节 + 一个值」就够，**与帧长、字段数量无关**——这正是逐字节状态机
//! 相对「先收完整帧」的核心优势，与握手侧 `codec_` 的结论同源。
//!
//! # 状态划分
//!
//! ```text
//! Start  --帧首字节-->  FieldHeader  --自描述头字节-->  FieldValue(width)
//! FieldValue --收满 width 字节-->  FieldHeader（或 Done）
//! ```
//!
//! - `Start`：等帧首字节，一次拆出 `kind`（低 4 位）与 `flags`（高 4 位）；
//! - `FieldHeader`：等自描述头字节，一次拆出 `field_id`（低 4 位）与 `val_type`
//!   （bit 4..=6；bit 7 是保留位，按 [`K_VAL_TYPE_MASK`] 忽略）。**标识与宽度的
//!   相容性校验在这里完成**，这样「宽度非法」不必等值字节到齐才报；
//! - `FieldValue`：把值字节**就地缓存进内置定长缓冲**，收满即按大端解码、按标识归位，
//!   然后回到 `FieldHeader`。读到 `PayloadLen` 时头结束。
//!
//! # 消费不变量（调用方最需要知道的一条）
//!
//! [`FrameHeaderParser::consume_byte_`] **只要还需要更多输入，就必定消费这 1 个字节**
//! ——包括「这 1 个字节正好让帧头完成」（[`Consume_::Done`]）或「正好判定帧头非法」
//! （[`Consume_::Failed`]）两种情况。反过来，一旦出了结论，重复喂入一律返回既成结论、
//! **不消费**。
//!
//! 这条不变量让喂字节的循环不必回退、也不必自己缓存字节，因此在字节流上**不会丢字节**；
//! 调用方只有在「已经知道帧头完成、但最后一个字节已经喂进去」时才需要
//! [`FrameHeaderParser::finish_`]。
//!
//! # 与旧实现的关系
//!
//! 校验规则（必需字段、字段重复、字段与 `kind` 的组合、保留值）与旧入口**逐条对应**；
//! 输出仍是同一个 [`FrameHeader`]，出错仍是同一组 [`MuxError`] 变体。其中
//! [`decode_dock_field_`](super::frame_::decode_dock_field_) 与
//! [`requires_window_report_`](super::frame_::requires_window_report_) 直接复用旧模块的
//! 实现，避免两处各写一份「哪些帧需要窗口通告」。
//!
//! # 为什么不能按字段索要字节
//!
//! 旧入口用 `ReadCursor` 按字段索要 `Demand::exactly(width)`。`buffex::ring` 在
//! `min_len > cap` 时直接返回 `ConsumerError::Unsatisfiable`（终态，不会被 pump 循环
//! 重试），因此**环容量小于字段宽度时整条连接失败**：容量 `1` 的环连一个 2 字节的
//! `RemoteDock` 都读不出来。按字节索要（`Demand::exactly(1)`）后 `min_len == 1`，
//! 恒不超过任何合法容量，这条失败路径**从构造上不再存在**。

// 本模块尚未被中心循环调用：接入点在 `session_.rs` 的解复用循环，切换留待下一轮
// （sans-IO 状态机的逐字节推进）。与 `frame_.rs` 同样保留
// `dead_code` 允许；**接入完成后必须连同 `frame_.rs` 的那一行一起移除**。
#![allow(dead_code)]

use abs_buff::{
    TrBuffRead,
    error::{ReadErrTag, TrTaggedError},
    x_deps::abs_cancel,
};
use abs_cancel::TrCancellationToken;
use buffex::x_deps::abs_buff;


use crate::{
    connection::{
        Dock, MuxError,
        frame_::{
            FieldId, FrameHeader, FrameKind, decode_dock_field_,
            flags, flags_from_frame_head_, requires_window_report_,
        },
    },
    flow_ctrl::{Credit, RecvTotal},
    handshake::opts::NegotiationValType as FieldValType,
    wire_io_::{CursorError, ReadCursor},
};

/// 内置字节数组的容量：一个自描述头字节 + 一个最宽的值（`BeU64` 的 8 字节）。
///
/// 字段逐个串行解析，因此这是**上界**而不是「一帧的头部长度」——帧头可以比它长得多。
pub const K_HEADER_BUFFER_LEN: usize = 9;

/// 字段值累加进 `usize` 时，超出本机 `usize` 表示范围即帧结构非法。
///
/// 在 64 位目标上恒不触发；32 位目标上，用 `BeU64` 编码的 `RecvTotal` 若数值超过
/// `u32::MAX` 会走到这里（与旧实现的 `decode_value_` 结论一致）。
const K_VALUE_TOO_WIDE: MuxError = MuxError::MalformedFrame;

/// 一次 [`FrameHeaderParser::consume_byte_`] 的结果。
///
/// # 相等性
///
/// 本类型实现 [`PartialEq`]，其中 [`Consume_::Done`] 按帧头的**协议可观测字段**比较
/// （帧种类、标志、dock 对、载荷长度、窗口通告）。之所以不派生
/// `PartialEq`：本模块不改动 `FrameHeader` 的既有派生集。
#[derive(Debug, Clone, Copy)]
pub enum Consume_ {
    /// 已消费该字节，帧头还没解析完，继续喂下一个字节。
    Pending,

    /// 已消费该字节，且帧头恰好在此字节完成。
    Done(FrameHeader),

    /// 该字节使帧头成为非法，解析**已终止**；后续喂入不再消费任何字节、并返回同一个
    /// 错误（幂等）。
    Failed(MuxError),
}

/// 按帧头的协议可观测字段比较两个帧头（见 [`Consume_`] 的「相等性」一节）。
fn same_header_(lhs: &FrameHeader, rhs: &FrameHeader) -> bool {
    lhs.kind() == rhs.kind()
        && lhs.flags() == rhs.flags()
        && lhs.local_dock() == rhs.local_dock()
        && lhs.remote_dock() == rhs.remote_dock()
        && lhs.payload_len() == rhs.payload_len()
        && lhs.recv_window() == rhs.recv_window()
        && lhs.recv_total() == rhs.recv_total()
}

impl PartialEq for Consume_ {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Consume_::Pending, Consume_::Pending) => true,
            (Consume_::Done(lhs), Consume_::Done(rhs)) => same_header_(lhs, rhs),
            (Consume_::Failed(lhs), Consume_::Failed(rhs)) => lhs == rhs,
            _ => false,
        }
    }
}

impl Eq for Consume_ {}

/// 状态机的内部状态。
#[derive(Debug, Clone, Copy)]
enum State_ {
    /// 还没读到帧首字节。
    Start,

    /// 正等一个字段的自描述头字节。
    FieldHeader,

    /// 已读到自描述头，正等这条字段的 `width` 个值字节。
    FieldValue {
        /// 该字段是什么（决定值往哪个槽位归位）。
        id_: FieldId,
        /// 该字段的值是几个字节（1 / 2 / 3 / 4 / 8）。
        width_: usize,
        /// 值字节已经收到几个（顺序写在 `buffer_[1..=got_]`）。
        got_: usize,
    },

    /// 帧头已解析完成；结论在此**只算一次**，重复调用直接取回。
    Done(FrameHeader),

    /// 帧头已判定非法；结论在此**只算一次**，重复调用直接取回。
    Failed(MuxError),
}

/// 已解析字段的暂存槽位。
///
/// 只在读到 `PayloadLen`（头字段序列的定界符）时才把各槽位拼成 [`FrameHeader`]，
/// 因此「字段是否重复」「字段组合是否合法」可以在**头结束之后**一次性判定。
#[derive(Debug, Clone, Copy)]
struct Slots_ {
    kind_: FrameKind,
    flags_: u8,
    local_dock_: Option<Dock>,
    remote_dock_: Option<Dock>,
    recv_window_: Option<Credit>,
    recv_total_: Option<RecvTotal>,
    reason_seen_: bool,
}

impl Slots_ {
    /// 由帧首字节建出空槽位。
    const fn new_(kind: FrameKind, flags: u8) -> Self {
        Slots_ {
            kind_: kind,
            flags_: flags,
            local_dock_: Option::None,
            remote_dock_: Option::None,
            recv_window_: Option::None,
            recv_total_: Option::None,
            reason_seen_: false,
        }
    }
}

/// 把内置缓冲里 `[1..=width]` 的值字节按大端解码。
///
/// 值字节就存在 [`FrameHeaderParser`] 的定长缓冲里，`buffer_[0]` 是这条字段的自描述
/// 头字节，因此值的起点固定是下标 `1`。解码不额外申请缓冲、也不额外保存副本。
fn decode_value_(buffer: &[u8; K_HEADER_BUFFER_LEN], width: usize) -> Option<usize> {
    debug_assert!((1usize..K_HEADER_BUFFER_LEN).contains(&width));
    // 把值字节拷到 8 字节槽的**末尾**，其余高位保持 0：宽度不足 8 时左侧补 0，
    // 于是 `u64::from_be_bytes` 给出的就是该宽度上的无符号大端解码值。
    //
    // 注意这里必须按 `width` 显式取下标的字节切片来迭代：`Iterator::zip` 会在
    // 较短一侧耗尽时停止，若拿整个 `[u8; 8]` 与 `[1..=width]` 对拉，剩余的高位
    // 槽**不会被赋值**而留在末位，结果会像左移一样被放大。
    let mut bytes = [0u8; 8];
    let start = 8usize - width;
    for (dst, src) in bytes[start..].iter_mut().zip(&buffer[1..=width]) {
        *dst = *src;
    }
    usize::try_from(u64::from_be_bytes(bytes)).ok()
}

/// 把一条已解码的字段归位到槽位。
///
/// 返回 `Ok(None)` 表示这条字段已经把帧头收尾（`PayloadLen`），其中 [`Option`] 内是载荷
/// 长度；返回 `Ok(Some(()))` 表示还要继续读字段。
///
/// # Errors
///
/// - 字段重复 → [`MuxError::MalformedFrame`]；
/// - 值超出目标类型的表示范围（`RecvWindow` 装不进 `Credit`、`ReasonCode` 装不进
///   `u8`）→ [`MuxError::MalformedFrame`]；
/// - dock 取保留值 → [`MuxError::ReservedDock`]（经
///   [`decode_dock_field_`](super::frame_::decode_dock_field_)。
fn place_value_(
    slots: &mut Slots_,
    id: FieldId,
    value: usize,
) -> Result<Option<usize>, MuxError> {
    match id {
        FieldId::LocalDock => {
            if slots.local_dock_.is_some() {
                return Result::Err(MuxError::MalformedFrame);
            }
            slots.local_dock_ = Option::Some(decode_dock_field_(value)?);
        }
        FieldId::RemoteDock => {
            if slots.remote_dock_.is_some() {
                return Result::Err(MuxError::MalformedFrame);
            }
            slots.remote_dock_ = Option::Some(decode_dock_field_(value)?);
        }
        FieldId::RecvWindow => {
            if slots.recv_window_.is_some() {
                return Result::Err(MuxError::MalformedFrame);
            }
            // 窗口值就是 `Credit`（`u32`）：装不进即结构非法。
            slots.recv_window_ =
                Option::Some(Credit::try_from(value).map_err(|_| MuxError::MalformedFrame)?);
        }
        FieldId::RecvTotal => {
            if slots.recv_total_.is_some() {
                return Result::Err(MuxError::MalformedFrame);
            }
            // 累计字节数是 `u64`：`usize` 只有 32 位时也容得下 `u32` 以上的取值。
            slots.recv_total_ = Option::Some(value as RecvTotal);
        }
        FieldId::ReasonCode => {
            if slots.reason_seen_ {
                return Result::Err(MuxError::MalformedFrame);
            }
            // 只校验取值域，不保存：拒绝理由由 `REJECT` 的载荷承载（模块文档）。
            if u8::try_from(value).is_err() {
                return Result::Err(MuxError::MalformedFrame);
            }
            slots.reason_seen_ = true;
        }
        // 头字段序列的定界符：读到它即知头结束、其后就是载荷。
        FieldId::PayloadLen => return Result::Ok(Option::Some(value)),
    }
    Result::Ok(Option::None)
}

/// 把槽位拼成 [`FrameHeader`]，并做「字段与 `kind` 的组合」校验。
///
/// 与旧入口逐条对应：
///
/// - dock 对在本版本的**每一种**帧上都必需（本版本的帧都是子流作用域）；
/// - 窗口通告（`RecvWindow` + `RecvTotal`）只在 `OPEN` / `PULSE` / `WINDOW_UPDATE`
///   上出现，且必须成对齐全；
/// - `ReasonCode` 只允许出现在 `REJECT` 帧上；
/// - [`flags::K_TOTAL_RESET`] 只对窗口通告有意义，且 `OPEN` 时还没有 epoch。
///
/// # Errors
///
/// 任一组合不满足即 [`MuxError::MalformedFrame`]。
fn assemble_(slots: &Slots_, payload_len: usize) -> Result<FrameHeader, MuxError> {
    let (local_dock, remote_dock) = match (slots.local_dock_, slots.remote_dock_) {
        (Option::Some(local), Option::Some(remote)) => (local, remote),
        _ => return Result::Err(MuxError::MalformedFrame),
    };

    let window = match (requires_window_report_(slots.kind_), slots.recv_window_, slots.recv_total_)
    {
        (true, Option::Some(window), Option::Some(total)) => Option::Some((total, window)),
        (false, Option::None, Option::None) => Option::None,
        _ => return Result::Err(MuxError::MalformedFrame),
    };

    if slots.reason_seen_ && slots.kind_ != FrameKind::Reject {
        return Result::Err(MuxError::MalformedFrame);
    }

    if slots.flags_ & flags::K_TOTAL_RESET != 0
        && !matches!(slots.kind_, FrameKind::Pulse | FrameKind::WindowUpdate)
    {
        return Result::Err(MuxError::MalformedFrame);
    }

    Result::Ok(FrameHeader::new_(
        slots.kind_,
        slots.flags_,
        local_dock,
        remote_dock,
        payload_len,
        window,
    ))
}

/// 帧头的逐字节解析状态机。
///
/// # 用法
///
/// 下面的示例针对**将来的公开路径**书写。本模块当前是 crate 私有模块，示例因此标记
/// `ignore`、不参与文档测试；接入并导出后应改回可编译的示例。
///
/// ```ignore
/// use smux_v1::connection::{Consume_, FrameHeaderParser, FrameKind};
///
/// // DATA 帧：帧首 0x05，LocalDock=3、RemoteDock=2、PayloadLen=0。
/// let bytes: [u8; 7] = [0x05, 0x00, 0x03, 0x01, 0x02, 0x02, 0x00];
/// let mut parser = FrameHeaderParser::new();
/// let mut header = None;
/// for byte in bytes {
///     if let Consume_::Done(done) = parser.consume_byte_(byte) {
///         header = Some(done);
///     }
/// }
/// let header = header.expect("帧头应当解析完成");
/// assert_eq!(header.kind(), FrameKind::Data);
/// assert_eq!(header.payload_len(), 0usize);
/// ```
#[derive(Debug, Clone)]
pub struct FrameHeaderParser {
    /// 内置定长缓冲：`[0]` 是当前字段的自描述头字节，`[1..=width]` 是它的值字节
    /// （大端，未用到的尾部恒为 0）。
    ///
    /// **值字节就存在这里**，收到一条字段的最后 1 个字节后直接从本缓冲解码，不额外
    /// 申请、也不额外保存副本。之所以定长：值与头字节都是**定长**的，宽度也由头字节
    /// 当场给出，因此不存在「按声明长度申请缓冲」的路径（对比握手侧 `codec_` 对扩展
    /// 键的处理）。
    buffer_: [u8; K_HEADER_BUFFER_LEN],
    /// 当前状态。
    state_: State_,
    /// 已归位的字段槽位；只在头结束时使用。
    slots_: Option<Slots_>,

    /// **本轮解析**已消费的字节数：自这一轮的**帧首字节**起算（它计为 `1`）。
    ///
    /// # 生命周期：一轮解析一个值
    ///
    /// - 解析出结论（[`Consume_::Done`] / [`Consume_::Failed`]）时，它就是这一帧的
    ///   **帧头长度**；出结论后重复喂入不改变它（幂等，见
    ///   [`FrameHeaderParser::consume_byte_`] 的「消费不变量」）；
    /// - **回到解析开头即重新起算**：状态机重新进入 [`State_::Start`] 时本字段被重置，
    ///   因此它**不是**跨解析、更不是跨实例的累计量。
    ///
    /// 后一条是硬约束而不是风格：把「累计已消费」当帧头长度用，一旦解析器被复用，
    /// 第一帧之后的每一帧都会得到一个偏大的长度（它包含了此前所有帧的字节）。
    ///
    /// # 为什么由状态机自己数
    ///
    /// 帧头长度曾经「拿不到」，理由是解析入口逐字节推进、并不回报吃了多少。但换一个
    /// 角度就有一个零歧义的来源：**状态机本来就逐个吃掉这些字节**，它自己数最准。
    ///
    /// 不能改从环状态推：解析在环空时会 park，而外侧读泵会在那一刻往**同一个**环里
    /// 填新字节，因此「前后 `data_size()` 之差」量的是「期间消费 − 期间写入」；而且
    /// `buffex` 的环状态只有物理读写指针（`IoPos { rp, wp }`，都在 `[0, capacity)` 内
    /// 环绕），**没有单调累计量**可用。
    consumed_: usize,
}

impl Default for FrameHeaderParser {
    fn default() -> Self {
        Self::new()
    }
}

impl FrameHeaderParser {
    /// 建一个处于起始状态的解析器。
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use smux_v1::connection::FrameHeaderParser;
    ///
    /// let parser = FrameHeaderParser::new();
    /// assert!(parser.is_started_());
    /// ```
    pub const fn new() -> Self {
        FrameHeaderParser {
            buffer_: [0u8; K_HEADER_BUFFER_LEN],
            state_: State_::Start,
            slots_: Option::None,
            consumed_: 0usize,
        }
    }

    /// 帧头是否还**一个字节都没读**。
    pub const fn is_started_(&self) -> bool {
        matches!(self.state_, State_::Start)
    }

    /// 帧头是否已经解析完成（可取 [`FrameHeaderParser::finish_`]）。
    pub const fn is_done_(&self) -> bool {
        matches!(self.state_, State_::Done(_))
    }

    /// 帧头是否已判定非法（结论在 [`FrameHeaderParser::finish_`]）。
    pub const fn is_failed_(&self) -> bool {
        matches!(self.state_, State_::Failed(_))
    }

    /// 逐字节推进一步。
    ///
    /// **消费不变量**（见模块文档）：只要还需要更多输入就必定消费 `byte`——包括它正好
    /// 让帧头完成（[`Consume_::Done`]）或正好判定帧头非法（[`Consume_::Failed`]）的情况。
    /// 出结论之后的重复喂入一律返回既成结论、不再消费。
    ///
    /// # Errors
    ///
    /// 通过 [`Consume_::Failed`] 返回，与旧入口同一组变体：
    ///
    /// - 未知 / 保留的 `kind`、字段标识，或该标识不接受的宽度 →
    ///   [`MuxError::UnsupportedField`]；
    /// - 字段重复、值超出目标类型范围、缺少必需字段、字段与 `kind` 的组合不符 →
    ///   [`MuxError::MalformedFrame`]；
    /// - dock 取保留值 → [`MuxError::ReservedDock`]。
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use smux_v1::connection::{Consume_, FrameHeaderParser};
    ///
    /// // 最小 DATA 帧头：帧首 + LocalDock=1 + RemoteDock=2 + PayloadLen=0。
    /// let mut parser = FrameHeaderParser::new();
    /// assert_eq!(parser.consume_byte_(0x05), Consume_::Pending);
    /// assert_eq!(parser.consume_byte_(0x00), Consume_::Pending);
    /// assert_eq!(parser.consume_byte_(0x01), Consume_::Pending);
    /// assert_eq!(parser.consume_byte_(0x01), Consume_::Pending);
    /// assert_eq!(parser.consume_byte_(0x02), Consume_::Pending);
    /// assert_eq!(parser.consume_byte_(0x02), Consume_::Pending);
    /// // 最后一个字节让帧头完成，它同样被消费。
    /// assert!(matches!(parser.consume_byte_(0x00), Consume_::Done(_)));
    /// assert!(parser.is_done_());
    /// ```
    pub fn consume_byte_(&mut self, byte: u8) -> Consume_ {
        // 已出结论：重复喂入不消费字节（幂等），因此也**不**计入 `consumed_`。
        if self.is_done_() || self.is_failed_() {
            match self.state_ {
                State_::Done(header) => return Consume_::Done(header),
                State_::Failed(err) => return Consume_::Failed(err),
                // 上面的判定已排除其余状态。
                _ => {}
            }
        }
        // 计数**只覆盖本轮解析**：`Start` 是新一轮解析的开头，从它重新起算（本字节算
        // 第 1 个，因此回到 `Start` 就等于把计数 reset）；其余状态在本轮内累加。
        // 终态在上面已经短路，所以出结论之后不会再进来。
        if matches!(self.state_, State_::Start) {
            self.consumed_ = 1usize;
        } else {
            self.consumed_ = self.consumed_.saturating_add(1usize);
        }

        match self.state_ {
            // 已出结论：重复喂入不消费字节（幂等）。
            State_::Done(header) => Consume_::Done(header),
            State_::Failed(err) => Consume_::Failed(err),

            // 1. 帧首字节：一次拆出 `kind` 与 `flags`。
            State_::Start => {
                let Option::Some(kind) = FrameKind::try_from(byte).ok() else {
                    return self.fail_(MuxError::UnsupportedField);
                };
                // 字段序列还没开始，槽位在这里建出（此后不再改动帧首信息）。
                self.slots_ = Option::Some(Slots_::new_(kind, flags_from_frame_head_(byte)));
                self.state_ = State_::FieldHeader;
                Consume_::Pending
            }

            // 2. 自描述头字节：拆出标识与宽度，并当场校验两者相容。
            State_::FieldHeader => {
                let Option::Some(id) = FieldId::from_header_(byte) else {
                    return self.fail_(MuxError::UnsupportedField);
                };
                let Ok(val_type) = FieldValType::try_from(byte) else {
                    return self.fail_(MuxError::UnsupportedField);
                };
                if !id.accepts_val_type_(val_type) {
                    return self.fail_(MuxError::UnsupportedField);
                }

                let width = val_type.value_len();
                // 宽度取值域是 1 / 2 / 3 / 4 / 8，恒落在内置缓冲的头字节之外的部分。
                debug_assert!((1usize..K_HEADER_BUFFER_LEN).contains(&width));
                // 头字节与随后的值字节共用内置缓冲：`[0]` 是头字节，`[1..]` 是值。
                self.buffer_ = [0u8; K_HEADER_BUFFER_LEN];
                self.buffer_[0] = byte;
                self.state_ = State_::FieldValue { id_: id, width_: width, got_: 0usize };
                Consume_::Pending
            }

            // 3. 值字节：就地缓存进内置缓冲；收满即解码归位。
            State_::FieldValue { id_, width_, got_ } => {
                // `got_` 是「已收到几个」，因此本字节落在 `1 + got_`；值字节从下标 1
                // 起依次落位，`buffer_[0]` 是这条字段的自描述头字节。
                let Some(slot) = self.buffer_.get_mut(1usize + got_) else {
                    // 不入此分支：`got_ < width_` 且 `width_` 落在缓冲范围内。
                    return self.fail_(MuxError::MalformedFrame);
                };
                *slot = byte;

                let got = got_ + 1usize;
                if got < width_ {
                    self.state_ = State_::FieldValue {
                        id_,
                        width_,
                        got_: got,
                    };
                    return Consume_::Pending;
                }

                let Option::Some(value) = decode_value_(&self.buffer_, width_) else {
                    return self.fail_(K_VALUE_TOO_WIDE);
                };
                let Option::Some(slots) = self.slots_.as_mut() else {
                    return self.fail_(MuxError::MalformedFrame);
                };
                let outer = match place_value_(slots, id_, value) {
                    Result::Ok(outer) => outer,
                    Result::Err(err) => return self.fail_(err),
                };

                match outer {
                    // 还有更多头字段。
                    Option::None => {
                        self.state_ = State_::FieldHeader;
                        Consume_::Pending
                    }
                    // `PayloadLen` 收尾：拼出帧头，检查组合约束。
                    Option::Some(payload_len) => {
                        let Option::Some(slots) = self.slots_ else {
                            return self.fail_(MuxError::MalformedFrame);
                        };
                        match assemble_(&slots, payload_len) {
                            Result::Ok(header) => {
                                self.state_ = State_::Done(header);
                                Consume_::Done(header)
                            }
                            Result::Err(err) => self.fail_(err),
                        }
                    }
                }
            }
        }
    }

    /// 取回已完成（或已失败）的结论。
    ///
    /// 两个用途：
    ///
    /// 1. 供「先按需要喂字节、再统一取结果」的调用方收尾——此时最后一个字节已被
    ///    [`FrameHeaderParser::consume_byte_`] 消费，只需把结论取出来；
    /// 2. 供字节来源不归调用方管的场合复核结论。
    ///
    /// 已出结论时幂等；否则返回 `None`（还没解析完，或还一个字节都没读）。
    pub const fn finish_(&self) -> Option<Result<FrameHeader, MuxError>> {
        match self.state_ {
            State_::Done(header) => Option::Some(Result::Ok(header)),
            State_::Failed(err) => Option::Some(Result::Err(err)),
            _ => Option::None,
        }
    }

    /// **本轮解析**已消费的字节数；解析完成时它**就是帧头长度**（含帧首字节与全部
    /// 头字段）。
    ///
    /// 契约只覆盖**单轮**：状态机回到 [`State_::Start`] 时本计数重新起算，因此它**不是**
    /// 累计量——把累计量当帧头长度用，一旦解析器被复用就会给出偏大的长度（理由见字段
    /// 文档）。失败路径上它同样是「这一轮已经吃掉多少字节」，但那条路径的调用方只需要
    /// 错误本身。
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use smux_v1::connection::{Consume_, FrameHeaderParser};
    ///
    /// let mut parser = FrameHeaderParser::new();
    /// // ……逐字节喂入，直到 `Consume_::Done`……
    /// let head_len = parser.consumed_len_();
    /// ```
    pub const fn consumed_len_(&self) -> usize {
        self.consumed_
    }

    /// 记下失败结论并终止解析。
    ///
    /// 失败结论与完成结论一样**只算一次**：此后 [`FrameHeaderParser::consume_byte_`]
    /// 一律返回同一个错误、不再消费任何字节。
    fn fail_(&mut self, err: MuxError) -> Consume_ {
        self.state_ = State_::Failed(err);
        Consume_::Failed(err)
    }
}

/// 从读半边解析出一个帧头（逐字节喂入状态机）。
///
/// 与旧入口 [`read_header_async_`](super::frame_::read_header_async_) 的区别只在
/// **索取粒度**：本函数内部**每次只索要 1 个字节**（`Demand::exactly(1)`），
/// 因此 `min_len == 1` 恒不超过任何合法环容量——旧入口「按字段索要 `width` 字节」
/// 在环容量小于 `width` 时会拿到终态的 `Unsatisfiable` 而使整条连接失败
/// （见模块文档 §「为什么不能按字段索要字节」）。
///
/// 语义与旧入口一致：读到 `PayloadLen` 字段为止，随后由调用方按
/// [`FrameHeader::payload_len`] 读取载荷；任一步失败即整帧失败（不保留部分状态，
/// 因为连接随后即终止）。
///
/// # Errors
///
/// - 未知 / 保留的 `kind` 或字段标识、非法宽度 → [`MuxError::UnsupportedField`]；
/// - 缺少必需字段、字段重复、字段与 `Kind` 的组合不符 → [`MuxError::MalformedFrame`]；
/// - dock 取保留值 → [`MuxError::ReservedDock`]；
/// - 底层读失败 → [`MuxError::Transport`]`{ write: false }`；
/// - 读满一帧头之前对端关闭 → [`MuxError::PeerClosed`]；
/// - 令牌被触发 → [`MuxError::Cancelled`]。
pub(crate) async fn read_header_async_<R, K>(
    rx: &mut R,
    cancel: K,
) -> Result<(FrameHeader, usize), MuxError>
where
    R: TrBuffRead<u8>,
    K: TrCancellationToken,
{
    let mut parser = FrameHeaderParser::new();
    let mut cursor = ReadCursor::new_(rx);

    loop {
        // 每次只要 1 个字节：任何合法容量（`buffex` 的下限是 1）都满足这个下限，
        // 因此不会出现上限低于下限的 `Unsatisfiable`。
        let byte = match cursor.read_byte_async_(cancel.child_token()).await {
            Result::Ok(byte) => byte,
            Result::Err(err) => return Result::Err(map_read_cursor_err_(err)),
        };

        match parser.consume_byte_(byte) {
            Consume_::Pending => continue,
            // 第二个分量是**帧头长度**（逐字节状态机自己数的已消费字节数）：
            // 调用方把它与 `payload_len` 相加，就得到该帧的**线上总字节数**。
            Consume_::Done(header) => return Result::Ok((header, parser.consumed_len_())),
            Consume_::Failed(err) => return Result::Err(err),
        }
    }
}

/// 把读游标错误映射为连接错误。
///
/// 与 [`super::frame_::read_header_async_`] 的映射基本同一套，两处按标签细化：
///
/// - [`ReadErrTag::Cancelled`] → [`MuxError::Cancelled`]（而不是笼统的读方向传输错误）；
/// - [`ReadErrTag::Closing`] → [`MuxError::PeerClosed`]：底层用这个标签表示「不会再有
///   数据了」。切片读源在耗尽时报的就是它，环读端在写端已关闭且环已排空时报的也是它。
///
/// 于是「对端关闭」无论经 [`CursorError::PeerClosed`] 还是经 `Closing` 标签抵达，
/// 都收敛成同一个 [`MuxError::PeerClosed`]。
fn map_read_cursor_err_<E>(err: CursorError<E, ()>) -> MuxError
where
    E: TrTaggedError<ReadErrTag>,
{
    match err {
        CursorError::Read(err) => match err.err_tag() {
            ReadErrTag::Cancelled => MuxError::Cancelled,
            ReadErrTag::Closing => MuxError::PeerClosed,
            _ => MuxError::Transport { write: false },
        },
        CursorError::Write(()) => MuxError::Transport { write: true },
        CursorError::PeerClosed => MuxError::PeerClosed,
    }
}

#[cfg(test)]
mod tests_ {
    use super::*;
    use crate::connection::frame_::encode_header_into_;

    /// 断言用的错误种类：把底层错误折叠掉，只保留协议层语义。
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum ErrKind {
        UnsupportedField,
        MalformedFrame,
        ReservedDock,
        PeerClosed,
        Rx,
    }

    /// 取出错误的协议层种类，忽略底层读错误的细节。
    /// - 手段：对 [`MuxError`] 的变体做匹配；出现测试未覆盖的变体即 panic。
    /// - 判断：返回的 [`ErrKind`] 与被测代码产生的变体一一对应。
    fn err_kind_(err: MuxError) -> ErrKind {
        match err {
            MuxError::UnsupportedField => ErrKind::UnsupportedField,
            MuxError::MalformedFrame => ErrKind::MalformedFrame,
            MuxError::ReservedDock => ErrKind::ReservedDock,
            MuxError::PeerClosed => ErrKind::PeerClosed,
            MuxError::Transport { write: false } => ErrKind::Rx,
            other => panic!("测试未覆盖的错误种类：{other}"),
        }
    }

    /// 把一个帧头编码成字节。
    /// - 手段：调用 `encode_header_into_` 得到 `Vec`。
    /// - 判断：编码成功；返回写出的完整帧头字节。
    fn encode_(header: &FrameHeader) -> Vec<u8> {
        let mut bytes = Vec::new();
        encode_header_into_(&mut bytes, header).expect("编码帧头应当成功");
        bytes
    }

    /// 把 `bytes` **逐字节**喂进一个新解析器，返回结论。
    /// - 手段：循环调用 [`FrameHeaderParser::consume_byte_`]，只在拿到 `Done` / `Failed`
    ///   时收尾；字节喂完仍未出结论则 panic（说明状态机漏掉了收尾条件）。
    /// - 判断：`Ok` 为解析出的帧头，`Err` 为被测代码报出的错误种类。
    fn feed_(bytes: &[u8]) -> Result<FrameHeader, ErrKind> {
        let mut parser = FrameHeaderParser::new();
        for &byte in bytes {
            match parser.consume_byte_(byte) {
                Consume_::Pending => continue,
                Consume_::Done(header) => return Result::Ok(header),
                Consume_::Failed(err) => return Result::Err(err_kind_(err)),
            }
        }
        panic!("字节喂完仍未得出帧头结论：{bytes:?}");
    }

    /// 逐字节喂入并只取错误，便于对失败原因做断言。
    /// - 手段：复用 [`feed_`]，丢弃成功值。
    /// - 判断：`Some` 为被测代码报出的错误种类，`None` 表示竟然读成功了。
    fn feed_err_(bytes: &[u8]) -> Option<ErrKind> {
        feed_(bytes).err()
    }

    /// 测试状态机逐字节解析数据帧，字段值与编码侧完全一致。
    /// - 手段：写一个 `DATA` + `FIN`、local=3、remote=0x0102、载荷 1024 的帧头，
    ///   把字节**一个一喂**进解析器，并顺带逐字节校验每个中间状态。
    /// - 判断：字节序列恰为 `15 00 03 11 01 02 12 04 00`；前 8 个字节都返回
    ///   `Pending`（含最后一个值字节之前的全部），第 9 个字节才 `Done`；
    ///   解析结果的种类、标志、dock 对与载荷长度与写入值一致。
    #[test]
    fn parses_data_frame_byte_by_byte() {
        let header = FrameHeader::new_(
            FrameKind::Data,
            flags::K_FIN,
            Dock::new(3u32),
            Dock::new(0x0102u32),
            1024usize,
            Option::None,
        );
        let bytes = encode_(&header);
        assert_eq!(
            bytes,
            vec![0x15u8, 0x00, 0x03, 0x11, 0x01, 0x02, 0x12, 0x04, 0x00]
        );

        let mut parser = FrameHeaderParser::new();
        assert!(parser.is_started_());
        for (i, &byte) in bytes.iter().enumerate() {
            let step = parser.consume_byte_(byte);
            if i + 1 < bytes.len() {
                assert_eq!(step, Consume_::Pending, "第 {i} 个字节不应提前收尾");
            } else {
                assert!(matches!(step, Consume_::Done(_)), "最后一个字节应收尾");
            }
        }

        let parsed = parser.finish_().expect("应当有结论").expect("应当解析成功");
        assert_eq!(parsed.kind(), FrameKind::Data);
        assert_eq!(parsed.flags(), flags::K_FIN);
        assert!(parsed.is_fin());
        assert!(!parsed.is_reset());
        assert_eq!(parsed.local_dock(), Dock::new(3u32));
        assert_eq!(parsed.remote_dock(), Dock::new(0x0102u32));
        assert_eq!(parsed.payload_len(), 1024usize);
        assert_eq!(parsed.recv_window(), Option::None);
    }

    /// 测试「消费不变量」：让帧头完成的那个字节同样被消费，且结论幂等。
    /// - 手段：喂到最后一个字节拿到 `Done` 后，继续喂若干字节。
    /// - 判断：收尾字节返回 `Done`；重复喂入返回**同一个**帧头且状态仍是 done；
    ///   [`FrameHeaderParser::finish_`] 与 `Done` 的返回值一致。
    #[test]
    fn completing_byte_is_consumed_and_conclusion_is_idempotent() {
        let bytes = [0x05u8, 0x00, 0x01, 0x01, 0x02, 0x02, 0x00];
        let mut parser = FrameHeaderParser::new();
        let mut done = Option::None;
        for &byte in &bytes {
            if let Consume_::Done(header) = parser.consume_byte_(byte) {
                done = Option::Some(header);
            }
        }
        let done = done.expect("应当收尾");
        assert!(parser.is_done_());

        // 重复喂入：不消费、结论不变。
        assert_eq!(parser.consume_byte_(0xFFu8), Consume_::Done(done));
        assert_eq!(parser.consume_byte_(0x00u8), Consume_::Done(done));
        match parser.finish_() {
            Option::Some(Result::Ok(from_finish)) => {
                assert!(same_header_(&from_finish, &done), "finish_ 应与 Done 给出同一个结论");
            }
            other => panic!("finish_ 应当给出成功结论，实际：{other:?}"),
        }
    }

    /// 测试保留的 `kind` 与保留的字段标识都被拒绝。
    /// - 手段：帧首分别取 `0x00`（保留）与 `0x0A`（保留），以及字段标识 `0x06`
    ///   （保留）。
    /// - 判断：三种输入都报 [`MuxError::UnsupportedField`]。
    #[test]
    fn reserved_kinds_and_fields_are_rejected() {
        assert_eq!(feed_err_(&[0x00u8]), Option::Some(ErrKind::UnsupportedField));
        assert_eq!(feed_err_(&[0x0Au8]), Option::Some(ErrKind::UnsupportedField));
        // 帧首合法（DATA），第二个字节是保留字段标识 0x06。
        assert_eq!(
            feed_err_(&[0x05u8, 0x06]),
            Option::Some(ErrKind::UnsupportedField)
        );
    }

    /// 测试非法宽度在**读到自描述头字节时**就被拒绝，不必等值字节到齐。
    /// - 手段：`PULSE` 帧里用一个 `BeU8` 编码的 `RecvTotal`（该字段只接受 2 / 4 / 8
    ///   字节）；喂到该头字节为止。
    /// - 判断：帧首（`0x08`）之后的头字节 `0x04`（`BeU8 | RecvTotal`）立即报
    ///   [`MuxError::UnsupportedField`]，状态机随之进入失败态且结论幂等。
    #[test]
    fn illegal_field_width_is_rejected_at_header_byte() {
        let mut parser = FrameHeaderParser::new();
        assert_eq!(parser.consume_byte_(0x08u8), Consume_::Pending);
        assert_eq!(
            parser.consume_byte_(0x04u8),
            Consume_::Failed(MuxError::UnsupportedField)
        );
        assert!(parser.is_failed_());
        // 失败结论幂等，且后续字节一律不消费。
        assert_eq!(
            parser.consume_byte_(0x00u8),
            Consume_::Failed(MuxError::UnsupportedField)
        );
        match parser.finish_() {
            Option::Some(Result::Err(MuxError::UnsupportedField)) => {}
            other => panic!("finish_ 应当给出 UnsupportedField，实际：{other:?}"),
        }
    }

    /// 测试 dock 的宽度边界：按数值大小在 1 / 2 / 4 字节间选宽，且能原值往返。
    /// - 手段：对 `0xFF` / `0x100` / `0xFFFF` / `0x1_0000` / `0xFFFF_FFFE` 各写一个
    ///   `DATAGRAM` 帧头（帧首置 0），逐字节喂入解析器，并检查 `LocalDock` 头字节里的
    ///   `val_type`。
    /// - 判断：宽度分别为 `BeU8` / `BeU16` / `BeU16` / `BeU32` / `BeU32`；
    ///   解析回来的 dock 数值与写入值相等。
    ///   （`0x1_0000` 装得下它的最小宽度本可以是 3 字节，但 dock 字段只接受
    ///   1 / 2 / 4 字节——`Dock` 的数值语义是规范的、3 字节没有对应类型，见
    ///   [`FieldId::accepts_val_type_`]；`u32::MAX` 是保留的 `wildcard`，
    ///   故用 `u32::MAX - 1` 覆盖 4 字节宽度。）
    #[test]
    fn dock_widths_follow_value_magnitude() {
        let cases = [
            (0x00FFu32, FieldValType::BeU8),
            (0x0100u32, FieldValType::BeU16),
            (0xFFFFu32, FieldValType::BeU16),
            (0x0001_0000u32, FieldValType::BeU32),
            (0xFFFF_FFFEu32, FieldValType::BeU32),
        ];
        for (value, expect) in cases {
            let header = FrameHeader::new_(
                FrameKind::Datagram,
                0u8,
                Dock::new(value),
                Dock::new(2u32),
                0usize,
                Option::None,
            );
            let bytes = encode_(&header);
            // 第 0 字节是帧首，第 1 字节是 LocalDock 的自描述头。编码侧取宽要同时满足
            // 「能容纳该值」与「该字段接受这个宽度」，断言这个字节即覆盖了两者。
            assert_eq!(bytes[1], (expect as u8) | FieldId::LocalDock.as_u8());
            let parsed = feed_(&bytes).expect("读回 dock 应当成功");
            assert_eq!(parsed.local_dock(), Dock::new(value));
        }
    }

    /// 测试窗口更新帧的往返，与「窗口通告两字段必需」两条约束。
    /// - 手段：带增量 4096 的 `WINDOW_UPDATE` 帧头逐字节往返；再分别用「缺字段」
    ///   「非 `WINDOW_UPDATE` 帧带窗口通报」两种畸形输入触发校验。
    /// - 判断：正常帧的增量与 dock 对原值返回；两种畸形输入都报
    ///   [`MuxError::MalformedFrame`]。
    #[test]
    fn window_update_frame_requires_window_report() {
        let header = FrameHeader::new_(
            FrameKind::WindowUpdate,
            0u8,
            Dock::new(7u32),
            Dock::new(9u32),
            0usize,
            Option::Some((8192u64, 4096u32)),
        );
        let parsed = feed_(&encode_(&header)).expect("读回窗口更新帧应当成功");
        assert_eq!(parsed.recv_window(), Option::Some(4096u32));
        assert_eq!(parsed.recv_total(), Option::Some(8192u64));
        assert_eq!(parsed.local_dock(), Dock::new(7u32));
        assert_eq!(parsed.remote_dock(), Dock::new(9u32));

        // 帧首声明 WINDOW_UPDATE，但字段序列里完全没有窗口通告（直接以 PayloadLen 收尾）。
        let without_field = [0x07u8, 0x00, 0x07, 0x01, 0x09, 0x02, 0x00];
        assert_eq!(
            feed_err_(&without_field),
            Option::Some(ErrKind::MalformedFrame)
        );

        // DATA 帧却带上了窗口通告（0x14 = BeU16 | RecvTotal，值为 1）。
        let wrong_kind = [0x05u8, 0x00, 0x01, 0x01, 0x02, 0x14, 0x00, 0x01, 0x02, 0x00];
        assert_eq!(
            feed_err_(&wrong_kind),
            Option::Some(ErrKind::MalformedFrame)
        );
    }

    /// 测试保活帧 `PULSE` 是子流作用域，且必须携带接收窗口通告。
    /// - 手段：写一个 dock 对为 `(1, 2)`、接收窗口为 4096、累计量为 0 的 `PULSE`
    ///   帧头并逐字节比对；再把「缺 dock 对」「完全缺窗口通告」「只带窗口值」三种
    ///   畸形 `PULSE` 交给解析器。
    /// - 判断：写出恰为 `88 00 01 01 02 14 00 00 13 10 00 02 00`；解析结果与写入
    ///   一致；三种畸形输入都报 [`MuxError::MalformedFrame`]。
    #[test]
    fn pulse_is_substream_scoped_and_carries_window() {
        let header = FrameHeader::new_(
            FrameKind::Pulse,
            0u8,
            Dock::new(1u32),
            Dock::new(2u32),
            0usize,
            Option::Some((0u64, 4096u32)),
        );
        let bytes = encode_(&header);
        assert_eq!(
            bytes,
            vec![0x08u8, 0x00, 0x01, 0x01, 0x02, 0x14, 0x00, 0x00, 0x13, 0x10, 0x00, 0x02, 0x00]
        );

        let parsed = feed_(&bytes).expect("读回 PULSE 帧应当成功");
        assert_eq!(parsed.kind(), FrameKind::Pulse);
        assert_eq!(parsed.local_dock(), Dock::new(1u32));
        assert_eq!(parsed.remote_dock(), Dock::new(2u32));
        assert_eq!(parsed.recv_window(), Option::Some(4096u32));
        assert_eq!(parsed.recv_total(), Option::Some(0u64));

        // 缺 dock 对。
        let without_docks = [0x08u8, 0x14, 0x00, 0x00, 0x13, 0x10, 0x00, 0x02, 0x00];
        assert_eq!(
            feed_err_(&without_docks),
            Option::Some(ErrKind::MalformedFrame)
        );

        // 完全缺窗口通告（dock 对齐全，直接以 PayloadLen 收尾）。
        let without_report = [0x08u8, 0x00, 0x01, 0x01, 0x02, 0x02, 0x00];
        assert_eq!(
            feed_err_(&without_report),
            Option::Some(ErrKind::MalformedFrame)
        );

        // 只带窗口值、缺累计已收字节数。
        let without_total = [0x08u8, 0x00, 0x01, 0x01, 0x02, 0x13, 0x10, 0x00, 0x02, 0x00];
        assert_eq!(
            feed_err_(&without_total),
            Option::Some(ErrKind::MalformedFrame)
        );
    }

    /// 测试字段重复被拒绝。
    /// - 手段：`DATA` 帧里把 `LocalDock` 写两遍（合法宽度、合法数值）。
    /// - 判断：报 [`MuxError::MalformedFrame`]。
    #[test]
    fn duplicated_fields_are_rejected() {
        let duplicated = [
            0x05u8, // DATA
            0x00, 0x01, // LocalDock = 1
            0x00, 0x02, // LocalDock = 2（重复）
            0x01, 0x02, // RemoteDock = 2
            0x02, 0x00, // PayloadLen = 0
        ];
        assert_eq!(
            feed_err_(&duplicated),
            Option::Some(ErrKind::MalformedFrame)
        );
    }

    /// 测试 `wildcard` 与 `unspecified` 两个保留 dock 不能作为子流 dock。
    /// - 手段：在 `DATA` 帧里分别把 `LocalDock` 写成 `0`（`unspecified`）与
    ///   `u32::MAX`（`wildcard`，用 4 字节编码）。
    /// - 判断：两种输入都报 [`MuxError::ReservedDock`]。
    #[test]
    fn reserved_docks_are_rejected() {
        // unspecified = 0：0x00 是 LocalDock 的字段头，值为 1 字节 0x00。
        let unspecified = [0x05u8, 0x00, 0x00, 0x01, 0x02, 0x02, 0x00];
        assert_eq!(
            feed_err_(&unspecified),
            Option::Some(ErrKind::ReservedDock)
        );

        // wildcard = u32::MAX：LocalDock 用 4 字节编码（0x30 | 0x00）。
        let wildcard = [
            0x05u8, 0x30, 0xFF, 0xFF, 0xFF, 0xFF, 0x01, 0x02, 0x02, 0x00,
        ];
        assert_eq!(feed_err_(&wildcard), Option::Some(ErrKind::ReservedDock));
    }

    /// 测试 `TOTAL_RESET` 标记只允许出现在保活 / 窗口更新帧上。
    /// - 手段：写一个带 `K_TOTAL_RESET` 的 `WINDOW_UPDATE` 帧并读回；再把同一标记
    ///   单独放到 `DATA` 帧的帧首上。
    /// - 判断：读回的 `is_total_reset()` 为真且累计量是重置前的值；`DATA` 上的标记报
    ///   [`MuxError::MalformedFrame`]。
    #[test]
    fn total_reset_flag_is_a_window_report_variant() {
        let header = FrameHeader::new_(
            FrameKind::WindowUpdate,
            flags::K_TOTAL_RESET,
            Dock::new(4u32),
            Dock::new(6u32),
            0usize,
            Option::Some((65_536u64, 1024u32)),
        );
        let parsed = feed_(&encode_(&header)).expect("读回重置变体应当成功");
        assert!(parsed.is_total_reset());
        assert_eq!(parsed.recv_total(), Option::Some(65_536u64));

        let on_data = [
            0x85u8, // 帧首：TOTAL_RESET(8) << 4 | DATA(5)
            0x00, 0x01, 0x01, 0x02, 0x02, 0x00,
        ];
        assert_eq!(feed_err_(&on_data), Option::Some(ErrKind::MalformedFrame));
    }

    /// 测试 `ReasonCode` 只允许出现在 `REJECT` 帧上，且不进入 `FrameHeader`。
    /// - 手段：`REJECT` 帧带 1 字节 `ReasonCode`；随后把同一个字段放到 `DATA` 帧上；
    ///   再给一个取值超过 `u8` 的 `ReasonCode`（2 字节编码）。
    /// - 判断：`REJECT` 帧解析成功且没有窗口字段；后两种输入都报
    ///   [`MuxError::MalformedFrame`]。
    #[test]
    fn reason_code_only_on_reject() {
        let on_reject = [
            0x03u8, // REJECT
            0x00, 0x01, // LocalDock = 1
            0x01, 0x02, // RemoteDock = 2
            0x05, 0x07, // ReasonCode = 7（BeU8）
            0x02, 0x00, // PayloadLen = 0
        ];
        let parsed = feed_(&on_reject).expect("REJECT 带 ReasonCode 应当成功");
        assert_eq!(parsed.kind(), FrameKind::Reject);
        assert_eq!(parsed.payload_len(), 0usize);

        // 同一个字段放到 DATA 帧上：非法组合。
        let on_data = [
            0x05u8, 0x00, 0x01, 0x01, 0x02, 0x05, 0x07, 0x02, 0x00,
        ];
        assert_eq!(feed_err_(&on_data), Option::Some(ErrKind::MalformedFrame));

        // 取值装不进 u8：ReasonCode 用 BeU16 编码，值为 0x0100。
        let too_large = [
            0x03u8, 0x00, 0x01, 0x01, 0x02, 0x15, 0x01, 0x00, 0x02, 0x00,
        ];
        assert_eq!(
            feed_err_(&too_large),
            Option::Some(ErrKind::MalformedFrame)
        );
    }

    /// 测试 `PayloadLen` 超过 `u8` 时用更宽的宽度编码，解析结果不受影响。
    /// - 手段：`DATA` 帧的载荷长度取 `u16::MAX`（2 字节）与 `0x1_0000`（3 字节），
    ///   逐字节往返。
    /// - 判断：两种情况都解析成功且载荷长度原值返回。
    #[test]
    fn payload_len_supports_wide_encodings() {
        for len in [u16::MAX as usize, 0x0001_0000usize] {
            let header = FrameHeader::new_(
                FrameKind::Data,
                0u8,
                Dock::new(1u32),
                Dock::new(2u32),
                len,
                Option::None,
            );
            let parsed = feed_(&encode_(&header)).expect("宽载荷长度应当解析成功");
            assert_eq!(parsed.payload_len(), len);
        }
    }

    /// 测试「缺少 dock 对」与「空输入」两条边界。
    /// - 手段：只给帧首与 `PayloadLen`（缺两个 dock）；以及完全不给字节。
    /// - 判断：前者报 [`MuxError::MalformedFrame`]；后者解析器仍处于起始态、
    ///   [`FrameHeaderParser::finish_`] 返回 `None`。
    #[test]
    fn missing_docks_are_rejected_and_empty_input_stays_started() {
        let without_docks = [0x05u8, 0x02, 0x00];
        assert_eq!(
            feed_err_(&without_docks),
            Option::Some(ErrKind::MalformedFrame)
        );

        let parser = FrameHeaderParser::new();
        assert!(parser.is_started_());
        assert!(parser.finish_().is_none(), "还没读字节时不应有结论");
    }

    /// 测试异步入口在**容量 1 的环**上也能解析出帧头。
    ///
    /// 这是本模块存在的**唯一理由**：旧入口按字段索要 `width` 字节，
    /// `buffex::ring` 在 `min_width > cap` 时返回终态的 `ConsumerError::Unsatisfiable`，
    /// 因此容量 1 的环连一个 2 字节的 dock 都解析不了；新入口每次只索要 1 字节，
    /// 与容量无关。
    ///
    /// - 手段：用 `ring_::test_support_::make_test_channel_` 建容量 1 的环，把三个
    ///   字节数分别为 7（最小帧头）、9（2 字节 dock + 2 字节载荷长度）、13（含 8 字节
    ///   宽字段）的完整帧头**一次全写完**（容量 1 的环装不下整帧，因此边写边读），
    ///   再调 [`read_header_async_`]。
    /// - 判断：三个帧头都能解析出来，各字段与写入值一致。
    async fn capacity_one_ring_parses_header_() {
        for bytes in [
            encode_(&FrameHeader::new_(
                FrameKind::Data,
                0u8,
                Dock::new(1u32),
                Dock::new(2u32),
                0usize,
                Option::None,
            )),
            encode_(&FrameHeader::new_(
                FrameKind::Data,
                flags::K_FIN,
                Dock::new(3u32),
                Dock::new(0x0102u32),
                1024usize,
                Option::None,
            )),
            // RecvTotal 取 BeU64，帧头因此宽达 13 字节，远超容量 1。
            encode_(&FrameHeader::new_(
                FrameKind::Pulse,
                0u8,
                Dock::new(1u32),
                Dock::new(2u32),
                0usize,
                Option::Some((u32::MAX as u64 + 1u64, 4096u32)),
            )),
        ] {
            // 容量 1：生产端一次只能提交 1 个字节，消费端一次只能借出 1 个字节。
            let (mut tx, mut rx) = crate::connection::ring_::test_support_::make_test_channel_(1);

            // 边写边读：容量 1 的环无法在解析前装下整帧，因此两边必须并发推进。
            let writer = async {
                for &byte in &bytes {
                    let one = [byte];
                    crate::wire_io_::write_all_async_(
                        &mut tx,
                        &one,
                        abs_cancel::NonCancellableToken::new(),
                    )
                    .await
                    .expect("写入容量 1 的环应当成功");
                }
                tx.close();
            };

            let reader = async {
                read_header_async_::<_, _>(&mut rx, abs_cancel::NonCancellableToken::new())
                    .await
                    .expect("容量 1 的环应当能解析出帧头")
                    // 第二个分量是帧头长度，本用例只需帧头本身。
                    .0
            };

            let ((), parsed) = futures::join!(writer, reader);
            let expect = feed_(&bytes).expect("同一批字节的同步解析应当成功");
            assert_eq!(parsed.kind(), expect.kind());
            assert_eq!(parsed.flags(), expect.flags());
            assert_eq!(parsed.local_dock(), expect.local_dock());
            assert_eq!(parsed.remote_dock(), expect.remote_dock());
            assert_eq!(parsed.payload_len(), expect.payload_len());
            assert_eq!(parsed.recv_window(), expect.recv_window());
            assert_eq!(parsed.recv_total(), expect.recv_total());
        }
    }
    dual_runtime_test_!(capacity_one_ring_parses_header_);

    /// 测试异步入口的错误映射：输入耗尽视为对端关闭。
    /// - 手段：用一个空切片做读源，调 [`read_header_async_`]。
    /// - 判断：返回 [`MuxError::PeerClosed`]。
    async fn exhausted_input_reports_peer_closed_() {
        let mut empty: &[u8] = &[];
        let err = read_header_async_::<_, _>(&mut empty, abs_cancel::NonCancellableToken::new())
            .await
            .expect_err("空输入应当报错");
        assert_eq!(err_kind_(err), ErrKind::PeerClosed);
    }
    dual_runtime_test_!(exhausted_input_reports_peer_closed_);

    /// 测试异步入口的协议错误归类：畸形帧头照旧报协议层错误。
    /// - 手段：把「保留字段标识」这一畸形帧头交给异步入口（用切片读源）。
    /// - 判断：返回 [`MuxError::UnsupportedField`]。
    async fn protocol_errors_survive_async_entry_() {
        let mut bytes: &[u8] = &[0x05u8, 0x06];
        let err = read_header_async_::<_, _>(&mut bytes, abs_cancel::NonCancellableToken::new())
            .await
            .expect_err("保留字段标识应当报错");
        assert_eq!(err_kind_(err), ErrKind::UnsupportedField);
    }
    dual_runtime_test_!(protocol_errors_survive_async_entry_);

    /// 测试解析器如实累计**帧头长度**（metrics 读侧「帧总字节」口径的来源）。
    ///
    /// - 手段：把最小 `DATA` 帧头的 7 个字节逐字节喂进 `consume_byte_`，直到拿到
    ///   `Consume_::Done`；随后再喂一个字节走幂等路径；最后用一个**新解析器**只喂
    ///   帧首字节，观察计数起点。
    /// - 判断：`consumed_len_()` 恰好等于实际喂入的字节数——多算会让读侧的帧总长偏大、
    ///   少算会偏小——且出结论后的重复喂入**不再增长**。后者正是 `consume_byte_` 文档里
    ///   那条「消费不变量」的直接体现（出结论之后的重复喂入不消费字节）；计数起点那一
    ///   组则钉住「回到解析开头即重新起算」，即它**不是**跨轮累计量。
    #[test]
    fn consumed_len_counts_exactly_the_header_bytes_() {
        // 帧首 + LocalDock=1 + RemoteDock=2 + PayloadLen=0（见 `consume_byte_` 的示例）。
        let bytes = [0x05u8, 0x00, 0x01, 0x01, 0x02, 0x02, 0x00];
        let mut parser = FrameHeaderParser::new();
        assert_eq!(
            parser.consumed_len_(),
            0usize,
            "还没喂入任何字节时，本轮已消费为 0"
        );
        let mut fed = 0usize;
        for byte in bytes {
            fed += 1usize;
            match parser.consume_byte_(byte) {
                Consume_::Pending => {}
                Consume_::Done(header) => {
                    assert_eq!(header.kind(), FrameKind::Data);
                    break;
                }
                Consume_::Failed(err) => panic!("合法的最小帧头不应失败：{err:?}"),
            }
        }
        assert_eq!(fed, bytes.len(), "7 个字节应当刚好喂完整个帧头");
        assert_eq!(
            parser.consumed_len_(),
            bytes.len(),
            "本轮已消费字节数就是帧头长度"
        );

        // 幂等：出结论之后重复喂入不消费字节，计数因此不变。
        assert!(matches!(parser.consume_byte_(0xFFu8), Consume_::Done(_)));
        assert_eq!(parser.consumed_len_(), bytes.len());

        // **计数只覆盖本轮**：解析器回到解析开头（`Start`）时重新起算，帧首字节计为 1。
        // 若这里继承了上一轮的 7，就说明实现退回了「自建立以来累计」——那正是被否掉的
        // 设计（解析器一旦复用，每帧长度都会偏大）。
        let mut next_round = FrameHeaderParser::new();
        assert_eq!(next_round.consumed_len_(), 0usize);
        assert!(matches!(next_round.consume_byte_(0x05u8), Consume_::Pending));
        assert_eq!(
            next_round.consumed_len_(),
            1usize,
            "新一轮解析从帧首字节重新起算"
        );
    }
}
