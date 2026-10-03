//! 复用连接与子流的错误类型。
//!
//! 控制面（`TrConnection` / `TrDockBinding` / `TrChannelListener` /
//! `TrChannelHandle` / `TrTelegraph`）统一用 [`MuxError`]。
//!
//! 数据面（子流的 `TrBuffTryRead` / `TrBuffTryWrite`）**不复用** `MuxError`：
//! 子流半边只是 `buffex` 端半部的薄包装，因此它们的 `Err` 直接是 `buffex` 的
//! `ConsumerError` / `ProducerError`——这两者已经实现
//! [`TrTaggedError`](abs_buff::error::TrTaggedError) 并携带
//! `ReadErrTag` / `WriteErrTag`，足以让 `is_drained_closing()` 之类的判定脱离
//! 具体错误类型工作。

use buffex::x_deps::abs_buff::error::{ReadErrTag, TrTaggedError, WriteErrTag};
use buffex::x_deps::abs_buff::{Demand, TrBuffTryRead, TrBuffTryWrite};
use buffex::x_deps::anylr::SomeOf;

use crate::flow_ctrl::FlowCtrlError;

/// 复用连接与子流操作失败的统一类型：**用两个传输半边参数化**，载荷由它们派生。
///
/// 参数是**传输**（`R` / `W`）而不是它们的错误类型：开发者手里就是两个半边，
/// 因此 `MuxError<WireRx, WireTx>` 可以直接写出来——不必（也常常无法）知道两个错误
/// 载荷类型叫什么，更不必写 `<T as TrBuffTryRead<u8>>::Err` 这样的投影。
///
/// ```
/// use smux_v1::connection::MuxError;
///
/// // 开发者手里是两个传输半边；这里用切片当例子（它实现了 abs_buff 的两个 try 半边）。
/// fn classify(err: MuxError<&'static [u8], &'static mut [u8]>) -> u8 {
///     match err {
///         MuxError::Rx(_) => 1,
///         MuxError::Tx(_) => 2,
///         MuxError::Transport { write: true } => 3,
///         MuxError::Transport { write: false } => 4,
///         _ => 0,
///     }
/// }
///
/// let err = MuxError::<&'static [u8], &'static mut [u8]>::PeerClosed;
/// assert_eq!(classify(err), 0);
/// ```
///
/// 注意上面**没有出现**任何错误载荷类型：它们由两个半边派生，写 match 分支时载荷
/// 的类型会被自动推断出来。
///
/// # 为什么参数是传输而不是载荷
///
/// 连接内部有些层只碰一个方向（帧解析只会产生读侧载荷），那些地方无法提供
/// 「另一侧的传输类型」；本枚举为此保留一个**不可能存在的半边**占位
/// （`NoHalfway_`，其载荷是 `Infallible`），于是单边形式可以写成
/// `MuxError<R, NoHalfway_>`——**类型上精确表达「另一侧不可能出错」**，而不必给整条
/// 调用链塞进一个用不到的传输类型参数。它只在本 crate 内部使用。
///
/// # 为什么既有带载荷的变体又有不带载荷的变体
///
/// `Rx` / `Tx` 只表示**本次操作直接遇到**的底层读写错误，载荷因此可以原样给出。
/// 但连接级失败要经共享状态回传给 API 面，而底层错误值只存在于驱动循环那一侧、
/// 跨不过来，所以另外给出载荷无关的 [`MuxError::Transport`]——它只保留**方向**，
/// 用于表达「连接因传输错误中断」，与「对端主动关闭」（[`MuxError::PeerClosed`]）
/// 是两件不同的事。
///
/// 因此：`Rx` / `Tx` 只在连接内部的循环里构造（循环把错误投影成 `FailKind_` 再
/// 回传），API 面上产出的一律是载荷无关的变体。
///
/// # Panics
///
/// 本类型自身不 panic；`NoHalfway_` 的占位实现若被调用会 `unreachable!()`（不可能，
/// 该类型无法构造）。
pub enum MuxError<R, W>
where
    R: TrBuffTryRead<u8>,
    W: TrBuffTryWrite<u8>,
{
    /// 底层网络读失败（本次操作直接遇到）。
    Rx(<R as TrBuffTryRead<u8>>::Err),

    /// 底层网络写失败（本次操作直接遇到）。
    Tx(<W as TrBuffTryWrite<u8>>::Err),

    /// 连接因底层传输错误而中断，无法继续收发。
    ///
    /// `write` 指出是哪一半先出的错：`true` = 写方向，`false` = 读方向。
    /// 底层错误值本身只在驱动循环侧可见，因此这里不带载荷。
    Transport { write: bool },

    /// 操作被取消令牌终止。
    Cancelled,

    /// **对端主动**关闭了连接或该子流（收到 `CLOSE` / `FIN`，或对端读端正常收尾）。
    ///
    /// 与 [`MuxError::Transport`] 的区别是「谁先断的、是否优雅」：本变体表示对端
    /// 主动收尾，而 `Transport` 表示传输层出错导致的中断。
    PeerClosed,

    /// 目标子流已被关闭（本端关闭或已拆流）。
    Closed,

    /// 子流空闲超时：`max_channel_timeout` 内既无数据往来、也无保活应答
    /// （保活报文见 [`FrameKind::Pulse`](crate::connection::FrameKind::Pulse)）。
    IdleTimeout,

    /// 该 dock 是保留取值，不能作为子流 dock。
    ///
    /// `wildcard`（全 1）与 `unspecified`（全 0）是 dock 类型自带的特殊值，双方都
    /// 保留它们；正常子流的 `LocalDock` / `RemoteDock` 一律不得取这两个值。
    ReservedDock,

    /// 帧结构非法：缺少必需字段、字段重复、字段顺序与种类不符等。
    MalformedFrame,

    /// 未知 / 保留的字段标识，或字段宽度对该字段非法。
    UnsupportedField,

    /// 帧总长超过协商出的 `max_packet_size`。
    FrameTooLarge,

    /// 请求的 local_dock 已被占用：已被某个 `DockBinding` 绑定，或已作 telegraph
    /// 端点（channel 与 telegraph 不得共用 dock）。
    DockInUse,

    /// 该 dock 上的活动子流数已达 `max_dock_chan_count`。
    DockChanLimit,

    /// 连接上的活动子流数已达 `max_channel_count`。
    ChanLimit,

    /// 对端拒绝或无人监听（收到 `REJECT`）。
    Refused,

    /// 同一条子流上出现重复的建立 / 应答请求。
    Duplicate,

    /// 该 dock 对刚关闭，仍在**拆流宽限期**内，暂不可复用。
    ///
    /// 宽限期由协商项 `max_channel_wait_close` 决定（见 [`crate::connection`]
    /// 模块文档 §4.3）：期间到达的在途帧被静默丢弃，同一
    /// `(local_dock, remote_dock)` 也不允许重新登记。
    WaitClose,

    /// 流控失败（窗口违例或计数溢出）。
    FlowCtrl(FlowCtrlError),
}

impl<R, W> core::fmt::Display for MuxError<R, W>
where
    R: TrBuffTryRead<u8>,
    W: TrBuffTryWrite<u8>,
{
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            MuxError::Rx(_) => f.write_str("复用连接读取失败"),
            MuxError::Tx(_) => f.write_str("复用连接写入失败"),
            MuxError::Transport { write: true } => f.write_str("复用连接写方向因传输错误中断"),
            MuxError::Transport { write: false } => f.write_str("复用连接读方向因传输错误中断"),
            MuxError::Cancelled => f.write_str("复用操作被取消"),
            MuxError::PeerClosed => f.write_str("对端已主动关闭连接或子流"),
            MuxError::Closed => f.write_str("子流已关闭"),
            MuxError::IdleTimeout => f.write_str("子流空闲超时：保活无应答"),
            MuxError::ReservedDock => f.write_str("wildcard / unspecified dock 不能作为子流 dock"),
            MuxError::MalformedFrame => f.write_str("复用帧结构非法"),
            MuxError::UnsupportedField => f.write_str("复用帧包含未知或非法的字段"),
            MuxError::FrameTooLarge => f.write_str("复用帧超过协商的最大报文长度"),
            MuxError::DockInUse => f.write_str("该 dock 已被绑定或已作其他用途占用"),
            MuxError::DockChanLimit => f.write_str("该 dock 上的活动子流数已达上限"),
            MuxError::ChanLimit => f.write_str("连接上的活动子流数已达上限"),
            MuxError::Refused => f.write_str("对端拒绝建立子流"),
            MuxError::Duplicate => f.write_str("同一条子流上出现重复请求"),
            MuxError::WaitClose => f.write_str("该 dock 对刚关闭，仍在拆流宽限期内"),
            MuxError::FlowCtrl(_) => f.write_str("流控失败"),
        }
    }
}

impl<R, W> core::error::Error for MuxError<R, W>
where
    R: TrBuffTryRead<u8>,
    W: TrBuffTryWrite<u8>,
{
}

impl<R, W> core::fmt::Debug for MuxError<R, W>
where
    R: TrBuffTryRead<u8>,
    W: TrBuffTryWrite<u8>,
{
    /// 手写而非派生：派生会给 `R` / `W` 本身加 `Debug` 约束，而实际需要的是
    /// **载荷**可打印（载荷由 `TrTaggedError: core::error::Error` 保证）。
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            MuxError::Rx(e) => f.debug_tuple("Rx").field(e).finish(),
            MuxError::Tx(e) => f.debug_tuple("Tx").field(e).finish(),
            MuxError::Transport { write } => {
                f.debug_struct("Transport").field("write", write).finish()
            }
            MuxError::Cancelled => f.write_str("Cancelled"),
            MuxError::PeerClosed => f.write_str("PeerClosed"),
            MuxError::Closed => f.write_str("Closed"),
            MuxError::IdleTimeout => f.write_str("IdleTimeout"),
            MuxError::ReservedDock => f.write_str("ReservedDock"),
            MuxError::MalformedFrame => f.write_str("MalformedFrame"),
            MuxError::UnsupportedField => f.write_str("UnsupportedField"),
            MuxError::FrameTooLarge => f.write_str("FrameTooLarge"),
            MuxError::DockInUse => f.write_str("DockInUse"),
            MuxError::DockChanLimit => f.write_str("DockChanLimit"),
            MuxError::ChanLimit => f.write_str("ChanLimit"),
            MuxError::Refused => f.write_str("Refused"),
            MuxError::Duplicate => f.write_str("Duplicate"),
            MuxError::WaitClose => f.write_str("WaitClose"),
            MuxError::FlowCtrl(err) => f.debug_tuple("FlowCtrl").field(err).finish(),
        }
    }
}

impl MuxError<NoHalfway_, NoHalfway_> {
    /// 把「载荷无关」的错误搬到目标类型上。
    ///
    /// 注册表 / 建流登记这类路径只知道「哪一类错误」，不持有底层错误值，因此它们
    /// 用 `MuxError<NoHalfway_, NoHalfway_>` 表达（两侧载荷都是 `Infallible`），再由
    /// API 面 `cast_` 成目标类型。两个载荷变体在这里不可能出现，若出现即退化为保留
    /// 方向的 [`MuxError::Transport`]。
    pub(crate) fn cast_<R, W>(self) -> MuxError<R, W>
    where
        R: TrBuffTryRead<u8>,
        W: TrBuffTryWrite<u8>,
    {
        match self {
            MuxError::Rx(never) => match never {},
            MuxError::Tx(never) => match never {},
            MuxError::Transport { write } => MuxError::Transport { write },
            MuxError::Cancelled => MuxError::Cancelled,
            MuxError::PeerClosed => MuxError::PeerClosed,
            MuxError::Closed => MuxError::Closed,
            MuxError::IdleTimeout => MuxError::IdleTimeout,
            MuxError::ReservedDock => MuxError::ReservedDock,
            MuxError::MalformedFrame => MuxError::MalformedFrame,
            MuxError::UnsupportedField => MuxError::UnsupportedField,
            MuxError::FrameTooLarge => MuxError::FrameTooLarge,
            MuxError::DockInUse => MuxError::DockInUse,
            MuxError::DockChanLimit => MuxError::DockChanLimit,
            MuxError::ChanLimit => MuxError::ChanLimit,
            MuxError::Refused => MuxError::Refused,
            MuxError::Duplicate => MuxError::Duplicate,
            MuxError::WaitClose => MuxError::WaitClose,
            MuxError::FlowCtrl(err) => MuxError::FlowCtrl(err),
        }
    }
}


/// 「不存在的那一半」的错误载荷：**不可构造**（无变体），因此「另一侧出错」这件事
/// 在类型上不可能发生。
///
/// 用途只有一个：让连接内部**只碰一个方向**的层仍能写出精确的错误类型。例如帧解析
/// 只会产生读侧载荷，它的错误类型是 `MuxError<R, NoHalfway_>`——类型上就注明「写侧
/// 不可能出错」，而不是含混地写 `MuxError<R, W>` 再假设 `W` 永不出现。
///
/// 由于本类型无法构造，它的两个 `try_*` 实现永远不会被调用。
pub(crate) enum NoHalfwayErr_ {}

impl core::fmt::Debug for NoHalfwayErr_ {
    fn fmt(&self, _f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match *self {}
    }
}

impl core::fmt::Display for NoHalfwayErr_ {
    fn fmt(&self, _f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match *self {}
    }
}

impl core::error::Error for NoHalfwayErr_ {}

impl TrTaggedError<ReadErrTag> for NoHalfwayErr_ {
    fn err_tag(&self) -> ReadErrTag {
        match *self {}
    }
}

impl TrTaggedError<WriteErrTag> for NoHalfwayErr_ {
    fn err_tag(&self) -> WriteErrTag {
        match *self {}
    }
}

/// 「不存在的那一半」：**永不构造**的传输半边，其载荷类型是 [`NoHalfwayErr_`]。
///
/// 它只用于让内部「只碰一个方向」的层写出精确的错误类型，见本模块文档。
#[derive(Clone, Copy)]
pub(crate) enum NoHalfway_ {}

impl TrBuffTryRead<u8> for NoHalfway_ {
    /// 借用「真实半边」的段类型即可：本类型永不构造，段类型只用于满足 trait。
    type SegmRef<'f>
        = <&'static [u8] as TrBuffTryRead<u8>>::SegmRef<'f>
    where
        Self: 'f;

    type Err = NoHalfwayErr_;

    fn try_read<'f>(
        &'f mut self,
        _demand: &'f Demand<usize>,
    ) -> SomeOf<Self::SegmRef<'f>, Self::Err> {
        match *self {}
    }
}

impl TrBuffTryWrite<u8> for NoHalfway_ {
    /// 同 [`NoHalfway_`] 的读侧说明。
    type SegmMut<'f>
        = <&'static mut [u8] as TrBuffTryWrite<u8>>::SegmMut<'f>
    where
        Self: 'f;

    type Err = NoHalfwayErr_;

    fn try_write<'f>(
        &'f mut self,
        _demand: &'f Demand<usize>,
    ) -> SomeOf<Self::SegmMut<'f>, Self::Err> {
        match *self {}
    }
}

/// 只可能出现**读侧**载荷的连接错误（写侧不存在）。内部专用。
pub(crate) type MuxReadErr_<R> = MuxError<R, NoHalfway_>;

/// 只可能出现**写侧**载荷的连接错误（读侧不存在）。内部专用。
pub(crate) type MuxWriteErr_<W> = MuxError<NoHalfway_, W>;
