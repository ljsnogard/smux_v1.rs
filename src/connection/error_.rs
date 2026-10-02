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

use crate::flow_ctrl::FlowCtrlError;

/// 复用连接与子流操作失败的统一类型。
///
/// # 泛型参数
///
/// - `RE`：底层网络读半边的错误类型；
/// - `WE`：底层网络写半边的错误类型。
///
/// 数据面不复用本类型直接作为 `Err`：子流半边的错误就是 `buffex` 端半部的
/// `ConsumerError` / `ProducerError`（已携带方向标签），见模块文档。
///
/// # 为什么既有带载荷的变体又有不带载荷的变体
///
/// `Rx` / `Tx` 只表示**本次操作直接遇到**的底层读写错误，载荷因此可以原样给出。
/// 但连接级失败要经共享状态回传给 API 面，而底层错误值（`RE` / `WE`）只存在于
/// 驱动循环那一侧、无法跨过去，所以另外给出载荷无关的
/// [`MuxError::Transport`]——它只保留**方向**，用于表达「连接因传输错误中断」，
/// 与「对端主动关闭」（[`MuxError::PeerClosed`]）是两件不同的事。
#[derive(Debug)]
pub enum MuxError<RE, WE> {
    /// 底层网络读失败（本次操作直接遇到）。
    Rx(RE),

    /// 底层网络写失败（本次操作直接遇到）。
    Tx(WE),

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

    /// 请求的 local_dock 已被占用：channel 与 telegraph 不得共用 dock。
    DockInUse,

    /// 该 dock 上的活动子流数已达 `max_dock_chan_count`。
    DockChanLimit,

    /// 连接上的活动子流数已达 `max_channel_count`。
    ChanLimit,

    /// 对端拒绝或无人监听（收到 `REJECT`）。
    Refused,

    /// 同一条子流上出现重复的建立 / 应答请求。
    Duplicate,

    /// 流控失败（窗口违例或计数溢出）。
    FlowCtrl(FlowCtrlError),
}

impl<RE, WE> core::fmt::Display for MuxError<RE, WE> {
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
            MuxError::DockInUse => f.write_str("该 dock 已被 channel 或 telegraph 占用"),
            MuxError::DockChanLimit => f.write_str("该 dock 上的活动子流数已达上限"),
            MuxError::ChanLimit => f.write_str("连接上的活动子流数已达上限"),
            MuxError::Refused => f.write_str("对端拒绝建立子流"),
            MuxError::Duplicate => f.write_str("同一条子流上出现重复请求"),
            MuxError::FlowCtrl(_) => f.write_str("流控失败"),
        }
    }
}

impl<RE, WE> core::error::Error for MuxError<RE, WE>
where
    RE: core::error::Error,
    WE: core::error::Error,
{
}

impl MuxError<(), ()> {
    /// 把「载荷无关」的错误搬到另一个底层错误类型上。
    ///
    /// 注册表 / 建流登记这类路径只知道「哪一类错误」，不持有底层错误值，因此它们
    /// 用 `MuxError<(), ()>` 表达，再由 API 面 `cast_` 成目标类型。`Rx(())` /
    /// `Tx(())` 没有可搬运的载荷，退化为保留方向的 [`MuxError::Transport`]。
    pub(crate) fn cast_<RE, WE>(self) -> MuxError<RE, WE> {
        match self {
            MuxError::Rx(()) => MuxError::Transport { write: false },
            MuxError::Tx(()) => MuxError::Transport { write: true },
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
            MuxError::FlowCtrl(err) => MuxError::FlowCtrl(err),
        }
    }
}
