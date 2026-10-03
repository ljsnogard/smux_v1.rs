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

use buffex::x_deps::abs_buff::{TrBuffTryRead, TrBuffTryWrite};

use crate::{
    connection::TrConnCfg,
    flow_ctrl::FlowCtrlError,
};

/// 复用连接与子流操作失败的统一类型：**用两个传输半边参数化**，载荷由它们派生。
///
/// 参数是**传输**（`R` / `W`）而不是它们的错误类型：开发者手里就是两个半边，
/// 因此 `MuxError<WireRx, WireTx>` 可以直接写出来——不必（也常常无法）知道两个错误
/// 载荷类型叫什么，更不必写 `<T as TrBuffTryRead<u8>>::Err` 这样的投影。
///
/// ```
/// use smux_v1::connection::{DefaultConnCfg, MuxError};
///
/// // 这里用切片当例子（它实现了 abs_buff 的两个 try 半边）。
/// type Cfg = DefaultConnCfg<&'static mut [u8], &'static [u8]>;
///
/// fn classify(err: MuxError<Cfg>) -> u8 {
///     match err {
///         MuxError::Rx(_) => 1,
///         MuxError::Tx(_) => 2,
///         MuxError::Transport { write: true } => 3,
///         MuxError::Transport { write: false } => 4,
///         _ => 0,
///     }
/// }
///
/// let err = MuxError::<Cfg>::PeerClosed;
/// assert_eq!(classify(err), 0);
/// ```
///
/// 各 API 面的错误类型都把它包在 `Mux(..)` 里，例如
/// [`HandleError::Mux`](crate::connection::HandleError::Mux)。
///
/// 注意上面**没有出现**任何错误载荷类型：它们由 `C` 声明的那对传输半边派生，
/// 写 match 分支时载荷的类型会被自动推断出来。
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
pub enum MuxError<C>
where
    C: TrConnCfg,
{
    /// 底层网络读失败（本次操作直接遇到）。
    Rx(<C::ConnRx as TrBuffTryRead<C::Data>>::Err),

    /// 底层网络写失败（本次操作直接遇到）。
    Tx(<C::ConnTx as TrBuffTryWrite<C::Data>>::Err),

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

    /// 流控失败（窗口违例或计数溢出）。
    FlowCtrl(FlowCtrlError),

}

impl<C> core::fmt::Display for MuxError<C>
where
    C: TrConnCfg,
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
            MuxError::FlowCtrl(_) => f.write_str("流控失败"),
        }
    }
}

impl<C> core::error::Error for MuxError<C>
where
    C: TrConnCfg + 'static,
{}

impl<C> core::fmt::Debug for MuxError<C>
where
    C: TrConnCfg,
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
            MuxError::FlowCtrl(err) => f.debug_tuple("FlowCtrl").field(err).finish(),
        }
    }
}


// /// 「不存在的那一半」的错误载荷：**不可构造**（无变体），因此「另一侧出错」这件事
// /// 在类型上不可能发生。
// ///
// /// 用途只有一个：让连接内部**只碰一个方向**的层仍能写出精确的错误类型。例如帧解析
// /// 只会产生读侧载荷，它的错误类型是 `MuxError<R, NoHalfway_>`——类型上就注明「写侧
// /// 不可能出错」，而不是含混地写 `MuxError<C>` 再假设 `W` 永不出现。
// ///
// /// 由于本类型无法构造，它的两个 `try_*` 实现永远不会被调用。
// pub(crate) enum NoHalfwayErr_ {}
//
// impl core::fmt::Debug for NoHalfwayErr_ {
//     fn fmt(&self, _f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
//         match *self {}
//     }
// }
//
// impl core::fmt::Display for NoHalfwayErr_ {
//     fn fmt(&self, _f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
//         match *self {}
//     }
// }
//
// impl core::error::Error for NoHalfwayErr_ {}
//
// impl TrTaggedError<ReadErrTag> for NoHalfwayErr_ {
//     fn err_tag(&self) -> ReadErrTag {
//         match *self {}
//     }
// }
//
// impl TrTaggedError<WriteErrTag> for NoHalfwayErr_ {
//     fn err_tag(&self) -> WriteErrTag {
//         match *self {}
//     }
// }
//
// /// 「不存在的那一半」：**永不构造**的传输半边，其载荷类型是 [`NoHalfwayErr_`]。
// ///
// /// 它只用于让内部「只碰一个方向」的层写出精确的错误类型，见本模块文档。
// #[derive(Clone, Copy)]
// pub(crate) enum NoHalfway_ {}
//
// impl TrBuffTryRead<u8> for NoHalfway_ {
//     /// 借用「真实半边」的段类型即可：本类型永不构造，段类型只用于满足 trait。
//     type SegmRef<'f>
//         = <&'static [u8] as TrBuffTryRead<u8>>::SegmRef<'f>
//     where
//         Self: 'f;
//
//     type Err = NoHalfwayErr_;
//
//     fn try_read<'f>(
//         &'f mut self,
//         _demand: &'f Demand<usize>,
//     ) -> SomeOf<Self::SegmRef<'f>, Self::Err> {
//         match *self {}
//     }
// }
//
// impl TrBuffTryWrite<u8> for NoHalfway_ {
//     /// 同 [`NoHalfway_`] 的读侧说明。
//     type SegmMut<'f>
//         = <&'static mut [u8] as TrBuffTryWrite<u8>>::SegmMut<'f>
//     where
//         Self: 'f;
//
//     type Err = NoHalfwayErr_;
//
//     fn try_write<'f>(
//         &'f mut self,
//         _demand: &'f Demand<usize>,
//     ) -> SomeOf<Self::SegmMut<'f>, Self::Err> {
//         match *self {}
//     }
// }
//
// /// 只可能出现**读侧**载荷的连接错误（写侧不存在）。内部专用。
// pub(crate) type MuxReadErr_<R> = MuxError<R, NoHalfway_>;
//
// /// 只可能出现**写侧**载荷的连接错误（读侧不存在）。内部专用。
// pub(crate) type MuxWriteErr_<W> = MuxError<NoHalfway_, W>;

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// 注册表内部的「预留 / 绑定」失败
// -- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

/// 注册表在「预留 / 绑定一个身份」时能给出的失败。
///
/// 它**不是公开类型**：不同的 API 面会把它映射进各自那个公开错误枚举（哪些面该看到
/// 哪些失败，由各面自己决定）。这样注册表不必知道公开错误的形状。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ReserveErr_ {
    /// 该 local_dock 已被占用（binding / channel / telegraph / listener）。
    DockInUse,

    /// 同一 dock 对上已有活跃子流。
    Duplicate,

    /// 该 dock 对刚关闭，仍在拆流宽限期内。
    WaitClose,

    /// 该 dock 上的活动子流数已达上限。
    DockChanLimit,

    /// 连接上的活动子流数已达上限。
    ChanLimit,
}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// 各 API 面的错误类型
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
//
// 每个面只**声明自己会报的失败**，并把连接级失败包在 `Mux(..)` 里；`source()` 指回
// 内层 `MuxError`，`From<MuxError<..>>` 让 `?` 直通。

/// 为各面错误类型生成公共实现。
///
/// 手写而非派生：派生会给 `R` / `W` 本身加 `Debug` 约束，而这两者只出现在
/// `MuxError<C>` 里（它有自己的手写 `Debug`）。调用点传入「变体 → 中文说明」表；
/// `Mux(..)` 一档统一委托给内层 `MuxError`。
macro_rules! face_error_impls {
    ($name:ident $(, $pat:pat => $text:expr)* $(,)?) => {
        impl<C> core::fmt::Debug for $name<C>
        where
            C: TrConnCfg + 'static,
        {
            fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
                match self {
                    $name::Mux(err) => f.debug_tuple("Mux").field(err).finish(),
                    $( $pat => f.write_str(stringify!($pat)), )*
                }
            }
        }

        impl<C> core::fmt::Display for $name<C>
        where
            C: TrConnCfg + 'static,
        {
            fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
                match self {
                    $name::Mux(err) => core::fmt::Display::fmt(err, f),
                    $( $pat => f.write_str($text), )*
                }
            }
        }

        impl<C> From<MuxError<C>> for $name<C>
        where
            C: TrConnCfg + 'static,
        {
            fn from(err: MuxError<C>) -> Self {
                $name::Mux(err)
            }
        }

        impl<C> core::error::Error for $name<C>
        where
            C: TrConnCfg + 'static,
        {
            fn source(&self) -> Option<&(dyn core::error::Error + 'static)> {
                match self {
                    $name::Mux(err) => Option::Some(err),
                    _ => Option::None,
                }
            }
        }
    };
}

/// [`TrConnection`](abs_smux::conn::TrConnection) 的错误类型：目前只有 `bind_async`。
pub enum BindError<C>
where
    C: TrConnCfg,
{
    /// 要绑定的 dock 是协议保留值（`unspecified` / `wildcard`），不能当身份用。
    ReservedDock,

    /// 该 local_dock 已被占用。
    DockInUse,

    /// 连接级失败。
    Mux(MuxError<C>),
}

/// [`TrDockBinding`](abs_smux::conn::TrDockBinding) 的错误类型。
///
/// 一个 binding 上可以做三件事——`listen_async` / `open_telegraph_async` /
/// `open_channel_async`——它们共用这一个错误类型（`abs_smux` 只给了一个 `Err` 槽），
/// 因此这里放的是三者失败变体的并集：
///
/// - `listen` / `open_telegraph` 的 local dock 在绑定时就验过，**不会**报 `ReservedDock`；
/// - `ReservedDock` 只会来自 `open_channel` 的**对端 dock**（channel 的身份是 dock 对，
///   两端都必须是真实 dock）；
/// - telegraph 的报文目的地址允许保留值，与本类型无关（见 `TelegraphError`）。
pub enum BindingError<C>
where
    C: TrConnCfg,
{
    /// 对端 dock 是协议保留值，不能当身份用（只可能来自 `open_channel`）。
    ReservedDock,

    /// 同一 dock 对上已有活跃子流（`open_channel`）。
    Duplicate,

    /// 该 dock 对刚关闭，仍在拆流宽限期内（`open_channel`）。
    WaitClose,

    /// 该 dock 上的活动子流数已达上限（`open_channel`）。
    DockChanLimit,

    /// 连接上的活动子流数已达上限（`open_channel`）。
    ChanLimit,

    /// 该 local_dock 已被占用（三者都可能：listener / telegraph / channel 不得冲突）。
    DockInUse,

    /// 子流 / 连接已关闭（`open_channel`）。
    Closed,

    /// 连接级失败。
    Mux(MuxError<C>),
}

/// [`TrChannelListener`](abs_smux::conn::TrChannelListener) 的错误类型（`income_async`）。
pub enum ListenerError<C>
where
    C: TrConnCfg,
{
    /// 本次等待被取消。
    Cancelled,

    /// 连接级失败。
    Mux(MuxError<C>),
}

/// [`TrChannelHandle`](abs_smux::chan::TrChannelHandle) 的错误类型
/// （`accept_async` / `reject_async` 共用）。
pub enum HandleError<C>
where
    C: TrConnCfg,
{
    /// **拒绝接受**调用方给出的环内存：大小不合用（连接不替调用方改尺寸）。
    RingRejected,

    /// 对端拒绝建立这条子流（`accept_async` 的发起方一侧）。
    Refused,

    /// 流控失败（窗口违例或计数溢出）。
    FlowCtrl(FlowCtrlError),

    /// 本次操作被取消。
    Cancelled,

    /// 连接级失败。
    Mux(MuxError<C>),
}

/// [`TrTelegraph`](abs_smux::conn::TrTelegraph) 的错误类型（`send_async` / `recv_async`）。
pub enum TelegraphError<C>
where
    C: TrConnCfg,
{
    /// 报文超过协商的 `max_packet_size`。
    ///
    /// 注意 remote dock 取 `wildcard` / `unspecified` **不是**错误：那是合法目的地址，
    /// 收不收由对端策略决定。
    FrameTooLarge,

    /// 连接级失败。
    Mux(MuxError<C>),
}

face_error_impls!(
    BindError,
    BindError::ReservedDock => "要绑定的 dock 是协议保留值，不能作为身份",
    BindError::DockInUse => "该 local_dock 已被占用",
);

face_error_impls!(
    BindingError,
    BindingError::ReservedDock => "对端 dock 是协议保留值，不能作为身份",
    BindingError::Duplicate => "同一 dock 对上已有活跃子流",
    BindingError::WaitClose => "该 dock 对刚关闭，仍在拆流宽限期内",
    BindingError::DockChanLimit => "该 dock 上的活动子流数已达上限",
    BindingError::ChanLimit => "连接上的活动子流数已达上限",
    BindingError::DockInUse => "该 local_dock 已被占用",
    BindingError::Closed => "子流 / 连接已关闭",
);

face_error_impls!(
    ListenerError,
    ListenerError::Cancelled => "本次等待被取消",
);

face_error_impls!(
    HandleError,
    HandleError::RingRejected => "调用方给出的环内存大小不合用，已拒绝接受",
    HandleError::Refused => "对端拒绝建立这条子流",
    HandleError::FlowCtrl(_) => "流控失败",
    HandleError::Cancelled => "本次操作被取消",
);

face_error_impls!(
    TelegraphError,
    TelegraphError::FrameTooLarge => "报文超过协商的最大报文长度",
);
