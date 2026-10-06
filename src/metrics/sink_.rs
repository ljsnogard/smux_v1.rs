//! 指标上报的**接收方** trait 与事件类型。
//!
//! # 本模块只描述「mux 愿意报什么」
//!
//! [`TrMetricsSink`] 是**依赖倒置**的那一半：mux 只声明自己愿意报告的事件，具体
//! 怎么接收、存到哪、怎么聚合、要不要跨进程导出，全部由**需求方**自己决定。
//! 因此本 trait 的每个方法都**带默认空实现**——只关心其中几项的实现者不必写
//! 一堆空函数；mux 后续增补指标也因此不构成破坏性变更。
//!
//! # mux 侧不承担任何灵活性代价
//!
//! 这是本特性最要紧的一条边界（需求方 2026-10-06 的裁决）：
//!
//! - mux **不**做 `Arc` 分配：sink 实例归需求方所有，mux 只经
//!   [`TrConnCfg::metrics`](crate::connection::TrConnCfg::metrics) 借一个
//!   `Option<&Self::Metrics>`，并在建连时 `clone` 一份进内部共享量；
//! - mux **不**提供运行时替换 sink 的入口；
//! - mux **不**做 `dyn` 适配（也**不**提供 `Arc<dyn TrMetricsSink>` 之类的便利
//!   impl）——上报调用点在 mux 内部一律是**具体类型的静态分派**，因此会被单态化。
//!   「实际上是否走了虚调用」完全取决于需求方把 `Metrics` 填成什么类型：
//!   填 `Arc<dyn TrMetricsSink>` 就有间接调用，填具体类型就能被内联。
//!
//! # 硬契约（写进 trait 的所有实现）
//!
//! - `Send + Sync + 'static`：[`MuxCore`](crate::connection::MuxConnection) 在
//!   tokio 装配下要 `Send + Sync`（`tests/thread_safety.rs` 把它钉成了编译期断言），
//!   sink 挂在配置上，因此它必须同时满足这两个约束；
//! - 方法必须**同步、非阻塞、不 `panic`、不做隐式分配**：连接关闭事件在核心的
//!   `Drop` 里上报，而那条路径的硬纪律是**不取锁、不阻塞**（最后一个句柄可能在任意
//!   线程上被丢弃）。需要跨线程聚合时请用原子或自己实现的通道 `try_send`。
//!
//! # 引用环禁令
//!
//! sink **不得**持有 [`MuxConnection`](crate::connection::MuxConnection) 或任何会话
//! 句柄，否则会形成 `MuxCore → sink → MuxConnection → MuxCore` 的引用环，核心永不
//! 析构、取消令牌永不触发，「丢弃连接即关闭连接」这条既有不变量随之失效。
//!
//! # Examples
//!
//! ```
//! use smux_v1::metrics::{NoMetrics, TrMetricsSink};
//!
//! // `NoMetrics` 是零大小的空实现：feature 关闭时的缺省占位。
//! let sink = NoMetrics;
//! sink.on_conn_opened();
//! ```
//!
//! 只关心一两项的实现者只写那一两项：
//!
//! ```
//! use core::sync::atomic::{AtomicU64, Ordering};
//! use smux_v1::metrics::{ConnCloseReason, TrMetricsSink};
//!
//! /// 只记录「见过的最长连接寿命」。
//! #[derive(Debug, Default)]
//! struct AgeSink(AtomicU64);
//!
//! impl TrMetricsSink for AgeSink {
//!     fn on_conn_closed(&self, _reason: ConnCloseReason, lifetime_millis: u64) {
//!         self.0.fetch_max(lifetime_millis, Ordering::Relaxed);
//!     }
//! }
//!
//! let sink = AgeSink::default();
//! sink.on_conn_closed(ConnCloseReason::PeerClosed, 1234);
//! assert_eq!(sink.0.load(Ordering::Relaxed), 1234);
//! ```

use crate::connection::{Dock, FrameKind, MuxError};

/// 帧与字节的**落地方向**。
///
/// 「本端视角」：`Send` 是本端写出去的方向，`Recv` 是本端读进来的方向。它与
/// `MuxError::Transport { write }` 的方向语义一致。
///
/// # Examples
///
/// ```
/// use smux_v1::metrics::FrameDir;
///
/// assert_ne!(FrameDir::Send, FrameDir::Recv);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FrameDir {
    /// 本端发出（写方向）。
    Send,

    /// 本端收到（读方向）。
    Recv,
}

/// **连接**关闭的原因。
///
/// 与 [`MuxError`] 的三种「断开」对应，但分类口径不同：这里问的是「连接为什么结束」，
/// 而不是「某次操作返回了什么错误」。`mod.rs` 模块文档 §8 强调过这三种断开不可混用。
///
/// # Examples
///
/// ```
/// use smux_v1::metrics::ConnCloseReason;
///
/// let reason = ConnCloseReason::Transport;
/// assert_eq!(reason, ConnCloseReason::Transport);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnCloseReason {
    /// 正常收尾：最后一个应用面句柄被丢弃，核心析构并触发五个循环退出。
    ///
    /// 「被取消令牌终止的收尾」也归入这一类：那条路径**不**在注册表上留下失败标志，
    /// 语义上就是正常关闭（见 `session_::fail_mux_loop_` 的文档）。
    Local,

    /// **对端**主动关闭（收到对端 `CLOSE`，或对端读端正常收尾）。
    PeerClosed,

    /// 传输层读 / 写错误导致的中断。
    Transport,

    /// **协议错误**（非法帧、状态机错误、流控违例等）导致连接终止。
    ProtocolError,
}

/// **子流**关闭的原因。
///
/// `FIN` / `RESET` 是两条正交方向（见 [`crate::connection`] 模块文档 §7.0），但
/// **本枚举描述的是「子流为什么结束」，不是「过程中收没收到 RESET」**：`RESET` 是
/// 一个**事件**（可能发生多次、也可能只发生在一个方向），因此由
/// [`TrMetricsSink::on_reset`] 单独报告，不在这里重复表达。
///
/// # Examples
///
/// ```
/// use smux_v1::metrics::ChannelCloseReason;
///
/// assert_ne!(ChannelCloseReason::Fin, ChannelCloseReason::IdleTimeout);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChannelCloseReason {
    /// 两个方向都协议收尾（本端与对端的 `FIN` 都完成），身份进入宽限期。
    Fin,

    /// 已建立子流的空闲超时：`max_channel_timeout` 内没有收到任何字节。
    IdleTimeout,

    /// 建流尚未裁决即超时（连接代替调用方向对端回 `REJECT`）。
    EstablishTimeout,

    /// 连接级失败牵连：连接已经不可用，在册子流一同终结。
    ///
    /// 注意当前实现**只在文档层面**表达这一原因：连接级失败时
    /// `ChannelRegistry_::mark_failed_` 不逐条释放身份（见 [`crate::metrics`] 模块
    /// 文档 §6 的「已知缺口」），因此这些子流不会有各自的关闭回调，需求方只能从
    /// [`TrMetricsSink::on_conn_closed`] 推断它们一并结束。
    ConnFailed,
}

/// 连接指标的**接收方**。
///
/// 详见模块文档：这里只声明「mux 愿意报什么」，怎么接收、存哪、怎么聚合全部由
/// 实现方决定；所有方法都带默认空实现。
///
/// # 实现约束
///
/// 实现必须 `Send + Sync + 'static`，且所有方法**同步、非阻塞、不 `panic`、不做
/// 隐式分配**——原因见模块文档的「硬契约」一节。
///
/// # Examples
///
/// ```
/// use smux_v1::metrics::{NoMetrics, TrMetricsSink};
///
/// fn report(sink: &impl TrMetricsSink) {
///     sink.on_conn_opened();
///     sink.on_conn_closed(smux_v1::metrics::ConnCloseReason::Local, 42);
/// }
///
/// // 空实现：什么都不做，也不会 panic。
/// report(&NoMetrics);
/// ```
pub trait TrMetricsSink: Send + Sync + 'static {
    /// 连接建立成功（复用层收口：五个循环已经投递）。
    fn on_conn_opened(&self) {}

    /// 连接关闭。
    ///
    /// `lifetime_millis` 是**连接内毫秒**寿命（自建连 epoch 起算），与协议里的
    /// 时间量用同一把尺子。
    ///
    /// 本方法在核心的 `Drop` 里被调用，因此实现**不得**取锁或阻塞（见模块文档）。
    fn on_conn_closed(&self, _reason: ConnCloseReason, _lifetime_millis: u64) {}

    /// 子流建成（两个环已建好、两侧半部已移交）。
    fn on_channel_opened(&self, _local: Dock, _remote: Dock) {}

    /// 子流关闭（两个方向都协议收尾，或超时 / 连接级失败牵连）。
    ///
    /// `lifetime_millis` 同样是连接内毫秒。
    fn on_channel_closed(
        &self,
        _local: Dock,
        _remote: Dock,
        _reason: ChannelCloseReason,
        _lifetime_millis: u64,
    ) {
    }

    /// 收到 / 发出**一个**帧。
    ///
    /// `local` / `remote` 是该帧所属**子流**的 dock 对。本版本的每一种帧都是子流
    /// 作用域（见 [`crate::connection`] 模块文档 §3），因此需求方可以据此按子流聚合
    /// 字节数——「每个子流发送 / 接收字节数」不需要额外的回调。
    ///
    /// `bytes` 是该帧的**线上总字节数**（帧头 + 载荷），两侧口径一致：写侧取
    /// 帧头的实际编码长度，读侧取逐字节状态机自己数出的帧头长度
    /// （`FrameHeaderParser::consumed_len_`），因此**同一批帧在两个方向的字节数之和
    /// 相等**。需要「子流有效载荷」的实现请自行扣减帧头，或把它当线上开销口径使用。
    ///
    /// 这是热路径上唯一的逐帧回调：请保持轻量。
    fn on_frame(
        &self,
        _dir: FrameDir,
        _local: Dock,
        _remote: Dock,
        _kind: FrameKind,
        _bytes: u32,
    ) {
    }

    /// 帧处理失败 / 协议错误。
    ///
    /// `kind` 为「已经解析出帧种类」时的种类；帧头都没解析出来时为 [`Option::None`]。
    fn on_frame_error(&self, _kind: Option<FrameKind>, _err: MuxError) {}

    /// 流控阻塞事件：某条子流有数据待发，但对端窗口额度不足以继续。
    ///
    /// `millis` 是该子流已经连续处于该状态的连接内毫秒数。
    fn on_flow_stalled(&self, _local: Dock, _remote: Dock, _millis: u64) {}

    /// 本端发出或收到一条 `CLOSE(RESET)`。
    ///
    /// `by_peer` 为 `true` 表示是对端发来的（本端为接收方）。
    fn on_reset(&self, _local: Dock, _remote: Dock, _by_peer: bool) {}

    /// 连接级**原始字节**流量（两泵口径：字节流上真实搬运的字节，不分帧）。
    ///
    /// 它与 [`TrMetricsSink::on_frame`] 的字节数**不是**同一口径：后者是帧总长，
    /// 前者包含字节流上的一切（含被丢弃的帧、以及尚未成帧的零头）。要算「线上
    /// 开销比」时用两者相除。
    fn on_transport_bytes(&self, _dir: FrameDir, _bytes: u64) {}
}

/// **空实现**：什么都不做，零大小。
///
/// 它有两个用途：
///
/// 1. 作为 `feature = "metrics"` 关闭时
///    [`TrConnCfg::Metrics`](crate::connection::TrConnCfg::Metrics) 的缺省关联类型；
/// 2. 作为「明确不上报」的显式选择。
///
/// `#[inline(always)]` + 零大小意味着：所有上报调用点在单态化后**完全没有指令**
/// ——这是「不开 feature 时热路径零开销」的来源。
///
/// # Examples
///
/// ```
/// use smux_v1::metrics::{NoMetrics, TrMetricsSink};
///
/// let sink = NoMetrics;
/// sink.on_frame(
///     smux_v1::metrics::FrameDir::Send,
///     smux_v1::connection::Dock::new(3u32),
///     smux_v1::connection::Dock::new(7u32),
///     smux_v1::connection::FrameKind::Data,
///     1024,
/// );
/// ```
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct NoMetrics;

impl TrMetricsSink for NoMetrics {
    #[inline(always)]
    fn on_conn_opened(&self) {}

    #[inline(always)]
    fn on_conn_closed(&self, _reason: ConnCloseReason, _lifetime_millis: u64) {}

    #[inline(always)]
    fn on_channel_opened(&self, _local: Dock, _remote: Dock) {}

    #[inline(always)]
    fn on_channel_closed(
        &self,
        _local: Dock,
        _remote: Dock,
        _reason: ChannelCloseReason,
        _lifetime_millis: u64,
    ) {
    }

    #[inline(always)]
    fn on_frame(
        &self,
        _dir: FrameDir,
        _local: Dock,
        _remote: Dock,
        _kind: FrameKind,
        _bytes: u32,
    ) {
    }

    #[inline(always)]
    fn on_frame_error(&self, _kind: Option<FrameKind>, _err: MuxError) {}

    #[inline(always)]
    fn on_flow_stalled(&self, _local: Dock, _remote: Dock, _millis: u64) {}

    #[inline(always)]
    fn on_reset(&self, _local: Dock, _remote: Dock, _by_peer: bool) {}

    #[inline(always)]
    fn on_transport_bytes(&self, _dir: FrameDir, _bytes: u64) {}
}
