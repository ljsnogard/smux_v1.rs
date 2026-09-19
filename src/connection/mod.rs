//! # smux v1 复用连接（connection phase）
//!
//! 本模块在 [`crate::handshake`] 的成果之上构建**流复用**框架：把一条已经建立、
//! 可靠且有序的字节流切分成若干条互不干扰的子流，并对外实现
//! [`abs_smux`] 定义的一组 trait。
//!
//! 握手模块只产出 [`HandshakeDelivery`](crate::handshake::agent::HandshakeDelivery)
//! ——一对 `Tx` / `Rx` 与一份 [`BasicOpts`](crate::handshake::opts::BasicOpts)。
//! 本模块消费这份交付物：`Rx` 交给**读会话**做解复用，`Tx` 交给**写会话**做复用
//! 调度，`BasicOpts` 提供连接级配额（`max_packet_size`、`max_channel_count`、
//! `max_dock_chan_count`、`max_channel_timeout`）。
//!
//! ## 1. 与 `abs_smux` 的对应关系
//!
//! | `abs_smux` trait | 本模块类型 | 作用 |
//! | --- | --- | --- |
//! | [`TrConnection`](abs_smux::conn::TrConnection) | [`MuxConnection`] | 连接根对象；`bind_async(local_dock)` 在某个 dock 上派生一个会话 |
//! | [`TrDockBinding`](abs_smux::conn::TrDockBinding) | [`DockBinding`] | **一个业务逻辑持有的会话**：可 listen / open_telegraph / open_channel |
//! | [`TrChannelListener`](abs_smux::conn::TrChannelListener) | [`ChannelListener`] | 某 dock 上的类 `TcpListener`；`income_async()` 取下一个入向请求 |
//! | [`TrChannelHandle`](abs_smux::conn::TrChannelHandle) | [`ChannelHandle`] | 入向请求的待决句柄；`accept_async` / `reject_async` |
//! | [`TrChannelTx`](abs_smux::conn::TrChannelTx) | [`ChannelTx`] | 子流发送半边，包一个 `buffex` 生产端（`TrBuffTryWrite`） |
//! | [`TrChannelRx`](abs_smux::conn::TrChannelRx) | [`ChannelRx`] | 子流接收半边，包一个 `buffex` 消费端（`TrBuffTryRead`） |
//! | [`TrTelegraph`](abs_smux::conn::TrTelegraph) | [`Telegraph`] | 数据报端点；`send_async` / `recv_async` |
//! | [`TrDock`](abs_smux::conn::TrDock) | [`Dock`] | dock 的具体类型（见 §4） |
//!
//! 子流的 `Tx` / `Rx` 实现的是**非阻塞（try）**接口：应用只与本地环形缓冲打交道，
//! 真正的网络收发由读写会话在后台推进。因此「快生产者 + 慢网络」不会把网络阻塞
//! 传播给应用，反之亦然。
//!
//! ## 2. 读写双 session
//!
//! 连接状态**不放在一把大锁后面**，而是按方向切分（对应「不同业务逻辑各自竞争
//! 读锁 / 写锁，因此各持一个 session」）：
//!
//! - [`ReadSession`]：独占网络读半边 `Rx`，负责**解复用**——逐帧解析后把载荷
//!   投递到目标子流的接收环，并维护接收方向窗口；
//! - [`WriteSession`]：独占网络写半边 `Tx`，负责**复用调度**——从各子流的发送环
//!   取数据、切分并编帧，按优先级 / 轮转写出，并维护发送方向窗口；
//! - 两者之间只共享**子流注册表**（dock → 子流映射、窗口、关闭标志），因此读路径
//!   与写路径绝大部分操作互不阻塞；只有建流 / 拆流这类稀有时刻需要碰注册表。
//!
//! 两个 session 各自暴露一个 `run_async` future，由**调用方**交给运行时驱动：
//! 本 crate 不 spawn、不自旋、不依赖任何具体运行时。只驱动其中一个是允许的
//! （例如只发不收的端点），但两者都不会自行启动。
//!
//! ## 3. 帧线格式（sans-IO）
//!
//! 复用帧沿用握手协议「**自描述字段字节**」的策略：每个头字段的第一个字节是
//!
//! ```text
//! header = (val_type << 4) | field_id
//! ```
//!
//! - 低 4 位 `field_id`（[`FieldId`]）说明**这个字段是什么**——因此
//!   [`FieldId::LocalDock`] 与 [`FieldId::RemoteDock`] 是两个不同的 `field_id`，
//!   同一个字节就同时表达了「宽度」与「这是哪个 dock」；
//! - 高 3 位 `val_type`（复用
//!   [`NegotiationValType`](crate::handshake::opts::NegotiationValType)）说明
//!   **值是几个字节**（1/2/3/4/8，大端）；发送方取能容纳数值的最小宽度，接收方
//!   接受任意足够宽的编码；
//! - bit 7 保留，发送方置 0，接收方按掩码忽略。
//!
//! 一帧 = `字段序列` + `载荷`：`Kind` 与 `PayloadLen` 是必需字段，`LocalDock` /
//! `RemoteDock` 在 channel 帧上必需，`WindowUpdate` 只在 `WINDOW_UPDATE` 帧上出现。
//! 帧总长受协商出的 `max_packet_size` 约束（超限即 [`MuxError::FrameTooLarge`]）。
//! 数据帧**不带 CRC**：底层字节流由握手层建立时的可靠传输承担，复用层再算一遍
//! CRC 只会给线速数据增加成本；控制帧如需额外保护，可后续以字段形式扩展。
//!
//! 编解码入口见私有模块 `frame_`（只重导出公开类型）。
//!
//! ## 4. dock
//!
//! dock 类似 TCP 端口：子流由 `(local_dock, remote_dock)` 唯一标识，`listen` 则是
//! 「在某个 local_dock 上等待任意 remote_dock 的请求」。具体类型取
//! `abs_smux::dock::Dock<u32>`（[`Dock`]）：
//!
//! - 数值语义是**规范**的（`unspecified` = 0，`wildcard` = `u32::MAX`），
//!   因此 `Dock::new(3)` 无论按 1 / 2 / 4 字节编码都是同一个 dock；
//! - 线格式支持 1 / 2 / 4 字节三种宽度（由 `LocalDock` / `RemoteDock` 字段的
//!   `val_type` 决定），由发送方按最小值选宽。
//!
//! `channel` 与 `telegraph` **不得共用同一个 local_dock**（见
//! [`TrTelegraph`](abs_smux::conn::TrTelegraph) 的文档）；绑定期由注册表拒绝，
//! 报 [`MuxError::DockInUse`]。
//!
//! ## 5. 缓冲区
//!
//! 子流收发环与协议帧的收发暂存**一律使用 `buffex::circular_buff`**：
//!
//! - 每条子流持有一对 `buffex` 半部（[`BufferedTx`] / [`BufferedRx`]），
//!   因此 [`ChannelTx`] / [`ChannelRx`] 的 `try_*` 直接作用在环形缓冲上；
//! - 读写会话各自持有一条「帧暂存环」，把网络字节流与帧解析 / 成帧解耦。
//!
//! 环的物理内存由**调用方注入**的分配器 `A` 分配（缺省 `buffex::CoreAlloc`），
//! 容量策略同样由调用方注入（见 [`crate::flow_ctrl::TrFlowCtrlPolicy`] 与
//! [`MuxConnection::new`] 的参数）。本 crate 不隐式分配、不隐藏内存预算。
//!
//! ## 6. 线程模型
//!
//! 由 cargo feature 决定（见 `Cargo.toml` 的 `[features]`）：
//!
//! - **缺省（不开启 `multi-thread`）**：面向单线程运行时（如 compio 的
//!   `spawn_local`），连接与 session 为 `!Send`，共享注册表用非原子容器；
//! - **开启 `multi-thread`**：面向多线程运行时，连接与 session 为 `Send + Sync`，
//!   共享注册表用原子原语，两个 `run_async` 可以跨线程 spawn。
//!
//! 注意：`buffex` 的端类型自身要求 `Send + Sync` 的参数，因此环存储与分配器在
//! 两种模型下都必须是 `Send + Sync`；feature 影响的是**连接对象本身**是否可跨
//! 线程共享，以及共享注册表用什么原语保护。
//!
//! ## 7. 生命周期与超时
//!
//! 子流支持**半关闭**：每个方向的结束是独立的，`is_tx_closed` / `is_rx_closed`
//! 分别反映；对端关闭一半只影响对应方向。`max_channel_timeout` 由**调用方驱动**
//! ——本 crate 不自带定时器，调用方把超时 / tick 作为取消令牌或独立的驱动 future
//! 施加（与握手模块 §10 的约定一致）。
//!
//! ## 8. 错误
//!
//! 控制面统一用 [`MuxError`]；数据面（子流半边的 `TrBuffTryRead` /
//! `TrBuffTryWrite`）直接使用 `buffex` 端半部的错误
//! （`ConsumerError` / `ProducerError`），它们已携带 `ReadErrTag` /
//! `WriteErrTag`，足以区分「取消」「对端关闭」与「暂时无数据」。

mod channel_;
mod error_;
mod frame_;
mod session_;
mod telegraph_;

pub use channel_::{
    ChannelHandle, ChannelListener, ChannelRx, ChannelTx, DockBinding, MuxConnection,
    TrMuxConfig,
};
pub use error_::MuxError;
pub use frame_::{FieldId, FrameHeader, FrameKind, flags};
pub use session_::{ReadSession, WriteSession};
pub use telegraph_::Telegraph;

/// smux v1 使用的 dock 类型。
///
/// 见模块文档 §4。`unspecified()` 为 0，`wildcard()` 为 `u32::MAX`；线格式按
/// 1 / 2 / 4 字节自适应宽度编码。
pub type Dock = abs_smux::dock::Dock<u32>;

/// 子流发送半边所用的 `buffex` 生产端类型。
pub type BufferedTx<B, A> =
    buffex::circular_buff::Producer<buffex::circular_buff::BufConsumer<u8>, B, u8, A>;

/// 子流接收半边所用的 `buffex` 消费端类型。
pub type BufferedRx<B, A> =
    buffex::circular_buff::Consumer<buffex::circular_buff::BufProducer<u8>, B, u8, A>;

/// 一条子流的收发环（双端被动，两端都交给会话与调用方）。
pub type BufferedChannel<B, A> = buffex::circular_buff::SpscPair<B, u8, A>;
