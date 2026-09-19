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
//! 连接**独占** `Rx` / `Tx`：握手完成后它们就交给 [`MuxConnection`]，**不再出现
//! 在任何公开签名里**；其它类型要完成任何功能，只能通过 [`MuxConnection`] 的 API
//! ——[`TrConnection`](abs_smux::conn::TrConnection) /
//! [`TrDockBinding`](abs_smux::conn::TrDockBinding) 这套 trait；**没有任何驱动
//! API**（收发由连接内部经 `abs_art` spawn 的任务自动推进）。
//!
//! 内部按方向切成两个循环（对外不可见）：
//!
//! - 读循环：独占 `Rx`，负责**解复用**——逐帧解析后把载荷投递到目标子流的接收环，
//!   并维护接收方向窗口；
//! - 写循环：独占 `Tx`，负责**复用调度**——从各子流的发送环取数据、切分并编帧，
//!   按「控制帧优先、数据帧按对端发送窗口排序」写出，并维护发送方向窗口；
//! - 两者之间只共享**子流注册表**（dock → 子流映射、窗口、关闭标志），因此读路径
//!   与写路径绝大部分操作互不阻塞；只有建流 / 拆流这类稀有时刻需要碰注册表。
//!
//! 连接在 [`MuxConnection::new`] 时接管 `Rx` / `Tx`，并**在内部经 `abs_art` spawn**
//! 读 / 写两个循环，因此对外**不需要也不提供**任何驱动 API：用户只使用
//! `abs_smux` 的 trait。本 crate 不依赖任何具体运行时，也不依赖
//! `abs_art-bridge`——后端由最终二进制选择，连接只带上运行时的**类型参数**
//! `Rt`（见 §6）。
//!
//! ### 2.1 两个循环的收尾方式：取消令牌 + 当场 detach
//!
//! **不靠句柄停任务。** `abs_art` 的 [`TrJoinHandle`](abs_art::TrJoinHandle)
//! **没有 `abort`**，只有 `detach` 与「可 await 的 join」；更麻烦的是三个后端对
//! 「drop 句柄」的语义并不一致——tokio 视作 detach（任务继续跑），compio 与 smol
//! 视作取消。依赖句柄的 drop / abort 会让同一份代码在不同后端上有不同的关闭语义。
//!
//! 因此收尾统一走**共享的取消令牌 + 「连接已失败」标志**：`new` 在 spawn 后立即
//! `detach()` 句柄，循环在**每个 await 点**（读 / 写 / park 都经
//! `may_cancel_with(令牌)`）检查令牌与失败标志并自行退出；连接 `Drop` 时置位即可。
//! 句柄因此不必存成字段，`Rt` 的关联类型 `JoinHandle` 也不必出现在任何类型签名里。
//! 顺带的好处是：设备 / 网络错误可以由同一个失败标志回传给被动端，避免被动端
//! 空等一个已经死掉的循环。
//!
//! ## 3. 帧线格式（sans-IO）
//!
//! 一帧 = `帧首字节` + `自描述字段序列` + `载荷`：
//!
//! ```text
//! +----------+--------------------------------+----------------------+
//! | 首字节    | 自描述字段序列                  | 载荷                  |
//! | kind+flag| （以 PayloadLen 字段收尾）      | （PayloadLen 字节）   |
//! +----------+--------------------------------+----------------------+
//! ```
//!
//! **首字节**把帧种类与标志位合并成一个定长字节
//! `head = (flags << 4) | kind`：低 4 位是 [`FrameKind`]（`0x01..=0x09`，其余
//! 保留），高 4 位是 [`flags`]（`FIN` / `RESET` / `ACK` / `NO_PAYLOAD`，恰好用满
//! 4 位）。两者是每帧都有的定长信息，单列成字段要各占「1 个头字节 + 至少 1 个值
//! 字节」，合并后只占 1 字节。
//!
//! **其余字段**沿用握手协议（[`crate::handshake::opts`]）的「**自描述字段字节**」
//! 策略：每个头字段的第一个字节是
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
//! `PayloadLen` 必需，且**必须是最后一个头字段**——读到它即知头结束、其后就是
//! 载荷，因此它同时充当字段序列的定界符；其余字段的顺序不承载语义。
//! `LocalDock` / `RemoteDock` 在除 `PING` / `PONG` 之外的所有帧上都必需（含
//! `WINDOW_UPDATE`，否则无从知道该更新属于哪条子流，见 §4.1）；`WindowUpdate`
//! 只在 `WINDOW_UPDATE` 帧上出现，`ReasonCode` 只在 `REJECT` 帧上出现。字段的
//! 完整约束见私有模块 `frame_`。
//!
//! 帧总长受协商出的 `max_packet_size` 约束（超限即 [`MuxError::FrameTooLarge`]），
//! 该检查由读会话完成：帧长上限来自 [`BasicOpts`](crate::handshake::opts::BasicOpts)，
//! 编解码入口只管子结构。
//!
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
//! ### 4.1 子流身份：dock 对即身份
//!
//! 一条子流的身份**就是** `(local_dock, remote_dock)` 这个有序对。协议中
//! **不存在**双方共识的、或任何一方分配的 channel id，也**不需要**有——同一
//! 时刻的「哪一条子流」只需**本方自己**能从 dock 对里认出来即可：
//!
//! - **发起方**：为每条并发子流分配一个**互不相同**的 `local_dock`（类似 TCP
//!   的临时端口），`remote_dock` 填对端监听的 dock；
//! - **响应方**：在自己的某个 `local_dock` 上监听，用请求帧里的 `remote_dock`
//!   区分「这是哪一条连接」。
//!
//! 由此得到两条硬性约束（因为是协议身份，不是实现细节）：
//!
//! 1. **同一个 `(local_dock, remote_dock)` 上，同一时刻至多存在一条活动子流**。
//!    发起方若把两条并发子流发往同一个 `remote_dock`，就必须用两个不同的
//!    `local_dock`；否则两端都无法区分。
//! 2. `max_dock_chan_count` 限制的是**单个 `local_dock`** 上同时活动的子流数
//!    （响应方因此可以「一个 dock 服务多条连接」），`max_channel_count` 限制
//!    整条连接上的总数。
//!
//! 之所以不引入 channel id：dock 本身已经是端口语义，再加一层 id 会让线格式、
//! 建流状态机与两侧映射表都多一份需要同步维护的状态，而这些状态正是最容易出
//! 错的地方。
//!
//! `channel` 与 `telegraph` **不得共用同一个 local_dock**（见
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
//! ## 6. 线程模型与运行时参数 `Rt`
//!
//! 连接带上运行时的**类型参数** `Rt`（`MuxConnection<R, W, C, Rt>`）：内部两个
//! 循环经 `abs_art` spawn，所以必须知道用哪个运行时。`Rt` 是纯类型——`abs_art`
//! 的 spawn 是无 `self` 的关联函数，连接不持有运行时的值；后端由最终二进制选择
//! （`abs_art-tokio::Runtime<{..}>` / `abs_art-compio::Runtime<{..}>`），本 crate
//! 内部不出现任何后端名，也不依赖 `abs_art-bridge`。它必须是**结构体**上的参数
//! 而非只做 `new` 的方法级泛型，否则内部循环的句柄类型无处可写。
//!
//! bound 由 cargo feature 二选一（见 `Cargo.toml` 的 `[features]`）：
//!
//! - **缺省（不开启 `multi-thread`）**：`Rt: TrSpawnLocal`，面向单线程运行时
//!   （compio 首要目标），连接与 session 为 `!Send`，共享注册表用非原子容器，
//!   循环经 `Rt::spawn_local` 投递；
//! - **开启 `multi-thread`**：`Rt: TrSpawnSend`，面向多线程运行时，连接与 session
//!   为 `Send + Sync`，共享注册表用原子原语，循环经 `Rt::spawn` 投递。
//!
//! 注意：`buffex` 的端类型自身要求 `Send + Sync` 的参数，因此环存储与分配器在
//! 两种模型下都必须是 `Send + Sync`；feature 影响的是**连接对象本身**是否可跨
//! 线程共享、共享注册表用什么原语保护，以及循环往哪个队列投递。
//!
//! ## 7. 生命周期与超时
//!
//! 子流支持**半关闭**：每个方向的结束是独立的，`is_tx_closed` / `is_rx_closed`
//! 分别反映；对端关闭一半只影响对应方向。这两个标志**不额外维护**：它们直接读
//! `buffex` 环两端各自的关闭位（会话通过关闭 / 丢弃自己那一端来表达 `FIN` /
//! `RESET` / 拆流），映射与理由见私有模块 `channel_` 的「关闭态」一节。
//! `max_channel_timeout` 的**空闲超时由连接内部用 `abs_art` 的 `TrDelay` 维持**，
//! 不再要求调用方代为计时。
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
