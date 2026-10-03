//! # smux v1 复用连接（connection phase）
//!
//! 本模块在 [`crate::handshake`] 的成果之上构建**流复用**框架：把一条已经建立、
//! 可靠且有序的字节流切分成若干条互不干扰的子流，并对外实现
//! [`abs_smux`] 定义的一组 trait。
//!
//! 握手模块只产出 [`HandshakeDelivery`](crate::handshake::agent::HandshakeDelivery)
//! ——一对 `Tx` / `Rx` 与一份 [`BasicOpts`](crate::handshake::opts::BasicOpts)。
//! 本模块消费这份交付物：`Rx` / `Tx` 在 [`MuxConnection::new`] 里被移进两个
//! **local spawn** 出来的读 / 写循环；`BasicOpts` 提供连接级配额（`max_packet_size`、
//! `max_channel_count`、`max_dock_chan_count`、`max_channel_timeout`）。
//!
//! 建连需要调用方给出一个**本地作用域值**（`abs_art` 的 `TrLocalScope`）：两个循环
//! 经它 `spawn_local` 投递，调用方负责驱动它（见 §6）。决策与落地经过见
//! `dev-notes/connection-20261002-0548.md` §17 与 §18。
//!
//! ## 1. 与 `abs_smux` 的对应关系
//!
//! | `abs_smux` trait | 本模块类型 | 作用 |
//! | --- | --- | --- |
//! | [`TrConnection`](abs_smux::conn::TrConnection) | [`MuxConnection`] | 连接根对象；`bind_async(local_dock)` 在某个 dock 上派生一个会话 |
//! | [`TrDockBinding`](abs_smux::conn::TrDockBinding) | [`DockBinding`] | **一个业务逻辑持有的会话**：可 listen / open_telegraph / open_channel |
//! | [`TrChannelListener`](abs_smux::conn::TrChannelListener) | [`ChannelListener`] | 某 dock 上的类 `TcpListener`；`income_async()` 取下一个入向请求 |
//! | [`TrChannelHandle`](abs_smux::chan::TrChannelHandle) | [`ChannelHandle`] | 入向请求的待决句柄；`accept_async` / `reject_async` |
//! | [`TrChannelTx`](abs_smux::chan::TrChannelTx) | [`ChannelTx`] | 子流发送半边，包一个 `buffex` 生产端（`TrBuffTryWrite`） |
//! | [`TrChannelRx`](abs_smux::chan::TrChannelRx) | [`ChannelRx`] | 子流接收半边，包一个 `buffex` 消费端（`TrBuffTryRead`） |
//! | [`TrTelegraph`](abs_smux::conn::TrTelegraph) | [`Telegraph`] | 数据报端点；`send_async` / `recv_async` |
//! | [`TrDock`](abs_smux::dock::TrDock) | [`Dock`] | dock 的具体类型（见 §4） |
//!
//! 子流的 `Tx` / `Rx` 实现的是**非阻塞（try）**接口：应用只与本地环形缓冲打交道，
//! 真正的网络收发由内部循环在后台推进。因此「快生产者 + 慢网络」不会把网络阻塞
//! 传播给应用，反之亦然。
//!
//! ## 2. 架构：`MuxCore` 演员 + `MuxConnection` 智能指针
//!
//! ### 2.1 两个对象
//!
//! - **`MuxCore<C, S>`**（crate 内部类型，不对外导出）：连接的**演员核心**，持有
//!   全部共享状态——资源策略 `C`、握手协商结果、两个循环的取消令牌与身份索引
//!   （包在 `ChannelRegistry_` 里）、两条事件通道的发送端，以及作用域值的保活槽。
//!   对它的每一次访问都经内部读写锁串行化：**不使用 actor 框架，也不使用消息通道**。
//!   它不持有 `Rx` / `Tx`——那两个半边在 `MuxConnection::new` 里就被移进循环。
//! - **[`MuxConnection`]**：**对一个 `MuxCore` 的智能指针的薄封装**
//!   （`mm_ptr::Shared<MuxCore<C, S>, C::Alloc>`）。`Clone` 一份就是多一个强引用，
//!   分配走调用方注入的分配器。于是它可以被任意分发：存进结构体、传进函数、跨层
//!   持有。公开类型因此是 `MuxConnection<C, S>`——参数是策略与作用域类型；
//!   **两个传输类型**由 `C::ConnTx` / `C::ConnRx` 声明（收发半边本身不在了，但
//!   「这条连接建在什么传输上」这个类型级事实留下）。
//!
//! 会话侧的四个句柄（[`DockBinding`] / [`ChannelListener`] / [`ChannelHandle`] /
//! [`Telegraph`]）与两个 channel 半部同样**各自持有一份智能指针**，而不是借用上层
//! 对象。这是本架构的全部要点：**把「谁借用谁」换成「谁持有谁的一份指针」**，
//! 于是「绑定 → 监听 → 接流 → 业务」可以拆到不同函数、不同结构体里表达，
//! 不再被生命周期参数绑成一串（`tests/layered_rpc.rs` 是这条结论的编译期探针）。
//!
//! ### 2.2 若干 `local spawn`，以及循环**不持有核心**
//!
//! 连接在**一个线程**（或 thread-local runtime）里跑若干个 `TrLocalScope::spawn_local`
//! 出来的任务：
//!
//! - **读循环**：从 `Rx` 解复用——逐帧解析后按 dock 对找到子流，把载荷投递进对应的
//!   接收环，并按收到的窗口通告更新对端的发送窗口；
//! - **写循环**：复用调度——从各子流的发送环取数据、切分并编帧，按「控制帧优先、
//!   数据帧按对端发送窗口排序」写出；
//! - （后续）保活定时任务。
//!
//! 两个循环**只持有 `LoopShared_`**（注册表句柄 + 三个标量），**不持有核心强引用**。
//! 这不是随手的选择：若循环持有核心，核心将永远无法析构，`Drop` 里的取消令牌永远
//! 不会触发，两个任务与整个连接状态会永久泄漏。现在的收尾链条是——
//!
//! ```text
//! 最后一个应用面对象被丢弃 ⇒ MuxCore 析构 ⇒ Drop 取消两个令牌
//!                        ⇒ 循环在下一个 await 点退出 ⇒ 传输半边被 drop、连接真正关闭
//! ```
//!
//! 因此「丢弃连接即关闭连接」是可观测的事实（`tests/inmem_mux.rs` 的
//! `dropping_connection_stops_both_loops_tokio_` 用析构探针钉住了它）。
//!
//! 因此状态**按子流分散**在各自的共享实体里，注册表只在建流 / 拆流这类稀有时刻
//! 被写；循环之间不靠大锁，而是靠「需求 + 事件通道」协作。
//!
//! ### 2.3 需求的投递方式：按需选择，**不是纪律**
//!
//! 所有对共享状态的**修改**都经锁串行化；但**需求怎么送进循环**是逐个决定的——
//! **不存在**「一律抢锁」这条规矩。高频的水位提示走一次性通道投递完全合理，
//! 抢锁反而是纯浪费。
//!
//! | 需求 | 发起方与频率 | 本轮的投递方式 |
//! | --- | --- | --- |
//! | 水位置位 | [`ChannelTx`] 每次写入，**极高频** | 事件通道（`WriteEvent_::TxReady`，按子流去重） |
//! | 水位回落 | [`ChannelRx`] 每次消费，**高频** | 事件通道（`WriteEvent_::RxConsumed`） |
//! | 建流 / accept / reject | 应用面，**低频** | 锁（短临界区） |
//! | 绑定 / 解绑 / listen | 应用面，**极低频** | 锁 |
//! | 方向收尾 | 半部被丢弃，低频 | 事件通道（`TxClosed` / `RxClosed`） |
//!
//! **三条不变量才是纪律**：(a) 无论怎么投递，最终修改共享状态**必须串行化**；
//! (b) 每条需求都要有**明确的顺序保证**——唤醒一律「先登记等待者、再复检需求」，
//! 否则丢唤醒；(c) **高频路径优先免锁**，低频路径优先简单。
//!
//! 于是 `signal_` 模块**保留**：它是「应用面 → 写循环」的事件通道。当前形态是
//! *无界*通道（每条子流至多一条待处理水位事件 + 协议状态机约束的控制帧，因此队列
//! 长度天然有界）。**换成定容通道是待办的优化**：无界通道每条消息都可能分配且走
//! 全局分配器（`flume` 不支持 `allocator_api`），换定容后只在建立时分配一次；
//! 但定容需要「队列满 → 置连接级全扫标志」的兜底机制，且 `flume` 内部仍是全局
//! 分配，因此**留待实测的竞争与分配数据再定**（见 dev-notes §18 的遗留项）。
//!
//! 锁的划分：身份索引与取消令牌用协作式读写锁（争用时 **park 等待**许可释放，
//! 零 CPU 忙等，见 `sync_::TrBlockingAcquire_`）；每条子流的共享标量仍用自旋读写
//! 锁（它按子流分配，换协作式锁会为每条子流多一次全局 `Arc` 分配）。
//! 两类锁的**临界区一律是短闭包，不得 `await`、不得重入**；会话句柄的 `Drop`
//! **不取任何锁**，只向核心投一条释放消息（`signal_::SessionEvent_`），真正的身份
//! 表改动由核心执行者 drain 后落实。改造因果链见 dev-notes
//! `thread-safety-20261003-1425.md`。
//!
//! ### 2.4 循环的收尾方式：取消令牌 + 当场 detach
//!
//! **不靠句柄停任务。** `abs_art` 的 [`TrJoinHandle`](abs_art::TrJoinHandle)
//! **没有 `abort`**，只有 `detach` 与「可 await 的 join」；更麻烦的是三个后端对
//! 「drop 句柄」的语义并不一致。依赖句柄的 drop / abort 会让同一份代码在不同后端
//! 上有不同的关闭语义。
//!
//! 因此收尾统一走**共享的取消令牌 + 「连接已失败」标志**：`new` 在 spawn 后立即
//! `detach()` 句柄；循环在**每个 park 点**都挂在令牌上——读循环经 `may_cancel_with`，
//! 写循环的两处 park 都与 `cancellation()` 竞争（见 `session_` 模块文档）。核心
//! `Drop` 时触发令牌即可。句柄因此不必存成字段，`S` 的关联类型 `JoinHandle` 也不必
//! 出现在任何类型签名里。
//!
//! 顺带的好处是：设备 / 网络错误可以由同一个失败标志回传给被动端，避免被动端
//! 空等一个已经死掉的循环。注意「因取消而退出」不会被记成连接失败（`fail_loop_`）。
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
//! **本版本的每一种帧都是子流作用域**，因此 `LocalDock` / `RemoteDock` 在所有帧上
//! 都必需——包括保活帧 `PULSE`（保活要维持的是**某条**子流并通告它的接收窗口）；
//! 两个 dock 还**不得取保留值**（见 §4）。接收窗口通告由
//! [`FieldId::RecvTotal`]（累计已收字节数 `R`）与 [`FieldId::RecvWindow`]（窗口值
//! `W`）**两个字段成对**表达，只在 `OPEN` / `PULSE` / `WINDOW_UPDATE` 三种帧上出现
//! 且必需，其余帧禁止携带；`ReasonCode` 只在 `REJECT` 帧上出现。其中 `RecvTotal`
//! 允许 **2 / 4 / 8 字节**三种宽度（发送方取最小者），并在累计量将超出当前规格时由
//! [`flags::K_TOTAL_RESET`] 变体宣告「累计量已重置」（携带重置前的绝对量）。字段的
//! 完整约束见私有模块 `frame_`。
//!
//! 帧总长受协商出的 `max_packet_size` 约束（超限即 [`MuxError::FrameTooLarge`]），
//! 该检查由中心循环的读路径完成：帧长上限来自 [`BasicOpts`](crate::handshake::opts::BasicOpts)，
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
//! **`unspecified` 与 `wildcard` 不能充当「身份」**：dock 对是子流的身份，因此
//! **channel 作用域的帧**（`OPEN` / `ACCEPT` / `REJECT` / `CLOSE` / `PULSE` /
//! `WINDOW_UPDATE` / 数据帧）的 `LocalDock` / `RemoteDock` 取到这两个值即协议违例
//! （接收侧报 [`MuxError::ReservedDock`]，发送侧同样拒绝构造这种帧头）。
//!
//! 但**`DATAGRAM`（telegraph）不是身份，而是地址**：它的 remote dock 取 `wildcard` /
//! `unspecified` 是**合法的**——「发到 wildcard / unspecified」由对端自己的策略决定收
//! 还是不收。发送方的 local dock 来自已绑定的 telegraph，天然是真实 dock。
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
//! [`TrTelegraph`](abs_smux::conn::TrTelegraph) 的文档）；绑定期由注册表拒绝，
//! 报 [`BindError::DockInUse`]。
//!
//! 同样在绑定期：一个 `local_dock` 在任意时刻**至多被一个 `DockBinding` 占用**
//! ——对已绑定的 dock 再次
//! [`bind_async`](abs_smux::conn::TrConnection::bind_async) 报
//! [`BindError::DockInUse`]，丢弃 binding 即解绑。这是「dock 对即身份」在**绑定层**
//! 的前置检查：子流层的唯一性检查（[`BindingError::Duplicate`]）只能发现同一个 dock 对上
//! 的重复子流，发现不了同一个 dock 上两个独立 binding 各自向不同 `remote_dock`
//! 建流、却同时对外代表同一个 local_dock 身份。
//!
//! ### 4.2 子流建立：**最终裁决**是唯一的提交点
//!
//! 建流的过程被设计成**对称**的，使两侧可以用同一套状态机实现（人类决策）。自
//! `abs_smux` 把「子流缓冲由调用方在最终裁决时给出」写进契约后，两侧都变成**两步**：
//!
//! ```text
//! 主动方 A（DockBinding::open_channel_async）   被动方 B（ChannelListener::income_async）
//!    | ① 本地登记身份 + 暂存开场消息（**不发帧**） |  （等 A 的 OPEN；收到后只登记，**不回帧**）
//!    | ② accept_async(welcome, prepare)          | ② accept_async(welcome, prepare)
//!    |    -- OPEN(开场消息, A 的接收窗口) -------> |
//!    |    <-- OPEN(载荷为空, B 的接收窗口) ------- |    B 断言自己的接收窗口
//!    |    <-- ACCEPT(欢迎信息) ------------------ |    再发 ACCEPT
//!    |    拿到 (Tx, Rx)                          |    拿到 (Tx, Rx)
//! ```
//!
//! 要点：
//!
//! - **`OPEN` 一律等到 `accept_async` 才发**。原因不是风格，而是语义：`OPEN` 要通告
//!   本端的**接收窗口**，而窗口值取决于调用方在最终裁决时给出的**接收缓冲容量**。
//!   在拿到缓冲之前发 `OPEN` 只能撒谎（要么违约、要么白留额度）。因此主动方的
//!   `open_channel_async` 只做本地登记与开场消息搬运，返回一个**尚未在线上露面**的
//!   [`ChannelHandle`]；这也是为什么被动方读到 `OPEN` 后**不再抢先回一条 `OPEN`**
//!   （旧形状在读循环里回，因为那时容量由配置统一定死）。
//! - **`accept_async` 是唯一的提交点**：建两条环、把会话侧半部交给循环、把握手推完
//!   （主动方发 `OPEN` 后等对端 `OPEN` + `ACCEPT`；被动方发自己的 `OPEN` + `ACCEPT`
//!   后立即完成）。在它之前丢弃句柄 = 放弃建立：主动方**没有任何线上痕迹**，登记
//!   立刻撤销（同一 dock 对马上可复用）；被动方则补一条 `REJECT`，免得对端悬着。
//! - `OPEN` **双方都发**：主动方那条带自己的开场消息；被动方那条载荷为空，作用是
//!   **通告自己的接收窗口**。两条 `OPEN` 都必需携带窗口通告的两个字段
//!   （[`FieldId::RecvTotal`] + [`FieldId::RecvWindow`]）。
//! - `ACCEPT` **只有被动方发**，载荷为欢迎信息；它的接收窗口已经在自己的 `OPEN` 里
//!   通告过，因此 `ACCEPT` 不再带窗口字段。
//! - 于是两侧的状态机同形：**（本地准备）→ 发送 `OPEN` → 收到对端 `OPEN` →（被动方
//!   多一步发送 `ACCEPT`）→ 建立完成**；谁都不会先看到对方的数据帧。
//! - **缓冲归属**：`accept_async` 的 `prepare` 给出的两块缓冲**就是**本条子流的环
//!   存储。**类型**由使用环境在策略里声明（[`TrMuxConfig::Buff`](abs_smux::conf::TrMuxConfig::Buff)），并在 `abs_smux`
//!   的 `TrAcceptBuff` 上登记为「本连接接受的唯一一种」——于是两个循环、两条事件通道与
//!   两个半部都能静态参数化，**零装箱、零间接**；**分配**（每条子流多大、从哪来）由调用
//!   方在 `prepare` 里当场决定，容量不合适时连接**拒绝接受**
//!   （[`HandleError::RingRejected`]），不替调用方改尺寸。接收窗口与发送环大小按该子流自己
//!   的缓冲容量算出（逐条不同）。
//! - 主动方的 `local_dock` 是为这条子流新分配的临时 dock，`remote_dock` 是对端监听的
//!   dock；被动方**镜像**过来：自己的 `local_dock` 是监听 dock，`remote_dock` 是主动方
//!   的临时 dock（见 §4.1）。
//! - 因为两侧在建流阶段就交换了接收窗口，连接不需要假定「两端用同一条规则算出相同
//!   初窗」——窗口以通告值为准。通告的**时机与频率**（建流一次、收缩跌破
//!   `1/2`/`1/4`/`0`、扩张升过 `1/2`/`3/4`/满、两次之间至少若干个数据帧）见
//!   [`crate::flow_ctrl::RecvWindow::should_report`]。
//!
//! ### 4.3 统一身份表：channel / telegraph / listener / wait-close
//!
//! 三类子流的身份**元数并不相同**：channel 有 `(local_dock, remote_dock)` 两个
//! 具体值；telegraph（[`TrTelegraph`](abs_smux::conn::TrTelegraph)）的
//! `remote_dock` 是**逐次操作的实参**，端点本身只占一个 `local_dock`；listener
//! 则天然等待**任意** `remote_dock`。但连接内部只用**一张**索引表管理它们：
//!
//! | 身份 | 表的键 | 说明 |
//! | --- | --- | --- |
//! | channel | `(local, 具体值)` | 一条子流，见 §4.1 |
//! | telegraph | `(local, unspecified)` | 数据报端点，**独占**该 `local_dock` |
//! | listener | `(local, wildcard)` | 监听器，与 channel 可共存于同一 `local_dock` |
//! | 已关闭（宽限） | `(local, 具体值)` | 见下 |
//!
//! 第三段用**协议保留值**作内部哨兵：`unspecified`（`0`）与 `wildcard`（全 1）
//! 永远不会作为真实子流的 `remote_dock`（§4），因此可以安全地承担这个语义。
//! 实现见 `mux_connection::registry_`。
//!
//! **拆流宽限期**：TCP 关闭后短时间内不复用端口，是为了让网络陈旧报文过期；
//! 本协议跑在可靠、**有序**的字节流上，同连接内不存在重排 / 重传，经典 TIME_WAIT
//! 的动机不成立。但拆流本身有一个真实竞态——本端判定「两个方向都收尾」后立即摘掉
//! 接收侧表项，而对端在**收到我们 `CLOSE` 之前**发出的数据帧仍在途（两个方向是各自
//! 有序的独立字节流，「按序」拦不住它）。因此一条子流释放后其键**不立即消失**，
//! 而是转为**宽限态**并在协商项 `max_channel_wait_close` 秒内保留：
//!
//! - 宽限期内到达的在途帧被**静默丢弃**；真正的未知子流仍然按协议违例处理
//!   （[`MuxError::MalformedFrame`]），可检测性不丢；
//! - 同一 `(local_dock, remote_dock)` 在宽限期内**不得复用**，重新登记报
//!   [`BindingError::WaitClose`]——这是顺带得到的、与 TCP 同形的复用保护；
//! - 宽限期是**dock 对**级而非 dock 级：同一 `local_dock` 上发往其它 `remote_dock`
//!   的子流不受影响（响应方的共享监听 dock 因此不会被误伤）。
//!
//! `max_channel_wait_close` 与 `max_channel_timeout` 是**两个不同的量**：后者是
//! **活跃**子流的空闲超时（保活语义，见 §7），前者是**已关闭**子流的宽限期。
//!
//! ## 5. 缓冲区
//!
//! 子流收发环与协议帧的收发暂存**一律使用 `buffex::ring`**（上游已用 `ring` 取代
//! 早先的 `circular_buff`，见 dev-notes §12）：
//!
//! - 每条子流持有一对 `buffex` 半部（[`BufferedTx`] / [`BufferedRx`]），
//!   因此 [`ChannelTx`] / [`ChannelRx`] 的 `try_*` 直接作用在环形缓冲上；
//! - 中心循环持有一条「帧暂存环」，把网络字节流与帧解析 / 成帧解耦。
//!
//! 环的物理内存由**调用方注入**的分配器 `A` 分配（缺省 `buffex::CoreAlloc`），
//! 容量策略同样由调用方注入（见 [`crate::flow_ctrl::TrFlowCtrlPolicy`] 与
//! [`MuxConnection::new`] 的参数）。
//!
//! **分配纪律**（准确表述，勿过度解读为「不分配堆内存」）：本 crate 会做堆分配，
//! 但每一处都必须走**调用方注入的分配器**（`allocator_api`），不得隐式落到全局
//! 分配器，也不得靠 `Vec` 之类的临时堆结构绕开设计。索引 / 管理结构一律用
//! `BTreeMap` / `BTreeSet` 并在构造时显式注入分配器（见 `mux_connection::registry_`
//! 的三张索引表与 `session_` 的两个循环本地表）；连接内其余的堆分配点计划集中到
//! `TrMaxAllocConfig` 统一描述（下一轮落地）。
//!
//! ## 6. 线程模型与本地作用域
//!
//! 连接**不暴露运行时类型参数**：两个 `local spawn` 所需的「本地队列」被 `abs_art`
//! **值化**为 [`TrLocalScope`](abs_art::TrLocalScope) 的实现值（各后端的
//! `LocalScope`），由调用方在 [`MuxConnection::new`] 时注入了核心（同时保活）。
//! 于是：
//!
//! - **「运行时支持」与「此刻真有一条被驱动的队列」两件事都被表达出来**——后者
//!   曾是纯类型参数表达不了的环境前提（tokio 的 `LocalSet` 必须在上下文内才
//!   能 spawn）。拿不到作用域值就没法 `spawn_local`，编译期即拦住；
//! - `new` 里 `spawn_local` 后**立即 `detach()`**：实现契约保证「作用域活着且被
//!   驱动 ⇒ 任务继续推进」，不再依赖各后端对「drop 句柄」的语义（三后端不一致，
//!   见 §2.4）；
//! - **调用方负责驱动队列**：tokio 用 `scope.run_until(..)` 包住整段使用期，
//!   compio 由运行时自己驱动，smol 由 `LocalExecutor` 驱动。忘记驱动不会有编译
//!   错误，症状是两个循环从不推进（连接静默无响应）。
//!
//! 注意收发半边类型（`C::ConnTx` / `C::ConnRx`）由 [`TrConnCfg`] 声明，而不是作为
//! [`MuxConnection`] 的公开泛型参数；它们在 `new` 里被移进循环 future 并被
//! `'static` 化，此后不再参与公开类型。
//!
//! 连接级错误 [`MuxError`] 是**无载荷、可复制**的标准枚举：底层读写错误在帧层
//! 与循环里统一投影为 [`MuxError::Transport`]（只保留读写方向），因此同一份失败
//! 原因可以存进共享注册表，也可以直接返回给 API 面。
//!
//! 连接与句柄是否 `Send` 取决于**作用域类型**：compio 的作用域是零大小类型，因此
//! `MuxConnection` 与各会话句柄在 compio 下都是 `Send`（`tests/thread_safety.rs`
//! 把这条边界钉成可运行的断言）；tokio 的作用域含 `Rc<LocalSet>`，同一套公开类型
//! 在那套装配下仍是 `!Send`。由此得到两条实现纪律：
//!
//! - 共享状态的争用一律**零 CPU 忙等**：协作式锁只走 `try_*` 快路径，失败则 park
//!   等待许可释放；`Drop` 既不取锁也不阻塞，只投一条释放消息；
//! - 循环 future **不要求 `Send`**，因此 `!Send` 的传输（例如 compio 的 socket
//!   半边）可以直接当 `Rx` / `Tx`——跨线程的只是句柄，reactor 与循环不迁移。
//!
//! 另注：`buffex` 的端类型自身要求 `Send + Sync` 的参数，因此环存储与分配器仍然
//! 必须是 `Send + Sync`——这条与连接对象本身是否 `Send` 无关。
//!
//! ## 7. 生命周期与超时
//!
//! 子流支持**半关闭**：每个方向的结束是独立的，`is_tx_closed` / `is_rx_closed`
//! 分别反映；对端关闭一半只影响对应方向。这两个标志**不额外维护**：它们直接读
//! `buffex` 环两端各自的关闭位（会话通过关闭 / 丢弃自己那一端来表达 `FIN` /
//! `RESET` / 拆流），映射与理由见私有模块 `channel_half` 的「关闭态」一节。
//! ### 7.1 保活：只用一种 `PULSE` 帧

//! `max_channel_timeout` 的**空闲超时由连接内部用 `abs_art` 的 `TrDelay` 维持**，不要求
//! 调用方代为计时。接近超时时的保活**只用一个帧种类** [`FrameKind::Pulse`]
//! （早期设计里的 `PING` / `PONG` 两种已经取消）：

//! - 某条子流在一段时间内既无数据往来、也无保活往来时，由本端向对端发一条 `PULSE`，
//!   载荷是**本端当前的接收窗口**（既保活又顺便做窗口重同步）；
//! - 收到 `PULSE` 的一方**只**刷新该子流的活跃时间、并按载荷更新对端发送窗口，
//!   **不立即回复**——两个方向各自按自己的空闲计时发 `PULSE`；若收到就立刻回，
//!   两条 `PULSE` 会互相触发成死循环。
//! - 因此没有「请求 / 应答」之分，双方的处理栈是同一份——这正是取消 `PING` / `PONG`
//!   的原因。
//! - 若在 `max_channel_timeout` 内既无数据、也无任何方向的 `PULSE` 往来，该子流判为
//!   空闲超时，报 [`MuxError::IdleTimeout`]。
//!
//! ## 8. 错误
//!
//! 控制面统一用 [`MuxError`]；其中三种「断开」是**不同**的错误，不要混用：
//! 对端主动关闭是 [`MuxError::PeerClosed`]，传输层出错导致的中断是
//! [`MuxError::Transport`]（带方向），空闲超时是 [`MuxError::IdleTimeout`]。
//! 数据面（子流半边的 `TrBuffTryRead` /
//! `TrBuffTryWrite`）直接使用 `buffex` 端半部的错误
//! （`ConsumerError` / `ProducerError`），它们已携带 `ReadErrTag` /
//! `WriteErrTag`，足以区分「取消」「对端关闭」与「暂时无数据」。
//!
//! ## 9. 模块划分
//!
//! 对外类型按它们实现的 `abs_smux` trait 分文件（一个 trait / 一族类型一个子模块），
//! 便于按 trait 定位实现；基础设施（线格式、注册表、事件、循环）另立模块。
//!
//! | 模块 | 内容 | 对应 trait |
//! | --- | --- | --- |
//! | `mux_connection` | [`MuxConnection`]（智能指针薄封装）+ 建连 + `bind_async` | `TrConnection` |
//! | `mux_connection::core_` | `MuxCore` 演员核心：共享状态 + 资源句柄 + 收尾 | —（内部） |
//! | `dock_binding` | [`DockBinding`] + 监听 / 建流 | `TrDockBinding` |
//! | `channel_listener` | [`ChannelListener`] + `income_async` | `TrChannelListener` |
//! | `channel_handle` | [`ChannelHandle`] + accept / reject | `TrChannelHandle` |
//! | `channel_half` | [`ChannelTx`] / [`ChannelRx`]（各自即环半部的具名包装） | `TrChannelTx` / `TrChannelRx` / `TrChannelHalf` |
//! | `telegraph` | [`Telegraph`] | `TrTelegraph` |
//! | `config_` | [`TrConnCfg`] | —（本 crate 自有） |
//! | `types_` / `util_` | 公开类型别名 / 控制面小工具 | — |
//! | `test_support_` | 测试专用：无循环连接、空作用域、测试策略（`#[cfg(test)]`） | — |
//! | `session_` | 读 / 写两个内部循环（本地表也是 `BTreeMap`） | —（内部） |
//! | `mux_connection::registry_` | 统一身份表（channel / telegraph / listener / 宽限态）+ 反向索引 | —（内部） |
//! | `sync_` / `owner_` | 通用共享单元 / 每条子流的共享标量状态 | —（内部） |
//! | `signal_` / `frame_` / `ring_` / `error_` | 控制帧队列 / 线格式 / 环别名 / 错误 | —（内部） |
//!
//! 纪律：**任何 struct / enum 成员都不得带 `pub(crate)`**；跨模块访问一律经
//! `pub(crate)` 关联函数。同一个 `impl` 内按「`pub` → `pub(crate)` → 私有」集中排列。
//!
//! 注意 `signal_`：它是「应用面 → 两个循环」的事件通道（承载控制帧、水位与拆流
//! 通知）。把无界通道换成定容通道是**待办的优化**，见 §2.3 与 dev-notes §18。

mod channel_handle;
mod channel_half;
mod channel_listener;
mod config_;
mod dock_binding;
mod error_;
mod frame_;
mod mux_connection;
mod owner_;
pub(crate) mod ring_;
mod session_;
mod signal_;
mod sync_;
mod telegraph;
#[cfg(test)]
mod test_support_;
mod types_;
mod util_;

pub use channel_handle::{ChannelHandle, HandleError};
pub use channel_half::{ChannelRx, ChannelTx};
pub use channel_listener::{ChannelListener, ListenerError};
pub use config_::{BuffAllocError, DefaultConnCfg, TrConnCfg};
pub use dock_binding::{BindingError, DockBinding};
pub use error_::MuxError;
pub use frame_::{FieldId, FrameHeader, FrameKind, flags};
pub use mux_connection::{BindError, MuxConnection};
pub(crate) use mux_connection::ReserveErr_;
pub use ring_::{BufferedChannel, BufferedRx, BufferedTx, MuxChanBuff};
pub use telegraph::{Telegraph, TelegraphError};
pub use types_::Dock;
