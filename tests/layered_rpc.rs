//! 分层 RPC 冒烟：像真实 SDK 那样按层次、按阶段拆函数地用一遍公开 API。
//!
//! # ⚠️ 这不是设计确认，也不是行为验证
//!
//! 本文件是一个 **API 形状探针**（API-shape probe），专门用来回答一个工程问题：
//!
//! > 一个 RPC 框架把 smux 当作通信底座时，能不能像写真实客户端 / 服务端 SDK 那样，
//! > 把「L4 建链」「L5 握手」「L6 会话」「L7 业务」拆到**不同的函数**里，
//! > 各自只拿自己那一层需要的对象？
//!
//! 因此：
//!
//! - **它会随公开 API 的演进而变化**。这里出现的每一个分层边界、`impl Trait`
//!   返回值、泛型约束，都是「当前 API 恰好允许写到这一步」的记录，不是对 API
//!   应有形状的主张，更不是对 smux v1 设计的确认。
//! - **它不验证协议行为**。载荷正确性只是顺带的最小断言，用来证明链路真的通了；
//!   真正的行为验证在 `tests/inmem_mux.rs` / `tests/smoke_*.rs`。
//! - **当 API 变了，本文件的正确处置是跟着改**（或删掉重写），而不是把它的形状
//!   当成兼容性契约。
//!
//! # 分层
//!
//! ```text
//! L4 传输    loopback_wire_()          —— 一对已连接的全双工字节流（内存环 socketpair）
//! L5 复用    client_mux_connect_()     —— 客户端：发起握手（INVITE → ACCEPT → CONFIRM）
//!            server_mux_accept_()      —— 服务端：等待握手
//! L6 会话    client_open_channel_()    —— 客户端：绑定临时 dock，向服务 dock 建一条子流
//!            server_serve_()           —— 服务端：绑定监听 dock，循环接受子流
//! L7 业务    client_call_()            —— 客户端：一次请求 / 响应往返
//!            server_handle_()          —— 服务端：处理一条子流上的请求
//! ```
//!
//! 层次之间**只**通过两类东西交接：L5 交出 owned 的 `MuxConnection`，L6 交出 owned
//! 的两个 channel 半部。这是当前 API 允许的最大分层粒度，原因见下节。
//!
//! # 实测到的 API 摩擦（本文件的存在理由）
//!
//! 下面每条都是照着现在的签名读出来、**并用编译器验证过**的，不是猜测。带「✅」的
//! 是实测可行、带「⛔」的是实测不可行或实测有陷阱。
//!
//! > **注意**：这些结论针对的是**写下本文件时**的 API。本轮已正式确定把
//! > `MuxConnection` 重构为对 `MuxCore` 的智能指针的薄封装，句柄由「借用」改为
//! > 「持有」——F2 / F3 / F5 三项在该目标设计下都会消失（逐条对照见
//! > `dev-notes/connection-20261002-0548.md` §17.6）。因此本文件是**探针**而不是
//! > 契约：等新 API 落地后，这里的分层函数应当能直接展开成更细的粒度，届时按新
//! > 形状改写本文件即可。
//!
//! ## F1 ✅ `MuxConnection` 可以跨函数传递
//!
//! `MuxConnection<R, W, C, Rt>` 不带任何生命周期参数（`R` / `W` / `C` 都必须
//! `'static`），因此 [`client_mux_connect_`] / [`server_mux_accept_`] 可以直接把它
//! 当返回值交给上一层。这一层没有摩擦。
//!
//! ## F2 ⛔ `DockBinding` 无法与 `MuxConnection` 同处一个结构体
//!
//! `TrConnection::bind_async(&'f self, …)` 产出 `DockBinding<'f, …>`，它**借用**
//! 连接对象。于是：
//!
//! - `struct RpcSession { conn: MuxConnection<…>, binding: DockBinding<'_, …> }`
//!   是**自引用结构体**，不引入 `unsafe` / `ouroboros` 之类就无法表达；
//! - 想把「绑定会话」拆成一个独立函数再让调用方同时持有连接与 binding，就得把
//!   `&'f MuxConnection` 一路显式传递下去。
//!
//! 本文件的处置：**绑定只活在建流函数内部**，出层的只有两个 owned 半部
//! （见 [`client_open_channel_`]）。
//!
//! ## F3 ⛔ `ChannelListener` 可变借用 `DockBinding`，accept 循环拆不出去
//!
//! `TrDockBinding::listen_async(&mut self)` 产出 `ChannelListener<'s, 'f, …>`，
//! 其中 `'s` 就是那次 `&mut self` 借用。因此：
//!
//! - 不能从「持有 binding」的函数里 `return listener`（借用会悬垂）；
//! - 不能把 binding 与 listener 放进同一个结构体（同样是自引用）。
//!
//! 结果是服务端的 **bind → listen → income → accept 四步必须待在同一个函数体内**
//! （[`server_serve_`]）。理想形状是「建 listener」与「accept 循环」两个函数：
//!
//! ```text
//! // 现状下写不出：listener 借用 binding，而 binding 是本函数的局部变量。
//! async fn server_listen_(conn: &Mux<Rt>, dock: Dock) -> ChannelListener<'_, '_, …> {
//!     let mut binding = conn.bind_async(dock).await?;
//!     binding.listen_async().await          // ⛔ 返回借用局部的 listener
//! }
//! ```
//!
//! ## F4 ⚠️ `ChannelHandle` 不借用 listener，「一次一个待决请求」靠约定而非类型
//!
//! `TrChannelListener::income_async(&mut self) -> Self::IncomeAsync<'_>` 的
//! **关联类型输出是 `ChannelHandle<'s, 'f, …>`**——里面没有那次 `&mut self` 的
//! 生命周期。所以句柄可以比 `&mut listener` 的借用活得更久，同时持有多个未决句柄
//! 在类型上也成立（见文末 [`probe_two_pending_handles_`] 的类型层探针）。
//! 「同一个 dock 同时只有一个待决请求」是**协议约定**，类型系统没有兜住。
//!
//! ## F5 ⛔ 已建立 channel 的具体类型无法命名
//!
//! `TrConnection` / `TrDockBinding` 的 `Tx` / `Rx` 关联类型实例化后是
//! `ChannelTx<TxRing_<C::Buff, C::Alloc>>`，而 `TxRing_` / `RxRing_` **没有对外
//! 导出**（`connection::channel_half` 是私有模块，`connection` 只重导出
//! `ChannelTx` / `ChannelRx` 本体）。因此下游：
//!
//! - 不能写 `fn handle_(tx: ChannelTx<???>)`——里面的 `???` 无从写起；
//! - 不能把半部存进具名字段的结构体（字段类型同样要具名）；
//! - 只能 (a) 用 `impl Trait` 出层、在泛型函数里接住（本文件的做法），或
//!   (b) 走关联类型投影
//!   `<DockBinding<'f, R, W, C, Rt> as TrDockBinding>::Tx`（见 [`ProjectedTx_`]）。
//!
//! 对一个想「把 channel 存进 session 表」的 RPC 框架来说，这是最硬的一处摩擦。
//!
//! ## F6 ⛔ `abs_smux` 的 channel trait 只承诺 **try** 语义
//!
//! `TrChannelTx` 的 supertrait 是 `TrBuffTryWrite<Self::Data> + TrChannelHalf`
//! ——**不含**异步的 `TrBuffWrite`。本 crate 的 `ChannelTx` 确实实现了
//! `TrBuffWrite`，但那条信息不在 `TrChannelTx` 的契约里。后果：
//!
//! - 用 `impl TrChannelTx<…>` 出层的半部，调用方**无法** `.await` 读写，只能
//!   `try_read` / `try_write` 自行处理 `Stuffed` / `Drained`；
//! - 想按异步方式用，就得在签名里自己补 `+ TrBuffWrite<u8>`（本文件的做法），
//!   也就是让业务层同时依赖 `abs_buff`。
//!
//! 对 RPC 框架而言这不是缺陷（自建就绪循环本来就要处理背压），但它意味着
//! **「按 trait 编程」并不能让上层完全不依赖 `abs_buff`**。
//!
//! ## F7 ⚠️ 建流帧的「开场消息 / 欢迎信息 / 拒绝理由」当前无处可取
//!
//! 被动方收到 `OPEN` 时，读循环只取出窗口通告（`reserve_inbound_`），`OPEN` 的
//! 载荷**没有被存下来**；`accept_async` 的 `welcome` 只用于回 `ACCEPT`，且按空
//! 载荷发出。所以 RPC 框架**无法把第一个请求搭在建流帧上**，必须等 channel 建立
//! 后再走一次数据往返（本文件的 [`client_call_`] 正是如此）。
//!
//! ## F8 ⛔ 关闭顺序是个陷阱：先 drop 接收半边会吃掉尚未发出的数据
//!
//! 两个半部的 drop 顺序**有语义**，而且写反了不会有任何编译错误。丢弃
//! `ChannelRx` 时，`session_` 的 `RxClosed` 分支会依次做三件事：
//!
//! 1. `table.remove(&pair)` —— 写循环丢掉发送环读端，**环里尚未发出的数据一起
//!    被丢弃**；
//! 2. 向读循环发 `ReadEvent_::Release` —— 摘掉读表项，于是对端在收到我们 `RESET`
//!    之前发出的帧变成「未知子流」（该 dock 对在注册表里仍是活跃 `Channel`，
//!    **不在** `WaitClose` 宽限态）→ `MalformedFrame` → 整条连接被杀；
//! 3. 回 `CLOSE(RESET)`。
//!
//! 丢弃 `ChannelTx` 则相反：`TxClosed` 分支是「先把环内数据 flush 完，再发 `FIN`」。
//! 因此**正确的收尾顺序是「先丢发送半边 → 等对端 `FIN` → 最后丢接收半边」**
//! （见 [`close_gracefully_`]）。
//!
//! 本文件第一版按函数参数的逆序自然析构（`rx` 先于 `tx`），于是服务端刚写好的响应
//! 被自己吃掉，客户端只读到 `Closing`——这条摩擦是实测撞出来的，不是读代码猜的。
//! 对一个 RPC 框架来说它很要命：handler 正常 `return` 就可能踩中，症状是「响应偶尔
//! 丢失 / 连接偶发断开」，很难归因。
//!
//! # 为什么传输层用内存环而不是 socket
//!
//! 本条测试要看的是 **L5 以上的 API 形状与生命周期**，不是传输层。用
//! `common::make_passive_ring_` 直连两端可以完全去掉「socket → 泵 → 环」这套装置，
//! 让每一层的边界在代码里一眼可见。L4 的 connect / accept 之所以合并成一个函数，
//! 也是因为内存环本来就是一次成对的（真实 socket 才有先后）。
//!
//! # feature 配置
//!
//! 整文件只在缺省（单线程）配置下编译：`multi-thread` 配置下连接驱动仍是
//! `todo!()`（见 dev-notes），这里用 crate 级 `cfg` 跳过而不是让用例失败。

#![cfg(not(feature = "multi-thread"))]

mod common;

use core::mem::MaybeUninit;

use abs_smux::conn::{
    TrChannelHalf, TrChannelHandle, TrChannelListener, TrChannelRx, TrChannelTx, TrConnection,
    TrDock, TrDockBinding,
};
use buffex::x_deps::abs_buff::{TrBuffRead, TrBuffWrite};
use mm_ptr::{Owned, x_deps::abs_mm::CoreAlloc};
use smux_v1::{
    connection::{Dock, DockBinding, MuxConnection},
    handshake::{
        agent::{AcceptAllEntries, HandshakeAgent},
        opts::BasicOpts,
    },
};

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// 场景常量：把「RPC 服务」映射成 smux 的 dock
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

/// 服务端监听的 dock：相当于 RPC 服务的「端口」。
const K_SERVICE_DOCK: u32 = 7u32;

/// 一次场景里并发发起的 RPC 次数（= 服务端要接受的子流条数）。
const K_CALL_COUNT: usize = 4usize;

/// 客户端临时 dock 的基址。
///
/// 每条并发子流必须用**互不相同**的临时 `local_dock`（dock 对即身份，见
/// `crate::connection` 模块文档 §4.1），因此这里按下标错开取。
const K_CLIENT_DOCK_BASE: u32 = 0x1000u32;

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// 类型别名
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

/// 内存环的存储类型（与 `common::make_passive_ring_` 的返回类型一致）。
type WireBuff = Owned<[MaybeUninit<u8>], CoreAlloc>;

/// L4 的读半边。
type WireRx = smux_v1::connection::BufferedRx<WireBuff, CoreAlloc>;

/// L4 的写半边。
type WireTx = smux_v1::connection::BufferedTx<WireBuff, CoreAlloc>;

/// L5 的连接对象。客户端与服务端同型，只是握手角色不同。
type Mux<Rt> = MuxConnection<WireRx, WireTx, common::SmokeMuxConfig, Rt>;

/// **F5 的绕行方案 (b)**：用关联类型投影给「已建立 channel 的发送半边」起个名字。
///
/// 下游不能写 `ChannelTx<TxRing_<…>>`（`TxRing_` 未导出），但可以投射
/// `TrDockBinding::Tx`。有了这个别名，「把半部存进具名字段」至少在**泛型上下文**
/// 里是可能的。
type ProjectedTx_<'f, Rt> =
    <DockBinding<'f, WireRx, WireTx, common::SmokeMuxConfig, Rt> as TrDockBinding>::Tx;

/// 同 [`ProjectedTx_`]，接收半边。
type ProjectedRx_<'f, Rt> =
    <DockBinding<'f, WireRx, WireTx, common::SmokeMuxConfig, Rt> as TrDockBinding>::Rx;

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// L4：传输层
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

/// 客户端侧的「已连接字节流」。
struct ClientWire_ {
    rx_: WireRx,
    tx_: WireTx,
}

/// 服务端侧的「已连接字节流」。
struct ServerWire_ {
    rx_: WireRx,
    tx_: WireTx,
}

/// **L4**：建立一条**已连接**的全双工字节流，并把两端分别交给客户端与服务端。
///
/// 现实中这里是「客户端 `connect()`」与「服务端 `accept()`」两步；本测试的传输是
/// 一次性成对的内存环，因此两步合并。分层重点在 L5 以上，这里刻意保持最薄。
fn loopback_wire_() -> (ClientWire_, ServerWire_) {
    // A→B 一条环：A 拿写端、B 拿读端；B→A 反过来。
    let (a_tx, b_rx) = common::make_passive_ring_(common::K_NET_BUFFER_SIZE);
    let (b_tx, a_rx) = common::make_passive_ring_(common::K_NET_BUFFER_SIZE);
    (
        ClientWire_ { rx_: a_rx, tx_: a_tx },
        ServerWire_ { rx_: b_rx, tx_: b_tx },
    )
}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// L5：复用层（握手 + MuxConnection）
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

/// **L5（客户端）**：发起握手，接管收发，建出连接对象。
///
/// 【F1 ✅】返回的 `MuxConnection` 不带生命周期参数，可以安全地交给上一层。
///
/// # Panics
///
/// 握手失败即 panic（测试专用）。
async fn client_mux_connect_<Rt>(wire: ClientWire_) -> Mux<Rt>
where
    Rt: common::TrSmokeRuntime,
{
    let ClientWire_ { rx_, tx_ } = wire;
    let opts = BasicOpts::default();
    let delivery = HandshakeAgent::new(rx_, tx_)
        .invite_async(&opts, AcceptAllEntries)
        .await
        .expect("客户端握手应当成功");
    MuxConnection::new(delivery, common::SmokeMuxConfig)
}

/// **L5（服务端）**：等待握手，接管收发，建出连接对象。
///
/// 与 [`client_mux_connect_`] 是同一层的两个角色：一个 `invite`、一个 `listen`，
/// 必须并发推进（彼此互为对端）。
///
/// # Panics
///
/// 握手失败即 panic（测试专用）。
async fn server_mux_accept_<Rt>(wire: ServerWire_) -> Mux<Rt>
where
    Rt: common::TrSmokeRuntime,
{
    let ServerWire_ { rx_, tx_ } = wire;
    let opts = BasicOpts::default();
    let delivery = HandshakeAgent::new(rx_, tx_)
        .listen_async(&opts, AcceptAllEntries)
        .await
        .expect("服务端握手应当成功");
    MuxConnection::new(delivery, common::SmokeMuxConfig)
}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// L6 / L7：会话层与业务层
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

/// **L6（客户端）**：在临时 dock 上绑定一次会话，并向服务 dock 建一条子流。
///
/// 【F2 ⛔】`DockBinding` 借用连接，不能与连接同处一个结构体，也不能由调用方长期
/// 持有；因此「绑定」这一步被关在本函数内，函数结束时 binding 随之 drop（= 解绑）。
/// 解绑**不影响**已经建立的子流半部（见 `DockBinding` 的文档），所以上层拿到的是
/// 两个完全 owned 的半部。
///
/// 【F5 ⛔】返回类型只能用 `impl Trait`：具体的 `ChannelTx<…>` 里的环包装类型没有
/// 对外导出，写不出名字。
///
/// 【F6 ⛔】`impl TrChannelTx` 只带出 **try** 语义，因此这里显式补上
/// `+ TrBuffWrite<u8>` / `+ TrBuffRead<u8>`，让上层能按异步方式收发。
///
/// # Panics
///
/// 绑定或建流失败即 panic（测试专用）。
async fn client_open_channel_<Rt>(
    conn: &Mux<Rt>,
    local_dock: Dock,
    service_dock: Dock,
) -> (
    impl TrChannelTx<Data = u8, Dock = Dock> + TrBuffWrite<u8>,
    impl TrChannelRx<Data = u8, Dock = Dock> + TrBuffRead<u8>,
)
where
    Rt: common::TrSmokeRuntime,
{
    let mut binding = conn
        .bind_async(local_dock)
        .await
        .expect("客户端绑定临时 dock 应当成功");
    // 【F7 ⚠️】开场消息当前没有消费者，这里按空载荷建流，业务数据走 channel。
    let mut message: &[u8] = &[];
    let (tx, rx) = binding
        .open_channel_async(service_dock, &mut message)
        .await
        .expect("客户端发起子流应当成功");
    (tx, rx)
}

/// **L6（服务端）**：绑定监听 dock，循环接受 `count` 条子流，逐条交给 L7 处理。
///
/// 【F3 ⛔】bind → listen → income → accept **四步同处一个函数**：`ChannelListener`
/// 可变借用 `DockBinding`（`'s` 就是那次 `&mut self`），既不能 `return` 出去，也不
/// 能与 binding 放进同一个结构体。想拆成「建 listener」+「accept 循环」两个函数，
/// 以现在的签名写不出来。
///
/// 一旦 `accept_async` 返回，两个半部就是 owned 值，可以自由跨函数传递——这是整个
/// 借用链上唯一的「出口」。
///
/// # Panics
///
/// 绑定 / 监听 / 接受任一步失败即 panic（测试专用）。
async fn server_serve_<Rt>(conn: &Mux<Rt>, service_dock: Dock, count: usize)
where
    Rt: common::TrSmokeRuntime,
{
    let mut binding = conn
        .bind_async(service_dock)
        .await
        .expect("服务端绑定监听 dock 应当成功");
    let mut listener = binding
        .listen_async()
        .await
        .expect("服务端建立 listener 应当成功");

    for _ in 0..count {
        let mut handle = listener
            .income_async()
            .await
            .expect("服务端应当取到下一条入向请求");
        let remote = handle.remote_dock();
        // `welcome` 的契约在 `abs_smux` 里是 `TrBuffWrite`（「由库写入应用缓冲」），
        // 与「把欢迎信息发给对端」方向相反，语义存疑；本轮按空载荷发出。
        let mut welcome_buf: [u8; 0] = [];
        let mut welcome: &mut [u8] = &mut welcome_buf[..];
        let (tx, rx) = handle
            .accept_async(&mut welcome)
            .await
            .expect("服务端接受入向子流应当成功");

        // 【层次边界】从这里开始只有两个 owned 半部，业务层不再知道 smux 的存在。
        server_handle_(remote, tx, rx).await;
    }
}

/// **L7（服务端业务）**：处理一条子流上的请求，写回响应，然后有序收尾。
///
/// 本函数**只**依赖 `abs_buff` 的 `TrBuffRead` / `TrBuffWrite`，不依赖任何 smux
/// 类型——这正是「业务层与复用层解耦」应当达到的样子，也是 F5 逼出来的写法：
/// 反正具体类型写不出来，索性只按 trait 编程。
///
/// # Panics
///
/// 读请求失败、或对端 dock 取到保留值即 panic（测试专用）。
async fn server_handle_<Tx, Rx>(remote: Dock, mut tx: Tx, mut rx: Rx)
where
    Tx: TrBuffWrite<u8>,
    Rx: TrBuffRead<u8>,
{
    assert!(
        !remote.is_special(),
        "入向子流的对端 dock 必须是具体值，不应是 reserved dock"
    );
    let request = read_message_(&mut rx, "服务端").await;
    let reply = handle_request_(&request);
    write_message_(&mut tx, &reply, "服务端").await;
    close_gracefully_(tx, &mut rx).await;
}

/// **L7（客户端业务）**：一次请求 / 响应往返，然后有序收尾。
///
/// 同样是纯 `abs_buff` 的泛型函数，与 [`server_handle_`] 对称。
///
/// # Panics
///
/// 写请求或读响应失败即 panic（测试专用）。
async fn client_call_<Tx, Rx>(mut tx: Tx, mut rx: Rx, request: &[u8]) -> Vec<u8>
where
    Tx: TrBuffWrite<u8>,
    Rx: TrBuffRead<u8>,
{
    write_message_(&mut tx, request, "客户端").await;
    let reply = read_message_(&mut rx, "客户端").await;
    close_gracefully_(tx, &mut rx).await;
    reply
}

/// **有序收尾一条子流**：先丢弃发送半边（写循环会把环内数据发完再发 `FIN`），
/// 再在对端 `FIN` 到达前把接收方向读到 EOF。
///
/// 【F8 ⛔】**顺序不能反，而且反了不会有编译错误**——理由与实测经过见模块文档 F8。
/// 一句话：先 drop 接收半边会让写循环丢掉环里尚未发出的数据，并把在途帧变成
/// 「未知子流」而杀掉连接。
///
/// # Panics
///
/// 对端没有正常 `FIN`（读到数据、或读到非 `Closing` 的错误）即 panic。
async fn close_gracefully_<Tx, Rx>(tx: Tx, rx: &mut Rx)
where
    Tx: TrBuffWrite<u8>,
    Rx: TrBuffRead<u8>,
{
    drop(tx);
    common::expect_eof_(rx).await;
}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// L7 的业务语义（与 smux 无关）
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

/// 服务端的业务处理：把请求原样包成响应。
///
/// 真实框架会在这里按方法名分发到具体的服务实现；这里保持最简，只用来证明字节确实
/// 穿过了两侧的复用层。
fn handle_request_(request: &[u8]) -> Vec<u8> {
    let mut out = b"reply:".to_vec();
    out.extend_from_slice(request);
    out
}

/// 生成第 `index` 次 RPC 的请求载荷。
fn make_request_(index: usize) -> Vec<u8> {
    format!("rpc-request-{index}").into_bytes()
}

/// 第 `index` 次 RPC 期望的响应（= 客户端对服务端变换的预期）。
fn expect_reply_(index: usize) -> Vec<u8> {
    handle_request_(&make_request_(index))
}

/// 带 4 字节大端长度前缀的消息写出。
///
/// 长度前缀是业务层的约定，smux 只提供字节流；这里显式做一遍是为了让「RPC 框架
/// 需要自己成帧」这件事在代码里可见。
///
/// # Panics
///
/// 写失败即 panic（测试专用）。
async fn write_message_<W>(tx: &mut W, body: &[u8], who: &str)
where
    W: TrBuffWrite<u8>,
{
    let mut framed = Vec::with_capacity(4usize + body.len());
    framed.extend_from_slice(&(body.len() as u32).to_be_bytes());
    framed.extend_from_slice(body);
    common::write_channel_all_(tx, &framed)
        .await
        .unwrap_or_else(|_| panic!("{who}：写出消息应当成功"));
}

/// 带 4 字节大端长度前缀的消息读取。
///
/// `who` 只用于定位失败方（两侧共用同一份实现）。
///
/// # Panics
///
/// 读失败即 panic（测试专用）。
async fn read_message_<R>(rx: &mut R, who: &str) -> Vec<u8>
where
    R: TrBuffRead<u8>,
{
    let mut head = [0u8; 4usize];
    common::read_channel_exact_(rx, &mut head)
        .await
        .unwrap_or_else(|err| panic!("{who}：读取消息长度应当成功，实际 {err:?}"));
    let len = u32::from_be_bytes(head) as usize;
    let mut body = vec![0u8; len];
    common::read_channel_exact_(rx, &mut body)
        .await
        .unwrap_or_else(|err| panic!("{who}：读取消息正文应当成功，实际 {err:?}"));
    body
}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// 场景装配
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

/// 把四层按 SDK 的方式串起来：L4 建链 → L5 双方握手 → L6 建流 → L7 收发。
///
/// 【F3 ⛔】服务端与客户端各自只能由一个函数「一口气」走完自己的借用链，因此这里
/// 用 `join!` 让两侧并发推进，而不是把中间对象取出来分别驱动。
///
/// # Panics
///
/// 任一层失败、或响应字节与预期不符即 panic。
async fn layered_rpc_scenario_<Rt>()
where
    Rt: common::TrSmokeRuntime,
{
    // L4：一条已连接的字节流，两端分头进入各自的协议栈。
    let (client_wire, server_wire) = loopback_wire_();

    // L5：客户端 `invite` 与服务端 `listen` 必须并发跑（互为对端）。
    let (client_conn, server_conn) = futures::join!(
        client_mux_connect_::<Rt>(client_wire),
        server_mux_accept_::<Rt>(server_wire),
    );

    // L6 + L7：服务端跑 accept 循环；客户端并发发起 K_CALL_COUNT 次调用。
    let serving = server_serve_::<Rt>(&server_conn, Dock::new(K_SERVICE_DOCK), K_CALL_COUNT);

    let calling = async {
        // 每个 future 只**借用**连接（`&Mux<Rt>` 是 `Copy`），因此并发发起多次
        // 调用不需要连接可被克隆或移动。
        let conn = &client_conn;
        let calls = (0..K_CALL_COUNT).map(move |index| async move {
            // 每次调用一个互不相同的临时 local_dock（§4.1 的硬约束）。
            let local = Dock::new(K_CLIENT_DOCK_BASE + index as u32);
            let (tx, rx) =
                client_open_channel_::<Rt>(conn, local, Dock::new(K_SERVICE_DOCK)).await;
            let reply = client_call_(tx, rx, &make_request_(index)).await;
            (index, reply)
        });
        let replies = futures::future::join_all(calls).await;
        for (index, reply) in replies {
            assert_eq!(
                reply,
                expect_reply_(index),
                "第 {index} 次 RPC 的响应应当与业务层预期一致"
            );
        }
    };

    futures::join!(serving, calling);
}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// 类型层探针：刻意不执行，只为把上面的摩擦结论钉在编译期
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

/// 【F4 探针】同时持有两个未决句柄在**类型上**成立。
///
/// `income_async` 的关联类型输出是 `ChannelHandle<'s, 'f, …>`，不含那次 `&mut self`
/// 的借用，因此第二个 `income_async` 调用不会被第一个句柄挡住。若哪天 API 收紧成
/// 「句柄借用 listener」，本函数会编译失败——这正是探针的价值。
///
/// **刻意不执行**：同时取出两条待决请求在协议上意味着该 dock 的两条请求被并发
/// 处理，与本 crate「同一 dock 串行化」的约定不符，因此只做类型检查。
#[allow(dead_code)]
async fn probe_two_pending_handles_<Rt>(conn: &Mux<Rt>, service_dock: Dock)
where
    Rt: common::TrSmokeRuntime,
{
    let mut binding = conn
        .bind_async(service_dock)
        .await
        .expect("探针：绑定应当成功");
    let mut listener = binding
        .listen_async()
        .await
        .expect("探针：监听应当成功");
    let first = listener
        .income_async()
        .await
        .expect("探针：第一次取入向请求应当成功");
    let second = listener
        .income_async()
        .await
        .expect("探针：第二次取入向请求应当成功");
    let _ = (first, second);
}

/// 【F5 探针】用关联类型投影为 channel 半部命名，并把它作为**具名参数**传递。
///
/// 这是「不导出 `TxRing_`」前提下，下游唯一能写出具体类型的途径。**刻意不执行**。
#[allow(dead_code)]
fn probe_projected_half_types_<'f, Rt>(tx: ProjectedTx_<'f, Rt>, rx: ProjectedRx_<'f, Rt>) -> usize
where
    Rt: 'f,
{
    core::mem::size_of_val(&tx) + core::mem::size_of_val(&rx)
}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// 用例
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

/// tokio 运行时下的类型标记（同时声明两种 spawn 能力，便于与多线程配置共用签名）。
type LayeredRtTokio =
    abs_art_tokio::Runtime<{ abs_art_tokio::SPAWN_SEND | abs_art_tokio::SPAWN_LOCAL }>;

/// compio 运行时下的类型标记。
type LayeredRtCompio =
    abs_art_compio::Runtime<{ abs_art_compio::SPAWN_SEND | abs_art_compio::SPAWN_LOCAL }>;

/// 测试目标：在 **tokio** 上按 L4→L5→L6→L7 分层跑通一次「客户端 4 次并发 RPC」。
///
/// - 手段：用 [`layered_rpc_scenario_`] 串起各层——内存环建链、两端并发握手、客户端
///   为每次调用绑定不同的临时 dock 并建流、服务端在监听 dock 上循环接受、两侧按
///   长度前缀成帧交换一次请求/响应。缺省（单线程）配置下连接内部走 `spawn_local`，
///   因此整个场景必须跑在 `LocalSet` 里。
/// - 判断：4 次 RPC 的响应字节与业务层预期**逐字节相等**；任一步骤的 API 接线失败
///   （绑定、建流、接受、读写、半关闭）都会 panic。本条**不**判定协议行为，只判定
///   「当前 API 允许这样分层地用」。
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn layered_rpc_tokio_() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(layered_rpc_scenario_::<LayeredRtTokio>())
        .await;
}

/// 测试目标：与 tokio 版逐字相同的分层场景，改用 **compio** 运行时。
///
/// - 手段：同一份 [`layered_rpc_scenario_`]，只把运行时标记类型换成
///   `abs_art_compio::Runtime`；compio 的运行时本身是线程本地的，无需 `LocalSet`。
/// - 判断：与 tokio 版相同——4 次 RPC 的响应逐字节相等，且全部分层接线可用。
#[compio::test]
async fn layered_rpc_compio_() {
    layered_rpc_scenario_::<LayeredRtCompio>().await;
}
