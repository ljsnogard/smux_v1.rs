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
//! - **它会随公开 API 的演进而变化**。这里出现的每一个分层边界、具名类型、泛型
//!   约束，都是「当前 API 恰好允许写到这一步」的记录，不是对 API 应有形状的主张，
//!   更不是对 smux v1 设计的确认。
//! - **它不验证协议行为**。载荷正确性只是顺带的最小断言，用来证明链路真的通了；
//!   真正的行为验证在 `tests/inmem_mux.rs` / `tests/smoke_*.rs`。
//! - **当 API 变了，本文件的正确处置是跟着改**（或删掉重写），而不是把它的形状
//!   当成兼容性契约。
//!
//! # 分层
//!
//! ```text
//! L4 传输    loopback_wire_()            —— 一对已连接的全双工字节流（内存环 socketpair）
//! L5 复用    client_mux_connect_()       —— 客户端：发起握手（INVITE → ACCEPT → CONFIRM）
//!            server_mux_accept_()        —— 服务端：等待握手
//! L6 会话    client_open_channel_()      —— 客户端：绑定临时 dock，向服务 dock 建一条子流
//!            server_bind_and_listen_()   —— 服务端：绑定监听 dock 并建 listener（返回对象）
//!            server_serve_()             —— 服务端：accept 循环，逐条交给 L7
//! L7 业务    client_call_()              —— 客户端：一次请求 / 响应往返
//!            server_handle_()            —— 服务端：处理一条子流上的请求
//! ```
//!
//! 层次之间通过三类东西交接：L5 交出 owned 的 `MuxConnection`（可克隆的智能指针），
//! L6 交出 owned 的 listener（或 binding + listener + 连接三者同处一个结构体），
//! 以及 owned 的两个 channel 半部。**这三样都不带生命周期参数**。
//!
//! # 上一轮的摩擦与现在的结论
//!
//! 下面每条都是照着签名读出来、并用编译器验证过的。上一轮（句柄**借用**连接的模型）
//! 实测到 F2 / F3 / F5 三处「写不出来」；本轮改成「句柄各自持有一份连接智能指针」
//! 之后，它们都消失了——本节记录**现在的**结论，历史经过见
//! `dev-notes/connection-20261002-0548.md` §16（旧）与 §17（新设计）。
//!
//! ## F1 ✅ `MuxConnection` 可以跨函数传递，也可以多次克隆
//!
//! `MuxConnection<C, S, RE, WE>` 不带任何生命周期参数：`R` / `W`（收发半边）在
//! `new` 里就被移进循环，只剩 `C`（策略）、`S`（作用域）与两个错误载荷类型。
//! `Clone` 只是多一个强引用，因此连接可以被同时放进多个结构体、传给多个函数。
//!
//! ## F2 ✅ `DockBinding` 与连接可以同处一个结构体（旧模型下不行）
//!
//! 旧模型 `TrConnection::bind_async(&'f self, …)` 产出 `DockBinding<'f, …>`，它**借用**
//! 连接，于是 `struct { conn, binding }` 是自引用结构体。现在句柄自己持有一份
//! `MuxConnection` 克隆，因此 [`RpcServer_`] 这种形状直接成立。
//!
//! ## F3 ✅ 建 listener 与 accept 循环可以拆成两个函数（旧模型下不行）
//!
//! 旧模型 `listen_async(&mut self)` 的返回值借用 `DockBinding`，导致
//! bind → listen → income → accept 四步必须同处一个函数。现在返回值是 **owned 的
//! [`ChannelListener`]**，因此本文件把服务端拆成
//! [`server_bind_and_listen_`]（建 listener 并连同连接、绑定一起返回）与
//! [`server_serve_`]（accept 循环）两个函数。
//!
//! ## F4 ⚠️ `ChannelHandle` 不借用 listener，「一次一个待决请求」仍靠约定
//!
//! `TrChannelListener::income_async(&mut self)` 的关联类型输出不含那次 `&mut self`
//! 的生命周期，因此同时持有多个未决句柄在类型上成立（见文末
//! [`probe_two_pending_handles_`]）。「同一个 dock 同时只有一个待决请求」是**协议
//! 约定**，类型系统没有兜住——这一条与上一轮相同。
//!
//! ## F5 ✅ 已建立 channel 的具体类型可以命名
//!
//! 旧模型里 `Tx` / `Rx` 关联类型实例化后含未导出的环包装类型，下游写不出
//! `ChannelTx<???>`。现在两个半部直接以 `<C, S, RE, WE>` 参数化，因此本文件的
//! [`ChanTx`] / [`ChanRx`] 就是**可直接写出的具名别名**，也可以作为结构体字段类型。
//!
//! ## F6 ⛔ `abs_smux` 的 channel trait 只承诺 **try** 语义
//!
//! `TrChannelTx` 的 supertrait 是 `TrBuffTryWrite<Self::Data> + TrChannelHalf`
//! ——**不含**异步的 `TrBuffWrite`。本 crate 的 `ChannelTx` 确实实现了
//! `TrBuffWrite`，但那条信息不在 `TrChannelTx` 的契约里。后果：
//!
//! - 用 `impl TrChannelTx<…>` 出层的半部，调用方**无法** `.await` 读写，只能
//!   `try_read` / `try_write` 自行处理 `Stuffed` / `Drained`；
//! - 想按异步方式用，就得在签名里自己补 `+ TrBuffWrite<u8>`，也就是让业务层同时
//!   依赖 `abs_buff`。
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
//! 丢失 / 连接偶发断开」，很难归因。**本轮明确不修**（人类裁决），仍按规避写法收尾。
//!
//! # 传输层为什么用内存环而不是 socket
//!
//! 本条测试要看的是 **L5 以上的 API 形状与生命周期**，不是传输层。用
//! `common::make_passive_ring_` 直连两端可以完全去掉「socket → 泵 → 环」这套装置，
//! 让每一层的边界在代码里一眼可见。L4 的 connect / accept 之所以合并成一个函数，
//! 也是因为内存环本来就是一次成对的（真实 socket 才有先后）。
//!
//! # 运行方式
//!
//! 连接的读 / 写循环经 `abs_art` 的**本地作用域值**（`TrLocalScope`）投递，因此
//! 场景函数把作用域作为第一个参数一路传下去，用例自己取得并驱动它。

mod common;

use core::mem::MaybeUninit;

use abs_art::TrLocalScope;
use abs_smux::conn::{
    TrChannelHalf, TrChannelHandle, TrChannelListener, TrConnection, TrDock, TrDockBinding,
};
use buffex::x_deps::abs_buff::{TrBuffRead, TrBuffTryRead, TrBuffTryWrite, TrBuffWrite};
use mm_ptr::{Owned, x_deps::abs_mm::CoreAlloc};
use smux_v1::{
    connection::{ChannelListener, ChannelRx, ChannelTx, Dock, DockBinding, MuxConnection},
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

/// 两个传输半边的错误类型：连接对外的错误类型就是 `MuxError<读错误, 写错误>`。
type WireRxErr = <WireRx as TrBuffTryRead<u8>>::Err;
type WireTxErr = <WireTx as TrBuffTryWrite<u8>>::Err;

/// L5 的连接对象。客户端与服务端同型，只是握手角色不同。
type Mux<S> = common::SmokeConn<WireRx, WireTx, S>;

/// 【F5 ✅】已建立 channel 的发送半边：**可以直接写出的具名类型**。
type ChanTx<S> = ChannelTx<common::SmokeMuxConfig, S, WireRxErr, WireTxErr>;

/// 同 [`ChanTx`]，接收半边。
type ChanRx<S> = ChannelRx<common::SmokeMuxConfig, S, WireRxErr, WireTxErr>;

/// 【F3 ✅】listener 也是具名类型，可以作为返回值与结构体字段。
type Listener<S> = ChannelListener<common::SmokeMuxConfig, S, WireRxErr, WireTxErr>;

/// 【F2 ✅】binding 同上。
type Binding<S> = DockBinding<common::SmokeMuxConfig, S, WireRxErr, WireTxErr>;

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
async fn client_mux_connect_<S>(scope: &S, wire: ClientWire_) -> Mux<S>
where
    S: common::TrSmokeScope + Clone,
{
    let ClientWire_ { rx_, tx_ } = wire;
    let opts = BasicOpts::default();
    let delivery = HandshakeAgent::new(rx_, tx_)
        .invite_async(&opts, AcceptAllEntries)
        .await
        .expect("客户端握手应当成功");
    MuxConnection::new(scope, delivery, common::SmokeMuxConfig)
}

/// **L5（服务端）**：等待握手，接管收发，建出连接对象。
///
/// 与 [`client_mux_connect_`] 是同一层的两个角色：一个 `invite`、一个 `listen`，
/// 必须并发推进（彼此互为对端）。
///
/// # Panics
///
/// 握手失败即 panic（测试专用）。
async fn server_mux_accept_<S>(scope: &S, wire: ServerWire_) -> Mux<S>
where
    S: common::TrSmokeScope + Clone,
{
    let ServerWire_ { rx_, tx_ } = wire;
    let opts = BasicOpts::default();
    let delivery = HandshakeAgent::new(rx_, tx_)
        .listen_async(&opts, AcceptAllEntries)
        .await
        .expect("服务端握手应当成功");
    MuxConnection::new(scope, delivery, common::SmokeMuxConfig)
}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// L6 / L7：会话层与业务层
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

/// **L6（客户端）**：在临时 dock 上绑定一次会话，并向服务 dock 建一条子流。
///
/// 【F2 ✅】这里仍然「用完即弃」binding（解绑不影响已建立的子流半部），但**不再是
/// 被迫如此**：binding 现在可以随连接一起长期持有，见 [`RpcServer_`]。
///
/// 【F5 ✅】返回类型是两个**具名类型**（[`ChanTx`] / [`ChanRx`]），不再是
/// `impl Trait`。
///
/// 【F6 ⛔】`TrChannelTx` 只带出 **try** 语义，因此这里显式补上
/// `+ TrBuffWrite<u8>` / `+ TrBuffRead<u8>`，让上层能按异步方式收发。
///
/// # Panics
///
/// 绑定或建流失败即 panic（测试专用）。
async fn client_open_channel_<S>(
    conn: &Mux<S>,
    local_dock: Dock,
    service_dock: Dock,
) -> (ChanTx<S>, ChanRx<S>)
where
    S: Clone,
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

/// **L6（服务端）**：连接、绑定与 listener **同处一个结构体**。
///
/// 【F2 ✅ / F3 ✅】这是旧模型下写不出来的形状：三者各自持有一份指向同一演员核心的
/// 智能指针，因此不存在自引用，字段也不需要任何生命周期参数。
struct RpcServer_<S> {
    /// 连接：**刻意只持有不使用**——它在这里的作用是证明「连接与它的句柄可以同处
    /// 一个结构体」这条形状成立（旧模型下这是自引用结构体）。保活本身由
    /// `binding_` / `listener_` 各自持有的克隆承担。
    #[allow(dead_code)]
    conn_: Mux<S>,

    /// 本次监听绑定的 dock。
    binding_: Binding<S>,

    /// 在 `binding_` 的 dock 上建立的监听器。
    listener_: Listener<S>,
}

impl<S> RpcServer_<S> {
    /// **L6（服务端）**：建 listener —— 与 accept 循环**分开**的第一步。
    ///
    /// 一次走完「握手 → 绑定 → 监听」，把三者一起交出去；accept 循环由调用方在
    /// 需要的时候、需要的地方跑（[`server_serve_`]）。旧模型下 listener 借用
    /// binding、binding 借用连接，这四步必须挤在同一个函数体里。
    ///
    /// # Panics
    ///
    /// 握手 / 绑定 / 监听任一步失败即 panic（测试专用）。
    async fn bind_and_listen(scope: &S, wire: ServerWire_, dock: Dock) -> Self
    where
        S: common::TrSmokeScope + Clone,
    {
        let conn_ = server_mux_accept_(scope, wire).await;
        let mut binding_ = conn_
            .bind_async(dock)
            .await
            .expect("服务端绑定监听 dock 应当成功");
        let listener_ = binding_
            .listen_async()
            .await
            .expect("服务端建立 listener 应当成功");
        RpcServer_ {
            conn_,
            binding_,
            listener_,
        }
    }

    /// **L6（服务端）**：accept 循环——与建 listener **分开**的第二步。
    ///
    /// 逐条取出入向请求、接受它，再交给 L7 的业务处理函数。
    ///
    /// # Panics
    ///
    /// 取请求或接受任一步失败即 panic（测试专用）。
    async fn serve(&mut self, count: usize) {
        // binding 与 listener 都持有一份连接克隆、指向同一条连接，因此两者的
        // local_dock 必然一致；这里顺手读一次 binding（否则它只是被持有）。
        assert_eq!(
            self.binding_.local_dock(),
            self.listener_.local_dock(),
            "binding 与 listener 必须落在同一个 dock 上"
        );
        for _ in 0..count {
            let mut handle = self
                .listener_
                .income_async()
                .await
                .expect("服务端应当取到下一条入向请求");
            let remote = handle.remote_dock();
            // `welcome` 的契约在 `abs_smux` 里是 `TrBuffWrite`（「由库写入应用
            // 缓冲」），与「把欢迎信息发给对端」方向相反，语义存疑；本轮按空载荷发出。
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
}

/// **L7（服务端业务）**：处理一条子流上的请求，写回响应，然后有序收尾。
///
/// 本函数**只**依赖 `abs_buff` 的 `TrBuffRead` / `TrBuffWrite`，不依赖任何 smux
/// 类型——这正是「业务层与复用层解耦」应当达到的样子。
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
/// 服务端现在先「建 listener」再「跑 accept 循环」（两个函数、两个阶段），客户端
/// 侧并发发起 `K_CALL_COUNT` 次调用。
///
/// # Panics
///
/// 任一层失败、或响应字节与预期不符即 panic。
async fn layered_rpc_scenario_<S>(scope: &S)
where
    S: common::TrSmokeScope + Clone,
{
    // L4：一条已连接的字节流，两端分头进入各自的协议栈。
    let (client_wire, server_wire) = loopback_wire_();

    // L5 + L6：客户端 `invite`、服务端 `listen` 必须并发跑（互为对端）；
    // 服务端在同一个 future 里继续完成绑定与监听，交出一个 owned 的 `RpcServer_`。
    let (client_conn, mut server) = futures::join!(
        client_mux_connect_::<S>(scope, client_wire),
        RpcServer_::<S>::bind_and_listen(scope, server_wire, Dock::new(K_SERVICE_DOCK)),
    );

    // L6 + L7：服务端跑 accept 循环；客户端并发发起 K_CALL_COUNT 次调用。
    let serving = server.serve(K_CALL_COUNT);

    let calling = async {
        // 每个 future 只**借用**连接（连接本身可克隆，这里借用够了），因此并发
        // 发起多次调用不需要连接被移动。
        let conn = &client_conn;
        let calls = (0..K_CALL_COUNT).map(move |index| async move {
            // 每次调用一个互不相同的临时 local_dock（§4.1 的硬约束）。
            let local = Dock::new(K_CLIENT_DOCK_BASE + index as u32);
            let (tx, rx) = client_open_channel_::<S>(conn, local, Dock::new(K_SERVICE_DOCK)).await;
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
/// `income_async` 的关联类型输出不含那次 `&mut self` 的借用，因此第二个
/// `income_async` 调用不会被第一个句柄挡住。若哪天 API 收紧成「句柄借用
/// listener」，本函数会编译失败——这正是探针的价值。
///
/// **刻意不执行**：同时取出两条待决请求在协议上意味着该 dock 的两条请求被并发
/// 处理，与本 crate「同一 dock 串行化」的约定不符，因此只做类型检查。
#[allow(dead_code)]
async fn probe_two_pending_handles_<S>(conn: &Mux<S>, service_dock: Dock)
where
    S: Clone,
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

/// 【F5 ✅ 探针】把两个半部按**具名类型**收下并放进结构体字段。
///
/// 旧模型下这里只能写成关联类型投影（`<DockBinding<…> as TrDockBinding>::Tx`），
/// 因为环包装类型没有导出。现在 [`ChanTx`] / [`ChanRx`] 就是可直接写出的类型。
/// **刻意不执行**，只为把这条结论钉在编译期。
#[allow(dead_code)]
fn probe_named_half_types_<S>(tx: ChanTx<S>, rx: ChanRx<S>) -> HalfHolder_<S> {
    HalfHolder_ { tx_: tx, rx_: rx }
}

/// 【F5 ✅ 探针】两个具名半部作为**结构体字段**。
#[allow(dead_code)]
struct HalfHolder_<S> {
    tx_: ChanTx<S>,
    rx_: ChanRx<S>,
}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// 用例
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

/// 测试目标：在 **tokio** 上按 L4→L5→L6→L7 分层跑通一次「客户端 4 次并发 RPC」。
///
/// - 手段：用 [`layered_rpc_scenario_`] 串起各层——内存环建链、两端并发握手、
///   服务端「建 listener」与「accept 循环」分两个函数、客户端为每次调用绑定不同的
///   临时 dock 并建流、两侧按长度前缀成帧交换一次请求/响应。缺省配置下连接的两个
///   循环经作用域 `spawn_local` 投递，因此整个场景由 `scope.run_until(..)` 驱动。
/// - 判断：4 次 RPC 的响应字节与业务层预期**逐字节相等**；任一步骤的 API 接线失败
///   （绑定、建流、接受、读写、半关闭）都会 panic。本条**不**判定协议行为，只判定
///   「当前 API 允许这样分层地用」。
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn layered_rpc_tokio_() {
    let scope = abs_art_tokio::LocalScope::new();
    let scenario = layered_rpc_scenario_(&scope);
    scope.run_until(scenario).await;
}

/// 测试目标：与 tokio 版逐字相同的分层场景，改用 **compio** 运行时。
///
/// - 手段：同一份 [`layered_rpc_scenario_`]，只把作用域换成
///   `abs_art_compio::LocalScope`（零大小，队列由运行时自己驱动）。
/// - 判断：与 tokio 版相同——4 次 RPC 的响应逐字节相等，且全部分层接线可用。
#[compio::test]
async fn layered_rpc_compio_() {
    let scope = abs_art_compio::LocalScope::new();
    let scenario = layered_rpc_scenario_(&scope);
    scope.run_until(scenario).await;
}
