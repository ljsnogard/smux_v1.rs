//! **单线程**上起一条 smux_v1 连接：具体运行时（compio）× `abs_art-bridge` 的最小闭环。
//!
//! ```bash
//! cargo run --example connect_single_thread
//! ```
//!
//! # 建连要凑齐的五样东西
//!
//! | 东西 | 从哪来 | 为什么 |
//! | --- | --- | --- |
//! | 运行时值 `Rt` | [`abs_art_bridge::current`] | 提供「现在几点」与「怎么等」（`TrTime`），并负责交出本地作用域 |
//! | 本地作用域 `Scope` | `rt.local_scope()` | 五个循环的投递点；`!Send`，与取得它的线程绑定 |
//! | `HandshakeDelivery` | `HandshakeAgent::invite_async` / `listen_async` | 握手产物，**建连接的唯一入口** |
//! | 连接配置 `C` | `DefaultConnCfg::new_with_rt` | 资源策略 + **运行时值**（本示例把它存进配置） |
//! | 两块帧暂存缓冲 | 调用方当场分配 | 连接级帧暂存的环内存（容量由调用方定） |
//!
//! # 为什么这一版用 `DefaultConnCfg`
//!
//! 单线程下「连接的每一处调用都在同一条线程上」是天然成立的，因此把运行时值**存进配置**
//! 最省事：`TrConnCfg::runtime` 每次交出一个克隆，各处共享同一条时间轴，调用点也不必在
//! 上下文里。代价是配置里含运行时值——compio 的它是 `!Send + !Sync`，于是
//! `MuxConnection` 也是 `!Send + !Sync`，句柄过不了线程。
//!
//! 要跨线程用句柄，见隔壁 `connect_multi_thread`（它用不存储运行时值的 `CurrentConnCfg`）。
//!
//! # 后端
//!
//! `abs_art-bridge` 的裸名按 **feature** 解析，本仓缺省 **compio**：因此这里直接写
//! `#[compio::main]` 与 compio 的运行时类型。换后端时，「取运行时值的那几行」要跟着换
//! （tokio 是 `#[tokio::main]`、smol 是 `smol::block_on(scope.run_until(..))`），
//! 而 `MuxConnection` 那一半代码一行都不用动。

use core::mem::MaybeUninit;

use abs_art::TrLocalScope;
use abs_smux::{
    chan::{RingBuffAlloc, TrChannelHandle, TrPrepareRing},
    conn::{TrChannelListener, TrConnection, TrDockBinding},
};
use mm_ptr::{Owned, x_deps::abs_mm::CoreAlloc};
use smux_v1::{
    connection::{
        BufferedRx, BufferedTx, DefaultConnCfg, Dock, MuxChanBuffOwnedBy, MuxConnection,
        new_buffered_channel,
    },
    flow_ctrl::DefaultPolicy,
    handshake::{
        agent::{AcceptAllEntries, HandshakeAgent},
        opts::BasicOpts,
    },
    metrics::NoMetrics,
};

/// 本示例的连接类型：运行时值由配置携带（`DefaultConnCfg` 的第五个参数）。
type Cfg = DefaultConnCfg<BufferedTx, BufferedRx, NoMetrics, DefaultPolicy, abs_art_bridge::Runtime>;
/// 一端的连接对象。
type Conn = MuxConnection<Cfg>;

/// 传输环 / 帧暂存环的容量（只在吞吐上有意义，取多大都不影响正确性）。
const K_RING_CAP: usize = 64usize * 1024usize;
/// 发起侧绑定的 dock（每条并发子流都需要一个**独有**的 local dock）。
const K_DOCK_A: u32 = 0x2001;
/// 被动侧监听的 dock。
const K_DOCK_B: u32 = 1u32;
/// 子流上要传的字节。
const K_PAYLOAD: &[u8] = b"hello, smux_v1!";

/// 缺省后端即 **compio**（bridge 的 `default-backend-compio`），因此本示例不需要任何 feature。
#[compio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // ① 运行时值：必须在运行时上下文内取得（`#[compio::main]` 提供了它）。
    //    它由 `abs_art-bridge` 的裸名解析到**当前编译里那个唯一的后端**。
    let rt = abs_art_bridge::current();
    // ② 本地作用域：五个循环的投递点。同一线程上多次取得拿到的是**同一条**队列。
    let scope = rt.local_scope();

    // ③ 传输：两条内存被动环直连两个端点（A→B、B→A）。示例刻意不引入 socket 与泵，
    //    把注意力留在「起连接」这件事上；真实 socket 的装配见 `active_passive`。
    let (a_tx, b_rx) = passive_ring_(K_RING_CAP);
    let (b_tx, a_rx) = passive_ring_(K_RING_CAP);

    // ④ 握手：两条环各交给一个端点，invite 与 listen 并发完成。
    //    两个握手 future 先各自建出来，再包一层 `async`：它们实现的不是 `Future`
    //    而是「可 await」的东西，`join!` 要的是前者。
    let opts = BasicOpts::default();
    let invite_fut = HandshakeAgent::new(a_tx, a_rx).invite_async(&opts, AcceptAllEntries);
    let listen_fut = HandshakeAgent::new(b_tx, b_rx).listen_async(&opts, AcceptAllEntries);
    let (invited, accepted) =
        futures::join!(async { invite_fut.await }, async { listen_fut.await });
    let (delivery_a, delivery_b) = (invited?, accepted?);

    // ⑤ 配置：把运行时值**存进**配置。`new_with_rt` 交出 `(delivery, cfg)`，
    //    delivery 原样交回——两者总是成对出现。
    let (delivery_a, cfg_a) =
        Cfg::new_with_rt(delivery_a, DefaultPolicy, rt.clone());
    let (delivery_b, cfg_b) =
        Cfg::new_with_rt(delivery_b, DefaultPolicy, rt.clone());

    // ⑥ 建连：两块连接级帧暂存缓冲由**调用方当场给出**（这里各 64 KiB）。
    let conn_a = MuxConnection::new(delivery_a, cfg_a, stage_buff_(), stage_buff_());
    let conn_b = MuxConnection::new(delivery_b, cfg_b, stage_buff_(), stage_buff_());
    println!("已建立两个 MuxConnection（同一线程）");

    // ⑦ 同一条线程上跑完：A 开一条子流写数据并半关闭，B 接收并读回。
    scope
        .run_until(async {
            let (sent, received) = futures::join!(send_(conn_a), recv_(conn_b));
            // **连接必须活到数据真的走完**：`drop(连接) = 拆连接`，五个循环会随之收尾，
            // 因此两个半边把连接**交回**这里保管，而不是在函数返回时丢掉。
            let (conn_a, conn_b) = (sent?, received?);
            drop((conn_a, conn_b));
            Ok::<(), Box<dyn std::error::Error>>(())
        })
        .await?;

    println!(
        "单线程示例完成：A 写 {} 字节 → B 读回逐字节相等",
        K_PAYLOAD.len()
    );
    Ok(())
}

/// **发起侧**：绑一个自己的 dock，向对端监听的 dock 开一条子流，写满载荷后半关闭。
async fn send_(conn: Conn) -> Result<Conn, Box<dyn std::error::Error>> {
    let mut binding = conn.bind_async(Dock::new(K_DOCK_A)).await?;
    let mut invitation: &[u8] = b"Hi, SMUX!";
    let mut handle = binding
        .open_channel_async(Dock::new(K_DOCK_B), &mut invitation)
        .await?;
    // 最终裁决：本端此刻交出这条子流要用的两块环内存。
    let mut welcome: [u8; 0] = [];
    let mut welcome: &mut [u8] = &mut welcome[..];
    let (mut tx, _rx) = handle
        .accept_async(&mut welcome, DemoRing_::new(K_RING_CAP))
        .await?;

    tx.write_all(K_PAYLOAD).await?;
    // 丢弃发送半边 = 发 FIN：对端读尽之后会看到 EOF。
    drop(tx);
    Ok(conn)
}

/// **被动侧**：在本地 dock 上监听，accept 第一条入向子流，读到 EOF 前逐字节比对。
async fn recv_(conn: Conn) -> Result<Conn, Box<dyn std::error::Error>> {
    let mut listener = conn
        .bind_async(Dock::new(K_DOCK_B))
        .await?
        .listen_async_default()
        .await?;

    let mut incoming = listener.income_async().await?;
    let mut welcome: [u8; 0] = [];
    let mut welcome: &mut [u8] = &mut welcome[..];
    let (_tx, mut rx) = incoming
        .accept_async(&mut welcome, DemoRing_::new(K_RING_CAP))
        .await?;

    let mut buf = vec![0u8; K_PAYLOAD.len()];
    rx.read_exact(&mut buf).await?;
    assert_eq!(buf, K_PAYLOAD, "收到的载荷应与发出的逐字节相等");
    Ok(conn)
}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// 后端无关的胶水：环内存的三种「交出」形态
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

/// 环内存的拥有者类型（一块 `u8` 的未初始化内存 + `CoreAlloc`）。
type Buf = Owned<[MaybeUninit<u8>], CoreAlloc>;

/// 造一条**全被动**环，切成 `(写端, 读端)`——两端都是 `smux_v1` 认得的半边。
fn passive_ring_(capacity: usize) -> (BufferedTx, BufferedRx) {
    new_buffered_channel(Buf::new_uninit_slice(capacity, CoreAlloc))
        .expect("容量应当落在 buffex 允许的区间内")
}

/// 造一块**连接级**帧暂存缓冲（`MuxConnection::new` 的两个尾参）。
fn stage_buff_() -> MuxChanBuffOwnedBy<Buf> {
    MuxChanBuffOwnedBy::new(Owned::new_uninit_slice(K_RING_CAP, CoreAlloc))
}

/// 子流环的准备策略：`accept_async` 时按给定容量向 `CoreAlloc` 要两块内存。
///
/// 这正是上游 `TrPrepareRing` 的用法——**类型与容量都由调用方当场决定**，配置不规定它们。
struct DemoRing_ {
    cap_: usize,
}

impl DemoRing_ {
    const fn new(cap: usize) -> Self {
        DemoRing_ { cap_: cap }
    }
}

impl TrPrepareRing<MuxChanBuffOwnedBy<Buf>, u8> for DemoRing_ {
    fn prepare(self) -> RingBuffAlloc<MuxChanBuffOwnedBy<Buf>, u8> {
        RingBuffAlloc::new(
            MuxChanBuffOwnedBy::new(Owned::new_uninit_slice(self.cap_, CoreAlloc)),
            MuxChanBuffOwnedBy::new(Owned::new_uninit_slice(self.cap_, CoreAlloc)),
        )
    }
}
