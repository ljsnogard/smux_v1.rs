//! **多线程**上起一条 smux_v1 连接：句柄走线程、reactor 不走。
//!
//! ```bash
//! cargo run --example connect_multi_thread
//! ```
//!
//! # 与 `connect_single_thread` 的**唯一**差别：配置
//!
//! | | 单线程版 | 本示例 |
//! | --- | --- | --- |
//! | 配置 | `DefaultConnCfg`（把运行时值**存进**配置） | `CurrentConnCfg`（**不存**，每次从当前上下文取） |
//! | 运行时值的 `Send` | 取决于 `Rt`：compio 的它是 `!Send` | 无关——配置里根本没有值 |
//! | `MuxConnection` | compio 装配下 `!Send + !Sync` | **`Send + Sync`** |
//!
//! 于是「谁持有连接」与「谁驱动循环」可以分到不同线程上：
//!
//! ```text
//! 主线程：取运行时值 + 作用域 → 建连（五个循环投在本线程队列）→ 驱动队列 + 收数据
//! worker：拿连接的**克隆** → 自建一个 compio 运行时 → bind / open / 写数据
//! ```
//!
//! # 代价：调用点必须在后端上下文内
//!
//! 「不存」意味着每次取「现在几点」都要求调用线程**处于所选后端的运行时上下文内**
//! ——compio 的运行时是**线程本地**的，所以 worker 线程必须自建一份（本示例的
//! `compio::runtime::Runtime::new()` + `block_on` 就是这件事）。这是 `CurrentConnCfg`
//! 的**调用者责任**：做不到时 debug 构建会先在 `TrRtCurrent` 的断言处给出提示。
//!
//! 反过来，传输半边（socket / 环端）与自驱动泵仍然是 `!Send`，所以 **reactor 不迁移**：
//! 五个循环始终在主线程的队列上跑，worker 只发句柄操作（它们经共享注册表与主线程的
//! 循环交互）。
//!
//! # 后端
//!
//! 与单线程版相同：`abs_art-bridge` 的裸名按 feature 解析，本仓缺省 **compio**。

use core::mem::MaybeUninit;
use std::process::ExitCode;

use abs_art::TrLocalScope;
use abs_smux::{
    chan::{RingBuffAlloc, TrChannelHandle, TrPrepareRing},
    conn::{TrChannelListener, TrConnection, TrDockBinding},
};
use mm_ptr::{Owned, x_deps::abs_mm::CoreAlloc};
use smux_v1::{
    connection::{
        BufferedRx, BufferedTx, CurrentConnCfg, Dock, MuxChanBuffOwnedBy, MuxConnection,
        new_buffered_channel,
    },
    flow_ctrl::DefaultPolicy,
    handshake::{
        agent::{AcceptAllEntries, HandshakeAgent},
        opts::BasicOpts,
    },
};

/// 本示例的连接类型：配置**不携带**运行时值（`CurrentConnCfg` 的后三个参数取默认值）。
type Cfg = CurrentConnCfg<BufferedTx, BufferedRx>;
/// 一端的连接对象——本示例的关键：它是 `Send + Sync` 的。
type Conn = MuxConnection<Cfg>;

/// 传输环 / 帧暂存环的容量。
const K_RING_CAP: usize = 64usize * 1024usize;
/// 发起侧（worker 线程）绑定的 dock。
const K_DOCK_A: u32 = 0x3001;
/// 被动侧（主线程）监听的 dock。
const K_DOCK_B: u32 = 2u32;
/// 子流上要传的字节。
const K_PAYLOAD: &[u8] = b"hello from another thread!";

/// 入口：**主线程**自己建一个 compio 运行时（而不是 `#[compio::main]`）——因为下面还要
/// 用 `std::thread` 起 worker，并在 `block_on` 之后回来 join 它。
fn main() -> ExitCode {
    let rt = match compio::runtime::Runtime::new() {
        Ok(rt) => rt,
        Err(err) => {
            eprintln!("创建主线程的 compio 运行时失败：{err}");
            return ExitCode::FAILURE;
        }
    };
    match rt.block_on(run_()) {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("多线程示例失败：{err}");
            ExitCode::FAILURE
        }
    }
}

/// 主线程侧的全过程：取运行时值 → 建连 → 把句柄交给 worker → 同时收数据。
async fn run_() -> Result<(), String> {
    // ① 运行时值与作用域：`block_on` 已经 enter 了这份 compio 运行时，因此 `current()`
    //    有上下文可用。作用域仍然是**本线程**那条队列的别名。
    let art = abs_art_bridge::current();
    let scope = art.local_scope();

    // ② 两条内存被动环直连两个端点（A→B、B→A）。传输不需要跨线程，也不需要泵。
    let (a_tx, b_rx) = passive_ring_(K_RING_CAP);
    let (b_tx, a_rx) = passive_ring_(K_RING_CAP);

    // ③ 握手：与单线程版逐字相同。
    let opts = BasicOpts::default();
    let invite_fut = HandshakeAgent::new(a_tx, a_rx).invite_async(&opts, AcceptAllEntries);
    let listen_fut = HandshakeAgent::new(b_tx, b_rx).listen_async(&opts, AcceptAllEntries);
    let (invited, accepted) =
        futures::join!(async { invite_fut.await }, async { listen_fut.await });
    let (delivery_a, delivery_b) = (
        invited.map_err(|e| format!("发起方握手失败：{e:?}"))?,
        accepted.map_err(|e| format!("等待方握手失败：{e:?}"))?,
    );

    // ④ 配置：`CurrentConnCfg::new` 交出 `(delivery, cfg)`——注意它**不接收**运行时值。
    let (delivery_a, cfg_a) = Cfg::new(delivery_a, DefaultPolicy);
    let (delivery_b, cfg_b) = Cfg::new(delivery_b, DefaultPolicy);

    // ⑤ 建连：五个循环照旧投在**本线程**的本地队列上（`MuxConnection::new` 自己取作用域）。
    let conn_a = MuxConnection::new(delivery_a, cfg_a, stage_buff_(), stage_buff_());
    let conn_b = MuxConnection::new(delivery_b, cfg_b, stage_buff_(), stage_buff_());

    // 编译期自证：这份配置让连接真的可以过线程——不成立的话本示例根本编译不过。
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<Conn>();

    // ⑥ 主线程**先**把 listener 建起来：worker 的 `OPEN` 才有落点。
    let mut binding_b = conn_b
        .bind_async(Dock::new(K_DOCK_B))
        .await
        .map_err(|e| format!("主线程绑定监听 dock 失败：{e:?}"))?;
    let mut listener = binding_b
        .listen_async_default()
        .await
        .map_err(|e| format!("主线程建立 listener 失败：{e:?}"))?;

    // ⑦ 把连接的**克隆**送到另一条线程。`CurrentConnCfg` 之下这一步才成立。
    let (done_tx, done_rx) = futures::channel::oneshot::channel::<Result<(), String>>();
    let conn_a_worker = conn_a.clone();
    let worker = std::thread::spawn(move || {
        // worker 在**自己的** compio 上下文里跑：`CurrentConnCfg` 的取用前提（见模块文档）。
        let rt = match compio::runtime::Runtime::new() {
            Ok(rt) => rt,
            Err(err) => {
                let _ = done_tx.send(Err(format!("worker 创建 compio 运行时失败：{err}")));
                return;
            }
        };
        let out = rt.block_on(worker_send_(conn_a_worker));
        let _ = done_tx.send(out);
    });

    // ⑧ 主线程一边驱动本地队列（`run_until` 里含五个循环），一边收 worker 开的那条子流。
    let main_out = scope
        .run_until(async {
            let mut incoming = listener
                .income_async()
                .await
                .map_err(|e| format!("主线程取入向子流失败：{e:?}"))?;
            let mut welcome: [u8; 0] = [];
            let mut welcome: &mut [u8] = &mut welcome[..];
            let (_tx, mut rx) = incoming
                .accept_async(&mut welcome, DemoRing_::new(K_RING_CAP))
                .await
                .map_err(|e| format!("主线程裁决子流失败：{e:?}"))?;
            let mut buf = vec![0u8; K_PAYLOAD.len()];
            rx.read_exact(&mut buf)
                .await
                .map_err(|e| format!("主线程读子流失败：{e}"))?;
            if buf != K_PAYLOAD {
                return Err("主线程读到的载荷与 worker 写出的不相等".to_owned());
            }
            Ok::<(), String>(())
        })
        .await;

    // ⑨ 汇总两侧：worker 通过 oneshot 回传（它的结果类型是 `Result<(), String>`，可跨线程）。
    let worker_out = match done_rx.await {
        Ok(out) => out,
        Err(_) => Err("worker 在回传结果前消失".to_owned()),
    };
    worker.join().expect("worker 线程不应 panic");

    main_out?;
    worker_out?;
    println!(
        "多线程示例完成：worker 线程 bind + 开子流并写 {} 字节 → 主线程读回逐字节相等",
        K_PAYLOAD.len()
    );
    Ok(())
}

/// **worker 线程**侧：拿连接的克隆，绑一个自己的 dock，向主线程监听的 dock 开一条子流，
/// 写满载荷后半关闭。
///
/// 返回 `Result<(), String>`（而不是 `Box<dyn Error>`）是刻意的：它要经 oneshot 跨回主线程，
/// 而 trait object 不是 `Send`。
async fn worker_send_(conn: Conn) -> Result<(), String> {
    let mut binding = conn
        .bind_async(Dock::new(K_DOCK_A))
        .await
        .map_err(|e| format!("worker 绑定 dock 失败：{e:?}"))?;
    let mut invitation: &[u8] = b"Hi from worker!";
    let mut handle = binding
        .open_channel_async(Dock::new(K_DOCK_B), &mut invitation)
        .await
        .map_err(|e| format!("worker 发起子流失败：{e:?}"))?;
    let mut welcome: [u8; 0] = [];
    let mut welcome: &mut [u8] = &mut welcome[..];
    let (mut tx, _rx) = handle
        .accept_async(&mut welcome, DemoRing_::new(K_RING_CAP))
        .await
        .map_err(|e| format!("worker 裁决子流失败：{e:?}"))?;

    // 写完之后 `conn` 会在本函数结尾被丢弃——**这里安全**：它只是 `conn_a` 的一个克隆，
    // 主线程还持有强引用；「最后一个强引用消失」才等于拆连接（那是 `run_` 结束的时候）。
    tx.write_all(K_PAYLOAD)
        .await
        .map_err(|e| format!("worker 写子流失败：{e}"))?;
    drop(tx); // 半关闭：主线程读尽后看到 EOF
    Ok(())
}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// 后端无关的胶水：环内存的三种「交出」形态（与单线程示例逐字相同）
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

/// 环内存的拥有者类型（一块 `u8` 的未初始化内存 + `CoreAlloc`）。
type Buf = Owned<[MaybeUninit<u8>], CoreAlloc>;

/// 造一条**全被动**环，切成 `(写端, 读端)`。
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
/// 这个类型会被**跨线程**用到（worker 与主线程各有一处 `accept_async`），因此它必须是
/// `Send`——`usize` 字段天然满足。
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
