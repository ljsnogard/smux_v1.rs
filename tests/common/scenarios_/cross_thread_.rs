//! 跨线程场景：**一条线程建连并驱动全部循环，另一条线程只做句柄操作**。
//!
//! # 它验收什么
//!
//! 「句柄可以走，reactor 不走」这条既有设计承诺（见 `src/connection/mod.rs` §6）在
//! **三个后端**上都要成立：
//!
//! - 建连（含五个循环的 `spawn_local`）留在主线程——循环是 `!Send` 的，且本地队列
//!   绑定该线程；
//! - 另一条线程持有连接的**克隆**，在自己的运行时上下文内调 `bind_async` /
//!   `open_channel_async` / 读写：这些都是对共享注册表的操作，经核心的事件通道与
//!   主线程的循环交互；
//! - 因此该线程必须在**所选后端的运行时上下文内**——这是 `CurrentConnCfg` 契约里的
//!   调用者责任（见 `TrRtCurrent` 与 `CurrentConnCfg` 的类型文档），本场景由壳负责
//!   建立那个上下文。
//!
//! # 为什么用 `CurrentConnCfg`
//!
//! `DefaultConnCfg` 把运行时值存进配置，于是 compio 装配下 `MuxConnection` 是
//! `!Send + !Sync`，跨不过线程。`CurrentConnCfg` 不存储、按需从上下文取，连接因此
//! `Send + Sync`（`tests/current_conn_cfg.rs` 已把这条钉成编译期断言）。
//!
//! # 时序
//!
//! 主线程**先**绑定监听 dock 并建立 listener，**再**起 worker 线程：否则 worker 的
//! `OPEN` 会落在没有 listener 的 dock 上。之后两侧并发推进——worker 在 `join_all`
//! 里并发开 `K_CROSS_THREAD_CHANNELS` 条子流，主线程串行 `income_async` 收齐句柄、
//! 再并发完成收发与半关闭。

use buffex::x_deps::abs_buff::{TrBuffRead, TrBuffWrite};
use abs_smux::{
    chan::TrChannelHalf,
    conn::{TrChannelListener, TrConnection, TrDockBinding},
};
use smux_v1::{
    connection::{BufferedRx, BufferedTx, CurrentConnCfg, Dock, MuxConnection},
    flow_ctrl::DefaultPolicy,
    handshake::{
        agent::{AcceptAllEntries, HandshakeAgent},
        opts::BasicOpts,
    },
};

use crate::common::{
    AcceptAsyncClosureExt,
    K_NET_BUFFER_SIZE,
    expect_eof_,
    make_channel_buff_,
    make_passive_ring_,
    make_payload_,
    make_stage_buffs_,
    read_channel_exact_,
    write_channel_all_,
};

/// 跨线程验收使用的连接配置：**不存储**运行时值（[`CurrentConnCfg`]），因此连接对象
/// 在 compio 装配下也是 `Send + Sync`——这正是「句柄可以走」的前提。
pub type CrossThreadCfg = CurrentConnCfg<BufferedTx, BufferedRx>;

/// 跨线程验收使用的连接类型（两端同型）。
pub type CrossThreadConn = MuxConnection<CrossThreadCfg>;

/// 被动方（主线程）监听的 dock：取冒烟区间内的一个值，与 worker 的临时 dock 区间不重叠。
pub const K_CROSS_THREAD_DOCK: u32 = 7u32;

/// 跨线程开通 / 接受的子流条数。
///
/// 取 256：跨线程的并发压力要够（每一步都是一次跨线程的注册表操作），而每条子流还要
/// 完成双向收发与半关闭——实测三后端都在 1 秒内跑完。
pub const K_CROSS_THREAD_CHANNELS: usize = 256usize;

/// worker 侧每条子流使用的临时 local dock 起点。
///
/// 与监听 dock（[`K_CROSS_THREAD_DOCK`]）不重叠，且同侧 `+ index` 后互不相同——
/// 满足「每条并发子流一个互不相同的 local_dock」这条约定。
pub const K_CROSS_THREAD_FIRST_LOCAL_DOCK: u32 = 0x8000u32;

/// 跨线程场景的看门狗时长。
///
/// 跨线程场景的典型失效模式**不是断言失败而是死锁**（主线程队列没人驱动、worker 少了
/// 上下文、两侧互相等）。正常路径在百毫秒级完成，因此这里取一个很宽松的值：它只是把
/// 「挂死」变成一条可读的 panic，而不是让 CI 挂到超时。
pub const K_CROSS_THREAD_WATCHDOG: core::time::Duration = core::time::Duration::from_secs(20);

/// 给整段场景套上看门狗：`watchdog` 到点仍未完成即 panic（见 [`K_CROSS_THREAD_WATCHDOG`]）。
///
/// 计时用**运行时值**自己的 `delay`（`TrDelay`），因此三后端共用同一份实现——壳只需把
/// `scope.run_until(..)` 的结果交进来。
///
/// # Panics
///
/// 超过 [`K_CROSS_THREAD_WATCHDOG`] 仍未完成时 panic，文案指明「疑似死锁」。
pub async fn with_watchdog_<R, F>(rt: &R, fut: F)
where
    R: abs_art::TrDelay,
    F: core::future::Future<Output = ()>,
{
    use futures::FutureExt;

    let watchdog = async {
        rt.delay(K_CROSS_THREAD_WATCHDOG).await;
        panic!(
            "跨线程场景超过 {:?} 仍未完成：疑似死锁（主线程的本地队列未被驱动，\
             或 worker 线程缺少所选后端的运行时上下文）",
            K_CROSS_THREAD_WATCHDOG
        );
    };
    // 两侧各自装箱：`select!` 要求 `Unpin + FusedFuture`，而 `async` 块两者都不是。
    // 测试场景里这点装箱开销无关紧要。
    futures::select! {
        _ = Box::pin(fut).fuse() => {},
        _ = Box::pin(watchdog).fuse() => {},
    }
}

/// 主线程侧的编排：建连 → 建立 listener → 起 worker 线程 → 收齐并完成全部子流。
///
/// `drive_worker` 由各后端壳给出：它接收连接的克隆，**在自己的运行时上下文内**用
/// `block_on` 跑 [`worker_body_`]（tokio / compio 的上下文是必需的，smol 无先决条件）。
/// 把它做成闭包参数，是为了让本函数与后端类型无关——壳只负责「怎么进上下文」。
///
/// # Panics
///
/// 任一侧失败（bind / listen / open / accept / 收发 / 半关闭校验）即 panic，
/// 文案里带上是哪一侧、哪一条子流。
pub async fn run_cross_thread_scenario_<D>(drive_worker: D)
where
    D: FnOnce(CrossThreadConn) -> Result<(), String> + Send + 'static,
{
    // 两条内存被动环直连两个端点（一条 A→B、一条 B→A）：传输不需要外部泵，
    // 于是本场景的全部驱动都落在「主线程的本地队列」这一处。
    let (a_tx, b_rx) = make_passive_ring_(K_NET_BUFFER_SIZE);
    let (b_tx, a_rx) = make_passive_ring_(K_NET_BUFFER_SIZE);
    let (conn_a, conn_b) = connect_pair_ct_(a_tx, a_rx, b_tx, b_rx).await;

    let dock = Dock::new(K_CROSS_THREAD_DOCK);

    // 主线程**先**把 listener 建立起来：worker 的 `OPEN` 才有落点。
    let mut binding = conn_b
        .bind_async(dock)
        .await
        .expect("主线程绑定监听 dock 应当成功");
    let mut listener = binding
        .listen_async_default()
        .await
        .expect("在本地 dock 上建立 listener 应当成功");

    // 起 worker：它拿连接的克隆，在**自己的**运行时上下文内做句柄操作。
    let (done_tx, done_rx) = futures::channel::oneshot::channel();
    let conn_a_worker = conn_a.clone();
    let worker = std::thread::spawn(move || {
        let out = drive_worker(conn_a_worker);
        // 回传结果；主线程若不在了就丢弃（那时测试已经失败）。
        let _ = done_tx.send(out);
    });

    // 主线程侧：串行收齐全部入向句柄，再并发完成每条的收发与半关闭。
    let main_fut = async {
        let mut tasks = Vec::with_capacity(K_CROSS_THREAD_CHANNELS);
        for index in 0..K_CROSS_THREAD_CHANNELS {
            let mut handle = listener
                .income_async()
                .await
                .expect("应当取到下一条入向建流请求");
            assert_eq!(
                handle.local_dock(),
                dock,
                "被动方的 local_dock 应当是监听 dock（镜像语义）"
            );
            let mut welcome_buf: [u8; 0] = [];
            let mut welcome: &mut [u8] = &mut welcome_buf[..];
            let (tx, mut rx) = handle
                .accept_async_closure(&mut welcome, || {
                    (make_channel_buff_(), make_channel_buff_())
                })
                .await
                .expect("accept 入向子流应当成功");
            tasks.push(async move {
                exchange_result_(tx, &mut rx, K_CROSS_THREAD_DOCK, index)
                    .await
                    .map_err(|e| format!("主线程侧第 {index} 条子流：{e}"))
            });
        }
        futures::future::try_join_all(tasks).await.map(|_| ())
    };

    // 两侧并发推进：主线程一边 await 自己的收发、一边等 worker 的结论。
    // （**不能**先 await main_fut 再 join worker：主线程一旦卡住，worker 的事件就没人
    // 服务，那条路径会退化成死锁。）
    let (main_out, worker_recv) = futures::join!(main_fut, done_rx);

    let worker_out = match worker_recv {
        Result::Ok(out) => out,
        Result::Err(_) => Result::Err("worker 线程在回传结果前消失".to_owned()),
    };
    worker.join().expect("worker 线程不应 panic");

    if let Result::Err(e) = main_out {
        panic!("跨线程场景失败（主线程侧）：{e}");
    }
    if let Result::Err(e) = worker_out {
        panic!("跨线程场景失败（worker 侧）：{e}");
    }
}

/// worker 线程侧的主体：并发开通 [`K_CROSS_THREAD_CHANNELS`] 条子流并完成双向收发。
///
/// 每条子流：`bind_async`（一个独有的临时 dock）→ `open_channel_async`（半建立）→
/// `accept_async_closure`（最终裁决、拿到两块缓冲）→ 收发 → 半关闭 → 等 EOF。
///
/// # Errors
///
/// 任一步失败时返回带步骤与序号的说明（本场景是测试专用，调用方直接 panic 呈现）。
pub async fn worker_body_(conn: CrossThreadConn, n: usize) -> Result<(), String> {
    let remote = Dock::new(K_CROSS_THREAD_DOCK);
    let mut tasks = Vec::with_capacity(n);

    for index in 0..n {
        let conn = conn.clone();
        let local = Dock::new(K_CROSS_THREAD_FIRST_LOCAL_DOCK + index as u32);
        tasks.push(async move {
            let mut binding = conn
                .bind_async(local)
                .await
                .map_err(|e| format!("第 {index} 条子流 bind({local:?}) 失败：{e:?}"))?;
            let mut message: &[u8] = &[];
            let mut handle = binding
                .open_channel_async(remote, &mut message)
                .await
                .map_err(|e| format!("第 {index} 条子流 open 失败：{e:?}"))?;
            let mut welcome_buf: [u8; 0] = [];
            let mut welcome: &mut [u8] = &mut welcome_buf[..];
            let (tx, mut rx) = handle
                .accept_async_closure(&mut welcome, || {
                    (make_channel_buff_(), make_channel_buff_())
                })
                .await
                .map_err(|e| format!("第 {index} 条子流最终裁决失败：{e:?}"))?;
            exchange_result_(tx, &mut rx, K_CROSS_THREAD_DOCK, index)
                .await
                .map_err(|e| format!("worker 侧第 {index} 条子流：{e}"))
        });
    }

    futures::future::try_join_all(tasks).await.map(|_| ())
}

/// 一条子流上的完整交互：写本端载荷 → 读对端载荷并校验 → 半关闭 → 在对端等 EOF。
///
/// 与 `scenarios_::kit_::exchange_and_half_close_` 同一套校验口径（载荷 tag 里编码
/// 发送方 `(dock, index)`，接收方据此复算，**不依赖 open / accept 的配对顺序**），
/// 差别只在这里返回 `Result` 而不是直接 panic——跨线程场景要区分「哪一侧、哪一条」。
async fn exchange_result_<T, R>(tx: T, rx: &mut R, dock: u32, index: usize) -> Result<(), String>
where
    T: TrBuffWrite<u8>,
    R: TrBuffRead<u8>,
{
    let payload = make_payload_(dock, index);
    let mut tx = tx;
    write_channel_all_(&mut tx, &payload)
        .await
        .map_err(|_| "写本端载荷失败".to_owned())?;

    let mut tag = [0u8; 4];
    read_channel_exact_(rx, &mut tag)
        .await
        .map_err(|_| "读对端载荷 tag 失败".to_owned())?;
    let raw = u32::from_be_bytes(tag);
    let expected = make_payload_(raw >> 16, (raw & 0xFFFF) as usize);
    if tag != expected[..4] {
        return Result::Err("对端载荷 tag 不自洽".to_owned());
    }

    let mut rest = vec![0u8; expected.len() - 4];
    read_channel_exact_(rx, &mut rest)
        .await
        .map_err(|_| "读对端载荷正文失败".to_owned())?;
    let got = [tag.as_slice(), rest.as_slice()].concat();
    if got != expected {
        return Result::Err("对端载荷与自算期望不相等".to_owned());
    }

    // 半关闭：丢弃发送半边（= 发 FIN），对端应当在读尽后看到 EOF。
    drop(tx);
    expect_eof_(rx).await;
    Result::Ok(())
}

/// 握手并由交付物建立两个 [`MuxConnection`]——与 `scenarios_::connect_::connect_pair_`
/// 同一套路径，唯一差别是**配置类型固定为 [`CurrentConnCfg`]**（不存储运行时值）。
///
/// 之所以不复用 `connect_pair_`：那条路径要求配置实现测试侧的 `TestConnCfg`（由运行时
/// 值构造），而 `CurrentConnCfg` 的设计前提正是**不接收**运行时值——它的构造入口
/// （`CurrentConnCfg::new`）只需要策略。
async fn connect_pair_ct_(
    tx_a: BufferedTx,
    rx_a: BufferedRx,
    tx_b: BufferedTx,
    rx_b: BufferedRx,
) -> (CrossThreadConn, CrossThreadConn) {
    let invite_opts = BasicOpts::default();
    let listen_opts = BasicOpts::default();
    let invite_fut = HandshakeAgent::new(tx_a, rx_a).invite_async(&invite_opts, AcceptAllEntries);
    let listen_fut = HandshakeAgent::new(tx_b, rx_b).listen_async(&listen_opts, AcceptAllEntries);
    let (invited, accepted) = futures::join!(async { invite_fut.await }, async { listen_fut.await });
    let delivery_a = invited.expect("发起方握手应当成功");
    let delivery_b = accepted.expect("等待方握手应当成功");

    // `CurrentConnCfg::new` 交出 `(delivery, cfg)`：delivery 原样交回，配置里**不含**
    // 运行时值——每次取用时由 `TrRtCurrent` 从当前上下文取（本场景：建连线程的上下文）。
    let (delivery_a, config_a) = CrossThreadCfg::new(delivery_a, DefaultPolicy);
    let (delivery_b, config_b) = CrossThreadCfg::new(delivery_b, DefaultPolicy);

    let (stage_ar, stage_aw) = make_stage_buffs_();
    let (stage_br, stage_bw) = make_stage_buffs_();

    (
        MuxConnection::new(delivery_a, config_a, stage_ar, stage_aw),
        MuxConnection::new(delivery_b, config_b, stage_br, stage_bw),
    )
}
