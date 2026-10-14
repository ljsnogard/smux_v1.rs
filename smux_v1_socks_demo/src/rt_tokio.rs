//! **tokio** 装配：TCP socket + `buffex_tokio_adapt` 的读写适配器 + 两段角色交替。
//!
//! # 为什么可以直接跑默认的 multi-thread
//!
//! 本地队列是**线程本地**的 `LocalSet`（`abs_art-tokio` 的实现），连接内部的五个循环都
//! 投递在它上面。本文件曾用 `current_thread`，理由写的是「多线程 flavor 下任务可能在
//! await 点被搬到别的 worker 线程，那里是另一条（甚至刚建的）本地队列」——**这条担心
//! 不成立**：`#[tokio::main]` 展开成 `Runtime::block_on(main_future)`，主 future 始终在
//! **当前线程**被 poll，不会被搬迁；本 peer 也不使用全局 `spawn`，worker 线程池只承担
//! 运行时自身的调度。于是「取作用域 → 建 socket → 驱动本地队列」全在同一线程，
//! 默认 multi-thread 即可。
//!
//! compio 与 smol 两侧没有对应的 flavor 可切：compio 的执行器就在运行时实例里（线程
//! 本地，由运行时自己 tick），smol 的本地队列由 `smol::block_on` 驱动。两者「默认跑法」
//! 不变，三格形态因此仍然一致。
//!
//! # 这一层只做后端相关的三件事
//!
//! 建 socket、造 `buffex_tokio_adapt` 的读写适配器（写侧走自驱动形态）、用作用域驱动
//! 「场景 + 排空」。协议与校验全在 [`crate::scenario`] 里，与运行时无关。

use std::{
    net::SocketAddr,
    process::ExitCode,
    time::{Duration, Instant},
};

use tokio::net::{TcpListener, TcpStream};

use buffex_tokio_adapt::DefaultAllocConfig;
use smux_v1::x_deps::abs_art::TrLocalScope;

use crate::{
    K_DRAIN_TICKS, Rt, Rx, Scope, Tx, assert_runtime,
    cli::{ParsedArgs, PeerOptions, Role, parse_env_args},
    drain_scope,
    metrics::DemoMetrics,
    report::{self, PeerResult},
    scenario::{PhaseParams, PhaseReport, run_phase},
};

/// bin 的入口：解析参数 → 跑一次实验 → 打印结果 → 给出退出码。
pub async fn run_async() -> ExitCode {
    let opts = match parse_env_args() {
        ParsedArgs::Options(opts) => opts,
        ParsedArgs::Exit(code) => return code,
    };
    report::finish(run_peer(&opts).await)
}

/// 跑完两段（阶段 1 与阶段 2）。
async fn run_peer(opts: &PeerOptions) -> Result<PeerResult, Box<dyn std::error::Error>> {
    let started = Instant::now();
    let rt: Rt = smux_v1::x_deps::abs_art_facade::default_rt_();
    assert_runtime(&rt, opts.runtime)?;
    let scope: Scope = rt.local_scope();

    let listener = TcpListener::bind(opts.listen).await?;
    let actual = listener.local_addr()?;
    // 绑定成功之后立刻告知编排器：对端可以起来了。
    report::print_ready_event(opts.runtime, opts.role, opts.listen, actual, &opts.tag);

    let params = PhaseParams {
        streams: opts.streams,
        bytes: opts.bytes,
        seed: opts.seed,
        chunk: opts.chunk,
        ring_cap: opts.ring_cap,
        max_packet_size: opts.max_packet_size,
        pre_flood_yield: opts.pre_flood_yield,
        base_dock: opts.base_dock,
        service_dock: opts.service_dock,
        mix_telegraph: opts.mix_telegraph,
        tg_frames: opts.tg_frames,
        tg_bytes: opts.tg_bytes,
    };
    let sink = DemoMetrics::global();
    let before = sink.snapshot();

    // 阶段 1：role=active 的一端拨号并当主动端；另一端接受连接当被动端。
    let (stream, initiate) = if opts.role == Role::Active {
        (connect_retry(opts.peer).await?, true)
    } else {
        (listener.accept().await?.0, false)
    };
    let phase1 = run_conn(&rt, &scope, stream, initiate, &params).await?;

    // 阶段 2：角色对调（前后交替扮演主动端与被动端）。
    let (stream, initiate) = if opts.role == Role::Active {
        (listener.accept().await?.0, false)
    } else {
        (connect_retry(opts.peer).await?, true)
    };
    let phase2 = run_conn(&rt, &scope, stream, initiate, &params).await?;

    // 给本地队列若干个 tick，让连接 / 子流的收尾回调有机会发出（见 `drain_scope`）。
    drain_scope(&scope, 256usize).await;
    let metrics = sink.snapshot().delta(&before);
    Ok(report::assemble(
        opts,
        actual,
        vec![phase1, phase2],
        metrics,
        started.elapsed().as_nanos() as u64,
    ))
}

/// 在一条已建立的 TCP 连接上跑完一段。
///
/// 传输两侧的适配器**直接交给 `smux_v1`**：入向 `BuffRead` 由连接的读泵反复
/// `read_async` 驱动；出向 `BuffWrite` 走 [`try_new_local`]（**自驱动**形态）——
/// 它把「环 → 设备」的泵挂在本线程的 `LocalSet` 上，因此连接的写泵只要把帧提交进环
/// 就一定会上网，不需要谁记得调 flush。
///
/// [`try_new_local`]: buffex_tokio_adapt::BuffWrite::try_new_local
///
/// 收尾分两步，都靠本地队列继续 tick：
///
/// 1. 主体完成后先 `drain_scope`：让连接内部循环把残余帧（回执、`FIN`）变成 DATA 帧、
///    经写泵提交进适配器的环；
/// 2. `drop(conn)` 之后**再** `drain_scope`：连接丢弃会让写端适配器「关生产端 +
///    detach 泵」，泵把环里最后一段搬完并 `shutdown` 设备——它仍需要本线程驱动。
async fn run_conn(
    rt: &Rt,
    scope: &Scope,
    stream: TcpStream,
    initiate: bool,
    params: &PhaseParams,
) -> Result<PhaseReport, Box<dyn std::error::Error>> {
    // 本实验发的是大块数据，Nagle 只会让「小回执跟在末尾」多等一轮，故关掉。
    stream.set_nodelay(true)?;

    // 适配器构造（尤其是 `spawn_local`）必须在本地作用域的驱动栈内，因此整段都放进
    // `run_until`。
    scope
        .run_until(async {
            let (rd, wr) = stream.into_split();
            let rx = Rx::try_new(rd, params.ring_cap, DefaultAllocConfig)?;
            let tx = Tx::try_new_local(wr, params.ring_cap, DefaultAllocConfig)?;

            crate::trace_!("准备跑一段：initiate={initiate}");
            let (report, conn) = run_phase(rt, tx, rx, initiate, params).await?;
            drain_scope(scope, K_DRAIN_TICKS).await;
            drop(conn);
            drain_scope(scope, K_DRAIN_TICKS).await;
            Ok(report)
        })
        .await
}

/// 带重试地连接对端：两端启动有先后，先起的一端可能还没 bind 上。
async fn connect_retry(addr: SocketAddr) -> Result<TcpStream, Box<dyn std::error::Error>> {
    let mut last: Option<std::io::Error> = None;
    for _ in 0..100usize {
        match TcpStream::connect(addr).await {
            Ok(stream) => return Ok(stream),
            Err(err) => {
                last = Some(err);
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    }
    Err(Box::new(last.expect("至少尝试过一次连接")))
}
