//! **smol** 装配：与另两个壳同构，差别在「谁来驱动本地队列」。
//!
//! smol **没有隐式运行时上下文**：本地队列是本线程 `thread_local!` 里的
//! `Rc<LocalExecutor<'static>>`，必须由调用方驱动。因此这里没有 `#[smol::main]`
//! 之类的入口，整段实验挂在 `smol::block_on(scope.run_until(..))` 上——`block_on` 是
//! 外部阻塞驱动源，`run_until` 在其中顺带推进本线程的本地队列。
//!
//! 也因此，运行时值与作用域在**进入 `block_on` 之前**取好、再传进去（与
//! `smux_v1/tests/smoke_smol.rs` 的写法一致）。

use std::{
    net::SocketAddr,
    process::ExitCode,
    time::{Duration, Instant},
};

use buffex_smol_adapt::DefaultAllocConfig;
use smol::net::{TcpListener, TcpStream};
use smux_v1::x_deps::abs_art::TrLocalScope;

use crate::{
    K_DRAIN_TICKS, Rt, Rx, Scope, Tx, assert_runtime,
    cli::{ParsedArgs, PeerOptions, Role, parse_env_args},
    drain_scope,
    metrics::DemoMetrics,
    report::{self, PeerResult},
    scenario::{PhaseParams, PhaseReport, run_phase},
};

/// bin 的入口（**同步**：`block_on` 自己就是驱动源）。
pub fn run_blocking() -> ExitCode {
    let opts = match parse_env_args() {
        ParsedArgs::Options(opts) => opts,
        ParsedArgs::Exit(code) => return code,
    };
    let rt: Rt = smux_v1::x_deps::abs_art_facade::default_rt_();
    match assert_runtime(&rt, opts.runtime) {
        Ok(()) => {}
        Err(err) => {
            eprintln!("运行失败：{err}");
            return ExitCode::FAILURE;
        }
    }
    let scope: Scope = rt.local_scope();
    let result = smol::block_on(run_peer(&rt, &scope, &opts));
    report::finish(result)
}

/// 跑完两段（阶段 1 与阶段 2）。
async fn run_peer(
    rt: &Rt,
    scope: &Scope,
    opts: &PeerOptions,
) -> Result<PeerResult, Box<dyn std::error::Error>> {
    let started = Instant::now();
    let listener = TcpListener::bind(opts.listen).await?;
    let actual = listener.local_addr()?;
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

    let (stream, initiate) = if opts.role == Role::Active {
        (connect_retry(opts.peer).await?, true)
    } else {
        (listener.accept().await?.0, false)
    };
    let phase1 = run_conn(rt, scope, stream, initiate, &params).await?;

    let (stream, initiate) = if opts.role == Role::Active {
        (listener.accept().await?.0, false)
    } else {
        (connect_retry(opts.peer).await?, true)
    };
    let phase2 = run_conn(rt, scope, stream, initiate, &params).await?;

    drain_scope(scope, 256usize).await;
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
/// 与 tokio 侧逐字同构：`BuffRead` + **自驱动**的 `BuffWrite`（[`try_new_local`]）直接
/// 交给 `smux_v1`，字节搬运全在适配器里。差别只在 smol 没有环境本地执行体——队列是本
/// 线程的 `thread_local!`，`try_new_local` 把泵投进去之后，仍由本处的
/// `scope.run_until(..)`（外层是 `smol::block_on`）驱动。
///
/// [`try_new_local`]: buffex_smol_adapt::BuffWrite::try_new_local
async fn run_conn(
    rt: &Rt,
    scope: &Scope,
    stream: TcpStream,
    initiate: bool,
    params: &PhaseParams,
) -> Result<PhaseReport, Box<dyn std::error::Error>> {
    stream.set_nodelay(true)?;
    // `async-net` 的 `TcpStream` 没有 `into_split`，用 `smol::io::split` 拆读写半边
    // （内部是一把短临界区的锁，两个半边仍指向同一条全双工连接）。
    let (rd, wr) = smol::io::split(stream);

    scope
        .run_until(async {
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

/// 带重试地连接对端。
async fn connect_retry(addr: SocketAddr) -> Result<TcpStream, Box<dyn std::error::Error>> {
    let mut last: Option<std::io::Error> = None;
    for _ in 0..100usize {
        match TcpStream::connect(addr).await {
            Ok(stream) => return Ok(stream),
            Err(err) => {
                last = Some(err);
                smol::Timer::after(Duration::from_millis(100)).await;
            }
        }
    }
    Err(Box::new(last.expect("至少尝试过一次连接")))
}
