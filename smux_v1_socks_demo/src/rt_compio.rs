//! **compio** 装配：与 [`crate::rt_tokio`] 逐字同构，只有设备类型、适配 crate 与
//! 「等一会儿」的原语不同。
//!
//! compio 的 `run_until` 等价于**直接 await**：它的执行器就在运行时实例里、由运行时
//! 自己 tick（见 `abs_art` 的 `TrLocalScope` 文档表）。因此本文件里不需要额外驱动
//! 本地队列，`#[compio::main]` 已经把一切都驱动起来了。

use std::{
    net::SocketAddr,
    process::ExitCode,
    time::{Duration, Instant},
};

use buffex_compio_adapt::DefaultAllocConfig;
use compio::net::{TcpListener, TcpStream};
use smux_v1::x_deps::abs_art::TrLocalScope;

use crate::{
    K_DRAIN_TICKS, Rt, Rx, Scope, Tx, assert_runtime,
    cli::{ParsedArgs, PeerOptions, Role, parse_env_args},
    drain_scope,
    metrics::DemoMetrics,
    report::{self, PeerResult},
    scenario::{PhaseParams, PhaseReport, run_phase},
};

/// bin 的入口。
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
    let phase1 = run_conn(&rt, &scope, stream, initiate, &params).await?;

    let (stream, initiate) = if opts.role == Role::Active {
        (listener.accept().await?.0, false)
    } else {
        (connect_retry(opts.peer).await?, true)
    };
    let phase2 = run_conn(&rt, &scope, stream, initiate, &params).await?;

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
/// compio 侧不必区分「按需 / 自驱动」：它的 `BuffWrite::try_new` **本来就**用
/// `compio::runtime::spawn` 挂了后台泵（该运行时是线程本地执行体，不要求 `Send`）。
/// 三个后端的形态因此在收尾上一致：连接丢弃时写端适配器「关生产端 + detach 泵」，
/// 残余字节由泵搬完并 `shutdown`。
async fn run_conn(
    rt: &Rt,
    scope: &Scope,
    stream: TcpStream,
    initiate: bool,
    params: &PhaseParams,
) -> Result<PhaseReport, Box<dyn std::error::Error>> {
    stream.set_nodelay(true)?;

    scope
        .run_until(async {
            let (rd, wr) = stream.into_split();
            let rx = Rx::try_new(rd, params.ring_cap, DefaultAllocConfig)?;
            let tx = Tx::try_new(wr, params.ring_cap, DefaultAllocConfig)?;

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
                compio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    }
    Err(Box::new(last.expect("至少尝试过一次连接")))
}
