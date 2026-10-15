//! `smux_v1` 的真实 TCP socket 跨运行时互通 demo（库部分）。
//!
//! # 这个 demo 验什么
//!
//! 两个进程（同一份源码按运行时 feature 编译出的两个可执行文件）在**真实 TCP socket**
//! 上互联，一次运行里**前后交替**扮演主动端与被动端：
//!
//! ```text
//! 阶段 1   P_a ──dial──▶ P_b     P_a = 主动端（握手 invite、开子流、发本体）
//!                                P_b = 被动端（握手 listen、accept 子流、收本体）
//! 阶段 2   P_b ──dial──▶ P_a     P_b = 主动端，P_a = 被动端
//! ```
//!
//! 每条连接上开 `--streams` 条子流，每条子流的**两个方向各走一份不同的确定性载荷**，
//! 线格式见 [`proto`]：发送方**先发 `ENVELOPE`（含长度与 digest）、再发本体**；接收方
//! 收齐之后**回一条 `RECEIPT`（含它实际算出的 digest）**。于是：
//!
//! - 接收方自己就知道「收得对不对」；
//! - **原先的主动方**拿到回执后比对 `digest`，也就证明了「我发的 == 对方收的」。
//!
//! 传输过程用 `smux_v1/metrics` 采集（见 [`metrics`]），结果每个进程打印一行 JSON，
//! 由 `scripts/` 下的编排器汇总。
//!
//! # 为什么「选运行时」是编译期的事
//!
//! 本 demo 的 `rt-tokio` / `rt-compio` / `rt-smol` 是**互斥三选一**：每个恰好打开
//! `smux_v1` 的一个 `test-*-runtime`，而门面的 `DefaultRt_` 正是按它
//! 三选一选出「当前装配的默认后端」（见 `smux_v1` 的 `scope_host_.rs`）。多开一个会让
//! 该别名没有唯一解，因此构建时非缺省的一格必须带 `--no-default-features`。`--runtime`
//! 只用于**自检与编排**：与编译期后端不一致就立刻报错退出（见 [`assert_runtime`]），
//! 不是进程内切换运行时。
//!
//! # 怎么跑
//!
//! ```bash
//! just loopback          # 本机 127.0.0.1 环回，3 组运行时配对
//! just one tokio compio  # 只跑一组
//! just lan <A> <B>       # 两台机器（见 README）
//! ```

#![deny(unsafe_op_in_unsafe_fn)]

mod cfg;
mod cli;
mod metrics;
mod payload;
mod proto;
mod report;
mod scenario;
mod transport;

#[cfg(feature = "rt-tokio")]
pub mod rt_tokio;

#[cfg(feature = "rt-compio")]
pub mod rt_compio;

#[cfg(feature = "rt-smol")]
pub mod rt_smol;

pub use cfg::DemoCfg;
pub use cli::{
    PEER_HELP, ParsedArgs, PeerOptions, Role, RuntimeSel, parse_env_args, parse_peer_args,
};
pub use metrics::{DemoMetrics, MetricsDelta, MetricsSnapshot};
pub use proto::{Envelope, K_HEADER_LEN, K_KIND_DATA, K_KIND_RECEIPT, ProtoError};
pub use report::{PeerResult, print_ready_event};
pub use scenario::{PhaseParams, PhaseReport, run_phase};
pub use transport::{Buff, expect_eof, read_exact, write_all};

use std::net::SocketAddr;

use smux_v1::x_deps::{
    RuntimeTag,
    abs_art::{TrAsyncRuntime, TrLocalScope},
};

/// 本 demo 用的**运行时值**类型：直接取 `smux_v1` 的默认后端别名。
///
/// `abs_art_facade::DefaultRt_`（经 `smux_v1::x_deps` 转出）由 `smux_v1` 的
/// `test-*-runtime` feature
/// **三选一**（本 demo 的 `rt-*` feature 各自恰好打开一个），因此这里不必自己按
/// feature 分派，也不会踩上「bridge 的裸名恒为缺省 compio」那个坑。
pub type Rt = smux_v1::x_deps::abs_art_facade::DefaultRt_;

/// 本 demo 用的**本地作用域**类型（连接内部五个循环的投递点，也由它驱动）。
///
/// 它就是 `Rt` 交出的那一种：经 [`ScopeHost`](smux_v1::x_deps::abs_art_facade::ScopeHost)
/// 的关联类型取出，不另写一份按 feature 分派的别名。
///
/// 作用域宿主 trait 已随 `smux_v1` 的 `connection/scope_host_.rs` 一起收归**后端门面**，
/// 因此下游向门面取——`smux_v1::x_deps` 把门面转出，本 crate 不必直接依赖它。
pub type Scope = <Rt as smux_v1::x_deps::abs_art_facade::ScopeHost>::Scope;

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// 传输侧的两个半边：由**适配器 crate** 直接提供
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

/// 连接侧**写半边**（环 → socket）：`buffex_*_adapt` 的出向适配器。
///
/// 它是「设备 + 环」的组合体：本 demo 只把 slice 交给 smux，字节搬运全在适配器里。
/// 各运行时的 socket 半边类型不同，因此按 feature 各写一份别名。
#[cfg(feature = "rt-tokio")]
pub type Tx = buffex_tokio_adapt::BuffWrite<tokio::net::tcp::OwnedWriteHalf>;

/// 连接侧**读半边**（socket → 环）：`buffex_*_adapt` 的入向适配器。
#[cfg(feature = "rt-tokio")]
pub type Rx = buffex_tokio_adapt::BuffRead<tokio::net::tcp::OwnedReadHalf>;

/// 连接侧**写半边**（环 → socket）。
#[cfg(feature = "rt-compio")]
pub type Tx = buffex_compio_adapt::BuffWrite<compio::net::TcpStream>;

/// 连接侧**读半边**（socket → 环）。
#[cfg(feature = "rt-compio")]
pub type Rx = buffex_compio_adapt::BuffRead<compio::net::TcpStream>;

/// 连接侧**写半边**（环 → socket）。
#[cfg(feature = "rt-smol")]
pub type Tx = buffex_smol_adapt::BuffWrite<smol::io::WriteHalf<smol::net::TcpStream>>;

/// 连接侧**读半边**（socket → 环）。
#[cfg(feature = "rt-smol")]
pub type Rx = buffex_smol_adapt::BuffRead<smol::io::ReadHalf<smol::net::TcpStream>>;

/// 本 demo 的连接配置：进程级 metrics sink + 选定后端的运行时值。
///
/// **刻意不用 `DefaultConnCfg`**：显式持有一份 sink 句柄更直白，也不依赖
/// `M::default()` 的约定；缓冲则一律由调用方当场交出（新版 `smux_v1` 的配置里已经没有
/// 缓冲类型）。取舍与实测见 [`cfg`](crate::DemoCfg) 的模块文档与 README。
pub type Cfg = DemoCfg<Tx, Rx>;

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// 编译期守卫：恰好一个运行时后端
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

#[cfg(not(any(feature = "rt-tokio", feature = "rt-compio", feature = "rt-smol")))]
compile_error!(
    "smux_v1_sock_demo：必须启用恰好一个运行时 feature（rt-tokio / rt-compio / rt-smol）；\
     例如 `cargo build --no-default-features --features rt-tokio --bin peer_tokio`"
);

#[cfg(any(
    all(feature = "rt-tokio", feature = "rt-compio"),
    all(feature = "rt-tokio", feature = "rt-smol"),
    all(feature = "rt-compio", feature = "rt-smol"),
))]
compile_error!(
    "smux_v1_sock_demo：三个运行时 feature 互斥，一次构建只能启用一个；\
     换运行时请加 `--no-default-features`"
);

/// 排障用的 trace：只有设置了 `SMUX_DEMO_TRACE` 才输出到 stderr。
///
/// 关掉时连 `format_args!` 都不会被求值（宏体内联到条件里），因此不担心热路径开销。
#[macro_export]
macro_rules! trace_ {
    ($($arg:tt)*) => {
        if std::env::var_os("SMUX_DEMO_TRACE").is_some() {
            eprintln!("[trace] {}", format_args!($($arg)*));
        }
    };
}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// 公共小工具
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

/// 核对「命令行声明的运行时」与「编译期选定的后端」是否一致。
///
/// # 为什么要有这一步
///
/// 运行时是编译期定死的，`--runtime` 无法在进程内切换它；能骗过人的只有「拿错二进制
/// 去跑」这一种情况（例如把 `peer_compio` 当成 `peer_tokio` 传）。因此这里拿运行时
/// 值自报的身份与命令行比一次，不一致就**立刻**失败，而不是等出诡异现象再回头查。
///
/// # Errors
///
/// 命令行声明的运行时与编译期后端不一致时返回错误。
pub fn assert_runtime(rt: &Rt, expected: RuntimeSel) -> Result<(), Box<dyn std::error::Error>> {
    let actual: RuntimeTag = rt.about();
    if actual == expected.tag() {
        return Ok(());
    }
    Err(format!(
        "--runtime {} 与编译期后端 {actual:?} 不一致：这个可执行文件是按 {actual:?} 编译的，\
         请改用 `{}`，或用 `--no-default-features --features rt-{}` 重新编译",
        expected.name(),
        expected.bin(),
        expected.name()
    )
    .into())
}

/// 给本地队列若干个 tick，让连接 / 子流的收尾回调（`on_channel_closed` 等）有机会发出。
///
/// # 为什么要这个
///
/// 数据面完成（回执到齐）不等于连接内部的循环已经走完收尾路径：那些循环是投递到本地
/// 队列上的任务，`run_until(f)` 在 `f` 完成之后就不再把它们往前推了。而
/// `on_channel_closed` 的 `lifetime_millis` 正是本 demo 想采集的一项。因此场景收尾后
/// 主动给队列几个 tick：自唤醒的 `poll_fn` 每被 poll 一次，`run_until` 就顺带 tick
/// 一次本地队列（tokio 侧见 `LocalSet::run_until` 的语义）。
///
/// 这只是**读数的完整度**问题，不影响正确性判据。
pub async fn drain_scope<S>(scope: &S, rounds: usize)
where
    S: TrLocalScope,
{
    let mut left = rounds;
    scope
        .run_until(futures::future::poll_fn(move |cx| {
            if left == 0usize {
                return core::task::Poll::Ready(());
            }
            left -= 1usize;
            cx.waker().wake_by_ref();
            core::task::Poll::Pending
        }))
        .await;
}

/// 收尾排空时给本地队列的 tick 次数。
///
/// 收尾时环里通常只剩几十字节（回执 + `FIN`），几次 tick 就搬完了；给足余量是因为
/// tick 本身极便宜（不涉及真实等待），而漏搬的代价是整段判据失败。
pub const K_DRAIN_TICKS: usize = 512usize;

/// 带重试地连接对端：两端启动有先后，先起的那一端可能还没 bind 上。
///
/// `attempt` 是一个「连一次」的闭包，`pause` 是两次尝试之间的等待（由各运行时给出，
/// 因为「等一会儿」在不同运行时上是不同的原语）。
///
/// # Errors
///
/// 用尽 `tries` 次尝试仍然失败时，返回最后一次的错误。
pub async fn connect_with_retry<F, Fut, T, P, PF, E>(
    tries: usize,
    mut attempt: F,
    mut pause: P,
) -> Result<T, E>
where
    F: FnMut() -> Fut,
    Fut: core::future::Future<Output = Result<T, E>>,
    P: FnMut() -> PF,
    PF: core::future::Future<Output = ()>,
{
    let mut last: Option<E> = None;
    for _ in 0..tries.max(1) {
        match attempt().await {
            Ok(ok) => return Ok(ok),
            Err(err) => {
                last = Some(err);
                pause().await;
            }
        }
    }
    Err(last.expect("tries >= 1 时至少尝试过一次"))
}

/// 解析监听地址并给出人类可读的绑定结果（三个运行时壳共用）。
pub fn describe_endpoint(addr: SocketAddr, listen: SocketAddr, peer: SocketAddr) -> String {
    format!("listen={listen} actual={addr} peer={peer}")
}
