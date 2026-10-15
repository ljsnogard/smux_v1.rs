//! `smux_v1` 的 UNIX domain socket（UDS）性能测试项目（库部分）。
//!
//! # 这个 bench 测什么
//!
//! 三个 `smux_v1` 支持的异步后端（**compio / smol / tokio**）各编译一个可执行文件，
//! 每个可执行文件里跑**同一个**场景矩阵：
//!
//! ```text
//! 被测量（mode）             客户端一侧                      服务端一侧
//! ----------                ----------                     ----------
//! raw   = 基准/对照组        N 条**真实** UDS 连接             N 条 accept 出来的连接
//! smux  = 被测复用实现       1 条真实 UDS + N 条复用 channel    1 条 accept + N 条子流
//! ```
//!
//! 两条路径的**并发度序列一致**（`--cases 2,4,8,16`）、**载荷逐字节一致**
//! （同一个确定性载荷生成器与同一个 `[长度][CRC-32]` 头部）、**分块与环容量一致**，
//! 差别只有「传输是 N 条真 socket 还是 1 条 socket 上的 N 条复用子流」。
//! 这正是「用 smux 的复用 channel 代替真实 unix domain stream，完成完全相同的任务」。
//!
//! # 测什么指标
//!
//! 1. **平均带宽**：`总字节 / 墙钟`。墙钟取「最后一个客户端任务开始执行」到
//!    「服务端读完最后一条流的最后一字节」，两端都含在窗口内。
//! 2. **每个发送任务完成时间的方差**：N 个发送任务各自的 `完成 − 开始` 时长，
//!    报出样本方差 / 总体方差 / 标准差 / 极差 / 变异系数。
//! 3. **真并发的证据**：`并发因子 = Σ(任务时长) / 墙钟`。串行执行时它趋近 1.0，
//!    真正并发时趋近并发数 N。三个量的解读见 [`stats`] 与 [`report`]。
//!
//! # 为什么运行时是**编译期**的事
//!
//! 三个后端在 `abs_art-facade` 里是互斥三选一（「谁是默认后端」必须有唯一解），
//! 而本 crate 一次构建只接通一个 `rt-*` feature。因此「三后端对照」是**三个可执行
//! 文件**之间的对照，不是进程内切换——`just bench` 会把三格依次编出来跑一遍。
//!
//! # 模块
//!
//! | 模块 | 职责 | 与运行时有关吗 |
//! | --- | --- | --- |
//! | [`cfg`] | 连接配置（`TrConnCfg` 实现） | 否（运行时类型从 [`Rt`] 取） |
//! | [`cli_`] | 命令行解析与取值校验 | 否 |
//! | [`stats`] | 方差 / 标准差 / 并发因子 | 否 |
//! | [`payload`] | 确定性载荷与 CRC-32 | 否 |
//! | [`proto`] | 12 字节头部与分块 | 否 |
//! | [`barrier`] | N 个任务「同时出发」的异步栅栏 | 否 |
//! | [`hub`] | 一格用例的共享时间轴 | 否 |
//! | [`transport`] | 段级 `read_exact` / `write_all` / `read_frame` | 否 |
//! | [`case_io`] | 用例级动作（等栅栏 / 发一份 / 收一份） | 否 |
//! | [`smux_case`] / [`raw_case`] | 两条路径的用例编排 | 否（socket 由运行时壳给出） |
//! | [`report`] | 控制台 / JSONL / CSV / Markdown 输出 | 否 |
//! | `rt_tokio` / `rt_compio` / `rt_smol` | 建 socket、造适配器、驱动本地队列、跑矩阵 | **是** |

// 本项目**不声明任何 nightly feature**：分配器扩展（`allocator_ext`）之类都由
// `smux_v1` 在它自己的编译单元里声明，本 crate 只用公开 API 与 `abs_buff` 的段接口。
// 声明了却用不到会触发 `unused_features` 警告，因此这里一项都不开。
#![deny(unsafe_op_in_unsafe_fn)]
#![allow(clippy::type_complexity)]

pub mod barrier;
pub mod bench_run;
pub mod case_io;
pub mod cfg;
pub mod cli_;
pub mod hub;
pub mod payload;
pub mod proto;
pub mod raw_case;
pub mod report;
pub mod smux_case;
pub mod stats;
pub mod transport;

#[cfg(feature = "rt-tokio")]
pub mod rt_tokio;

#[cfg(feature = "rt-compio")]
pub mod rt_compio;

#[cfg(feature = "rt-smol")]
pub mod rt_smol;

use smux_v1::connection::ChannelListener;
use smux_v1::x_deps::{
    RuntimeTag,
    abs_art::{TrAsyncRuntime, TrLocalScope},
};

/// 本 bench 用的**运行时值**类型：直接取 `smux_v1` 的默认后端别名。
///
/// `abs_art_facade::DefaultRt_` 经 `smux_v1::x_deps` 转出，它由 `smux_v1` 的
/// `test-*-runtime` feature 三选一（本 crate 的 `rt-*` feature 各自恰好打开一个），
/// 因此这里不写按 feature 分派的别名，也不会踩上「bridge 裸名恒为缺省 compio」那个坑。
pub type Rt = smux_v1::x_deps::abs_art_facade::DefaultRt_;

/// 本 bench 用的**本地作用域**类型（连接内部三个循环的投递点，也由它驱动）。
pub type Scope = <Rt as smux_v1::x_deps::abs_art_facade::ScopeHost>::Scope;

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// 传输侧的两个半边：由**适配器 crate** 直接提供
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

/// 连接侧**写半边**（环 → UDS socket）。
#[cfg(feature = "rt-tokio")]
pub type Tx = buffex_tokio_adapt::BuffWrite<tokio::net::unix::OwnedWriteHalf>;

/// 连接侧**读半边**（UDS socket → 环）。
#[cfg(feature = "rt-tokio")]
pub type Rx = buffex_tokio_adapt::BuffRead<tokio::net::unix::OwnedReadHalf>;

/// 连接侧**写半边**（环 → UDS socket）。
#[cfg(feature = "rt-compio")]
pub type Tx = buffex_compio_adapt::BuffWrite<compio::net::UnixStream>;

/// 连接侧**读半边**（UDS socket → 环）。
#[cfg(feature = "rt-compio")]
pub type Rx = buffex_compio_adapt::BuffRead<compio::net::UnixStream>;

/// 连接侧**写半边**（环 → UDS socket）。
#[cfg(feature = "rt-smol")]
pub type Tx = buffex_smol_adapt::BuffWrite<smol::io::WriteHalf<async_net::unix::UnixStream>>;

/// 连接侧**读半边**（UDS socket → 环）。
#[cfg(feature = "rt-smol")]
pub type Rx = buffex_smol_adapt::BuffRead<smol::io::ReadHalf<async_net::unix::UnixStream>>;

/// 本 bench 的连接配置：运行时值 + 零开销 metrics sink。
pub type Cfg = cfg::BenchCfg<Tx, Rx>;

/// smux 子流的**发送半边**（raw 路径不用它）。
pub type ChanTx = smux_v1::connection::ChannelTx<Cfg>;

/// smux 子流的**接收半边**（raw 路径不用它）。
pub type ChanRx = smux_v1::connection::ChannelRx<Cfg>;

/// 连接级的 metrics sink（零开销；本 bench 不用内置采集器，见 [`cfg`]）。
pub type Sink = smux_v1::metrics::NoMetrics;

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// 编译期守卫：恰好一个运行时后端
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

#[cfg(not(any(feature = "rt-tokio", feature = "rt-compio", feature = "rt-smol")))]
compile_error!(
    "smux_v1_uds_bench：必须启用恰好一个运行时 feature（rt-tokio / rt-compio / rt-smol）；\
     例如 `cargo build --no-default-features --features rt-tokio --bin bench_tokio`"
);

#[cfg(any(
    all(feature = "rt-tokio", feature = "rt-compio"),
    all(feature = "rt-tokio", feature = "rt-smol"),
    all(feature = "rt-compio", feature = "rt-smol"),
))]
compile_error!(
    "smux_v1_uds_bench：三个运行时 feature 互斥，一次构建只能启用一个；\
     换运行时请加 `--no-default-features`"
);

/// 排障用的 trace：只有设置了 `SMUX_UDS_BENCH_TRACE` 才输出到 stderr。
#[macro_export]
macro_rules! trace_ {
    ($($arg:tt)*) => {
        if std::env::var_os("SMUX_UDS_BENCH_TRACE").is_some() {
            eprintln!("[trace] {}", format_args!($($arg)*));
        }
    };
}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// 运行时壳 → 编排层 的接口
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

/// 运行时壳交给编排层的东西：运行时值、本地作用域、socket 工厂、以及**收尾动作**。
///
/// # 为什么 socket 工厂在壳里
///
/// 三个后端的 UDS 类型互不相同（`tokio::net::UnixStream` / `compio::net::UnixStream` /
/// `smol::net::UnixStream`），而「环 → 设备」的适配器构造也各不相同（tokio / smol 要
/// `try_new_local` 投进本地队列、compio 用后台泵）。把这三个方法实现在壳里，用例逻辑
/// （[`raw_case`] / [`smux_case`]）就能对**任意**后端写一份。
///
/// # 收尾为什么也是壳的责任
///
/// smux 用例跑完之后，连接内部还有帧躺在环里（`FIN` 与尾部数据），必须再驱动本地
/// 队列若干轮才能上网；而「谁来驱动」在三个后端上各不相同（tokio / smol 要显式
/// `run_until`，compio 的执行体就在运行时实例里）。因此壳实现 [`CaseEnv::settle`]，
/// 编排层只管在**丢弃连接之后**调它。
pub trait CaseEnv: Sized + Clone + 'static {
    /// raw 路径的监听类型（每个后端一份）。
    type RawListener;

    /// smux 路径的客户端类型：已建好的复用连接 + 已绑定的本地 dock。
    ///
    /// 壳持有它（因此连接的析构时机由壳决定：**在所有子流用完之后**）。
    type SmuxClient;

    /// 取**当前线程**的用例环境。
    ///
    /// # 为什么是这个形状而不是 `fn rt(&self)`
    ///
    /// 多跑器模式下每格用例在**自己的线程**上跑，它要的运行时值与作用域必须是那个
    /// 线程的（`smux_v1` 的连接循环是 `!Send` 的本地任务）。因此「取环境」这件事
    /// 必须由壳在运行时里现场做，编排层只拿到结果。
    fn current_thread() -> Self;

    /// 运行时值（建连接时进配置）。
    fn rt(&self) -> Rt;

    /// 本地作用域（连接内部循环的投递点）。
    fn scope(&self) -> &Scope;

    /// 绑定一个 UDS 监听。
    ///
    /// # 为什么也是关联函数
    ///
    /// 与 [`CaseEnv::raw_accept`] 同一条理由：`&self` 会被 `async fn` 捕获进 future，
    /// 而监听一旦建出来就要被**移进**服务端任务（见 `raw_case::run_raw_server`），
    /// 因此这个 future 最好完全不借任何东西。写成关联函数（只吃 `addr`）之后，
    /// 它捕获的只有 `&str`——调用点在 `await` 完就把它丢掉了，不产生跨任务借用。
    fn raw_listen(addr: &str)
    -> impl core::future::Future<Output = Result<Self::RawListener, String>> + '_;

    /// accept 一条连接并造两条适配器半边（返回读半边；raw 用例只会用到读侧）。
    ///
    /// # 为什么是**关联函数**而不是方法
    ///
    /// 服务端把 N 条连接各自包成一个读任务、用 `join_all` 并发跑，因此这个 future
    /// 必须是 `'static`。若它带 `&self`，即使函数体根本不用 `self`，`async fn` 也会把
    /// `&'a self` 捕获进 future，`'static` 立刻不成立。写成关联函数（`listener` 按值
    /// 进、按值出）之后，future 捕获的只有所有权，`'static` 天然成立。
    fn raw_accept(
        listener: Self::RawListener,
        ring_cap: usize,
    ) -> impl core::future::Future<Output = Result<(Self::RawListener, Rx, Tx), String>> + 'static;

    /// 拨号一条连接并造两条适配器半边（返回写半边；raw 用例只用到写侧）。
    ///
    /// 同样是关联函数：客户端把每条连接的发送包成一个任务，future 必须 `'static`。
    fn raw_connect(
        addr: &'static str,
        ring_cap: usize,
    ) -> impl core::future::Future<Output = Result<(Rx, Tx), String>> + 'static;

    /// smux 客户端：拨号 + 握手（主动侧）+ 建连接。
    fn smux_connect<'a>(
        &'a self,
        addr: &'a str,
        ring_cap: usize,
    ) -> impl core::future::Future<Output = Result<Self::SmuxClient, String>> + 'a;

    /// smux 客户端：在会话上开一条子流（本地 dock 由壳按 `idx` 依次分配）。
    ///
    /// 返回的是**子流两条半边**（`ChannelTx` / `ChannelRx`），不是连接级的适配器半边。
    fn smux_open<'a>(
        &'a self,
        client: &'a mut Self::SmuxClient,
        idx: usize,
        ring_cap: usize,
    ) -> impl core::future::Future<Output = Result<(ChanRx, ChanTx), String>> + 'a;

    /// smux 服务端：等一条入向子流并裁决（accept）。
    fn smux_accept<'a>(
        &'a self,
        listener: &'a mut ChannelListener<Cfg>,
        ring_cap: usize,
    ) -> impl core::future::Future<Output = Result<(ChanRx, ChanTx), String>> + 'a;

    /// smux 服务端：建「1 条连接 + 绑定 dock + 开始监听」。
    fn smux_listen<'a>(
        &'a self,
        addr: &'a str,
        ring_cap: usize,
    ) -> impl core::future::Future<Output = Result<ChannelListener<Cfg>, String>> + 'a;

    /// 收尾：给本地队列若干 tick，把环里剩余的帧搬上网。
    fn settle(&self) -> impl core::future::Future<Output = ()> + '_;

    /// 把一个 future 变成**本线程的任务**，返回它的句柄。
    ///
    /// # 为什么编排层必须经过这里
    ///
    /// compio 的 IO future 在**主任务**里被 poll 时能注册唤醒，但「谁在它就绪后来 poll
    /// 主任务」并不由我们自己的 `join!` 保证——实测症状是主任务里的
    /// `UnixStream::connect` 永不返回，而另一侧的 `accept` 已经就绪。把整格用例投成
    /// 运行时自己的任务就绕开了这件事；tokio / smol 侧用 `spawn_local`，语义一致。
    /// # 为什么收 `&'static self` 且要求 `F: 'static`
    ///
    /// `spawn_local` 本身要求任务 `'static`（它要被放进执行体的队列、寿命与线程相同）。
    /// 由于 [`CaseEnv`] 的实现类型都是 `'static`，`&'static self` 这个约束在调用点
    /// 总能满足——编排层把环境放进 `Box::leak` 得到的静态槽里，再把任务投出去。
    ///
    /// 句柄的 join 结果用 `Result` 表达：任务 panic（或执行体本身出问题）时把它折成
    /// 本格的失败，而不是把整个矩阵带崩。
    fn spawn<F>(
        &'static self,
        future: F,
    ) -> impl core::future::Future<Output = Result<F::Output, Box<dyn std::error::Error + Send + Sync>>>
    where
        F: core::future::Future + 'static,
        F::Output: 'static;
}

/// 在**某个后端上下文**里把一格用例跑到完成，并提供一个「驱动」入口。
///
/// # 为什么需要一个 trait
///
/// 「把 future 跑到完成」在三个后端上写法完全不同（tokio 要 `LocalSet` / `run_until`、
/// smol 要 `block_on` + `run_until`、compio 直接在运行时实例上 `block_on`），而编排层
/// （[`bench_run`]）不该知道任何后端细节。一个 `Copy` 的实现类型就够了——它不持有
/// 状态，所有上下文都现场取自「当前线程」。
///
/// # 为什么用例必须活在一个显式作用域里
///
/// 连接内部的循环（解复用 / 复用 / 计时）是经 `spawn_local` 投到本地队列上的任务；
/// tokio / smol 的写侧适配器还会再投一个「环 → 设备」的自驱动泵。**这些任务只有在本地
/// 队列被持续驱动时才会推进**——若用例被拆成几个互不相连的 `run_until` 段，泵在段与段
/// 之间就没人推：实测症状是握手写出去的帧永远留在环里（对端永久等 INVITE）。
pub trait Drive: Copy + 'static {
    /// 「在某个上下文里跑一个 future」：闭包收运行时值与作用域，返回它自己的 future。
    ///
    /// 闭包签名是 `for<'s>`：运行时值与作用域都由实现现场创建，因此借用的生命周期
    /// 与实现内部的一次调用同寿，调用方（用例体）不必是 `'static`。
    fn with_scope<F, R>(
        body: F,
    ) -> impl core::future::Future<Output = Result<R, Box<dyn std::error::Error + Send + Sync>>>
    where
        F: for<'s> FnOnce(
            &'s Rt,
            &'s Scope,
        )
            -> core::pin::Pin<Box<dyn core::future::Future<Output = R> + 's>>,
        R: 'static;

    /// 把一个 future 跑到完成（**建自己的运行时**，供跑器线程的入口使用）。
    ///
    /// # Errors
    ///
    /// 由具体后端给出（建运行时失败等）。
    fn drive<F>(future: F) -> Result<F::Output, Box<dyn std::error::Error + Send + Sync>>
    where
        F: core::future::Future;

    /// 在当前线程**已有**的运行时上下文里把一个用例跑到完成。
    ///
    /// 与 [`Drive::drive`] 的分工：`drive` 负责「建运行时」，`run_one` 负责「在已建好的
    /// 那个运行时里驱动一格用例」。单跑器模式下两者配合使用（`drive(run_worker(..))`），
    /// 多跑器模式下每格走一次 `run_one`。
    ///
    /// # Errors
    ///
    /// 由具体后端给出（建运行时失败等）。
    fn run_one<F>(future: F) -> impl core::future::Future<Output = F::Output>
    where
        F: core::future::Future + 'static;
}

/// 三个后端共用的 [`Drive`] 实现类型：零状态，一切现场取自当前线程。
#[derive(Debug, Clone, Copy)]
pub struct DriveHere;

/// 核对「命令行声明的运行时」与「编译期选定的后端」是否一致。
///
/// # 为什么要有这一步
///
/// 后端是编译期定死的，`--runtime` 无法在进程内切换它；能骗过人的只有「拿错二进制
/// 去跑」这一种情况（例如把 `bench_compio` 当成 `bench_tokio` 传）。因此这里拿运行时
/// 值自报的身份与命令行比一次，不一致就**立刻**失败。
///
/// # Errors
///
/// 命令行声明的运行时与编译期后端不一致时返回错误。
pub fn assert_runtime(
    rt: &Rt,
    expected: cli_::RuntimeSel,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
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
/// 数据面跑完不等于连接内部循环已经走完收尾路径：那些循环是投递到本地队列上的任务，
/// `run_until(f)` 在 `f` 完成之后就不再把它们往前推了。收尾因此要主动 tick 若干轮。
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
/// 收尾时环里通常只剩几十字节（`FIN` 与尾部数据帧），几次 tick 就搬完了；给足余量
/// 是因为 tick 本身极便宜（不涉及真实等待），而漏搬的代价是服务端读不到 EOF。
pub const K_DRAIN_TICKS: usize = 512usize;

/// 默认的用例并发度序列：**依次**发起 2、4、8、16 个并发客户端。
pub const K_DEFAULT_CASES: [usize; 4] = [2usize, 4usize, 8usize, 16usize];
