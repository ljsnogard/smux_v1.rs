//! 测试侧的连接配置与整个用例矩阵的**容量常量**。
//!
//! 两套配置刻意只差一处：冒烟用 64 KiB 连接级帧暂存；流控验收把子流环**上钳**到
//! [`K_FLOW_CTRL_RING_CAPACITY`]，以逼出「窗口用尽 → 回补」这条平时走不到的路径。
//! 全部数字集中于此，便于审计用例的量纲。

use core::marker::PhantomData;
use buffex::x_deps::abs_buff::{TrBuffRead, TrBuffWrite};
use mm_ptr::x_deps::abs_mm::CoreAlloc;
use abs_smux::conf::TrMuxConfig;
use smux_v1::{
    connection::{Dock, K_STAGE_RING_CAPACITY, MuxConnection, TrConnCfg},
    flow_ctrl::DefaultPolicy,
    metrics::NoMetrics,
};

use crate::common::SmokeBuff;

/// 连接层对**作用域值**的要求：值化的本地队列（`abs_art::TrLocalScope`）。
///
/// 它是一个**空标记 trait**（对所有 `TrLocalScope + Clone + 'static` 的类型 blanket
/// 实现）：Rust 还没有稳定的 trait alias，而场景函数里满是 `S: TrSmokeScope` 这样的
/// 约束，逐处展开只会把同一件事抄很多遍。
///
/// # 计时**不**在这里
///
/// 上一版把这个标记写成 `TrLocalScope + TrTime`，因为当时后端的计时能力也挂在
/// `LocalScope` 上。`abs_art` 改版后计时与时刻都回到**运行时值**上
/// （见 [`TrSmokeRt`]），作用域只回答「`!Send` 任务投到哪、由谁驱动」。
///
/// 场景函数一律把作用域值（`scope: &S`）与运行时值（`rt: RT`）分别转发给
/// [`MuxConnection::new`]；两个测试目标各自给出具体后端的值
/// （tokio / compio 各自的 `Runtime` 与 `LocalScope`）。契约是「谁取得作用域，
/// 谁负责驱动」：tokio 用 `scope.run_until(..)` 包住整段使用期，compio 由运行时
/// 自己驱动。
pub use abs_art::{TrLocalScope, TrTime};
pub use smux_v1::connection::ScopeHost;

/// **测试用默认运行时值类型**：就是生产侧的 `smux_v1::connection::DefaultRt_`
/// ——由 `test-*-runtime` **三选一**（缺省 compio）。测试里的连接类型因此就是生产
/// 默认装配下的那一个。
pub type DefaultRt = smux_v1::connection::DefaultRt_;

pub trait TrSmokeScope: abs_art::TrLocalScope + Clone + 'static {}

impl<S> TrSmokeScope for S where S: abs_art::TrLocalScope + Clone + 'static {}

/// 连接层对**运行时值**的要求：计时与时刻（`abs_art::TrTime` = `TrDelay + TrClock`），
/// 外加「能交出本地作用域」（[`ScopeHost`]）。
///
/// `abs_art` 的两个后端各有自己的一对具体值。
/// 与 [`TrSmokeScope`] 是一对：`TrSmokeRt` 管「怎么等、现在几点」，`TrSmokeScope`
/// 管「本地队列在哪」。
///
/// 它还要求 [`ScopeHost`]：连接建连时要**自己取**本地作用域（作用域不再由调用者
/// 传进来），因此「能当连接时间源」的运行时值必须也能交出作用域。
pub trait TrSmokeRt: abs_art::TrTime + Clone + 'static + ScopeHost {}

impl<R> TrSmokeRt for R where R: abs_art::TrTime + Clone + 'static + ScopeHost {}


/// 一端连接对象的类型。
///
/// 策略固定为 [`SmokeMuxConfig`]，传输类型由策略的类型参数 `R` / `W` 声明，
/// **运行时值也由策略携带**（`RT`）——于是 [`MuxConnection`] 只有一个类型参数
/// （配置本身），这正是本仓「取消除 `C` 外泛型参数」后的形状。
/// 连接对外的错误类型统一为无泛型的 [`MuxError`]。
pub type SmokeConn<R, W, RT> = MuxConnection<SmokeMuxConfig<W, R, RT>>;


/// 每个端点监听的 dock 数量（dock 取值 `1..=16`）。
pub const K_DOCK_COUNT: u32 = 16;


/// 每个 dock 上开通 / 接受的子流数量。
pub const K_CHANNELS_PER_DOCK: usize = 64;


/// 每端开通 / 接受的子流总数（`16 × 64 = 1024`）。
pub const K_TOTAL_CHANNELS: usize = 1024;


/// 单条子流每个方向的环容量（字节）。
pub const K_CHANNEL_CAPACITY: usize = 4096;


/// 网络侧主动设备环的容量（字节）。
pub const K_NET_BUFFER_SIZE: usize = 64usize * 1024usize;


/// 测试用复用资源策略：`mm_ptr::Owned` 存储 + `CoreAlloc` 分配器 +
/// [`DefaultPolicy`] 流控，两条传输半边由类型参数 `W` / `R` 给出。
///
/// `Clone` / `Copy` **手写**而不是 derive：`#[derive(Clone)]` 会给 `W` / `R` 加上
/// `Clone` 约束，而这里只放 `PhantomData<fn() -> _>`，本来不需要——握手里两条半边被
/// 移进 `'static` future，`W` / `R`（环端）并不 `Clone`。
#[derive(Debug)]
pub struct SmokeMuxConfig<W, R, RT> {
    _use_w_: PhantomData<fn() -> W>,
    _use_r_: PhantomData<fn() -> R>,
    /// 运行时值：`TrConnCfg::runtime` 要在这里交出**建连时抓住的**那一个。
    ///
    /// 测试的 `RT` 是可 `Clone` 的句柄风格值（`Runtime` / `ManualTime`），因此按值
    /// 存一份即可：每次 `runtime()` 交出一个克隆，各处仍共享同一条时间轴。
    rt_: RT,
}

impl<W, R, RT: Copy> Copy for SmokeMuxConfig<W, R, RT> {}

impl<W, R, RT: Clone> Clone for SmokeMuxConfig<W, R, RT> {
    fn clone(&self) -> Self {
        SmokeMuxConfig {
            _use_w_: PhantomData,
            _use_r_: PhantomData,
            rt_: self.rt_.clone(),
        }
    }
}

impl<W, R, RT> SmokeMuxConfig<W, R, RT> {
    /// 由**运行时值**构造：类型 `W` / `R` 由调用点的 `HandshakeDelivery` /
    /// `MuxConnection` 推断，`RT` 由传进来的值定死。
    pub const fn new(rt: RT) -> Self {
        SmokeMuxConfig {
            _use_w_: PhantomData,
            _use_r_: PhantomData,
            rt_: rt,
        }
    }
}

/// [`DefaultPolicy`] 是 ZST；取静态引用即可满足 `TrConnCfg::policy`。
static SMOKE_POLICY: DefaultPolicy = DefaultPolicy;

impl<W, R, RT> TrMuxConfig for SmokeMuxConfig<W, R, RT>
where
    W: TrBuffWrite<u8> + 'static,
    R: TrBuffRead<u8> + 'static,
{
    type Data = u8;
    type Dock = Dock;
}

impl<W, R, RT> TrConnCfg for SmokeMuxConfig<W, R, RT>
where
    W: TrBuffWrite<u8> + 'static,
    R: TrBuffRead<u8> + 'static,
    RT: TrSmokeRt,
{
    type Rt = RT;
    type Alloc = CoreAlloc;
    type Policy = DefaultPolicy;
    type ConnTx = W;
    type ConnRx = R;
    type Metrics = NoMetrics;

    fn runtime(&self) -> Self::Rt {
        self.rt_.clone()
    }

    fn allocator(&self) -> Self::Alloc {
        CoreAlloc
    }

    fn policy(&self) -> &Self::Policy {
        &SMOKE_POLICY
    }

    /// 本配置不上报；带 sink 的端到端用例见 `tests/metrics_e2e.rs`。
    fn metrics(&self) -> &Self::Metrics {
        &NoMetrics
    }


}


/// 子流环容量（字节）：流控验收用例的「窗口」就是它。
///
/// 取 512 是刻意的：它比要传的载荷小两个数量级，因此接收窗口**必然**会被用尽，
/// `WINDOW_UPDATE` 的「降到 0」与「回补」两条通告都一定会发生——这正是流控真正
/// 被执行的形状。用 [`K_CHANNEL_CAPACITY`]（4096）配 64 字节载荷时窗口永远用不完，
/// 流控代码一行都不会被执行（这是本仓库此前流控缺陷长期未被发现的直接原因）。
pub const K_FLOW_CTRL_RING_CAPACITY: usize = 512;


/// 单条子流要传的字节数：**远大于**窗口，从而把窗口反复用尽 / 回补。
pub const K_FLOW_CTRL_PAYLOAD_LEN: usize = 64usize * 1024usize;


/// 与 [`SmokeMuxConfig`] 逐字同构，**只把子流环容量换成
/// [`K_FLOW_CTRL_RING_CAPACITY`]** 的连接配置。
///
/// 连接级帧暂存仍走 [`make_stage_buffs_with_`] 的 64 KiB，避免把「帧暂存」这一无关
/// 变量带进对照。
pub struct FlowCtrlConfig<W, R, RT> {
    _use_w_: PhantomData<W>,
    _use_r_: PhantomData<R>,
    /// 运行时值，同 [`SmokeMuxConfig::rt_`]。
    rt_: RT,
}

impl<W, R, RT: Copy> Copy for FlowCtrlConfig<W, R, RT> {}

impl<W, R, RT: Clone> Clone for FlowCtrlConfig<W, R, RT> {
    fn clone(&self) -> Self {
        FlowCtrlConfig {
            _use_w_: PhantomData,
            _use_r_: PhantomData,
            rt_: self.rt_.clone(),
        }
    }
}

impl<W, R, RT> FlowCtrlConfig<W, R, RT> {
    /// 由**运行时值**构造；类型 `W` / `R` 由调用点的 `MuxConnection` 推断。
    pub const fn new(rt: RT) -> Self {
        FlowCtrlConfig {
            _use_w_: PhantomData,
            _use_r_: PhantomData,
            rt_: rt,
        }
    }
}

impl<W, R, RT> TrMuxConfig for FlowCtrlConfig<W, R, RT>
where
    W: TrBuffWrite<u8> + 'static,
    R: TrBuffRead<u8> + 'static,
{
    type Data = u8;
    type Dock = Dock;
}

impl<W, R, RT> TrConnCfg for FlowCtrlConfig<W, R, RT>
where
    W: TrBuffWrite<u8> + 'static,
    R: TrBuffRead<u8> + 'static,
    RT: TrSmokeRt,
{
    type Rt = RT;
    type Alloc = CoreAlloc;
    type Policy = DefaultPolicy;
    type ConnTx = W;
    type ConnRx = R;
    type Metrics = NoMetrics;

    fn runtime(&self) -> Self::Rt {
        self.rt_.clone()
    }

    fn allocator(&self) -> Self::Alloc {
        CoreAlloc
    }

    fn policy(&self) -> &Self::Policy {
        &SMOKE_POLICY
    }

    /// 本配置不上报；带 sink 的端到端用例见 `tests/metrics_e2e.rs`。
    fn metrics(&self) -> &Self::Metrics {
        &NoMetrics
    }


}


/// 小场景里的 dock 数量与每个 dock 上的子流数量（`2 × 2 = 4` 条/端）。
pub const K_SMALL_DOCK_COUNT: u32 = 2;


pub const K_SMALL_CHANNELS_PER_DOCK: usize = 2;


/// 慢读步长（字节）：接收方每次只消费这么多，迫使窗口在 0 与阈值之间往复。
pub const K_FLOW_CTRL_READ_STEP: usize = 64;


/// 流控场景的看门狗时长。
///
/// 流控失效的典型征兆**不是断言失败而是死锁**（发送方永远拿不到额度，两端互相等）。
/// 两个正常场景都在毫秒级完成，因此这个值取得很宽松：它只是把「挂死」变成一条
/// 可读的 panic，而不是让 CI 挂到超时。
pub const K_FLOW_CTRL_WATCHDOG_: core::time::Duration =
    core::time::Duration::from_secs(20);


/// 隔离用例里「畅通那条子流」的载荷长度（小于一个窗口，读一次即可完成）。
pub const K_FLOW_CTRL_SMALL_LEN: usize = 256;

/// 构造测试用的默认运行时值（等价于各后端的 `current()`）。
///
/// # Panics
///
/// 不在所选后端的运行时上下文内时 panic（与生产路径
/// [`DefaultConnCfg::new`](smux_v1::connection::DefaultConnCfg::new) 一致）。
pub fn default_rt_() -> DefaultRt {
    smux_v1::connection::default_rt_()
}


//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// 运行时装配断言：让测试自己声明「我跑在哪个后端上」
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

/// 断言 `rt` 确实来自**期待的后端**。
///
/// # 为什么每个「依赖某个运行时」的测试都该先调它
///
/// 同一个 `smux_v1` 可以在多个 feature 装配下编译，而**装配错了的失败模式很差**：
/// 轻则在别的上下文里 panic 出一句与病因无关的文案（compio 的
/// 「not in a compio runtime」、tokio 的「no reactor running」），重则**静默挂住**
/// （本地队列没人驱动，或 `block_on_advancing` 的驱动与连接用的计时器不是同一条时间
/// 轴）。无论哪一种，都要从现象反推配置，而配置本来是可以写在测试第一句里的。
///
/// 因此在**取得运行时值之后立刻**断言一次：装配不对时第一句就响亮失败，病因是
/// 「我期待 X，实际是 Y」。
///
/// # Panics
///
/// `rt` 报告的身份不是 `expected` 时 panic，文案里带上实际身份。
///
/// # Examples
///
/// ```ignore
/// let rt = common::default_rt_();
/// common::assert_runtime_is_(&rt, RuntimeTag::Tokio);
/// ```
pub fn assert_runtime_is_<R>(rt: &R, expected: smux_v1::x_deps::RuntimeTag)
where
    R: abs_art::TrAsyncRuntime,
{
    let actual = rt.about();
    assert_eq!(
        actual, expected,
        "本用例要求跑在 {expected:?} 后端上，但当前运行时值报告的身份是 {actual:?}；\
         请检查 Cargo feature（`test-tokio-runtime` / `test-compio-runtime` / \
         `test-smol-runtime`）"
    );
}
