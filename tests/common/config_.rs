//! 测试侧的连接配置与整个用例矩阵的**容量常量**。
//!
//! 两套配置刻意只差一处：冒烟用 64 KiB 连接级帧暂存；流控验收把子流环**上钳**到
//! [`K_FLOW_CTRL_RING_CAPACITY`]，以逼出「窗口用尽 → 回补」这条平时走不到的路径。
//! 全部数字集中于此，便于审计用例的量纲。

use core::marker::PhantomData;
use buffex::x_deps::abs_buff::{TrBuffRead, TrBuffWrite};
use mm_ptr::{Owned, x_deps::abs_mm::CoreAlloc};
use abs_smux::conf::TrMuxConfig;
use smux_v1::{
    connection::{BuffAllocError, Dock, K_STAGE_RING_CAPACITY, MuxConnection, TrConnCfg},
    flow_ctrl::DefaultPolicy,
    time::SystemClock,
};

use crate::common::{SmokeBuff, make_stage_buffs_with_};

/// 连接层要求的 bound：**值化的本地作用域**（`abs_art::TrLocalScope`）**再加后端的
/// 计时能力**（`abs_art::TrTime`，第五个循环要等「下一个期限」）。
///
/// 它是一个**空标记 trait**（对所有 `TrLocalScope + TrTime` 的类型 blanket 实现）：
/// Rust 还没有稳定的 trait alias，而场景函数里满是 `S: TrSmokeScope` 这样的约束，
/// 逐处加一条 `+ TrTime` 只会把同一件事抄很多遍。真实约束仍然只有
/// `TrLocalScope + TrTime` 两条。
///
/// 场景函数一律把作用域值作为第一个参数（`scope: &S`）并原样转发给
/// [`MuxConnection::new`]；两个测试目标各自给出具体后端的作用域值
/// （tokio / compio 各自的 `LocalScope`）。契约是「谁取得作用域，谁负责驱动」：
/// tokio 用 `scope.run_until(..)` 包住整段使用期，compio 由运行时自己驱动。
pub use abs_art::{TrLocalScope, TrTime};

pub trait TrSmokeScope: abs_art::TrLocalScope + abs_art::TrTime {}

impl<S> TrSmokeScope for S where S: abs_art::TrLocalScope + abs_art::TrTime {}


/// 一端连接对象的类型：策略固定为 [`SmokeMuxConfig`]，传输类型由策略的类型参数
/// `R` / `W` 声明；连接对外的错误类型统一为无泛型的 [`MuxError`]。
pub type SmokeConn<R, W, S> = MuxConnection<SmokeMuxConfig<W, R>, S>;


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
pub struct SmokeMuxConfig<W, R> {
    _use_w_: PhantomData<fn() -> W>,
    _use_r_: PhantomData<fn() -> R>,
}

impl<W, R> Copy for SmokeMuxConfig<W, R> {}

impl<W, R> Clone for SmokeMuxConfig<W, R> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<W, R> SmokeMuxConfig<W, R> {
    /// 由一个 ZST 值构造；类型由调用点的 `HandshakeDelivery` / `MuxConnection` 推断。
    pub const fn new() -> Self {
        SmokeMuxConfig {
            _use_w_: PhantomData,
            _use_r_: PhantomData,
        }
    }
}

impl<W, R> Default for SmokeMuxConfig<W, R> {
    fn default() -> Self {
        Self::new()
    }
}

/// [`DefaultPolicy`] 是 ZST；取静态引用即可满足 `TrConnCfg::policy`。
static SMOKE_POLICY: DefaultPolicy = DefaultPolicy;

impl<W, R> TrMuxConfig for SmokeMuxConfig<W, R>
where
    W: TrBuffWrite<u8> + 'static,
    R: TrBuffRead<u8> + 'static,
{
    type Data = u8;
    type Dock = Dock;
    type Buff = SmokeBuff;
}

impl<W, R> TrConnCfg for SmokeMuxConfig<W, R>
where
    W: TrBuffWrite<u8> + 'static,
    R: TrBuffRead<u8> + 'static,
{
    type Alloc = CoreAlloc;
    type Clock = SystemClock;
    type Policy = DefaultPolicy;
    type ConnTx = W;
    type ConnRx = R;
    type StageBuff = SmokeBuff;

    fn allocator(&self) -> Self::Alloc {
        CoreAlloc
    }

    fn clock(&self) -> Self::Clock {
        SystemClock
    }

    fn policy(&self) -> &Self::Policy {
        &SMOKE_POLICY
    }

    fn make_ring_buffs(
        &self,
        alloc: Self::Alloc,
        capacity: usize,
    ) -> Result<(Self::Buff, Self::Buff), BuffAllocError> {
        Result::Ok((
            Owned::new_uninit_slice(capacity, alloc),
            Owned::new_uninit_slice(capacity, alloc),
        ))
    }

    fn make_stage_buffs(
        &self,
        alloc: Self::Alloc,
    ) -> Result<(Self::StageBuff, Self::StageBuff), BuffAllocError> {
        Result::Ok((
            Owned::new_uninit_slice(K_STAGE_RING_CAPACITY, alloc),
            Owned::new_uninit_slice(K_STAGE_RING_CAPACITY, alloc),
        ))
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
pub struct FlowCtrlConfig<W, R> {
    _use_w_: PhantomData<W>,
    _use_r_: PhantomData<R>,
}

impl<W, R> Copy for FlowCtrlConfig<W, R> {}

impl<W, R> Clone for FlowCtrlConfig<W, R> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<W, R> FlowCtrlConfig<W, R> {
    /// 由一个 ZST 值构造；类型由调用点的 `MuxConnection` 推断。
    pub const fn new() -> Self {
        FlowCtrlConfig {
            _use_w_: PhantomData,
            _use_r_: PhantomData,
        }
    }
}

impl<W, R> Default for FlowCtrlConfig<W, R> {
    fn default() -> Self {
        Self::new()
    }
}

impl<W, R> TrMuxConfig for FlowCtrlConfig<W, R>
where
    W: TrBuffWrite<u8> + 'static,
    R: TrBuffRead<u8> + 'static,
{
    type Data = u8;
    type Dock = Dock;
    type Buff = SmokeBuff;
}

impl<W, R> TrConnCfg for FlowCtrlConfig<W, R>
where
    W: TrBuffWrite<u8> + 'static,
    R: TrBuffRead<u8> + 'static,
{
    type Alloc = CoreAlloc;
    type Clock = SystemClock;
    type Policy = DefaultPolicy;
    type ConnTx = W;
    type ConnRx = R;
    type StageBuff = SmokeBuff;

    fn allocator(&self) -> Self::Alloc {
        CoreAlloc
    }

    fn clock(&self) -> Self::Clock {
        SystemClock
    }

    fn policy(&self) -> &Self::Policy {
        &SMOKE_POLICY
    }

    fn make_ring_buffs(
        &self,
        alloc: Self::Alloc,
        capacity: usize,
    ) -> Result<(Self::Buff, Self::Buff), BuffAllocError> {
        // 容量的**上钳**在这里：验收用例无论请求多大，都只拿到小环，从而保证窗口
        // 一定被用尽（取 `min` 而不是直接替换，是为了让「调用方请求 0」这类边界
        // 仍然按原语义走）。
        let capacity = capacity.min(K_FLOW_CTRL_RING_CAPACITY);
        Result::Ok((
            Owned::new_uninit_slice(capacity, alloc),
            Owned::new_uninit_slice(capacity, alloc),
        ))
    }

    fn make_stage_buffs(
        &self,
        alloc: Self::Alloc,
    ) -> Result<(Self::StageBuff, Self::StageBuff), BuffAllocError> {
        // 分配器参数用不上（缓冲由 helper 直接给），显式消费掉以免告警。
        let _ = alloc;
        Result::Ok(make_stage_buffs_with_(K_STAGE_RING_CAPACITY))
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
