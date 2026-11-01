//! 循环共享量：`MuxLoopShared_` / `MuxShared_`、常量与小工具宏。

use crate::{
    connection::{
        TrConnAllocCfg, TrConnCfg, control_state_::ControlState_, mux_connection::ChannelRegistry_,
        public_channel_::PublicChannel_,
    },
    metrics::TrMetricsSink,
    time::{ConnClock_, TrTime},
};

/// 单条子流数据帧的载荷上限（一次成帧最多携带多少字节）。
///
/// 与 `max_packet_size` 无关：后者是**帧总长**上限，这里只是想避免一次借出过大的
/// 段而让其他子流等太久（公平性，见 `connection-20260919-1631.md` §5.2）。
pub(super) const K_MAX_DATA_CHUNK: usize = 16usize * 1024usize;

// 帧暂存环容量与**帧头上界**无关：帧头逐字节解析 / 编码（`frame_::encode_header_`
// 用栈上定长缓冲，长度上界 [`K_MAX_FRAME_HEADER`]），载荷按底层段长分块搬入 / 写出，
// 因此帧暂存容量取到环原语的下限也能跑通（见模块文档「连接级环的硬约束」）。

/// 两个**内侧**循环共享的、与调用方配置无关的量。
///
/// 它由建连路径从核心展开而来：注册表句柄 + 单帧上限 + **连接级时钟** + 空闲超时。
/// 注意它**不含核心引用**（模块文档「循环不持有连接核心」），因此核心可以在最后一个
/// 应用面对象被丢弃时正常析构并触发收尾。
///
/// 「初始接收窗口」与「通告阈值」**不在这里**：`abs_smux` 更新后，子流缓冲由调用方
/// 在最终裁决（`accept_async`）时给出，容量逐条子流不同，因此这两个量在**建流时**
/// 按该子流的接收缓冲容量算出，存进该子流的 [`ChannelState_`]（见
/// [`crate::connection::owner_`]）。循环侧不再需要任何连接级窗口快照。
///
/// # 时钟与运行时值
///
/// 注册表里与时间有关的操作（拆流宽限期回收、进入宽限态、判宽限态）都要「现在」，
/// 而两个内侧循环恰好都要调它们（解复用循环判宽限态、复用循环进宽限态）。把
/// [`ConnClock_`] 放在共享量里，三个循环因此都拿得到**同一个 epoch** 下的毫秒。
///
/// 运行时值在这里是「配置给的」（[`TrConnCfg::Rt`]），**不是**共享量自己的类型参数：
/// 核心**不直接**持有运行时值（compio 的运行时值是 `!Send + !Sync`），共享量只在
/// **本地**循环里活着，因此可以自由持有它。
pub(crate) struct MuxLoopShared_<C, R, M>
where
    C: TrConnAllocCfg + 'static,
    R: TrTime,
    M: TrMetricsSink,
{
    /// 注册表（dock / 子流索引与配额、失败标志、取消令牌、计时唤醒槽）。
    pub(super) reg_: ChannelRegistry_<C>,

    /// 控制面共享句柄：写循环据此 raise `HINT_MAIN_*`（例如发送环已排空）。
    pub(super) control_: ControlState_<C>,

    /// 协商出的单帧总长上限。
    pub(super) max_packet_size_: usize,

    /// 建流期 message 的每方向载荷上限（建连时从配置取一次的快照）。
    pub(super) pending_msg_cap_: usize,

    /// 连接级时钟（运行时值 + 建连 epoch）。
    pub(super) conn_clock_: ConnClock_<R>,

    /// 协商出的**活跃子流空闲超时**（毫秒）；计时循环的判据。
    pub(super) channel_timeout_millis_: u64,

    /// 公共报文子流的四路唤醒槽与关闭位（ring 半部按方向由各循环独占）。
    ///
    /// P2.1 先接线，搬运逻辑在 P2.2 接入；此前的短暂未读由 `allow` 标出。
    #[allow(dead_code)]
    pub(super) public_: PublicChannel_<C>,

    /// 本次连接的**指标接收方**（建连时从配置克隆的一份）。
    ///
    /// 三个循环不持有核心（见模块文档 §2.2），拿不到 `MuxCore`，只能像运行时值那样
    /// 自持一份。克隆的代价由需求方选的 sink 类型决定（见 `crate::metrics` 模块文档 §3）。
    ///
    /// **它不是 `Option`**：「有没有 sink」是编译期由 `C::Metrics` 决定的**类型事实**，
    /// 做成运行期状态只会让每个上报点多一次判空分支。不上报的装配里它是零大小的
    /// `NoMetrics`，上报调用点随即被单态化消除。
    pub(super) metrics_: M,
}

/// [`MuxLoopShared_`] 在具体连接策略上的简写：分配器配置与运行时值都取自 `C`。
///
/// 循环与事件处理的签名里满是这个类型，用别名把关联类型摊平；而只依赖
/// 「注册表 + 运行时值 + 指标」的辅助函数（例如 [`fail_mux_loop_`]）仍写成
/// 泛型于 `C` 的形状，不必被绑到某个连接配置上。
pub(crate) type MuxShared_<C> = MuxLoopShared_<
    crate::connection::ConnAllocCfg_<C>,
    <C as TrConnCfg>::Rt,
    <C as TrConnCfg>::Metrics,
>;

/// 手写 `Clone`：自动派生会给分配器配置参数 `C` 也加上 `Clone` 约束，而配置本身
/// 只需要在各字段里以**已取出的分配器实例**出现。
impl<C, R, M> Clone for MuxLoopShared_<C, R, M>
where
    C: TrConnAllocCfg + 'static,
    R: TrTime + Clone,
    M: TrMetricsSink + Clone,
{
    fn clone(&self) -> Self {
        MuxLoopShared_ {
            reg_: self.reg_.clone(),
            control_: self.control_.clone(),
            max_packet_size_: self.max_packet_size_,
            pending_msg_cap_: self.pending_msg_cap_,
            conn_clock_: self.conn_clock_.clone(),
            channel_timeout_millis_: self.channel_timeout_millis_,
            public_: self.public_.clone(),
            metrics_: self.metrics_.clone(),
        }
    }
}

impl<C, R, M> MuxLoopShared_<C, R, M>
where
    C: TrConnAllocCfg + 'static,
    R: TrTime,
    M: TrMetricsSink,
{
    /// 由建连路径展开后的量构造（成员私有，构造只能走这里）。
    #[allow(
        clippy::too_many_arguments,
        reason = "构造器一次接收建连时已展开的共享量"
    )]
    pub(crate) fn new_(
        reg: ChannelRegistry_<C>,
        control: ControlState_<C>,
        max_packet_size: usize,
        pending_msg_cap: usize,
        conn_clock: ConnClock_<R>,
        channel_timeout_millis: u64,
        public: PublicChannel_<C>,
        metrics: M,
    ) -> Self {
        MuxLoopShared_ {
            reg_: reg,
            control_: control,
            max_packet_size_: max_packet_size,
            pending_msg_cap_: pending_msg_cap,
            conn_clock_: conn_clock,
            channel_timeout_millis_: channel_timeout_millis,
            public_: public,
            metrics_: metrics,
        }
    }
}

impl<C, R, M> MuxLoopShared_<C, R, M>
where
    C: TrConnAllocCfg + 'static,
    R: TrTime,
    M: TrMetricsSink,
{
    /// 注册表句柄。
    pub(crate) fn reg_(&self) -> &ChannelRegistry_<C> {
        &self.reg_
    }

    /// 控制面共享句柄（写循环据此 raise `HINT_MAIN_*`）。
    pub(crate) fn control_(&self) -> &ControlState_<C> {
        &self.control_
    }

    /// 连接级时钟。
    pub(crate) fn conn_clock_(&self) -> &ConnClock_<R> {
        &self.conn_clock_
    }

    /// 协商出的活跃子流空闲超时（毫秒）。
    pub(crate) fn channel_timeout_millis_(&self) -> u64 {
        self.channel_timeout_millis_
    }

    /// 协商出的单帧总长上限。
    pub(crate) fn max_packet_size_(&self) -> usize {
        self.max_packet_size_
    }

    /// 建流期 message 的每方向载荷上限。
    pub(crate) fn pending_msg_cap_(&self) -> usize {
        self.pending_msg_cap_
    }

    /// 公共报文子流的唤醒槽。
    #[allow(dead_code, reason = "P2.2 起由 mux / main 的搬运逻辑读取")]
    pub(crate) fn public_(&self) -> &PublicChannel_<C> {
        &self.public_
    }

    /// 本次连接携带的**指标接收方**（总有值，见字段文档）。
    ///
    /// 循环侧的上报直接写成 `shared.metrics_().on_frame(..)`：**没有判空分支**；
    /// 缺省 sink（`NoMetrics`）下整句在单态化后消失（见 `crate::metrics` 模块文档 §4）。
    #[inline]
    pub(crate) fn metrics_(&self) -> &M {
        &self.metrics_
    }
}

/// 循环里等一把共享锁：**被取消就退出本循环**（连接正在收尾，`()` 返回值的循环用）。
macro_rules! lock_or_exit_ {
    ($e:expr) => {
        match $e.await {
            Result::Ok(value) => value,
            Result::Err(_) => return,
        }
    };
}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// 循环的本地表
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

pub(crate) use lock_or_exit_;
