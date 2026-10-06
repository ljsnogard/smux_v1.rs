//! # metrics：连接指标的**可选**上报
//!
//! 本模块让 smux 主动报告连接的运行数据，而「报告给谁」完全由需求方决定。整个模块
//! 挂在 `Cargo.toml` 的 `metrics` feature 上；**不打开 feature 时热路径上的上报调用点
//! 会被单态化消除为零指令**（见下「零开销」）。
//!
//! ## 1. 只有两个公开件
//!
//! | 件 | 角色 |
//! | --- | --- |
//! | [`TrMetricsSink`] | mux **愿意报告什么**：一组带默认空实现的方法 |
//! | [`NoMetrics`] | 零大小的空实现：不上报的实现就填它 |
//!
//! 外加一份**内置**的 bench / debug 采集器（[`DebugMetrics`]），
//! 供 mux 自己的基准与调试使用；它不含任何全局状态，需求方按需实例化。它与「重」的
//! 实现一起挂在 `feature = "metrics"` 上：**不打开 feature 时它整个不进编译单元**。
//! 常驻编译的只有 trait 形状所必需的两件（[`TrMetricsSink`] 与 [`NoMetrics`]）——
//! 少了它们，`TrConnCfg::Metrics` 与 `metrics()` 就无从表达（每个 `TrConnCfg` 实现都
//! 必须写出这两项）。
//!
//! ## 2. 注入点：配置上的两个成员
//!
//! sink 经 [`TrConnCfg`](crate::connection::TrConnCfg) 注入，形状是**一对必需项**：
//!
//! ```ignore
//! pub trait TrConnCfg: /* … */ {
//!     /// 每个实现都要写：不上报就填 `NoMetrics`。
//!     type Metrics: TrMetricsSink + Clone;
//!
//!     /// 返回**确定的引用**，不是 `Option`。
//!     fn metrics(&self) -> &Self::Metrics;
//! }
//! ```
//!
//! 两个设计点各有其不可替代的理由：
//!
//! - **关联类型是必需项**（没有默认值）：每个 `TrConnCfg` 实现显式表态。不上报的写法
//!   是 `type Metrics = NoMetrics;`——它零大小、方法全是空实现，`metrics()` 里回一个
//!   `&NoMetrics` 即可（零大小类型的常量引用会被提升为 `'static`，不涉及分配，也不需要
//!   静态存储）；
//! - **取用方法返回 `&Self::Metrics` 而不是 `Option<&…>`**：「有没有 sink」是**编译期**
//!   由关联类型决定的**类型事实**，做成运行期状态只会让每一个上报调用点多一次判空
//!   分支——而那个分支在「不上报」的装配下本可以彻底不存在。
//!
//! 代价是这两项**都得写**：本仓 8 处 `impl TrConnCfg`（库内 3 处、`tests/` 下 5 处）
//! 各自显式给出；「不上报」那两行是 `type Metrics = NoMetrics;` 与 `&NoMetrics`。
//!
//! ## 3. mux 侧不承担灵活性代价
//!
//! 需求方 2026-10-06 的裁决，逐条落在这里：
//!
//! - **不做 `Arc` 分配**：sink 实例归需求方所有，mux 只借一个 `&C::Metrics`，建连时
//!   `clone` 一份进内部共享量。`Clone` 因此是 trait 的硬约束；把 sink 做成什么形态是
//!   需求方的事——`NoMetrics` 那样的**零大小类型**最省事（`&T` 的常量借用会被提升为
//!   `'static`，既不分配也不需要静态存储），有状态的内置 [`DebugMetrics`] 则用 `Arc`
//!   包一份独立计数；
//! - **不做运行时替换 sink**：没有 `set_sink` 之类的入口，`Metrics` 是编译期选定的；
//! - **不做 `dyn` 适配**：不提供 `Arc<dyn TrMetricsSink>` 之类的便利 impl。mux 内部的
//!   上报调用点是 `C::Metrics` 上的**静态分派**，会被单态化；「实际是否走虚调用」
//!   完全取决于需求方把 `Metrics` 填成什么类型。
//!
//! 顺带一条禁令：sink **不得**持有连接句柄，否则构成
//! `MuxCore → sink → MuxConnection → MuxCore` 引用环，`Drop` 永不触发（详见
//! [`TrMetricsSink`] 的模块文档）。
//!
//! ## 4. 零开销
//!
//! 当 `C::Metrics` 取 [`NoMetrics`] 时（不上报的装配；`feature = "metrics"` 关闭时它
//! 也是常见形态——零大小、每个方法都是 `#[inline(always)]` 空实现），循环里的
//!
//! ```ignore
//! shared.metrics_().on_frame(dir, local, remote, kind, bytes);
//! ```
//!
//! 在单态化后完全消失——**既没有判空分支，也没有间接调用**。
//! `dev-notes/metrics-*.md` 记录了实测：开启优化后该调用点所在的函数体里
//! **没有任何 `callq`**。
//!
//! ## 5. 上报口径（与本协议的现实对齐）
//!
//! 需求方给的五类指标里有三项在本协议下不成立，已按现实裁掉或改写：
//!
//! - **重传次数**：协议跑在可靠、有序的字节流上，同连接内不存在重排 / 重传
//!   （见 [`crate::connection`] 模块文档 §4.3），该指标恒为 `0`，**不报**；
//! - **心跳延迟**：`PULSE` 没有「请求 / 应答」之分（收到的一方只刷新存活时钟、
//!   **不回复**，见 [`FrameKind::Pulse`] 的文档），因此 **RTT 在协议上不存在**；
//!   想度量先得改协议；
//! - **帧种类名**：本协议只有 8 种帧（[`FrameKind`]），没有 `HEADERS` / `PING`；
//!   `RST` 是 [`FrameKind::Close`] 上的一个 flag；
//! - **缓冲区水位 / 队列长度**：**不报**（2026-10-06 的决定）。子流环对象在内部循环
//!   的本地表里，而应用面的 `ChannelTx` / `ChannelRx` 只公开 `write_all` /
//!   `read_exact` / `abort_reason`，拿不到环水位；要报就得在循环侧维护原子镜像，
//!   属未被需求采纳的成本。
//!
//! 状态量（**当前活跃连接数** / **当前活跃子流数**）同样不单独上报：它们是**状态**
//! 而非事件，需求方从 [`TrMetricsSink::on_conn_opened`] /
//! [`TrMetricsSink::on_channel_opened`] 等事件自行累加即可——这也正是「sink 的问题
//! 交给 metrics 实现方」的一条推论。
//!
//! ## 6. 已知缺口
//!
//! **连接级失败时，在册子流不会有各自的关闭回调**：`ChannelRegistry_::mark_failed_`
//! 只置失败位、唤醒等待者、取消五个循环，**不逐条释放身份**。因此
//! [`ChannelCloseReason::ConnFailed`] 目前只在文档层面表达，需求方要从
//! [`TrMetricsSink::on_conn_closed`] 推断所有在册子流一并终结。
//!
//! ## 7. Examples
//!
//! 一个只关心「连接数 + 帧数」的实现：
//!
//! ```
//! use smux_v1::connection::{Dock, FrameKind};
//! use smux_v1::metrics::{FrameDir, TrMetricsSink};
//! use core::sync::atomic::{AtomicU64, Ordering};
//!
//! #[derive(Debug, Default)]
//! struct Tally {
//!     conns: AtomicU64,
//!     frames: AtomicU64,
//! }
//!
//! impl TrMetricsSink for Tally {
//!     fn on_conn_opened(&self) {
//!         self.conns.fetch_add(1, Ordering::Relaxed);
//!     }
//!
//!     fn on_frame(
//!         &self,
//!         _dir: FrameDir,
//!         _local: Dock,
//!         _remote: Dock,
//!         _kind: FrameKind,
//!         _bytes: u32,
//!     ) {
//!         self.frames.fetch_add(1, Ordering::Relaxed);
//!     }
//! }
//!
//! let tally = Tally::default();
//! tally.on_conn_opened();
//! tally.on_frame(FrameDir::Recv, Dock::new(1u32), Dock::new(2u32), FrameKind::Data, 64);
//! assert_eq!(tally.conns.load(Ordering::Relaxed), 1);
//! assert_eq!(tally.frames.load(Ordering::Relaxed), 1);
//! ```

#[cfg(feature = "metrics")]
mod debug_;
mod sink_;

#[cfg(feature = "metrics")]
pub use debug_::{DebugMetrics, DebugSnapshot, K_KIND_SLOTS};
pub use sink_::{ChannelCloseReason, ConnCloseReason, FrameDir, NoMetrics, TrMetricsSink};

#[cfg(test)]
mod tests_;
