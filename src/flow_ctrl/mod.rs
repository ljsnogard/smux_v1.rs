//! # 流控（flow control）
//!
//! 本模块实现**与 IO 无关**的字节窗口算法，供 [`crate::connection`] 的中心循环与子流
//! 两侧调用。它不碰任何缓冲、网络或异步原语，因此可以单独测试，也可以被将来别的
//! 复用协议复用。
//!
//! ## 并发：窗口内部各自一把零分配自旋锁
//!
//! [`SendWindow`] / [`RecvWindow`] 的方法一律取 `&self`（它们的宿主
//! `connection::owner_::ChannelState_` 被多个任务经共享句柄持有），而**内部
//! 状态整块放进一把 `SpinningMutexOwned`**：一次操作 = 一个临界区，`install_` /
//! `report` / `on_report` 这类多字段改写不存在半新半旧的中间态。
//!
//! 选 `preemptive`（自旋）而非 `cooperative`：后者每条实例内部持一个 `Arc<RwCore>`，
//! 等于给每条子流添一次**全局**分配，与本仓「每一处堆分配都走注入分配器」的纪律冲突
//! （该判断的来历见 `dev-notes/connection-20261002-0548.md` §14/§17）。
//!
//! ## 为什么需要它
//!
//! 复用层把若干条子流复用到同一条字节流上。若发送方可以无限制地把某条子流的
//! 数据推入网络，而接收方来不及把它交给应用，就会出现**一条慢子流拖垮整条
//! 连接**（head-of-line blocking 的内存版本）。因此每条子流的**每个方向**都
//! 各自维护一个窗口：
//!
//! - **接收窗口**（[`RecvWindow`]）：本端承诺还能接收多少字节。它只由两个事件
//!   驱动——「对端又发来 N 字节」与「应用又消费了 N 字节」；后者让窗口可以重新变大，
//!   并由连接层择机通告给对端。
//! - **发送窗口**（[`SendWindow`]）：本端还能向对端发送多少字节。它由两个事件驱动
//!   ——「本端又发了 N 字节」与「收到对端的窗口通告」。
//!
//! 两个窗口是**独立**的：一条子流的 `Tx` 与 `Rx` 可以分别阻塞，互不影响，也不
//! 影响别的子流。
//!
//! ## 方案：绝对量快照 `(R, W)` + 阈值通告
//!
//! 1. 子流建立时两端**互相通告**接收窗口：主动方在 `OPEN` 里带上，被动方在自己那条
//!    `OPEN` 里带上（建流三步见 [`crate::connection`] 模块文档 §4.2）。窗口以对端
//!    通告的值为准；本端实际能收多少仍由本端接收环的容量决定。
//! 2. 发送方在可用额度内切分数据帧；窗口用尽即**阻塞该子流**（不是丢弃、不是报错）。
//! 3. 通告是**绝对量快照** `(R, W)`：`R` 是通告发出时的累计已收字节数，`W` 是当时的
//!    剩余窗口。发送方据此算 `可用 = W − (已发 − R)`，从而精确扣掉在途数据；只发 `W`
//!    会把在途量重复计入（推导见 [`WindowReport`]）。
//! 4. 通告**什么时候发**由「分区 + 变动量」决定（[`RecvWindow::should_report`]）：
//!    建流时一次（通告最大窗口）；此后**临界区**（剩余 `≤ 容量/N`，缺省 1/4）里任何
//!    变化都发，临界区之外则要等累计**变动量**（收到 + 消费的字节数）达到
//!    [`TrFlowCtrlPolicy::min_advance_between_reports`]（缺省一档临界容量）才发。
//!    因为通告是快照，**延迟通告不会导致越权**。
//!
//!    这里**没有**「剩余够多就不通告」的上界：停止更新通告会让对端的可用额度永远停在
//!    旧快照上，连本端消费掉多少都传不过去。完整的因果见 [`RecvWindow::should_report`]
//!    的方法文档。
//! 5. 绝对上限由 [`TrFlowCtrlPolicy::max_window`] 钳制，防止两端来回加码导致窗口
//!    无限增长。
//!
//! ## 与帧的关系
//!
//! 通告作为 `OPEN` / `PULSE` / `WINDOW_UPDATE` 的字段出现（线格式上是
//! `RecvTotal` + `RecvWindow` 两个字段，见 `connection::frame_`）。本模块只负责窗口
//! 记账与「该不该发」的判断，不关心它被编进哪个帧、什么时候真正写出去。
//!
//! ## 违例处理
//!
//! 对端在**已通告**额度之外继续发数据属于**协议违例**，由
//! [`FlowCtrlError::PeerViolation`] 表达，调用方应当终止该子流（并可按需终止整条
//! 连接）。本端自己算错、溢出等属于内部错误，用 [`FlowCtrlError::Overflow`] 表达。

mod error_;
mod flow_;
mod policy_;
mod recv_window_;
mod report_;
mod send_window_;
mod types_;
#[cfg(test)]
mod reset_tests_;
#[cfg(test)]
mod tests_;

pub use error_::FlowCtrlError;
pub use flow_::FlowCtrl;
pub use policy_::{DefaultPolicy, TrFlowCtrlPolicy};
pub use recv_window_::RecvWindow;
pub use report_::WindowReport;
pub use send_window_::SendWindow;
pub use types_::{Credit, RecvTotal};
