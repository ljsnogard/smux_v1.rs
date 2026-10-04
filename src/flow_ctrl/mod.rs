//! # 流控（flow control）
//!
//! 本模块实现**与 IO 无关**的字节窗口算法，供 [`crate::connection`] 的中心循环与子流
//! 两侧调用。它不碰任何缓冲、网络或异步原语，因此可以单独测试，也可以被将来别的
//! 复用协议复用。
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
//! 4. 通告**什么时候发**由阈值 + 频率限制决定（[`RecvWindow::should_report`]）：
//!    建流时一次（通告最大窗口），此后**收缩**跌破 `1/2` / `1/4` / `0`、**扩张**升过
//!    `1/2` / `3/4` / 满时各发一次；两次通告之间一般至少隔若干个数据帧，避免窗口在
//!    阈值附近抖动时反复发同一条事件。因为通告是快照，**延迟通告不会导致越权**。
//!
//!    但「**窗口从 0 抬起**」这一次**不受**帧数门限约束：门限靠收到数据帧推进，而
//!    发送方一旦没额度就一条帧也发不出来，两者互为前提会把双方锁死（本仓库的流控
//!    验收用例撞到过）。详见 [`RecvWindow::should_report`] 的方法文档。
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
pub(crate) use recv_window_::ReportThresholds_;
pub use report_::WindowReport;
pub use send_window_::SendWindow;
pub use types_::{Credit, RecvTotal};
