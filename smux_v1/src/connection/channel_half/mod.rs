//! 子流的两个应用侧半部：[`ChannelTx`] / [`ChannelRx`]。
//!
//! 对应 `abs_smux` 的 `TrChannelTx` / `TrChannelRx` / `TrChannelHalf`；数据面则
//! 直接实现 `abs_buff` 的 `TrBuffTryWrite` / `TrBuffWrite` / `TrBuffRead`（发送半边
//! 还实现 `TrBuffFlush`）。
//!
//! # 关闭态：直接读环的两个端
//!
//! `TrChannelHalf::is_tx_closed` / `is_rx_closed` 不额外维护标志位，而是读
//! `buffex::ring` 环本身的**两端关闭态**（半部经 `ring_state()` 取
//! `is_producer_closed` / `is_consumer_closed`）：
//!
//! - 发送环的应用端是 [`ChannelTx`]（生产端），另一端由中心循环的写路径持有；
//!   接收环的会话端是生产端，应用端是 [`ChannelRx`]（消费端）；
//! - `ChannelTx::is_tx_closed()` 因此是「应用端已关闭发送环的生产端」——丢弃
//!   [`ChannelTx`] 即置位；`ChannelTx::is_rx_closed()` 是「中心循环已关闭发送环的
//!   消费端」，即连接已经拆掉这条子流；
//! - 接收方向对称：`ChannelRx::is_tx_closed()` 表示会话（生产端）已关闭接收环，
//!   即对端不再发送（EOF）；`ChannelRx::is_rx_closed()` 表示应用端（消费端）已关闭。
//!
//! 好处是**关闭态不需要再引入一份共享状态**：环本身就是两个端共享的那点状态。
//! 代价是这四个方法只在真正的 `buffex` 半部上成立，因此 `TrChannelHalf` 的 impl
//! 落在具体类型上（而不是泛型 `H`）。
//!
//! # 收尾：可等待的显式入口
//!
//! 除「丢弃半边 = 半关闭」之外，两个方向都有可等待的显式收尾，完成点见
//! `crate::connection` 模块文档 §7：
//!
//! | 入口 | 完成点 |
//! | --- | --- |
//! | [`TrBuffFlush::flush_async`](abs_buff::TrBuffFlush::flush_async) | 发送环已排空 |
//! | [`TrBuffFlush::close_async`](abs_buff::TrBuffFlush::close_async) | 封口且 `CLOSE(FIN)` 已上线 |
//! | [`TrChannelRx::close_async`](abs_smux::chan::TrChannelRx::close_async) | 接收方向已关且 `CLOSE(RESET)` 已上线 |
//!
//! 等待**不额外分配**：每条子流的共享状态里内联两个通知槽（发送 / 接收各一个），
//! 由写循环与终局路径在事实发生变化时唤醒（见 `owner_::ChannelState_`）。`drop` 与
//! 显式 `close` 走**同一套**收尾实现（幂等），因此两者的语义不可能漂移。
//!
//! 因此两个半部**各自就是那个具名类型**（`ChannelTx<C>` / `ChannelRx<C>`），可以
//! 直接写进下游的结构体字段——旧模型下内层包装未导出，下游连「把半部存进具名字段」
//! 都做不到（见 `dev-notes` §16.2 F5）。

mod halves_;

pub use halves_::{ChannelRx, ChannelTx};
