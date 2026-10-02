//! 子流的两个应用侧半部：[`ChannelTx`] / [`ChannelRx`]，以及它们的内部包装
//! [`TxRing_`] / [`RxRing_`]。
//!
//! 对应 `abs_smux` 的 `TrChannelTx` / `TrChannelRx` / `TrChannelHalf`；数据面则
//! 直接实现 `abs_buff` 的 `TrBuffWrite` / `TrBuffRead`。
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

mod halves_;

pub use halves_::{
    ChannelRx,
    ChannelTx,
    RxRing_,
    TxRing_,
};
