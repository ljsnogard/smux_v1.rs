use crate::dock::TrDock;

/// The shared types used across the smux connection.
///
/// **不含缓冲类型**：一条子流用哪种智能指针持有它的 ring 内存，是 `accept` 时
/// **当场**由调用方交出的（见 [`TrChannelHandle::accept_async`](crate::chan::TrChannelHandle::accept_async)
/// 的 `B` 参数），不是连接的静态配置。
pub trait TrMuxConfig {
    /// The type of data flows in channels through the network. e.g., `u8`;
    type Data: 'static + Sized;

    /// The dock type used to identify channel source and target;
    type Dock: 'static + TrDock;
}
