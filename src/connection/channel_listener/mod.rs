//! [`ChannelListener`]：实现 `abs_smux::conn::TrChannelListener` 的入向监听器。

mod listener_;

pub use listener_::{
    ChannelListener, ListenerError,
};
