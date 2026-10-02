//! [`MuxConnection`]：实现 `abs_smux::conn::TrConnection` 的复用连接对象。
//!
//! 本模块只负责「连接对象 + 建连（`new`）+ `bind_async`」。连接的读写循环在
//! `session_`，各会话对象（binding / listener / handle / 半部）在各自子模块。

mod conn_;
mod registry_;

pub use conn_::{
    MuxConnection,
};
pub(crate) use registry_::{
    ChannelRegistry_,
};
