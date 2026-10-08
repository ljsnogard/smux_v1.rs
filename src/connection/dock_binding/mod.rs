//! [`DockBinding`]：实现 `abs_smux::conn::TrDockBinding` 的会话对象。
//!
//! 一个 binding 固定一个 `local_dock`，并把它派生成 listener（监听入向）或
//! 主动建流（`open_channel_async`）。

mod binding_;

pub use binding_::{
    BindingError, DockBinding,
};

pub(crate) use binding_::map_reserve_err_;
