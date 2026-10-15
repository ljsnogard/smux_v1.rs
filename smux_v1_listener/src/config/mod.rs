//! 入向适配的契约与 listener 的策略参数。
//!
//! 逐项列出导出：本模块是 `config_` 的**唯一**出口，不使用 `*`（见 AGENTS.md 纪律 5）。

mod config_;
mod error_;

pub use config_::{
    AcceptOutput, K_DEFAULT_MAX_INFLIGHT, TrInbound,
    TrListenerConfig, TrNegotiatorFactory,
};
pub use error_::InboundError;

pub(crate) use config_::into_halves_async_;
