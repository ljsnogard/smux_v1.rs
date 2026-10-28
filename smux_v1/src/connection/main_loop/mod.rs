//! `main_loop`：控制面单写者循环。
//!
//! 本模块只做逐项导出，不放逻辑：
//!
//! - [`loop_`]：循环本体与 park 点；
//! - [`request_`]：请求 / 统一结果 / 统一错误。

mod loop_;
mod request_;

pub(crate) use loop_::main_loop_async_;
pub(crate) use request_::{EstablishRole_, MainLoopErr_, MainLoopReply_, MainLoopRequest_};
