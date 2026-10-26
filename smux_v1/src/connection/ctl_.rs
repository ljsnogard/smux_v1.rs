//! 控制面循环：Listener 入向、建流 / 拆流事务与 `Attach` 分发。
//!
//! 它是本连接的第 4 个 `spawn_local` 任务（与解复用 / 复用 / 计时并列），负责
//! 从中心循环里**移走**那些「等外部条件」的长事务：
//!
//! - Listener 入向（建流）与按 `local_dock` 的入向 registry；
//! - `Release` 宽限期、等排空后补 `CLOSE(FIN)` 这类可能长期 park 的收尾；
//! - 以 `local_dock` 为键的 Listener registry。
//!
//! # 当前进度
//!
//! 本轮先落**骨架**：独立的取消令牌与任务已经就位，循环只等取消。通道 `Attach`
//! 的环半部已经改走 `mux_inbox` / `demux_inbox`（见 `RegistryRoot_`），因此 ctl 不再
//! 需要搬运它们；下一轮接入 `ctl_rx` 与 Listener registry 之后，它负责入向与拆流
//! 事务。

use crate::{
    connection::{TrConnCfg, session_::MuxShared_},
    x_deps::abs_cancel::TrCancellationToken,
};

/// 控制面循环（骨架阶段）。
///
/// 它不搬字节、不持有传输半边；取消令牌触发即退出。
pub(crate) async fn ctl_loop_async_<C, K>(_shared: MuxShared_<C>, cancel: K)
where
    C: TrConnCfg,
    K: TrCancellationToken,
{
    if cancel.is_cancelled() {
        return;
    }
    // 骨架阶段只等取消；接入 `ctl_rx` 后这里改成「命令 / 提示 / 取消」三选一。
    cancel.child_token().cancellation().await;
}
