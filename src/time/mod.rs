//! # 时间：**绝对期限的算术层**
//!
//! 本模块只做一件事：把**绝对期限**（`C::Instant`）折算成 [`TrTime`] 能接受的
//! **相对时长**（`Duration`），以及把 `abs_art` 的计时能力与错误类型转出来给连接层用。
//!
//! # 为什么这里这么薄
//!
//! 保活（T6）施工单里原先设想「在 `smux_v1` 自建一个轮盘式计时器」，那是
//! **`abs_art` 还没有计时能力时的补救**：轮盘（`Rc<RefCell<BTreeMap>>` + waker 槽 +
//! 每条等待者一个节点）存在的唯一理由，是当时拿不到一个可等待的「睡到某时刻」。
//!
//! 现在能力已经在 `abs_art` 家族里（trait 在 `abs_art::time`，实现在三个后端），
//! 于是轮盘整个删除，本模块只剩下**算术**：
//!
//! ```text
//! D::sleep_until(&clock, t)   =  D::delay(t − clock.now())
//! D::timeout_at(&clock, t, f) =  D::timeout(t − clock.now(), f)
//! ```
//!
//! 两个都是**运行时类型的关联函数**（`D` 是最终二进制选中的后端），与 `abs_art`
//! 家族既有的 `Runtime::block_on(..)` / `Runtime::delay(..)` 同形——见 [`TrDeadline`]。
//!
//! # 分层：等待层归后端，算术与判定层留本地
//!
//! | 层 | 归谁 | 为什么 |
//! | --- | --- | --- |
//! | 「睡一段 / 每周期醒」 | 后端（[`TrTime`]） | 只有运行时知道怎么等；三个后端各自的实现由 `abs_art-smoke` 的契约矩阵钉住 |
//! | 「什么时候该醒」 | 本地（本模块 + 注入的 [`Clock`](embedded_timers::clock::Clock)） | 连接级 epoch、每子流空闲毫秒、宽限期判定都是**协议语义**；而注入式时钟让它们可以**用假时钟确定性验收** |
//!
//! 这条缝就是本轮把「绝对时刻」留在消费方的收益：`TrTime` 是 `Duration`-only 的
//! （见 `abs_art::time` 模块文档），因此后端的真实时钟**不**会挤进本模块的判定，
//! 而 [`TrDeadline::sleep_until`] / [`TrDeadline::timeout_at`] 这两个绝对形式在这里
//! 由本地时钟补上。
//!
//! # 与 `abs_art` 的关系
//!
//! 计时能力**不**在这里定义实现，也不在这里重新导出成新名字：需要相对形式
//! （`D::delay(Duration)` / `D::interval(period)` / `D::timeout(Duration, f)`）的
//! 调用方直接用 `abs_art` 的 [`TrTime`]。本模块只补绝对形式。

mod deadline_;

pub use abs_art::{Elapsed, TrInterval, TrTime};
pub use deadline_::TrDeadline;

#[cfg(test)]
mod tests_;
