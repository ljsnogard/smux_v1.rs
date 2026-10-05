//! # 时间：**每连接一个**的轮盘式计时器
//!
//! 本模块提供「等到某个时刻 / 某个期限」的那一层原语，供连接自身的保活（PULSE）、
//! 空闲超时与其它定时特性使用。它**仿照 compio 的 `runtime::time`** 设计，公开面
//! 与那边基本一一对应：[`Elapsed`]、[`Interval`]，以及 `sleep` / `sleep_until` /
//! `timeout` / `timeout_at` / `interval` / `interval_at`。
//!
//! ## 与 compio 的**唯一必要偏离**：没有全局运行时
//!
//! compio 的 `sleep_until` 之类的自由函数只收一个 `Instant`，是因为它背后有个
//! **全局** `Runtime`：`TimerFuture::try_new` 靠 `Runtime::with_current` 摸到运行
//! 时的 `TimerRuntime`。本仓的连接**可能跑在 tokio 或 compio 上**，不存在这样一个
//! 全局对象，因此把「compio 里由运行时隐式持有的那个轮盘」提成**显式参数**：
//! [`Timer`]。每个连接一个，由调用方构造并持有。
//!
//! | compio | 本模块 |
//! | --- | --- |
//! | `sleep_until(deadline)` | `sleep_until(&timer, deadline)` |
//! | `interval(period)` | `interval(&timer, period)` |
//! | 运行时事件循环驱动轮盘 | 连接的 tick 循环驱动轮盘（[`Timer::min_timeout`] + [`Timer::wake`]） |
//!
//! ## 时刻的来源：`embedded_timers`
//!
//! 时刻类型**不写死**为 `std::time::Instant`，而由
//! [`Clock`](embedded_timers::clock::Clock) 的关联类型给出（`embedded_timers` 的
//! [`Clock`](embedded_timers::clock::Clock) /
//! [`Instant`](embedded_timers::instant::Instant) trait）。收益有两个：
//!
//! 1. `src` 里除测试外**不再出现 `std::time::Instant`**（现在只剩
//!    `connection::owner_` 与 `connection::mux_connection::registry_` 两处等待迁移）；
//! 2. 超时与宽限期可以用**假时钟**确定性地验收，而不是靠看门狗掐时间。
//!
//! 具体取哪个类型由调用方实现 [`Clock`](embedded_timers::clock::Clock) 时给出；
//! `embedded_timers` 自带的
//! `Instant32` / `Instant64` / `TimespecInstant` 都是现成的时刻实现，本模块的用例
//! 就用 `Instant64<1_000_000>` 配合假时钟。
//!
//! ## 语义约定
//!
//! - **取消就是丢弃**：所有等待者（`sleep` 返回的 future、[`Interval::tick`]）
//!   一旦被丢弃就撤销登记，轮盘上不留悬空 waker 槽。
//! - **到期判据是 `deadline <= now`**（含相等），与 `std::time` 一致。
//! - **本模块的等待者是叶子 future**，因此**不**套 `gen_may_cancel_future`
//!   （AGENTS §4 的「优先考虑」）：连接层每个 park 点一律用既有的
//!   `race_cancel_` / `may_cancel_with` 与取消令牌竞争，套在外层的取消包装反而
//!   会把「可竞争」变成「不可竞争」。
//! - **登记不会唤醒驱动方**：见 [`Timer`] 的文档——新登记一个更早的期限时，
//!   驱动方可能正等在一个更晚的期限上，这个通知落点尚待裁决。
//!
//! ## 本模块**不做**的事
//!
//! - **不提供异步等待源**：轮盘只记录「谁想在什么时候被唤醒」，真正「睡到那时候」
//!   的定时源由调用方（连接的 tick 循环）提供。`embedded_timers` 给不了这一项
//!   （它只有阻塞式 `Delay`，在异步循环里禁用），因此这正是连接层尚待裁决的一项
//!   （见 `dev-notes/keepalive-20261005-0901.md` §2.2 与 §5.2）。
//! - **不做连接级策略**：保活 PULSE 的阈值与限频、空闲超时的宽限期、超时后是拆
//!   子流还是终连接，都是连接层的语义，不属于本模块。

mod interval_;
mod sleep_;
mod timeout_;
mod wheel_;

pub use interval_::{Interval, interval, interval_at};
pub use sleep_::{sleep, sleep_until};
pub use timeout_::{Elapsed, timeout, timeout_at};
pub use wheel_::Timer;

#[cfg(test)]
mod tests_;
