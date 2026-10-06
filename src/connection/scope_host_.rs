//! [`ScopeHost`]：**能交出本地作用域的运行时值**——连接建连路径的那一条本地约束。
//!
//! # 为什么需要它
//!
//! `abs_art` 里「取本地作用域」是各后端 `Runtime::local_scope()` 的**固有方法**
//! （不是 trait 入口，见 `abs_art/dev-notes/local-scope-thread-local-20261006-1420.md`
//! 的刻意裁决）。因此泛型代码里写不出 `rt.local_scope()`——这挡住的不只是
//! 「谁来传作用域」，而是「业务库能不能自己取」。
//!
//! 本 trait 就是那条缺口的**本地补丁**：把「固有方法」升成一条我们自己的约束，
//! 于是 [`MuxConnection::new`](super::MuxConnection::new) 可以在泛型参数只有 `C`
//! 的前提下自己取到作用域。副作用是它把「哪些运行时值能当连接的时间源」这件事
//! **显式化**了：只有实现了本 trait 的运行时值才能走自动取作用域那条路；
//! 其余（例如测试里的假运行时值）走
//! [`MuxConnection::new_with_rt`](super::MuxConnection::new_with_rt)，由调用者
//! 把作用域一并交进来。
//!
//! # 只对「默认后端」实现
//!
//! 实现写在 `abs_art_bridge` 的具名别名上（`CompioRuntime` / `TokioRuntime` /
//! `SmolRuntime`），由 smux 自己的测试 feature 二选一，因此**不依赖 bridge 的
//! 默认后端解析**——那样在同时启用多个后端时会直接编译失败（bridge 的守卫）。
//!
//! # Examples
//!
//! ```ignore
//! use smux_v1::connection::MuxConnection;
//!
//! // `DefaultConnCfg` 的运行时值就是 `ScopeHost` 的实现者，连接自己取作用域。
//! let conn = MuxConnection::from_delivery(delivery)?;
//! ```

use abs_art::{TrLocalScope, TrTime};

/// **能交出本地作用域**的运行时值。
///
/// 各后端的 `Runtime<CAPS>::local_scope()` 要求 `CAPS` 含
/// [`SPAWN_LOCAL`](abs_art::SPAWN_LOCAL)，因此本 trait 的实现也只覆盖
/// 「完整能力集」的那一档（`Runtime<{ FULL }>`）。
///
/// 实现是**委托**，不重复任何逻辑：作用域仍然是那个运行时值固有方法交出的
/// 本线程队列别名（`Clone` 只是多一个别名，同线程多次取得拿到同一条）。
pub trait ScopeHost {
    /// 本运行时值交出的本地作用域类型。
    ///
    /// 要求 `Clone + 'static`：建连路径要把它克隆进五个循环 future
    /// （见 [`MuxConnection::new`](super::MuxConnection::new)）。
    /// `Clone` 只是多一个别名，不是新建队列。
    type Scope: TrLocalScope + Clone + 'static;

    /// 取本线程那条本地队列的别名。
    ///
    /// 语义与各后端的 `Runtime::local_scope()` 完全一致：同一线程上多次调用拿到
    /// **同一条**队列，类型是 `!Send`。
    fn local_scope(&self) -> Self::Scope;
}

/// 默认后端：**compio**（本仓缺省；打开 `test-tokio-runtime` 时改用 tokio）。
///
/// 只对**完整能力集**那一档（`CompioRuntime<{ CompioFull }>`）实现：连接需要
/// `SPAWN_LOCAL` 才能取到作用域，而 `CompioFull = abs_art::FULL & !SPAWN_SEND`
/// 恰好含本位（compio 没有跨线程全局队列，所以它的「完整」少一位）。
///
/// 刻意**不**写成 `impl<const CAPS: usize>` 的泛型：compio 的 `Runtime` 上带一条
/// 「声明了 `SPAWN_SEND` 就一用即报错」的静态断言（`CompioCaps_`），泛型 `CAPS`
/// 无法满足它，会在 impl 定义处直接编译失败——这不是本 crate 能放宽的。
#[cfg(not(feature = "test-tokio-runtime"))]
impl ScopeHost for abs_art_bridge::CompioRuntime<{ abs_art_bridge::CompioFull }> {
    type Scope = abs_art_bridge::CompioLocalScope;

    fn local_scope(&self) -> Self::Scope {
        abs_art_bridge::CompioRuntime::<{ abs_art_bridge::CompioFull }>::local_scope(self)
    }
}

/// tokio 后端（`test-tokio-runtime` feature 下取代 compio 成为默认后端）。
#[cfg(feature = "test-tokio-runtime")]
impl ScopeHost for abs_art_bridge::TokioRuntime<{ abs_art_bridge::TokioFull }> {
    type Scope = abs_art_bridge::TokioLocalScope;

    fn local_scope(&self) -> Self::Scope {
        abs_art_bridge::TokioRuntime::<{ abs_art_bridge::TokioFull }>::local_scope(self)
    }
}

/// **本 crate 的默认运行时值类型**：由 smux 自己的测试 feature 二选一，缺省 compio。
///
/// 它是 [`TrConnCfg::Rt`](super::TrConnCfg::Rt) 的缺省值，也是
/// [`DefaultConnCfg`](super::DefaultConnCfg) 的默认运行时值类型。
///
/// 为什么不直接用 `abs_art_bridge::Runtime`（bridge 的裸名）：bridge 的守卫要求
/// 「同时启用多个 backend 时必须显式声明 `default-backend-*`」，而 `cargo test` 的
/// feature 并集里两个后端会同时出现；那时候裸名根本不存在。用**具名别名**配 smux
/// 自己的 feature 二选一，规则就只有一条、且与 bridge 的默认后端无关。
#[cfg(not(feature = "test-tokio-runtime"))]
pub type DefaultRt_ = abs_art_bridge::CompioRuntime<{ abs_art_bridge::CompioFull }>;

/// 见上：`test-tokio-runtime` 下换成 tokio。
#[cfg(feature = "test-tokio-runtime")]
pub type DefaultRt_ = abs_art_bridge::TokioRuntime<{ abs_art_bridge::TokioFull }>;

/// 编译期断言：默认运行时值必须满足 [`TrConnCfg::Rt`](super::TrConnCfg::Rt) 的约束。
const _: fn() = || {
    fn assert_rt_<R: TrTime + Clone + 'static + ScopeHost>() {}
    assert_rt_::<DefaultRt_>();
};

/// 构造**默认运行时值**：等价于各后端的 `current()`。
///
/// # Panics
///
/// 调用点不在所选后端的运行时上下文内时 panic（文案由各后端给出；tokio 为
/// 「no reactor running」、compio 为「not in a compio runtime」）。这是
/// [`DefaultConnCfg::new`](super::DefaultConnCfg::new) 的既定契约：自动取运行时值
/// 那条路要求调用点已经在运行时里；不在时请走
/// [`DefaultConnCfg::new_with_rt`](super::DefaultConnCfg::new_with_rt) 显式传入。
#[cfg(not(feature = "test-tokio-runtime"))]
pub fn default_rt_() -> DefaultRt_ {
    abs_art_bridge::CompioRuntime::<{ abs_art_bridge::CompioFull }>::current()
}

/// 见上：`test-tokio-runtime` 下换成 tokio。
#[cfg(feature = "test-tokio-runtime")]
pub fn default_rt_() -> DefaultRt_ {
    abs_art_bridge::TokioRuntime::<{ abs_art_bridge::TokioFull }>::current()
}

/// **虚拟时间**的运行时值：把作用域请求委托给被装饰的运行时值。
///
/// `abs_art_mock_clock::ManualTime<R, C>` 只把**时间**换成手动时钟（`TrDelay` /
/// `TrClock` / `TrTime`），其余能力（含「哪条本地队列」）委托给 `R`。连接要自己取
/// 作用域，因此这里把这条请求也一并委托下去：**时间可以是虚拟的，队列仍是那个后端
/// 本来的那条**。
///
/// # 为什么写在这里（本 crate）而不是测试里
///
/// [`ScopeHost`] 是本 crate 的 trait，而 `ManualTime` 是外部类型；测试 crate 对
/// 「外部 trait + 外部类型」写 impl 是 `E0117`。因此这条实现只能留在这里，并由
/// `test-mock-clock` feature 拉进 `abs_art-mock_clock`（生产依赖图里不出现）。
#[cfg(feature = "test-mock-clock")]
impl<R, C> ScopeHost for abs_art_mock_clock::ManualTime<R, C>
where
    R: ScopeHost,
    C: abs_art_mock_clock::ManualClockApi,
{
    type Scope = R::Scope;

    fn local_scope(&self) -> Self::Scope {
        ScopeHost::local_scope(self.inner())
    }
}
