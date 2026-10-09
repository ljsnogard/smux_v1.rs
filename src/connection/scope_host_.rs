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

/// **能从当前上下文取到一个运行时值**的运行时值。
///
/// 与 [`ScopeHost`] 同一性质、同一理由：`abs_art` 里「按上下文构造运行时值」是各后端
/// `Runtime::current()` 的**固有方法**（不是 trait 入口），泛型代码写不出来，因此这里
/// 补一条本地约束。
///
/// # 它存在的理由：让配置**不存储**运行时值
///
/// [`CurrentConnCfg`](super::CurrentConnCfg) 不在配置里存运行时值，而是在每次需要
/// 「现在几点」时经本 trait 从**当前上下文**取一个。于是 `C: Send + Sync` 无条件成立，
/// `MuxConnection` 在 compio 装配下也能跨线程——代价见下面的「调用者责任」。
///
/// # 调用者责任：调用点必须处于后端上下文内
///
/// 「取用」发生在**调用线程**上，因此那条线程必须在所选后端的运行时上下文内：
///
/// | 后端 | 上下文要求 | 不在上下文内时 |
/// | --- | --- | --- |
/// | tokio | 处于某个 tokio 运行时上下文内 | `current()` panic（`Handle::current()` 的行为） |
/// | compio | 处于 compio 运行时上下文内 | `current()` panic |
/// | smol | **无**（值是零大小标记） | 不会发生 |
///
/// 跨线程使用连接时「每条线程都自己处于上下文内」这件事**由调用者保证**：
/// debug 构建下 [`current_rt`](Self::current_rt) 会先经
/// [`try_current_rt`](Self::try_current_rt) 给出本 crate 的断言提示，release 构建下由
/// 后端的 panic 兜底——两者都属于调用者违约，不是实现缺陷。
pub trait TrRtCurrent: Sized {
    /// 取当前上下文里的运行时值。
    ///
    /// # Panics
    ///
    /// 调用点不在所选后端的运行时上下文内时 panic（debug 构建下先给出本 crate 的断言
    /// 提示，见 trait 文档）。
    fn current_rt() -> Self;

    /// 当前是否处于上下文内：`Option::None` = **确定不在**，`Option::Some` = 在。
    ///
    /// 只用于 debug 构建下的提示，不改变任何运行期语义。
    fn try_current_rt() -> Option<Self>;
}


/// **默认后端**的实现：`abs_art_bridge::Runtime` / `LocalScope` 是本 crate 的
/// 唯一后端入口。
///
/// 裸名由 bridge 按 **feature** 解析（本仓缺省 compio；打开 `test-tokio-runtime`
/// 时是 tokio），而 smux 自己的 feature 就是按同一条规则在 `Cargo.toml` 里转发给
/// bridge 的（`test-compio-runtime` → `backend-compio` + `default-backend-compio`），
/// 因此这里**只有一条**条件实现，不需要为每个后端各写一份、也不需要 smux 直接依赖
/// 任何后端 crate。
impl ScopeHost for abs_art_bridge::Runtime {
    type Scope = abs_art_bridge::LocalScope;

    fn local_scope(&self) -> Self::Scope {
        abs_art_bridge::Runtime::local_scope(self)
    }
}

/// 默认后端的「从当前上下文取一个运行时值」实现：委托给 bridge 的裸名
/// [`try_current`](abs_art_bridge::try_current) / [`current`](abs_art_bridge::current)。
impl TrRtCurrent for abs_art_bridge::Runtime {
    fn current_rt() -> Self {
        debug_assert!(
            <Self as TrRtCurrent>::try_current_rt().is_some(),
            "取运行时值时调用点不在所选后端的运行时上下文内：`CurrentConnCfg` 的这一前提\
             **由调用者保证**——跨线程使用连接时，每条使用它的线程都必须自己处于后端\
             上下文内（见 `TrRtCurrent` 文档）。",
        );
        abs_art_bridge::Runtime::current()
    }

    fn try_current_rt() -> Option<Self> {
        abs_art_bridge::Runtime::try_current()
    }
}

/// **本 crate 的默认运行时值类型**：就是 bridge 的裸名。
///
/// 它是 [`TrConnCfg::Rt`](super::TrConnCfg::Rt) 的缺省值，也是
/// [`DefaultConnCfg`](super::DefaultConnCfg) 的默认运行时值类型。
///
/// 「哪个后端」这件事因此**只有一个来源**：bridge 的 feature 解析。smux 的
/// `test-tokio-runtime` / `test-compio-runtime` 只是转发者，不再自己维护第二套规则。
pub type DefaultRt_ = abs_art_bridge::Runtime;

/// 编译期断言：默认运行时值必须满足 [`TrConnCfg::Rt`](super::TrConnCfg::Rt) 的约束。
const _: fn() = || {
    fn assert_rt_<R: TrTime + Clone + 'static + ScopeHost>() {}
    assert_rt_::<DefaultRt_>();
};

/// 构造**默认运行时值**：等价于当前默认后端的 `current()`。
///
/// # Panics
///
/// 调用点不在所选后端的运行时上下文内时 panic（文案由各后端给出；tokio 为
/// 「no reactor running」、compio 为「not in a compio runtime」）。这是
/// [`DefaultConnCfg::new`](super::DefaultConnCfg::new) 的既定契约：自动取运行时值
/// 那条路要求调用点已经在运行时里；不在时请走
/// [`DefaultConnCfg::new_with_rt`](super::DefaultConnCfg::new_with_rt) 显式传入。
pub fn default_rt_() -> DefaultRt_ {
    abs_art_bridge::Runtime::current()
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
