//! 「运行时值能交出本地队列」这条能力的**本地**契约。
//!
//! # 为什么本 crate 自己定义一条，而不直接用 `abs_art_facade::ScopeHost`
//!
//! `abs_art_facade::ScopeHost` 只对**门面自己那个** `Runtime` 实现，而门面的 `Runtime`
//! 由「哪个 `rt-*` feature 被点亮」决定。本 crate 的设计前提恰恰是**运行时选择不由
//! feature 决定**（`SmuxListener` 的运行时值由
//! [`TrListenerConfig::Rt`](crate::config::TrListenerConfig::Rt) 交出，因此同一个
//! listener 实现可以在任意后端上跑）。若直接依赖门面的 `ScopeHost`，使用者就必须让
//! 「门面选中的后端」与「实际传入的运行时值」一致——那正是本 crate 想避开的东西。
//!
//! 因此这里为三套运行时的具名 `Runtime` 各写一条实现。它们是外部类型 + 本地 trait，
//! 孤儿规则允许。
//!
//! # 与 [`TrTime`](smux_v1::time::TrTime) 的关系
//!
//! 两者正交：[`TrScopeHost`] 回答「把任务投到哪条队列」，`TrTime` 回答「怎么等、
//! 现在几点」。listener 两者都要，因此
//! [`TrListenerConfig::Rt`](crate::config::TrListenerConfig::Rt) 同时约束它们。

use abs_art::TrLocalScope;
use smux_v1::{
    time::TrTime,
    x_deps::abs_art,
};

/// **能交出本地作用域**的运行时值。
///
/// # 为什么它不能由 `abs_art` 统一给出
///
/// 「取本地作用域」在各后端里是 `Runtime::local_scope()` 这个**固有方法**（不是 trait
/// 入口），泛型代码里写不出 `rt.local_scope()`。本 trait 就是那条缺口的补丁，与
/// `abs_art_facade::ScopeHost` 目的相同，区别只在于**本 crate 自己定义**它，从而不再
/// 受门面的 feature 选择影响。
///
/// # 契约
///
/// 同一线程上多次调用必须交出**同一条**队列的别名（语义与各后端的 `local_scope()`
/// 一致）；返回的作用域是 `!Send` 的线程本地句柄。
pub trait TrScopeHost {
    /// 本运行时值交出的本地作用域类型。
    type Scope: TrLocalScope + Clone + 'static;

    /// 取本线程那条本地队列的别名。
    fn local_scope(&self) -> Self::Scope;
}

/// 编译期自查：三套运行时的具名 `Runtime` 都必须满足「可克隆 + 提供时间 + 能交出作用域」
/// 这组约束——那是 [`TrListenerConfig::Rt`](crate::config::TrListenerConfig::Rt) 的全部
/// 要求。
const _: fn() = || {
    fn assert_rt_<R>()
    where
        R: TrTime + Clone + TrScopeHost + 'static,
    {
    }

    #[cfg(feature = "rt-tokio")]
    assert_rt_::<abs_art_tokio::Runtime>();

    #[cfg(feature = "rt-compio")]
    assert_rt_::<abs_art_compio::Runtime>();

    #[cfg(feature = "rt-smol")]
    assert_rt_::<abs_art_smol::Runtime>();
};

#[cfg(feature = "rt-tokio")]
mod tokio_ {
    use abs_art_tokio::{LocalScope, Runtime};

    use super::TrScopeHost;

    impl TrScopeHost for Runtime {
        type Scope = LocalScope;

        fn local_scope(&self) -> Self::Scope {
            Runtime::local_scope(self)
        }
    }
}

#[cfg(feature = "rt-compio")]
mod compio_ {
    use abs_art_compio::{LocalScope, Runtime};

    use super::TrScopeHost;

    impl TrScopeHost for Runtime {
        type Scope = LocalScope;

        fn local_scope(&self) -> Self::Scope {
            Runtime::local_scope(self)
        }
    }
}

#[cfg(feature = "rt-smol")]
mod smol_ {
    use abs_art_smol::{LocalScope, Runtime};

    use super::TrScopeHost;

    impl TrScopeHost for Runtime {
        type Scope = LocalScope;

        fn local_scope(&self) -> Self::Scope {
            Runtime::local_scope(self)
        }
    }
}
