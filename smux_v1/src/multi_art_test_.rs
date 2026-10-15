//! 测试共用的支持代码（**多异步运行时**用例的生成器）。
//!
//! 与上游 `buffex` 的同名模块同形：**同一个用例逻辑，在多个真实异步运行时下各跑
//! 一遍**。理由与那边的模块文档一致——手写 `block_on`、或自己轮询 future，测的是
//! 「假设的世界」，而不是真实运行时里由 waker 驱动的行为；本项目至少支持三个异步
//! 运行时（tokio / compio / smol），因此凡是**涉及异步**的用例都必须三边都过。
//!
//! 用法：把用例逻辑写成 `async fn name_()`（不带任何测试属性），紧随其后写
//! `multi_art_test_!(name_);` 即可。
//!
//! 注：纯同步、完全不涉及 future 的用例（显示文案、窗口算术、字节序列编解码等）
//! 仍用普通 `#[test]`——给它们各起三个运行时没有收益，只会拖慢测试。

/// 让一个异步用例在 **tokio**、**compio** 与 **smol** 三种**真实运行时**下各跑一遍。
///
/// 用法：把用例写成 `async fn name_()`，紧随其后写 `multi_art_test_!(name_);`。
/// 不自己 `block_on`、不手动轮询——那样测的是「假设的世界」，而不是真实运行时里
/// 被 waker 驱动的行为。
///
/// # 三个变体各自的驱动方式
///
/// | 变体 | 生成的测试属性 | 谁来驱动 |
/// | --- | --- | --- |
/// | `tokio_` | `#[tokio::test]` | tokio 运行时（自带执行体） |
/// | `compio_` | `#[compio::test]` | compio 的线程本地运行时 |
/// | `smol_` | `#[test]` + `smol::block_on` | `block_on` 作为外部阻塞驱动源 |
///
/// smol 与前两者不同，**它没有隐式运行时上下文**：本地队列在本线程的
/// `thread_local!` 里，必须由调用方驱动，因此只能生成普通 `#[test]` 再自己
/// `block_on`（与 `tests/smoke_smol.rs` 的壳同因）。
///
/// # 三个变体**无条件**全部生成
///
/// 本宏不看 `test-*-runtime` feature：三个后端的测试属性与 `block_on` 在所有装配下
/// 都能编译（三者都在 dev-dependencies 里），而**覆盖面**正是本宏存在的理由——
/// `cargo test --all-targets`（缺省 compio 装配）因此也能把同一份逻辑在 tokio 与
/// smol 上跑一遍。前提仍是同一条：**用例体必须与运行时无关**（不取得后端专有的
/// 运行时值、不直接调用某个后端的 IO / 计时器）。需要真实后端能力（保活要真等一段
/// 时间、计时与时刻来自运行时值）的用例改用
/// [`single_runtime_test_!`](crate::single_runtime_test_)——它按 feature 只生成一格。
///
/// # 两种形态
///
/// - `multi_art_test_!(name_)`：正常跑；
/// - `multi_art_test_!(name_, "原因")`：三个运行时下的用例都**标记为 `#[ignore]`**。
///   注意 `#[ignore]` 必须加在**宏生成的测试函数**上：加在外层那个 `async fn` 包装上
///   是无效的（`#[ignore]` 只对测试函数本身生效）。
#[macro_export]
macro_rules! multi_art_test_ {
    ($name:ident) => {
        #[allow(non_snake_case, missing_docs)]
        mod $name {
            /// 本用例的 tokio 变体；测试目标 / 手段 / 判断见 `super` 下同名用例的文档。
            /// Tokio variant; see the same-named case in `super` for goal, means, criteria.
            #[tokio::test]
            async fn tokio_() {
                super::$name().await
            }

            /// 本用例的 compio 变体；测试目标 / 手段 / 判断见 `super` 下同名用例的文档。
            /// Compio variant; see the same-named case in `super` for goal, means, criteria.
            #[compio::test]
            async fn compio_() {
                super::$name().await
            }

            /// 本用例的 smol 变体；测试目标 / 手段 / 判断见 `super` 下同名用例的文档。
            /// Smol variant; see the same-named case in `super` for goal, means, criteria.
            ///
            /// smol 没有隐式运行时上下文，因此这里用普通 `#[test]` 加 `block_on`
            /// 作外部驱动源（理由见宏文档）。
            #[test]
            fn smol_() {
                smol::block_on(super::$name())
            }
        }
    };
    ($name:ident, $ignore_reason:literal) => {
        #[allow(non_snake_case, missing_docs)]
        mod $name {
            /// 本用例的 tokio 变体（暂被忽略）；目标 / 手段 / 判断见 `super` 下同名用例。
            /// Tokio variant (ignored); see the same-named case in `super`.
            #[tokio::test]
            #[ignore = $ignore_reason]
            async fn tokio_() {
                super::$name().await
            }

            /// 本用例的 compio 变体（暂被忽略）；目标 / 手段 / 判断见 `super` 下同名用例。
            /// Compio variant (ignored); see the same-named case in `super`.
            #[compio::test]
            #[ignore = $ignore_reason]
            async fn compio_() {
                super::$name().await
            }

            /// 本用例的 smol 变体（暂被忽略）；目标 / 手段 / 判断见 `super` 下同名用例。
            /// Smol variant (ignored); see the same-named case in `super`.
            #[test]
            #[ignore = $ignore_reason]
            fn smol_() {
                smol::block_on(super::$name())
            }
        }
    };
}

/// 让一个异步用例在**当前启用的那一个**运行时下跑一遍。
///
/// # 与 `multi_art_test_!` 的分工
///
/// `multi_art_test_!` 生成**三个**变体（tokio + compio + smol），前提是用例本身与
/// 运行时无关。但连接的计时循环（保活 / 空闲超时）要**真正等一段时间**，而计时与
/// 时刻来自**运行时值**（`abs_art::TrTime`）：三个后端的运行时值是三份不同类型
/// （作用域亦然），同一个用例体不可能同时是三者。于是这类用例改用本宏：**每个
/// feature 组合只生成一个变体**，用例体按同一个 feature 选运行时值与作用域
/// （见 `tests/inmem_mux.rs` 顶部的选择）。
///
/// 反例（本宏诞生的直接原因）：`tests/inmem_mux.rs` 原先一律用 tokio 的 `LocalScope`
/// 再套 `multi_art_test_!`。两个搬运循环不碰计时器，所以那种写法一直「看起来能跑」
/// （tokio 的 `LocalSet::run_until` 在 compio 运行时里也能驱动任务）；计时循环一落地
/// 就会在 compio 运行时里调 `tokio::time::sleep`，直接 panic「no reactor running」。
///
/// # 三个 feature **互斥三选一**
///
/// 生成的变体按 `test-tokio-runtime` → `test-smol-runtime` → compio（缺省）的顺序
/// 取**第一个**打开的：smol 与另外两个一样没有「隐式上下文」，必须由普通 `#[test]`
/// 加 `smol::block_on` 驱动（同 [`multi_art_test_!`] 的 smol 变体）。三个 feature
/// 同时打开时 smux 自己的 `DefaultRt_` 就没有唯一解，因此那本就是非法装配。
///
/// 用法与 `multi_art_test_!` 相同：把用例写成 `async fn name_()`，紧随其后写
/// `single_runtime_test_!(name_);`。
#[macro_export]
macro_rules! single_runtime_test_ {
    ($name:ident) => {
        #[cfg(feature = "test-tokio-runtime")]
        #[allow(non_snake_case, missing_docs)]
        mod $name {
            /// 本用例的 tokio 变体；测试目标 / 手段 / 判断见 `super` 下同名用例的文档。
            /// Tokio variant; see the same-named case in `super` for goal, means, criteria.
            #[tokio::test]
            async fn tokio_() {
                super::$name().await
            }
        }

        #[cfg(feature = "test-smol-runtime")]
        #[allow(non_snake_case, missing_docs)]
        mod $name {
            /// 本用例的 smol 变体；测试目标 / 手段 / 判断见 `super` 下同名用例的文档。
            /// Smol variant; see the same-named case in `super` for goal, means, criteria.
            #[test]
            fn smol_() {
                smol::block_on(super::$name())
            }
        }

        #[cfg(not(any(
            feature = "test-tokio-runtime",
            feature = "test-smol-runtime"
        )))]
        #[allow(non_snake_case, missing_docs)]
        mod $name {
            /// 本用例的 compio 变体（缺省后端）；目标 / 手段 / 判断见 `super` 下同名用例。
            /// Compio variant (default backend); see the same-named case in `super`.
            #[compio::test]
            async fn compio_() {
                super::$name().await
            }
        }
    };
}
