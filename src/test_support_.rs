//! 测试共用的支持代码（仅 `cfg(test)` 编译）。
//!
//! 与上游 `buffex` 的同名模块同形：**同一个用例逻辑，在 tokio 与 compio 两个真实
//! 运行时下各跑一遍**。理由与那边的模块文档一致——手写 `block_on`、或自己轮询
//! future，测的是「假设的世界」，而不是真实运行时里由 waker 驱动的行为；本项目至少
//! 支持两个异步运行时，因此凡是**涉及异步**的用例都必须两边都过。
//!
//! 用法：把用例逻辑写成 `async fn name_()`（不带任何测试属性），紧随其后写
//! `dual_runtime_test_!(name_);` 即可。
//!
//! 注：纯同步、完全不涉及 future 的用例（显示文案、窗口算术、字节序列编解码等）
//! 仍用普通 `#[test]`——给它们各起两个运行时没有收益，只会拖慢测试。

/// 让一个异步用例在 **tokio** 与 **compio** 两种**真实运行时**下各跑一遍。
///
/// 用法：把用例写成 `async fn name_()`，紧随其后写 `dual_runtime_test_!(name_);`。
/// 不自己 `block_on`、不手动轮询——那样测的是「假设的世界」，而不是真实运行时里
/// 被 waker 驱动的行为。
///
/// 两种形态：
///
/// - `dual_runtime_test_!(name_)`：正常跑；
/// - `dual_runtime_test_!(name_, "原因")`：两个运行时下的用例都**标记为 `#[ignore]`**。
///   注意 `#[ignore]` 必须加在**宏生成的测试函数**上：加在外层那个 `async fn` 包装上
///   是无效的（`#[ignore]` 只对测试函数本身生效）。
///
/// 让一个异步用例在**当前启用的那一个**运行时下跑一遍。
///
/// # 与 `dual_runtime_test_!` 的分工
///
/// `dual_runtime_test_!` 生成**两个**变体（tokio + compio），前提是用例本身与运行时
/// 无关。但连接的第五个循环（保活 / 空闲超时）要**真正等一段时间**，而等待能力挂在
/// 后端类型上：tokio 的 `LocalScope` 与 compio 的 `LocalScope` 是两个类型，同一个
/// 用例体不可能同时是两者。于是这类用例改用本宏：**每个 feature 组合只生成一个变体**，
/// 用例体按同一个 feature 选作用域类型（见 `tests/inmem_mux.rs` 顶部的 `LocalScope`
/// 选择）。
///
/// 反例（本宏诞生的直接原因）：`tests/inmem_mux.rs` 原先一律用 tokio 的 `LocalScope`
/// 再套 `dual_runtime_test_!`。前四个循环不碰计时器，所以那种写法一直「看起来能跑」
/// （tokio 的 `LocalSet::run_until` 在 compio 运行时里也能驱动任务）；第五个循环一落地
/// 就会在 compio 运行时里调 `tokio::time::sleep`，直接 panic「no reactor running」。
///
/// 用法与 `dual_runtime_test_!` 相同：把用例写成 `async fn name_()`，紧随其后写
/// `single_runtime_test_!(name_);`。
#[macro_export]
macro_rules! single_runtime_test_ {
    ($name:ident) => {
        #[cfg(feature = "test-tokio-runtime")]
        #[allow(non_snake_case, missing_docs)]
        mod $name {
            #[tokio::test]
            async fn tokio_() {
                super::$name().await
            }
        }

        #[cfg(not(feature = "test-tokio-runtime"))]
        #[allow(non_snake_case, missing_docs)]
        mod $name {
            #[compio::test]
            async fn compio_() {
                super::$name().await
            }
        }
    };
}

#[macro_export]
macro_rules! dual_runtime_test_ {
    ($name:ident) => {
        #[allow(non_snake_case, missing_docs)]
        mod $name {
            #[tokio::test]
            async fn tokio_() {
                super::$name().await
            }

            #[compio::test]
            async fn compio_() {
                super::$name().await
            }
        }
    };
    ($name:ident, $ignore_reason:literal) => {
        #[allow(non_snake_case, missing_docs)]
        mod $name {
            #[tokio::test]
            #[ignore = $ignore_reason]
            async fn tokio_() {
                super::$name().await
            }

            #[compio::test]
            #[ignore = $ignore_reason]
            async fn compio_() {
                super::$name().await
            }
        }
    };
}
