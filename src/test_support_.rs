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
