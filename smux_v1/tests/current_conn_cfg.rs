//! `CurrentConnCfg`：**不存储**运行时值的配置——它让连接在默认（compio）装配下也能
//! `Send + Sync`，代价是「取用点必须处于后端上下文内」这条**调用者责任**。
//!
//! 本文只钉**编译期**性质（类型是否 `Send + Sync`）。运行期语义（`try_current` 的
//! 成功 / 失败分支、debug 构建下的断言提示）由 `abs_art` 各后端的 doctest 与本仓类型
//! 文档承担。

use smux_v1::connection::{BufferedRx, BufferedTx, CurrentConnCfg, MuxConnection};

/// 测试 `CurrentConnCfg` 让 `MuxConnection` 在**默认（compio）装配**下也是
/// `Send + Sync`——这正是本配置存在的理由。
/// - 手段：取 `CurrentConnCfg<BufferedTx, BufferedRx>`（`M` / `P` / `Rt` 全取默认值）
///   与 `MuxConnection<该配置>` 两个类型，分别交给只接受 `Send + Sync` 的断言函数。
///   本文件不带任何 `test-*-runtime` feature 门，因此它也**必须**在默认 compio 装配下
///   编译通过。
/// - 判断：本测试能编译即证明两个断言成立。对照组 `DefaultConnCfg` 在同一装配下因
///   `config_: C` → `rt_: Rt` → `Rc<compio_executor::Executor>` 而是 `!Send + !Sync`，
///   无法用正向断言表达（Rust 没有稳定的「不实现某 trait」写法），只能由
///   `tests/thread_safety.rs`（tokio 侧）与 `mux_connection::core_` 的类型文档钉住。
#[test]
fn current_conn_cfg_makes_connection_send_sync_() {
    fn assert_send_sync<T: Send + Sync>() {}

    type Cfg = CurrentConnCfg<BufferedTx, BufferedRx>;

    assert_send_sync::<Cfg>();
    assert_send_sync::<MuxConnection<Cfg>>();
}
