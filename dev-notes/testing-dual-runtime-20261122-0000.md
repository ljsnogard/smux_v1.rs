# 测试的双运行时纪律：为什么、怎么做、踩了什么

## 0. 结论（先看这个）

- **涉及异步的测试一律双跑**：用例本体写成 `async fn name_()`，紧随其后写
  `dual_runtime_test_!(name_);` ⇒ 生成 `name_::tokio_` 与 `name_::compio_` 两个真实
  运行时下的测试。宏在 `src/test_support_.rs`，与上游 `buffex` 的同名模块同形。
- **纯同步测试保持单个 `#[test]`**：显示文案、窗口算术、帧编解码字节序列、握手字段
  编解码等完全不碰 future 的用例，给它们各起两个运行时没有收益，只会拖慢测试。
- **测试里不再用 `block_on` 把异步压成同步**。唯一保留的两处是
  `tests/thread_safety.rs`：那里的 `block_on` 是**被测对象本身**（每条竞争线程必须
  自建运行时、在自己的运行时上阻塞一次绑定操作，才能证明句柄跨线程可用），且该场景
  在 tokio 装配下不成立（`LocalScope` 含 `Rc<LocalSet>` ⇒ `!Send`），故只有 compio 版。

## 1. 为什么必须双跑

`block_on` + 同步测试代码测的是「假设的世界」：自己提供一个 waker、自己驱动 future，
于是真实运行时里「谁来唤醒、什么时候唤醒、park 与取消如何交互」这些**正是我们最容易
写错的地方**被整个跳过了。本项目至少支持 tokio 与 compio 两个异步运行时，两者的
park / 唤醒 / 本地队列语义并不相同（compio 是完成式 IO + 线程本地队列，tokio 是
`LocalSet` + epoll 驱动），因此**只在一个运行时上过**不足以说明问题。

## 2. 怎么落地（本轮改动）

| 位置 | 改动 |
| --- | --- |
| `src/test_support_.rs` | 新增：`dual_runtime_test_!` 宏（**不在 `cfg(test)` 下**编译，因为集成测试链接的是正常编译的库）。两种形态：`(name_)` 与 `(name_, "ignore 原因")`——后者把 `#[ignore]` 下发到**宏生成的两个测试函数**上 |
| `src/lib.rs` | `mod test_support_` + `#[doc(hidden)] pub use dual_runtime_test_ as _;`（`#[macro_export]` 只把宏放进宏命名空间，集成测试需要可 `use` 的项） |
| `src/connection/owner_.rs` | 删掉「测试专用同步壳」`with_` / `with_mut_`，用例改 `async` + `.await` |
| `src/connection/sync_.rs` | 同样改 `async`；「手工 poll waker」的两条用例保留手工 poll（那正是被测对象），但整体仍双跑 |
| `src/connection/mux_connection/registry_.rs` | 22 个 `_t_` 同步壳方法改成**异步扩展 trait**（`RegistryTestExt_`），14 个用例改 `async` 并双跑 |
| `src/connection/channel_handle/handle_.rs` | 非正式 benchmark 从 `block_on` 改为 `async` + 双跑 |
| `frame_` / `signal_` / `channel_half/halves_` / `handshake::codec_` / `handshake::agent` | 原本只有 `#[compio::test]` 的 35 个用例全部改成 `dual_runtime_test_!` |
| `handshake::agent` 的两条用例 | 原本用 `abs_art_bridge::Runtime::spawn_local`（compio 专属）→ 改成 `futures::join!` 在同一任务内并发，从而与运行时无关 |
| `tests/inmem_mux.rs` | 6 对 tokio/compio 重复用例合并成「一个 body + 宏」；两条只有 tokio 版的 drop 探针用例也补上双运行时（`yield_now` 改成运行时无关的 `yield_once_!` 宏） |
| `tests/layered_rpc.rs` | 两条重复用例合并成「一个泛型 body + 宏」 |
| `tests/smoke_{tokio,compio}.rs` | 两个近乎逐字重复的文件合并：共享场景挪到 `tests/smoke_common.inc`，两个壳只做「建作用域 + 选运行时」，用 `#[path]` 引入同一份文件 |
| `tests/thread_safety.rs` | 文件头补写「为什么只有 compio、为什么这里 `block_on` 是故意的」 |

## 3. 为什么 `tests/` 的双运行时装配要用 feature

两个运行时的 **socket 类型与设备适配 crate 不同**（tokio 用
`buffex_tokio_adapt::x_deps::abs_buff_tokio_adapt`，compio 用 `buffex_compio_adapt`），
无法用同一个函数体同时表达。做法：

- `Cargo.toml` 增加两个 feature：`test-tokio-runtime` / `test-compio-runtime`，
  **都进 `default`**；
- 共享文件 `tests/smoke_common.inc` 按「`test-tokio-runtime` 是否打开」二选一分派
  socket 装配与作用域类型；
- 两个冒烟 target 各自 `required-features` 选中自己那一侧。

于是：

```bash
cargo test --all-targets        # 默认两个 feature 都在：冒烟跑 tokio 侧
cargo test --test smoke_compio --no-default-features --features test-compio-runtime
                                # compio 侧单独跑（上面的 --all-targets 里它被 cfg 掉，不重复）
```

## 4. 踩到的坑

1. **`#[ignore]` 加在包装函数上无效**。`dual_runtime_test_!(name_)` 生成的是**模块里
   的两个测试函数**；`#[ignore]` 只对测试函数本身生效，加在外层 `async fn` 上会被忽略，
   `cargo test` 照样跑（本轮实测：以为 ignore 了，结果两个都挂住）。因此宏加了第二个
   参数形态专用于「整体标 ignore」。
2. **`#[macro_export]` 不足以让集成测试用宏**：它只把宏放进 crate 根的宏命名空间，
   不产生可 `use` 的项。需要 `#[doc(hidden)] pub use macro_name as _;`。同时宏所在的
   模块**不能**放在 `#[cfg(test)]` 下——集成测试链接的是正常编译的库。
3. **`cargo test --all-targets` 与「按 feature 分派的共享文件」会打架**：我一度让共享
   文件按「compio feature 是否打开」分派，同时把两个 feature 都设成默认，结果
   `smoke_tokio` 被 cfg 掉（跑 0 个用例）。最终统一成**只看 `test-tokio-runtime`**：
   打开 ⇒ tokio 分支，关闭 ⇒ compio 分支。
4. **`tests/` 下的共享 `.rs` 文件会被当成独立测试 target**，于是它会尝试自己编译
   （找不到 `mod common` 而失败）。改用非 `.rs` 后缀（`smoke_common.inc`）+ `#[path]`
   引入，顺带 `#![warn(unused)]` 抑制「同一文件出现在多个 target」的提示。
5. **批量转换会留下「文档注释与函数之间的空行」**，触发 clippy 的
   `empty_line_after_doc_comments`。改完记得 `cargo clippy --all-targets` 收尾。

## 5. 本轮顺带发现并修掉的测试装配缺陷

`tests/common/mod.rs` 的 `connect_pair_` 之前**根本不调用配置的
`make_stage_buffs`**，而是自己调固定的 64 KiB 工厂函数——也就是说上一轮新加的
`TrConnCfg::StageBuff` / `make_stage_buffs` 在集成测试路径上是**死代码**（配置里怎么写
都不生效）。现已改成由配置提供，并顺带把 `connect_pair_` 泛化到可传入自定义配置
（「极小帧暂存」用例就是靠它写的）。

## 6. 一条命令跑完所有 feature 组合

`just test`（配方在 `smux_v1/justfile`，从仓库任何目录调用都可以——内部用
`--manifest-path` 传绝对路径）：

```bash
just test
```

它串行跑五步（`just` 同一条配方的多个依赖是**并行**的，而 cargo 对 target 目录有独占
锁，因此这里刻意用链式依赖保证串行）：

| 步骤 | 命令 | 覆盖的格子 |
| --- | --- | --- |
| `clippy` | `cargo clippy --all-targets -- -D warnings` | 警告即失败 |
| `check-all-features` | 三条 `cargo check --all-targets`：无 feature / 只开 tokio / 只开 compio | 两套设备适配与两个 cfg 分支都能编译 |
| `test-tokio` | `cargo test --all-targets`（缺省 feature） | 133 lib + inmem + layered + thread + smoke_tokio |
| `test-compio` | `cargo test --test smoke_compio --no-default-features --features test-compio-runtime` | compio 侧冒烟 |
| `test-doc` | `cargo test --doc` | 4 个文档用例 |

局部自查另有 `just check` / `just test-target <名>` / `just test-one <名字模糊匹配>`。
`just --list` 可见全部配方。

> 为什么不是一条 `cargo test`：见 §3 —— 同一份共享场景要按 feature 分派两套设备类型，
> 因此缺省 feature 与 `--no-default-features` 各覆盖一侧，单侧 feature 组合再补编译
> 检查。

## 7. 直接跑 cargo 时的等价命令

```bash
cd smux_v1
cargo clippy --all-targets       # 零告警
cargo test --all-targets         # 133 lib + 14(2 ignored) inmem + 2 layered
                                 # + 2 smoke + 2 thread
cargo test --test smoke_compio --no-default-features --features test-compio-runtime
cargo test --doc                 # 4 passed
```

对比改造前：lib 侧 77 → 133 个测试（异步用例各多一份运行时实例），
集成侧用例数不变（重复的运行时副本被合并）。

## 8. 遗留

1. `tests/inmem_mux.rs` 的**最小帧暂存**用例（2 字节容量）当前标 `#[ignore]`：现有
   实现要求「帧暂存环能装下整帧」，逐字节流式解析落地后去掉 ignore 即可（用例本身
   不需要改）。
2. compio 侧冒烟必须显式 `--no-default-features` 才能跑（见 §3）：这是「同一份共享
   文件按 feature 分派 + `--all-targets` 用默认 feature」的必然结果。若将来希望一条
   命令跑全，需要把共享文件拆成两个 feature 各自独立的模块、或引入自定义测试框架。
