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
                                 # + 2 smoke + 2 thread   ← 本轮快照
cargo test --test smoke_compio --no-default-features --features test-compio-runtime
cargo test --doc                 # 4 passed
```

> 上面这段是**本轮快照**；用例数已随后续几轮上涨，最新的验收数字见 §9。

对比改造前：lib 侧 77 → 133 个测试（异步用例各多一份运行时实例），
集成侧用例数不变（重复的运行时副本被合并）。

## 8. 遗留

1. ~~`tests/inmem_mux.rs` 的**最小帧暂存**用例（2 字节容量）当前标 `#[ignore]`~~
   **已解决**（2026-10-05 更新）：逐字节流式解析落地后该用例已去掉 `#[ignore]` 并通过
   （见用例自身的文档说明）。
2. compio 侧冒烟必须显式 `--no-default-features` 才能跑（见 §3）：这是「同一份共享
   文件按 feature 分派 + `--all-targets` 用默认 feature」的必然结果。若将来希望一条
   命令跑全，需要把共享文件拆成两个 feature 各自独立的模块、或引入自定义测试框架。

## 9. 2026-10-05 追加：`tests/common/` 的模块拆分

**问题**：`tests/common/mod.rs` 涨到 **1978 行**，把「连接配置 / 缓冲构造 / 传输泵 /
socket 装配 / 子流读写 / 载荷生成 / 建连 / 五组场景」全塞在一个文件里，与纪律 5
（`mod.rs` 只负责导出层级控制、不得用 `*`）相悖，也不便于审计。

**落点**：`mod.rs` 现在只有 **89 行**——`//!` 文档（分工表 + 四条跨文件约定）+ 子模块
声明 + **逐项** `pub use`（无 `*`、无逻辑）。逻辑按「一件事一个文件」拆成 14 个：

| 文件 | 职责 |
| --- | --- |
| `config_.rs` | 两套连接配置与全部容量常量 |
| `buff_.rs` | 帧暂存 / 子流环缓冲构造 |
| `pump_.rs` | 调用方驱动的两条泵 + 全被动环 |
| `socket_.rs` | socket 装配与两个运行时的驱动入口 |
| `closure_.rs` | `TrPrepareChannelRing` 的测试侧适配 |
| `channel_io_.rs` | 子流半边整段读写与 EOF 等待 |
| `payload_.rs` | 确定性载荷生成 |
| `scenarios_/` | 场景主体：`connect_` / `kit_` / `smoke_` / `small_` / `bind_` / `buffers_` / `flow_ctrl_` |

**机械拆分暴露出的两处真实耦合**（不是拆分引入的，是原来被同一个文件掩盖的）：

1. **跨文件复用的私有件**：`pump_input_` / `pump_output_`（socket 装配用）与
   `run_mux_scenario_` / `drive_side_` / `exchange_and_half_close_`（`smoke_` 与
   `small_` / `buffers_` 互用）。兄弟模块看不见彼此的私有项，因此前者放宽为
   `pub(super)`（仍不出 `common`），后三者收进 `scenarios_/kit_.rs` 并 `pub(super)`
   ——按「共用件」而不是「放宽一堆可见性」处理。
2. **只在方法语法里用到的 trait 必须显式 `use`**：拆分后每个文件自带 `use`，而
   `segm.least_count()` / `err.err_tag()` / `handle.accept_async()` 这类调用不会在代码里
   出现 trait 名，漏了就报 `E0599`（`TrBuffSegmView` / `TrTaggedError` /
   `TrBuffSegmMut` / `TrBuffSegmRef` / `TrChannelHandle` / `TrChannelListener` /
   `TrConnection` / `TrDockBinding` / `TrChannelHalf` 都踩到了）。这是拆测试辅助模块时
   最容易漏的一类，**靠编译器提示逐个补齐**比肉眼找可靠。

**验收**（2026-10-05）：

- 拆分前后**顶层项名集合逐项相同**（`fn` / `struct` / `trait` / `enum` / `type` / `const`
  的名称与出现次数做 `diff`，为空）——没有任何东西被漏掉或重复；
- `cargo check --all-targets` 与 `cargo clippy --all-targets -- -D warnings`（缺省与
  compio 两侧）**零告警**；
- 可运行目标全绿：167 lib + 18 inmem + 4 smoke_tokio + 4 smoke_compio（`--no-default-features`）
  + 2 thread_safety + 4 doc；
- `layered_rpc`（2 条）在本环境**挂死**，与拆分无关：`git stash` 到 HEAD 原状同样挂死
  （见 `outlook-…` §12-T5 的排查项）。

**生成方式**：拆分由一次性脚本完成（`mod.rs` 只导出、子文件逐个搬运），脚本留在
`target/split3.py`（未跟踪，`cargo clean` 会清掉）；它在同样输入下**逐字节可复现**。
若希望把「怎么拆的」也纳入审计，可以把它移进仓库并跟踪。
