# smux_v1_uds_bench

`smux_v1` 在**真实 UNIX domain socket（UDS）**上的性能测试项目。三个后端
（**compio / smol / tokio**）各编译一个可执行文件，在每个可执行文件里跑**同一套**场景：

| 路径（`--modes`） | 客户端一侧 | 服务端一侧 |
| --- | --- | --- |
| `raw`（基准/对照组） | N 条**真实** UDS 连接 | N 条 `accept` 出来的连接 |
| `smux`（被测复用实现） | 1 条真实 UDS + N 条 `smux_v1` 复用子流 | 1 条 `accept` + N 条子流 |

两条路径的**并发度序列一致**（`--cases 2,4,8,16`）、**载荷逐字节一致**（同一个确定性
载荷生成器 + 同一个 `[长度][CRC-32]` 头部）、**分块与环容量一致**；差别只有「传输是 N 条
真 socket 还是 1 条 socket 上的 N 条复用子流」。因此两边的差值可以归因到「复用」本身。

## 1. 怎么跑

```bash
# 单线程环境（1 个跑器，当前线程 + 当前运行时）
./target/release/bench_tokio  --cases 2,4,8,16 --bytes 64M --repeat 2 --runners 1

# 多线程环境（4 个跑器线程，各自持有自己的运行时与本地作用域）
./target/release/bench_tokio  --cases 2,4,8,16 --bytes 64M --repeat 2 --runners 4

# 三个后端 × 两种环境一次跑完（编排器会按需编译）
python3 scripts/run_matrix.py --release            # 等价于 just bench
python3 scripts/run_matrix.py --release --only tokio --runners 1

# 合并成一张总表
python3 scripts/merge_results.py                   # 写 results/summary.md
```

`just --list` 里有全部配方（`just bench` / `just single` / `just multi` / `just one tokio` /
`just smoke` / `just merge`）。

### 命令行（`bench_<后端> --help`）

| 选项 | 缺省 | 含义 |
| --- | --- | --- |
| `--cases` | `2,4,8,16` | 并发客户端数序列（依次跑） |
| `--modes` | `raw,smux` | 被测路径 |
| `--bytes` | `64M` | **每个客户端 / 每条子流**发送的字节数 |
| `--chunk` | `64K` | 每次发送 / 接收的字节数 |
| `--ring-cap` | `64K` | 环容量（smux 子流环与适配器传输环）；必须 ≥ `--chunk` |
| `--repeat` | `3` | 每个用例重复轮数 |
| `--runners` | `1` | **跑器线程数**：`1` = 单线程环境；`N` = N 个跑器线程并列 |
| `--seed` | 固定值 | 载荷种子（两条路径必须一致） |
| `--no-verify` | 关 | 关闭逐帧长度 / CRC-32 核对 |
| `--timeout` | `120` | 单格墙钟上限（秒） |
| `--runtime` | 编译期后端 | 自检：与编译期后端不一致就立刻失败退出 |
| `--out` | `results` | 报告输出目录 |

## 2. 测什么、怎么测

```text
栅栏释放（t0＝所有发送任务共同的起点）
   ├─ 任务 0 写完最后一字节 → t_end(0)
   ├─ 任务 1 写完最后一字节 → t_end(1)
   │      …
   └─ 最后一个任务写完最后一字节 → t_wall（＝墙钟终点，带宽取自它）
                                   ↓ 尾部数据经环 / socket 排空
                                   服务端收齐（参考量，不进带宽）
```

三个量一起看，才能回答「是不是真并发」：

1. **平均带宽** = `总字节 / 墙钟`（MiB/s，1 MiB = 1024²）。
2. **每个发送任务完成时间的方差**：N 个任务的 `t_end(i) − t0`，报出样本方差 / 总体方差 /
   标准差 / 极差 / 变异系数。**假并发（串行化）会把完成时间拉开，方差随 N 线性增长。**
3. **并发因子** = `Σ(任务时长) / 墙钟`：串行时趋近 `1.0`，N 个任务真同时推进时趋近 `N`。
   它不依赖任务时长的分布形状，是判断「有没有真的同时跑」的独立证据。

> **为什么墙钟取「所有发送任务发完」而不是「服务端收齐」**：后者受环容量、`--chunk`、
> 适配器排空策略支配——「换个大环」凭空改变带宽读数。因此服务端收齐只作为参考量写进
> 报告（`drained_secs` 列），两条路径的墙钟口径严格一致。

### 单线程 / 多线程两种环境

| `--runners` | 环境 | 含义 |
| --- | --- | --- |
| `1` | 单线程 | 全部用例依次在**同一个线程、同一个运行时**上跑完 |
| `N > 1` | 多线程 | N 个工作线程，各自持有自己的运行时与本地作用域，从共享队列取用例并列跑 |

为什么要两种：它们回答的是不同问题。单线程给出「这套协议在最小上下文里的上限与方差」，
是干净的可复现基线；多线程给出「多条连接、多个运行时在同一台机器上并存时还成不成立」
——真实部署很少只开一条连接、一个运行时。

**用例内部**在两种环境下都跑在**同一线程的同一个作用域**里：`smux_v1` 的连接内部循环是
`spawn_local` 投出去的 `!Send` 任务，一条连接只能属于一个线程。并列来自「多个用例分布到
多个线程」，不是「把一个用例拆到多个线程」——后者在这套 API 下不成立，也不该成立。

## 3. 输出

每次运行写出三份产物（`--out` 目录下）：

| 文件 | 内容 |
| --- | --- |
| `bench-<后端>.jsonl` | 逐格 JSON 行（原始记录：方差、极差、逐轮明细一个不少） |
| `bench-<后端>-summary.md` | 本次运行的汇总：逐格表、`raw ↔ smux` 对照、结论、失败清单 |
| `bench-summary.csv` | 追加式的跨后端汇总（一个用例一行） |

编排器把「后端 × 环境」各格写进 `results/<后端>-r<跑器数>/`；`scripts/merge_results.py`
再把它们合并成 `results/summary.md`。

## 4. 结果

见 [`results/summary.md`](results/summary.md)（`smux_v1_uds_bench` 里不放结论快照：
数字随机器与版本变，跑一次就有）。

## 5. 实现要点（踩过的坑都在这）

三个后端的**运行时就绪条件**各不相同，本 bench 的壳（`src/rt_*.rs`）把差别收在一处：

- **tokio**：`LocalSet` 必须在**运行时上下文内**取。
  - 每格用例由 [`Drive::drive`](src/lib.rs) 建一个 current_thread 运行时 + 一个 `LocalSet`，
    连接的三个循环与写侧自驱动泵都投在它上面；
  - **写侧可以用 `try_new_local`（自驱动）**：它把「环 → 设备」的泵投到本线程队列上，
    由上面的作用域持续驱动。
    （历史：本 bench 立项时**必须**用它——那时 `smux_v1` 从不替传输收尾，按需形态
    `try_new` 下握手帧会永远留在环里、对端永久等 INVITE。`2026-10` 起 `smux_v1` 按
    `abs_buff::TrBuffFlush` 自己收尾：握手每帧后 `flush_async`、连接退出时 `close`，
    因此 `try_new` 也成立；本 bench 仍保留自驱动形态，因为它少一次显式收尾。）
- **smol**：本地队列在本线程 `thread_local!` 里，必须由调用方驱动；
  `smol::block_on(scope.run_until(..))` 同时驱动队列与用例。
- **compio**：`CURRENT_RUNTIME` 是 thread-local，**取运行时值之前必须先有它**；
  `Runtime::new()` 之后 `default_rt_()` 才成立。每个跑器线程一个 compio 运行时实例。
- **协议**：`raw` 与 `smux` 都走 `buffex_*_adapt` 的环（唯一差别是传输形态）。
- **拨号重试**：两端在同进程里并发起步，客户端可能比服务端的 `bind` 早一步（`ENOENT`），
  因此客户端做「重试 + 退避」。
- **一格一个 socket 路径**：路径里含进程号、用例号与轮次，多跑器线程之间也不会互踩。

## 6. 目录

```text
smux_v1_uds_bench/
├── Cargo.toml            # 独立 workspace（理由见文件内注释）
├── justfile
├── scripts/
│   ├── run_matrix.py     # 后端 × 环境的编排器
│   └── merge_results.py  # 合并成总表
├── src/
│   ├── lib.rs            # Rt / Scope / Tx / Rx 别名、CaseEnv、Drive
│   ├── cli_.rs           # 命令行
│   ├── stats.rs          # 方差 / 标准差 / 并发因子
│   ├── payload.rs        # 确定性载荷 + CRC-32
│   ├── proto.rs          # 12 字节头部与分块
│   ├── barrier.rs        # N 个任务「同时出发」的异步栅栏
│   ├── hub.rs            # 一格用例的共享时间轴
│   ├── transport.rs      # 段级 read_exact / write_all / read_frame
│   ├── case_io.rs        # 用例级动作（等栅栏 / 发一份 / 收一份）
│   ├── raw_case.rs       # 对照组：N 条真实 UDS
│   ├── smux_case.rs      # 被测组：1 条 UDS + N 条复用子流
│   ├── bench_run.rs      # 矩阵编排（单/多跑器）
│   ├── report.rs         # 结果结构 + 报告输出
│   ├── rt_tokio.rs / rt_compio.rs / rt_smol.rs   # 三个壳
│   └── bin/bench_tokio.rs / bench_compio.rs / bench_smol.rs
└── results/              # 报告产物（由运行生成）
```
