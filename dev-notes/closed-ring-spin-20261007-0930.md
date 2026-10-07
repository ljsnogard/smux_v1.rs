# 复用循环把「环已关闭且已排空」当成「可读」：间歇停摆的根因与修复

- 日期：2026-10-07（0930 初稿）
- 状态：**已定位、已修复**。修复在 `src/connection/session_.rs` 的三处（见 §4），带两个
  回归闸门（§5）；跨进程真机复现与修复后验证见 §5.3。
- 现象记录（本 note 的输入）：`smux_v1_sock_demo/dev-notes/intermittent-stall-20261007-0200.md`
  ——「debug + 8 子流 × 每方向 8 MiB + 双向」下约 1/3 的运行中途静止，两端不再有字节流动。
- 与已修的「一次积压超过一个帧」（`frame-cap-20261007-0540.md`）**不是同一个洞**：
  本条是概率性的、换帧大小不改变；那位是确定性的、换帧大小即消失。

## 1. 现象里最要紧的一条：停摆不是「睡着」，是**忙转**

旧记录只写了「两端不再有字节流动」，因此第一分岔（循环停了 / 循环在跑但不推进）没有
结论。这次先把它测出来：给两个 peer 每 2 秒采一次 `ps -o pcpu`（CPU 时间是**自进程启动
以来的平均**，因此「平均值不随时间衰减」就等于「一直在烧满一个核」）。

```text
# 排障期每 2 秒一次的 CPU 采样（复现轮：tokio 被 200 s 超时杀掉，compio 一直等到对端消失）
1791334380  199  97.5 peer_tokio      ← 全程 ~97%，从未停下
1791334417  235   5.3 peer_compio    ← 只在前 ~13 s 干活，之后一直睡着
```

原始采样留在 `smux_v1_sock_demo/results/stall-cpu-samples-20261007-0930.log`
（第 1、2 列是 epoch 与进程年龄秒数，第 3 列是 `ps -o pcpu`）。

**一侧忙转、另一侧睡着**：忙转的那侧在单线程本地队列上把其它任务（解复用循环、两个泵、
以及应用自己的任务）全部饿死——这就是「两端同时静止」的来源：它不是两端互等，而是
一端把自己的队列锁死、对端随之无数据可收。

## 2. 取证：一个只增计数的循环心跳看门狗

下一步要知道「谁在忙转」。做法：临时给五个本地循环（复用 / 解复用 / 两个泵 / 计时）以及
几处内层搬运循环各加一个只增的原子计数，再由一个**独立的 OS 线程**每 3 秒把所有计数落一行
（独立线程不受单线程运行时影响，即使主线程卡在某个循环的单次迭代里也照样有采样）。

复现那一轮的心跳现场（pid=11 = 忙转的那侧，pid=8 = 睡着的那侧）：

```text
[spin] pid=11 t=168s mux=2418825 demux=17826 rx-pump=3170 tx-pump=4917 timer=9
       enqueue=37108 drain-once=12169588 flush=2417912 write-ring=16562
[spin] pid=8  t=168s mux=591     demux=17864 rx-pump=3248 tx-pump=3970 timer=20
       enqueue=37108 drain-once=77150   flush=13      write-ring=16542
```

（完整 200 行原始日志：`smux_v1_sock_demo/results/stall-spin-probe-20261007-0930.log`。）

读法：pid=11 的 `mux` / `drain-once` / `flush` 三个计数一路暴涨到千万级，而
**`enqueue` 停在 37108 不动**（一个字节也没往连接写环里搬），`demux` / 两个泵 / 应用侧的
`demo-*` 也全部冻结。也就是：

> 复用循环在「回到顶部 → 排空失败 → 立刻又就绪」之间空转，一帧都没写出去。

再加一个「连续 N 轮无进展就 dump 一次状态」的临时钩子，拿到空转那一刻的完整表状态：

```text
[spin-note] pid=11 SPIN last_ready=Some((Dock(4101), Dock(1))) ring_seg=false
            pending_fin=3 evq=0 table=4
  | (4096,1): credit=Some(0)      ring=81952  pclosed=true  cclosed=false
  | (4100,1): credit=Some(81888)  ring=0      pclosed=true  cclosed=false
  | (4101,1): credit=Some(131040) ring=0      pclosed=true  cclosed=false   ← last_ready
  | (4103,1): credit=Some(0)      ring=262144 pclosed=false cclosed=false
  | pf=(4096,1) pf=(4100,1) pf=(4101,1)
```

四个事实一次说清：

1. `last_ready = (4101,1)` 的发送环**生产端已关闭**（应用 `drop(tx)`）**且已排空**
   （`ring=0`），额度为正（`credit=131040`）；
2. 那次环读**没有借到段**（`ring_seg=false`）——它只是「完成了」，返回的是 `Closing`；
3. 它**也在** `pending_fin` 里，却始终没被收尾（否则早被摘掉、`last_ready` 也就查不到它）；
4. `pending_fin` 里键序最小的 `(4096,1)` 额度为 0、环里还有 81952 字节——**推不动**。

## 3. 因果链

三处代码事实叠在一起：

1. **就绪判据把「错误完成」当「可读」**（`mux_loop_async_` 第 3 步 park 里的那条
   「最近通知过的发送环」）：判据是 `Future::poll(..).is_ready()`。而环的读 future 在
   **生产端已关闭且已排空**时**立刻**完成（返回 `Closing`），在消费端已关闭 / 需求不可
   满足时同样立刻完成。那几种完成都不意味着 `drain_one_` 有活可干：它 `try_read` 一样
   取不到段，返回 `false`；于是 `continue` 回顶部、drain 失败、park 又立刻就绪——
   **永久空转**。
2. **收尾扫描有头阻塞**（`mux_loop_async_` 第 2.5 步）：`while let Some(pair) =
   pending_fin.iter().next()`，每轮只取键序最小的一条，推不动就 `break`。于是「永远等不到
   额度」的 `(4096,1)` 把「只差一条 `FIN`」的 `(4100,1)` / `(4101,1)` 饿死，它们长期留在
   本地表里——正是第 1 条那条空转路径**能持续存在**的前提。
3. **`finalize_entry_` 的「不可判定」分支没登记**：`has_writable_` 返回 `None`（写者正占着
   环）时直接 `return Ok(false)`，注释写的是「下轮再来」，但**没有登记进 `pending_fin`**
   ——没有任何东西保证「下轮」会再来。同一条子流会在本地表里带着已关闭的环滞留。

为什么只在 debug 出现：debug 下单帧处理慢一个数量级、相位耗时 ~12.6 s（release ~1.5 s），
「先 `drop(tx)`、环里还压着数据、随后额度回补把环排空」这个窗口被放大约 8 倍，撞上
「前面还有一条推不动的子流」的概率随之上升。这也解释了为什么 release 一直没见到。

为什么看起来像「两端互等」：忙转发生在**连接内**的单线程本地队列上，应用任务与解复用
循环都被饿死；对端只是收不到数据，于是「一侧 100% CPU、另一侧 0 CPU」。此前把它当成
「丢唤醒」来找，方向正好相反。

## 4. 修法（都在 `src/connection/session_.rs`，无公开 API 变化）

1. **就绪判据取「真的借到了段」**：把 park 里那一段（额度检查 + 环读）抽成
   [`last_ready_has_segment_`]，环读只认 `Poll::Ready(outcome) if outcome.contains_left()`
   ——只有错误一律按「无可搬运」处理。额度检查（`send_available_try_`）保持不变：它原本
   就是为堵掉同一个空转形态的另一半而加的（额度为 0 时 drain 也发不出去）。
2. **收尾扫描去头阻塞**：第 2.5 步改用游标推进——推不动的那条**跳过**、继续试后面的；
   某条真正收尾（表项被摘）时再从最小的一条重扫。终止性：`Ok(true)` 必然摘掉一条
   （有限），`Ok(false)` 只推进游标，游标走到尽头即结束。
3. **「不可判定」也要登记**：`has_writable_ == None` 与 `Some(true)` 一样
   `pending_fin.insert(pair)`，让「下轮再来」真的有人触发。

三处都是内部行为修正，公开 API、协议、对外约定一律未动。

## 5. 回归闸门与验证

### 5.1 单元用例（钉第 1 条）

`src/connection/session_.rs` 的 `tests_::last_ready_has_segment_rejects_closed_ring_`：
用测试环 + 一份「有额度」的共享状态装出一张写侧表，在四种形态下问
[`last_ready_has_segment_`]——环里有数据 / 环空且生产端开着 / **环空且生产端已关闭** /
环里有数据但额度为 0。只有第一种必须为真。把判据改回 `is_ready()` 时，这个用例在形态三
上失败（已实测）。

### 5.2 集成用例（钉第 2、3 条）

`tests/inmem_mux.rs` 的 `mux_closed_ring_spin_dual_` →
`tests/common/scenarios_/closed_ring_spin_.rs`：`P`（对端窗口小且**永不消费**，额度用尽后
环里永远有积压）与 `Q`（载荷略大于窗口；应用写完即半关闭，对端读走已收到的部分触发窗口
回补，复用循环把 `Q` 环里剩下的字节发完 → `Q` 变成「生产端已关闭且已排空、额度为正」）。
判据：对端必须读满 `Q` 的载荷并读到 `EOF`，读与等 `EOF` 都包在「让出 20 万轮仍无进展即
panic」的看门狗里。

- 只回退第 2 条（保留第 1 条）：5.7 s 内被看门狗抓成 panic（`Q` 的 `FIN` 被 `P` 饿死）；
- 两条都回退：空转会把看门狗一起饿死，用例挂到测试超时（配方里的 `timeout 60`）。

### 5.3 跨进程真机与修复前后对照

同一台机器、两个进程、真实 TCP，`--pairs tokio-compio --streams 8 --bytes 8388608`
（debug，环 256 KiB，协议缺省帧上限 4096）：

| | 运行 | 结果 |
| --- | --- | --- |
| 修复前 | 3 轮 | 1 轮复现（tokio 200 s 超时、全程 ~97% CPU、没有结果行） |
| 修复前 | 1 轮，带心跳探针 | 立刻复现，并留下 §2 的现场与状态 dump |
| **修复后** | **8 轮** | **8/8 通过**；心跳计数全程平稳、无 runaway（`enqueue` 一直在涨） |
| **修复后** | **3 组运行时配对全量** | **3/3 通过、12 个相位全过**，`results/fix-verify-20261007-093105.md` |

上表前三行是本次排障时在 `smux_v1_sock_demo` 上用 `run_loopback.py` 跑的**临时**日志
（工作区临时目录、不随仓库保留）；最后一行是留在仓库里的全量结果，命令即
`python3 scripts/run_loopback.py --label fix-verify`（debug，三组配对，8 子流 × 每方向
8 MiB）。

### 5.4 仓库内全量

`cargo test --all-targets`（compio）与 `--no-default-features --features test-tokio-runtime`
下的 `--lib` + 六个集成目标：全绿；两格 clippy `-D warnings` 干净。

## 6. 顺带记录

- 第 3 条（`finalize_entry_` 的 `None` 分支）是这次一并堵上的**同族洞**：它与第 2 条合起来
  决定「环已关闭且已排空」的条目会不会长期滞留。只修第 1 条也能让空转消失，但那条子流的
  `FIN` 会一直欠着（对端只能靠空闲超时收场），因此三条一起修。
- 仍然**未修**、需要单独裁决的是失败传播语义：连接级失败（`mark_failed_`）只唤醒建流 /
  监听等待者，**不唤醒在册子流上的读写等待者**，因此任何连接级故障在应用侧都表现为无期限
  挂起（本仓 `src/metrics/mod.rs` 的「已知缺口」与
  `smux_v1_sock_demo/dev-notes/known-stall-20261007-0130.md` §3.2 都记着它）。这次的空转
  被修掉之后它不再被触发，但缺口本身还在。
- 这次用到的临时探针（循环心跳 + 独立线程看门狗 + 状态 dump）**已删除**，不在最终代码里。
