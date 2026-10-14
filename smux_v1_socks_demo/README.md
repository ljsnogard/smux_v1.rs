# smux_v1_sock_demo

`smux_v1` 在**真实 TCP socket** 上、**两个进程**之间、**三种异步运行时两两配对**的互通
正确性与 metrics demo。

## 1. 它验什么

两个进程（同一份源码按运行时 feature 编译出的两个可执行文件）互联，一次运行里**前后
交替**扮演主动端与被动端：

```text
阶段 1   P_a ──dial──▶ P_b      P_a = 主动端（握手 invite、开子流、发本体）
                                P_b = 被动端（握手 listen、accept 子流、收本体）
阶段 2   P_b ──dial──▶ P_a      P_b = 主动端，P_a = 被动端
```

每条连接上开 `--streams` 条子流（默认 8），每条子流的**两个方向各走一份不同的确定性
载荷**，线格式是极小的信封协议（`src/proto.rs`）：

```text
方向 d1（主动端 → 被动端）：ENVELOPE{ id, len, digest } + len 字节本体 + RECEIPT{ id, digest' }
方向 d2（被动端 → 主动端）：ENVELOPE{ id, len, digest } + len 字节本体 + RECEIPT{ id, digest' }
```

- **先 digest、后本体**：`ENVELOPE` 里带的是发送方**将要发**的那份数据的 CRC-32；
- **回执是交叉证明**：`RECEIPT.digest'` 是接收方**实际算出来**的值，于是
  - 接收方自己就知道「收得对不对」；
  - **原先的主动方**拿到回执后比对 `digest' == digest`，也就证明了「我发的 == 对方收的」。
- 载荷用确定性 PRNG 生成（两端用同一个 `(种子, 子流序号, 侧别)` 各算一份），因此
  「数据一模一样」是算出来的，不依赖传文件、也不依赖磁盘 IO。

三种运行时两两配对共 3 组：(tokio, compio)、(tokio, smol)、(compio, smol)。

### 1.1 可选的 telegraph 混合项（`--mix-telegraph`，默认关闭）

打开后，**同一条连接**上会额外开一条 **telegraph（数据报）**端点，与子流**并发**工作：

- 两端各自在 telegraph 上跑一个**停止等待 + 应用层 ACK** 循环：发一帧 → 等对端回 ACK →
  才发下一帧；ACK 本身也是一条 telegraph 报文（复用 `proto.rs` 的 `RECEIPT` 变体）；
- 每帧载荷是确定性生成的 `--tg-frames` × `--tg-bytes`，接收侧逐字节比对 + 校验 digest，
  并核对报文的**远端地址**（镜像语义：应等于对端的 telegraph dock）；
- 两条通道由**同一线程上的两个并发执行体**推进（`futures::join!`），而不是 `spawn`：
  真正的原因在**传输半边**——compio 的 socket 与三个后端的自驱动泵都绑定本线程
  （`!Send`），搬不到别的 worker 上。至于**连接句柄**本身，`smux_v1` 现在提供
  `CurrentConnCfg`（不在配置里存储运行时值、每次从当前上下文取），三个后端装配下的
  `MuxConnection` 都是 `Send + Sync`——跨线程使用见 `smux_v1/tests/cross_thread_*.rs`。

> **时序要求**：telegraph 端点必须在**子流建流之前**开启。数据报是尽力交付，到达一个
> 尚未注册接收方的 dock 时会被**静默丢弃**，而停止等待没有超时重传——两端各自建流是
> 天然的同步点，把它放在建流之后就能保证并发一开始两边都已有接收方。

## 2. 快速开始

```bash
# 本机环回：三组配对全跑（内部按配对逐格编译 peer，再起两个进程）
just loopback

# 只跑一组，自定义负载
python3 scripts/run_loopback.py --pairs tokio-compio --streams 4 --bytes 1048576

# 追加「同一连接上 channel + telegraph 并发」这一项（默认关闭，见 §3.1）
just mix
python3 scripts/run_loopback.py --mix-telegraph --tg-frames 32 --tg-bytes 512

# 用**真实网卡地址**（非 loopback）跑同一套编排：先验地址与路由，再上两台机器
python3 scripts/run_loopback.py --ip-a 192.168.7.29 --ip-b 192.168.7.100

# 手工起两个进程（示例：tokio 主动端 / compio 被动端）
cargo build --no-default-features --features rt-compio --bin peer_compio
cargo build --no-default-features --features rt-tokio  --bin peer_tokio
target/debug/peer_compio --runtime compio --role passive \
    --listen 0.0.0.0:7002 --peer 127.0.0.1:7001 --streams 8 --bytes 1048576
target/debug/peer_tokio  --runtime tokio  --role active \
    --listen 0.0.0.0:7001 --peer 127.0.0.1:7002 --streams 8 --bytes 1048576
```

每个 peer 结束时会往 stdout 打**一行 JSON**（`{"event":"result",...}`）供编排器回收，
以及一份人看的摘要。`just loopback` 把结果汇总成 `results/loopback-<时间戳>.md`
与同名 `.jsonl`。

### 命令行

`target/debug/peer_<运行时> --help` 有完整列表，要点：

| 参数 | 含义 |
| --- | --- |
| `--runtime <tokio\|compio\|smol>` | 本进程的运行时。**必须与编译期后端一致**，不一致立刻报错退出（运行时是编译期选定的，不能在进程内切换） |
| `--role <active\|passive>` | 本轮谁先当主动端（阶段 1 的 dialer） |
| `--listen <ip:port>` / `--peer <ip:port>` | 本端监听地址 / 对端监听地址（两端都要给全，因为阶段 2 角色对调） |
| `--streams <n>` | 每条连接的子流条数 |
| `--bytes <n>` | 每个方向的本体字节数 |
| `--seed <n>` | 确定性载荷种子（**两端必须一致**） |
| `--chunk <n>` | 单次借段/写入的字节数（默认 2048） |
| `--ring-cap <n>` | 子流环容量 = 本端接收窗口（默认 256 KiB） |
| `--max-packet-size <n>` | 协商的帧总长上限（默认 4096，与协议缺省一致） |
| `--pre-flood-yield <n>` | 建流完成后先给本地队列 n 个 tick 再灌数据（默认 256）。**不是正确性需要**，而是本负载下的节奏选择：实测 release 档 8 子流 × 8 MiB 时 0 约 30 MiB/s、256 约 38 MiB/s（见 §7 的 A/B 证据） |
| `--mix-telegraph` | **无值开关**：额外开一条 telegraph 端点，与子流并发跑「发一帧 → 等 ACK」的停止等待闭环（默认关闭；**两端都要带**，见 §1.1） |
| `--tg-frames <n>` | telegraph 每个方向的报文条数（默认 16） |
| `--tg-bytes <n>` | telegraph 每条报文的载荷字节数（默认 256）。它与 `--max-packet-size` / `--ring-cap` 的一致性在参数解析阶段就校验 |

> 测性能请用 **release** 档：`python3 scripts/run_loopback.py --release`。debug 档会低一个
> 数量级，报出来的数字没有参考价值。

> 后端是**编译期**选定的：`smux_v1` 的三个 `test-*-runtime` 是互斥三选一，所以
> 「选运行时」在实现上是「同一份源码按 feature 编译出的三个可执行文件」。换运行时请用
> `--no-default-features --features rt-<名字>`（`smol` 那格还要注意它是普通同步 `main`，
> 靠 `smol::block_on(scope.run_until(..))` 驱动本地队列）。

## 3. 判据

| # | 判据 | 落在哪 |
| --- | --- | --- |
| 1 | 每条子流、每个方向收到的字节数 == `ENVELOPE` 声明的 `len` | 阶段行的 `recv_bytes`；`is_clean()` |
| 2 | 接收方算出的 digest == 发送方声明的 digest（且逐字节比对通过） | 阶段行的 `recv_verified == streams` |
| 3 | 发起方收到的 `RECEIPT.digest'` == 自己声明的 digest | 阶段行的 `sent_acked == streams` |
| 4 | 子流 `abort_reason()` 为 `None`；对端 `FIN` 之后读到 `Closing` | 失败清单里不出现对应条目 |
| 5 | 两个进程退出码全 0；编排器总超时（默认 180 s）反证「无挂起」 | `scripts/run_loopback.py` |
| 6 | 3 组配对全通过，且每组两个进程都**角色交替**（阶段 1 一个主动端一个被动端，阶段 2 对调） | 编排器的交叉核对 + `report::assemble` 的角色计数 |
| 7 | 开启 `--mix-telegraph` 时：telegraph 每个方向 `tg_frames` 条**全部**被 ACK，且收到的每一条内容与远端地址都校验通过 | 阶段行的 `tg_sent_acked` / `tg_recv_verified`；`is_clean()` |

编排器还会做**跨进程对账**：一端的 `sent_bytes` 必须等于另一端同阶段的 `recv_bytes`，
一端的 `sent_acked` 必须等于另一端的 `recv_verified`。开启 `--mix-telegraph` 时对
telegraph 做同样形状的对账：一端被 ACK 的帧数 == 另一端校验通过的帧数，且两端对该开关的
开启状态必须一致。

## 4. metrics 口径

`smux_v1` 的 metrics 是**可选特性 + 静态分派**（`--features metrics`，零依赖）。本 demo
在 `src/metrics.rs` 里组合了内置采集器：

- **转发给 `DebugMetrics`**：帧数（按方向/帧种）、帧字节、传输字节（两泵口径）、连接数、
  子流数、`WINDOW_UPDATE` 数、`RESET` 数、流控阻塞次数、帧错误数；
- **自己另外记**：子流寿命（和 / 最大 / 分桶 `[<10ms,<100ms,<1s,<10s,>=10s]`）与流控
  阻塞持续时长——内置采集器把 `on_channel_closed` / `on_flow_stalled` 的毫秒参数丢掉了，
  想量延迟类口径必须自己实现 `TrMetricsSink`；
- **吞吐**由 demo 自己用墙钟算（`mib_per_s = 本体字节 / 阶段耗时`）；metrics 本身不带
  时间源。分方向、分阶段报告。

sink 句柄做成**进程级**（`DemoMetrics::global()`，内部 `Arc`）并显式交给连接配置，
因此场景结束后直接读快照、不需要把 sink 从连接里掏出来。

进程级与阶段级两个口径都在 JSON 里：阶段级是**数据面窗口**（连接 / 子流的收尾回调可能
落进下一阶段），进程级是整轮合计（收尾前会先给本地队列若干 tick 让其跑完）。

## 5. 曾经的拦路问题：一次积压超过一个帧（**已在上游修复**）

本 demo 第一轮跑真机时撞到：一条子流的发送环里**一次积压 ≥ `max_packet_size`（缺省 4096）**
字节时，连接静默失败，两端进程 0 CPU 挂住。定位结论是 `smux_v1` 写循环的 `drain_one_`
每次只搬一个逻辑读段，却在搬之前对 `段长 + 帧头 > max_packet_size` 判 `FrameTooLarge`，
而连接级失败又不唤醒在册子流——一次**正常的大写入**被当成了非法帧。触发条件当时被收敛成
`min(环容量, 16 KiB) + 帧头 > max_packet_size`，并用单一变量 A/B 钉住（`results/ab-mps-*`）。

修复已落在 `smux_v1`：每轮的搬运量收敛到 `max_packet_size − 帧头`，`FrameTooLarge` 退回
「真正的非法协商」这一语义；并带一个进程内回归用例（`tests/inmem_mux.rs` 的
`mux_frame_cap_burst_dual_`，修复前 0.07 s 内被看门狗抓成 panic）。因果、源码位置与全量
验证矩阵见 `smux_v1/dev-notes/frame-cap-20261007-0540.md`，本仓的现场记录见
[`dev-notes/known-stall-20261007-0130.md`](dev-notes/known-stall-20261007-0130.md)。

**本 demo 现在不需要任何规避参数**：`--max-packet-size` 默认就是协议缺省 4096，环容量可以
随便开大（默认 256 KiB）。修复前后对照（release，8 子流 × 每方向 8 MiB，双向）：

| | 环回吞吐 | 说明 |
| --- | --- | --- |
| 修复前（规避①：环压到 2048） | ~1 MiB/s | 窗口 2 KiB/子流，`WINDOW_UPDATE` ≈ 2 × `DATA` 帧 |
| 修复前（规避②：帧上限放大到 64 KiB） | 42–46 MiB/s | 绕开检查，但协议参数被改大 |
| **修复后（协议缺省）** | **41.5–44.0 MiB/s** | `results/final-release-20261007-014950.md` |

> **曾经还有一条间歇停摆，现已定位并修复**：在 **debug** 构建 + 8 子流 × 每方向 8 MiB 下，
> 约 1/3 的运行会中途静止（表现为两端不再有字节流动，随后各子流被空闲超时关掉、报
> `Closing`）。它与本节这个帧上限问题是两回事（换帧大小不改变复现率）。定位结论：静止的
> 一侧其实是**满载忙转**——`smux_v1` 复用循环 park 的就绪判据把「发送环生产端已关闭且已
> 排空」当成「可读」，在「回顶部 → drain 取不到段 → 立刻又就绪」之间空转，把同一条单线程
> 本地队列上的解复用循环、两个泵与应用任务全部饿死；让它持续存在的是收尾扫描的头阻塞。
> 三处修复与两个回归闸门见 `smux_v1/dev-notes/closed-ring-spin-20261007-0930.md`，本仓现场
> 记录见 [`dev-notes/intermittent-stall-20261007-0200.md`](dev-notes/intermittent-stall-20261007-0200.md)。
> 修复后 debug 8 × 8 MiB **连跑 8/8 通过**（`results/fix-verify-20261007-093105.md` 是同一
> 负载下三组配对的全量结果），因此「长跑只能用 release / 1 MiB 方向」这条规避建议已作废。

## 6. 跨机（真实局域网）

已实测：**`lino@192.168.7.10`（Deepin / glibc 2.38）↔ 本机 `192.168.7.29`**，三组配对两轮
全过（8 × 1 MiB 与 8 × 8 MiB；跨机 8 × 8 MiB 达 **41.5–46.0 MiB/s**/方向）。完整记录见
[`dev-notes/lan-verified-20261007-0230.md`](dev-notes/lan-verified-20261007-0230.md)。

```bash
export SMUX_LAN_SSH_PASSWORD='<远端密码>'      # 只有密钥认证时不需要
python3 scripts/run_lan.py \
    --ssh-a lino@192.168.7.10 --ip-a 192.168.7.10 \
    --dir-a /home/lino/projects/me.noli/lan-demo/smux_v1_sock_demo \
    --ssh-b local --ip-b 192.168.7.29 --dir-b "$PWD" \
    --ship-binaries --streams 8 --bytes 8388608 --timeout 180
```

脚本要点：

| 开关 | 作用 |
| --- | --- |
| `--ssh-a/--ssh-b` | ssh 目标；写 `local` 表示**本机侧**（不起 ssh，直接就地跑），便于半本机半远端联调 |
| `--ssh-password-env` | 从哪个环境变量读密码（默认 `SMUX_LAN_SSH_PASSWORD`）。实现是 OpenSSH 的 `SSH_ASKPASS` + `SSH_ASKPASS_REQUIRE=force` + `setsid`，**不需要 `sshpass`**；密码只落进 0600 临时脚本、退出即删 |
| `--sync` | 用 `tar` over `ssh` 把四棵源码树推到远端（不必本机有 `rsync`）；本机侧自动跳过 |
| `--ship-binaries` | **本机编译后投递可执行文件**（默认 release）。远端没有匹配 nightly 时走这条 |
| `--dry-run` | 只打印将执行的命令 |

两端都必须给全地址（阶段 2 角色对调）；先起被动端、等它 `ready` 再起主动端（脚本自己做）。

**远端前置条件**：`tar`。若要**在远端自行编译**，还需要与源码匹配的 nightly
（本仓用到 `#![feature(allocator_ext)]` 与 `AllocatorClone`，`1.100.0-nightly`（2026-09-14）
编不过）以及能到 git 依赖源／rustup 镜像。工具链不匹配时用 `--ship-binaries`：实测本机
（glibc 2.41）编出的二进制在 glibc 2.38 的机器上可直接运行。

## 7. 验证状态（本仓已实测到的边界）

| 阶段 | 配置 | 结果 | 证据 |
| --- | --- | --- | --- |
| 修复后：**之前必挂的配置** | 环 4096 / `max-packet-size` 4096 / 一次写 64 KiB | **通过** | `results/fixed-regression-20261007-014305.md` |
| 修复后：release 全量 | 3 组配对，8 子流 × 每方向 8 MiB，协议缺省 | **3/3 通过，41.5–44.0 MiB/s** | `results/final-release-20261007-014950.md` |
| 修复后：debug 全量（无让位） | 3 组配对，8 × 1 MiB | **3/3 通过** | `results/fixed-noyield-debug-20261007-014602.md` |
| **修复后：debug 全量（停摆回归）** | 3 组配对，**8 × 8 MiB**（协议缺省） | **3/3 通过、12 个相位全过** | `results/fix-verify-20261007-093105.md` |
| **传输侧改由适配器自驱动**（手写泵删除后） | 3 组配对，**8 × 8 MiB**（debug，协议缺省） | **3/3 通过、12 个相位全过** | `results/selfdrive-final-20261007-222807.md`（同配置另一次：`…-full-20261007-222133.md`） |
| 同上：复现件（只删手写泵） | 1 子流 × 64 KiB，tokio/compio | 失败（tokio 20 s 超时；对端见半帧） | `results/stage-b-probe-20261007-214645.md` |
| 节奏 A/B（`--pre-flood-yield`） | 0 / 256（其余相同，各一次） | 30.2 / 38.8 MiB/s | `results/ab-yield-yield0-*.md` / `…yield256-*.md` |
| 根因 A/B（历史） | 只改 `--max-packet-size`：4096 / 65536 | 挂 / 过 | `results/ab-mps-20261007-013553.md` / `…013554.md` |
| 基线（修复前，含环 2048 规避） | `127.0.0.1`，3 组，8 × 1 MiB（debug） | 3/3 | `results/loopback-local-20261007-013003.md` |
| 基线：同机双真实网卡地址 | `192.168.7.29` ↔ `192.168.7.100`，同上 | 3/3 | `results/loopback-lanaddr-20261007-013049.md` |
| **两台真实机器（局域网）** | A=`lino@192.168.7.10`，B=本机，3 组配对，8 × 8 MiB，release | **3/3 通过，41.5–46.0 MiB/s** | `results/lan-20261007-022322.md` |

「同机双地址」只是把**跨机要用的地址与命令形态**先验一遍，**不能**替代两台机器的实测：
跨机还多出真实链路时延、链路 MTU、两端时钟差这些变量，正是本实验要看的东西。

## 8. 目录结构

| 路径 | 职责 |
| --- | --- |
| `src/lib.rs` | 类型别名（`Rt` / `Scope` / `Buff` / `Tx` / `Rx` / `Cfg`）、运行时自检、作用域排空 |
| `src/cli.rs` | 手写命令行解析（不引 clap） |
| `src/proto.rs` | 32 字节定长信封（`DATA` / `RECEIPT`）编解码 |
| `src/payload.rs` | 确定性载荷生成 + CRC-32（分块无关） |
| `src/metrics.rs` | `DemoMetrics`（组合内置采集器）+ 快照 / 增量口径 |
| `src/transport.rs` | 缓冲供给（连接级帧暂存、子流环内存）+ 子流半边的段级读写（含严格版 `read_exact`）。**没有任何设备 IO** |
| `src/scenario.rs` | 与运行时、传输无关的场景主体：握手 → 建连接 → 开/收子流 → 信封/回执 → 汇总 |
| `src/cfg.rs` | 连接配置（自备子流环存储 + 进程级 sink） |
| `src/rt_tokio.rs` / `rt_compio.rs` / `rt_smol.rs` | 三格后端相关的三件事：建 socket、造 `buffex_*_adapt` 的读写适配器（写侧走自驱动形态）、用作用域驱动「场景 + 排空」 |
| `scripts/run_loopback.py` | 本机环回编排（编译 → 起两进程 → 收 JSON → 交叉核对 → 出报告） |
| `scripts/run_lan.py` | 跨机编排（经 ssh 起两进程、回收两边结果行） |
| `dev-notes/` | 实测记录（含上面那条停摆的现场与分辨实验） |

## 9. 与 `smux_v1` 的关系

- **不改 `smux_v1` 的公开面**：传输适配、泵、信封协议、metrics sink 全在本仓；
- 连接配置 `DemoCfg` 自己实现（形状照 `smux_v1/tests/common/config_.rs`），因此不依赖
  `DefaultConnCfg` 的 `M::default()` 约定，也不走 `MuxChanBuff` 那条仓库验收没覆盖的
  缓冲实现；
- 两条跨进程纪律（本 demo 的实测结论）：
  1. **主体结束后不能立刻丢连接**：环里剩下的回执 / `FIN` 会随连接消失。驱动层要先让
     本地队列继续 tick（`drain_scope`），再 `drop(conn)`，**然后再 tick 一轮**——写端适配器
     在 drop 时「关生产端 + detach 后台泵」，尾巴由泵搬完并 `shutdown` 设备，而泵仍需要
     本线程继续驱动队列；
  2. **`TrBuffWrite` 没有 flush 钩子**，所以「发完就完事」不成立——这正是写侧必须用
     **自驱动**形态（`BuffWrite::try_new_local`）的原因，也是判据必须建立在**对端回执**
     上的原因（本 demo 正是这么做的），而不是建立在「我写完了」上；
- **节奏旋钮**（不是纪律）：`--pre-flood-yield` 在建流完成与灌数据之间让位若干 tick。
  它最初是「建流帧被灌数据挤掉」那个现象的绕法，而那个现象的根因与 §5 是同一个
  `FrameTooLarge` 静默失败——上游修好之后它**不再是正确性需要**，但实测仍能让 release
  吞吐从约 30 提到约 38 MiB/s，因此保留为有文档、可 A/B 的性能旋钮。
