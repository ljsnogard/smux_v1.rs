# 连接级失败的通知与唤醒：每子流通知槽 + 收尾守卫

- 日期：2026-10-07（1040 初稿）
- 状态：**已实施**。改动都在 `src/connection/` 内，外加一处**公开枚举新增变体**
  （`MuxError::ConnFailed`，见 §5 的清单）。
- 起因：`smux_v1_sock_demo/dev-notes/known-stall-20261007-0130.md` §3.2 记的缺口——
  **连接级失败不唤醒在册子流上的读写等待者**，应用侧表现为无期限挂起，只能靠子流空闲
  超时兜底。本轮把它补上，并让应用在被唤醒后能读到**连接级**原因。

## 1. 问题拆成两半：唤醒与原因

| 半 | 事实 | 后果 |
| --- | --- | --- |
| **唤醒** | 事件通道的消费者正是**正在退出的那两个循环**；`buffex` 的环半部 **drop 不置关闭位、也不唤醒对端** | 「循环退出 ⇒ 本地表被丢掉」**不产生任何唤醒**：应用侧半部永远 park |
| **原因** | 连接级失败此前只写注册表里的 `fail_` / `fail_kind_`，逐子流状态只有 `IdleTimeout` 一档 | 即使被唤醒（例如空闲超时那条路），应用也读不到「是连接没了」 |

所以修法是两件独立的事：**持有环半部的循环在退出时显式 `close()`**（唤醒），
以及**逐子流写一个不可忽略通知槽**（原因）。

## 2. 通知槽：为什么是「每子流一个」而不是「连接级一个」

每条子流的状态节点 [`ChannelState_`] 上新增一个 `AtomicU32` 通知槽
（`src/connection/owner_.rs`），只放**不可忽略**的事实：

```rust
enum ChannelNotice {
    ConnFailed(ConnCloseReason),   // 连接级失败牵连（细节留在连接级）
    IdleTimeout,                   // 空闲超时（含建流未裁决那一档）
}
```

- **谁写**：`ChannelRegistry_::mark_failed_` 在它**本来就有**的那次遍历里，对每条
  `BindingSlot_::Channel` 顺手写一次（与叫醒建流等待者同一个临界区、同一阶代价，
  因此遍历是零增量）；空闲超时那条路由 `claim_abort_` 顺带写，本来就只有一条。
- **规则**：**只升级、不降级**，同优先级首个生效（优先级 CAS）。于是
  「连接判死前已经发生的大面积子流级错误」会被随后的 `ConnFailed` **一次性压过**；
  反过来永远不会。应用任意时刻读一次拿到的都是「至今最重的结论」。
- **为什么与 `AbortCode_` 分开**：`AbortCode_` 是**动作认领**（CAS 成功的一方负责发
  `CLOSE` / 投 `LocalAbort` / `Release`），必须首个生效、永不改写；通知是**应用读的
  结论**，规则相反。挤进一个字就会让已认领的动作被后到的通知改写。
- **读**：`abort_reason()` 改为由槽位投影——应用被唤醒后读它即可，`IdleTimeout`
  行为不变，新增 `ConnFailed` 一档。槽位是**无锁、零分配**的（`MuxError` 只有 2 字节，
  `ConnCloseReason` 1 字节，因此整个通知编码得进一个 `u32`），任何线程、任何上下文
  （包括 `Drop`）都能写。

## 3. 唤醒：两个内侧循环的收尾守卫

`mux_loop_async_` / `demux_loop_async_` 各自的**本地表**与**事件队列**包进守卫类型
（`MuxLocalGuard_` / `MuxEventsGuard_` / `DemuxLocalGuard_` / `DemuxEventsGuard_`）：

- 用 `Deref` / `DerefMut` 转发，**循环主体逐字不动**（方法调用经自动解引用照常工作），
  只有把表/队列传给辅助函数的那几处写成 `&mut *table` / `&mut *events`；
- `Drop` 里做两件事：**显式 `close()` 本循环持有的那些半部**
  （复用侧关发送环读端 → 唤醒被憋住的写者；解复用侧关接收环写端 → 唤醒等数据的读者），
  以及**排空事件队列里尚未处理的 `Attach`**——连接完全可能在子流刚建好、循环还没取走
  那条事件时就结束，那一拨半部不在表里，不排空同样永久挂起；
- 收尾因此与「表/队列的生命周期」绑定：**任何**退出路径（正常返回、取消、连接级失败，
  乃至 panic 展开）都会跑到，不必在每个 `return` 前手写；
- 顺带把 `ChannelCloseReason::ConnFailed` 的逐子流上报补实（此前**只在文档层面**）：
  判据是「本次收尾伴随连接级失败」+ `claim_release_` 的一次性 CAS，与 `maybe_release_`
  共用同一把闸门，因此每条子流恰好上报一次（正常收尾报 `Fin`，牵连的报 `ConnFailed`）。

**一个反直觉但必要的点**：这条守卫对所有退出路径都跑，不只是失败。对端关掉传输时
解复用循环也会自己退出——此前它的表被直接丢掉，应用读者同样永远醒不过来。守卫把
这一类也一并覆盖了（原因那一档没有连接级失败可报，应用读到的是 EOF 语义的 `Closing`，
与既有语义一致）。

## 4. 建流路径的两处对齐

- **`install_channel_` 里「半部交给循环」可能失败**（对应的循环已经不在了）：失败时
  绝不能把应用侧那半部交出去——它的对端会随失败的事件一起被丢弃，应用一用就永久
  park。现在按连接级失败如实返回，原因按**代价从低到高**取（子流通知槽 → 连接级的
  失败类别快照 → 已关闭），全是同步、锁外读（安装路径本身是同步的，不能为报原因去
  `await` 注册表锁）。
  唯一的例外是**单元测试专用的无循环连接**（`MuxConnection::new_test_`：只建核心与
  两条事件通道、接收端随即被丢弃），那里「投递失败」是构造方式使然，照旧交出去
  （`cfg!(test)`）。
- **`accept` / `reject` 的「建流阶段已有结论」**：原先一律报 `IdleTimeout`，现在先读
  通知槽、按实际原因回答（连接级失败必须如实表达），`IdleTimeout` 作为兜底保留。

（建流等待方本来就有一条 `reg.failure_()` 的检查，见 `owner_::wait_establish_` 第 1 步，
因此那条路不需要改。）

## 5. 公开面变更清单

| 变更 | 性质 | 理由 |
| --- | --- | --- |
| `MuxError::ConnFailed(ConnCloseReason)` | **公开枚举新增变体** | 「子流因连接级失败而结束」需要一个能诚实表达的 `MuxError` 变体：`abort_reason()` 要返回它，`accept` / `install` 那几处也要它 |
| `ChannelTx::abort_reason` / `ChannelRx::abort_reason` / `ChannelHandle::abort_reason` | 语义扩展（签名不变） | 现在还可能返回 `ConnFailed`；文档已重写为「本条子流为什么停下来」+ 固定读取顺序 |
| `ChannelCloseReason::ConnFailed` | 从「只在文档层面」变为**真上报** | 逐子流关闭回调（恰好一次） |
| `ChannelNotice`、四个守卫、`report_conn_failed_` | 内部（`pub(crate)` / 私有） | 无公开面影响 |

`ConnCloseReason` 的编码（`encode_conn_reason_` / `decode_conn_reason_`）刻意写成
`const fn` + `match`，不依赖 `as` 转换或内存布局。

## 6. 回归闸门

| 用例 | 钉住什么 |
| --- | --- |
| `owner_::tests_::conn_failed_notice_overrides_channel_level_reason` | 优先级：`IdleTimeout` → `ConnFailed` 升级成功；反向失败；同优先级首个生效 |
| `owner_::tests_::notice_and_abort_claim_are_independent` | 通知与中止认领是两件事：连接级通知不认领动作，认领也不改写连接级结论 |
| `inmem_mux::mux_conn_failed_wakes_dual_` → `scenarios_/conn_failed_.rs` | 端到端：A 侧传输写半部注入故障（`FailableTx_`）→ `mark_failed_(Transport{write:true})` → **三条子流的双向阻塞全部被唤醒**，且两侧半部的 `abort_reason()` 都是 `ConnFailed(Transport)` |

`mux_conn_failed_wakes_dual_` 的注入是**确定性**的：故障标志由用例置位，随后丢掉一条
子流的发送半边——那条 `CLOSE(FIN)` 会让写泵去碰传输并失败，因此不依赖竞态。
把守卫改成透明包装（不关半部、不排空队列）后，该用例在诊断输出里显示
「原因已发布（`ConnFailed(Transport)`）但半部未关闭」并挂到看门狗 panic——正好证明
**原因与唤醒是两件事**，缺一不可。

## 7. 全量验证

- `cargo test --all-targets`（compio，206 项）与 tokio 配置下 `--lib` + 六个集成目标：全绿；
- 两格 clippy `-D warnings` 干净；`cargo test --doc` 12 项通过；
- `smux_v1_sock_demo` 三组运行时配对全量：见
  `smux_v1_sock_demo/results/conn-failed-verify-*.md`（本轮的收尾守卫在真机 TCP 上同样
  每次都跑）。

## 8. 仍然保留的两条边界（有意为之）

1. **对端关掉传输**时，应用侧读到的是 EOF 语义的 `Closing`，通知槽**不写**
   （没有连接级失败可报）。这与既有语义一致；若将来要区分「对端关连接」与「对端关
   子流」，再加一档 `PeerClosed` 通知即可——优先级表已经为它留了位置。
2. **`MuxCore::drop`（应用主动丢连接）不写通知槽**：半部各自持有 `MuxConnection`
   **强引用**，因此「核心析构」时应用侧不可能还持有任何半部（也没有读者）。
   这条推论也是 `LocalDrop` 这一档从设计里被去掉的原因。
