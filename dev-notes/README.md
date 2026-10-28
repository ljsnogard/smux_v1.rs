# dev-notes

这个文件夹用来存放开发过程中遇到的技术困难，以及记录技术决策的前因后果。

## 文件规范

- 文件名：`[模块名]-[日期]`，日期格式 `YYYYMMDD-hhmm`；
- 每篇开头写清：分支 / 基线提交 / 状态 / 相关模块；
- 状态分四档：
  - **设计稿**：正在评审、尚未实施的方案；只能作为讨论依据，不得被当作现行实现引用；
  - **现行**：描述当前实现，可直接参考；
  - **历史**：已完成的决策，仍解释当前架构的由来；
  - **已归档**：与当前实现冲突，移到 `archive/`，开头必须加醒目说明与现行文档链接。

## 索引

| 文档 | 状态 | 一句话 |
| --- | --- | --- |
| `event-loop-redesign-20261026-1930.md` | **设计稿（已评审，P0–P3 完成，P4 位化完成、`PendingFin_` 待做，P5 请求/Attach 通道已换 `mpmc_chan`，分配器配置改 `TrConnAllocCfg` GAT、连接内复合结构已全部改为 `C` 参数（`EventAlloc` 未接））** | 三业务循环 + 计时服务 + 公共报文子流（ring + chunked）+ 建流期 message ring + arena 索引（bplus_tree_cmp）+ 请求通道 + TCS |
| `sw-scope-20261028-2130.md` | **现行（保守方案 + crate 级 `RootScope` 已实施）** | sw-scope 原理、`TrConnAllocCfg` 逐项可行性判定；`TcsAlloc` / `ScratchAlloc` 迁入连接级 Scope，删除 `EventAlloc`；crate 级 `RootScope` 显式发布 |
| `alloc-ownership-20261029-0230.md` | **现行（R1：Scope 基座；R2：建流期 message；R3：open/reject + tg_live；R4：retired 列表；R5：删除 PendingFin；R6：main 发 FIN；R7：main 发 RESET）** | 按对象真实生命周期重分 Global / 专用池 / Scope；每循环 `LoopScope_` 基座落地（一次性 scratch 暂留连接级）；R2 清 main message 与 `ControlFrame_.payload_`；R3 清 open/reject 与 demux；R4 清 retired 扫描列表；R5 删除 `PendingFin_`；R6 增加 `TX_DRAINED` / `FIN_ENQUEUED`，main 发 FIN；R7 把 RESET 也迁到 main（`RESET_ENQUEUED`）；后续 T1 尾 / P5 的清理表 |
| `connection-delivery-20261026-1845.md` | **现行** | 从 socket 收到什么帧、交给哪些对象、当前用什么队列/槽交付；含重复路径与待决问题 |
| `event-backpressure-20261026-0514.md` | **现行** | 拆流/释放通知位化、`Attach` 迁入内联 inbox、容量 256、锁与锁外状态收敛的实施记录 |
| `no-stage-20261015-1955.md` | 历史 | 去掉连接级 stage 环与两个贴传输的泵 |
| `flush-contract-20261015-2340.md` | 历史 | 写侧收尾从「必须自驱动」到 `TrBuffFlush` |
| `channel-close-20261017-0108.md` | 历史 | 子流级可等待收尾（`TrBuffFlush` / `TrChannelRx::close_async`） |
| `listener-queue-20261015-0330.md` | 历史 | `smux_v1_listener` 交付队列：ring + 写权仲裁 |
| `listener-close-20261015-0353.md` | 历史 | listener 关闭模型：去掉令牌参数 + `close_async` |
| `dock-macro-20261017-2254.md` | 历史 | dock 宏化与无损 `From` 转换 |
| `uds-bench-20261015-2240.md` | 历史 | 三后端 × 两环境的 UDS 基准项目 |
| `no-vec-audit-20261024-0008.md` | 历史（基线段已过时） | `Vec` 清单、必要性与替代方向 |
| `archive/mpmc-chan-20261026-0046.md` | 已归档 | `flume` → `mpmc_chan` 的历史决策与实测；事件分类/容量/取消令牌结论已被取代 |

## 当前架构速查

先读 `connection-delivery-20261026-1845.md` 的交付矩阵，再读
`event-backpressure-20261026-0514.md` 的实施记录。**在交付矩阵定稿前，不再新增队列、
通道、槽或 `Vec`。**

下一轮重构的方向已整理在 `event-loop-redesign-20261026-1930.md`：三业务循环 + 计时
服务 + 公共报文子流（ring + chunked）+ 建流期 message ring + arena 索引 + 请求通道 + TCS。该文档评审通过前，现有代码仍按上面的“不再新增”纪律执行；
评审通过后，新代码只允许按该文档的队列/槽/ring 形状落地。
