# smux_v1 的验收批次（`event-loop-redesign` §19 的 T28）。
#
# 每一批都带 `timeout`：挂起的用例（历史上 `thread_safety` 与若干集成场景都出现过）
# 不会把整轮拖死，而是以退出码 124 明确失败。超时值按 AGENTS.md §12 取：
# 单个单元测试批次 ≤60 s、全量批次 ≤5 min 量级。
#
# 用法：
#   just            # 等价于 `just all`
#   just lib        # 单元测试
#   just inmem      # inmem_mux，compio / tokio / smol 三后端
#   just integration
#   just alloc
#   just doc
#   just lint
#   just all        # 上面全部
#
# 离线环境：`export CARGO_NET_OFFLINE=true`（Cargo 自己识别该变量）。

# 默认目标：全量。
default: all

# 单元测试：全部 `--lib` 用例，硬上限 60 s。
lib:
    timeout 60 cargo test -p smux_v1 --lib

# 同一份内存场景在三个后端下各跑一遍。
inmem:
    timeout 120 cargo test -p smux_v1 --test inmem_mux
    timeout 120 cargo test -p smux_v1 --no-default-features --features test-tokio-runtime --test inmem_mux
    timeout 120 cargo test -p smux_v1 --no-default-features --features test-smol-runtime --test inmem_mux

# 集成验收：保活、冒烟、跨线程、线程安全、指标端到端。
integration:
    timeout 120 cargo test -p smux_v1 --test keepalive_compio
    timeout 120 cargo test -p smux_v1 --test smoke_compio
    timeout 120 cargo test -p smux_v1 --test cross_thread_compio
    timeout 60 cargo test -p smux_v1 --no-default-features --features test-tokio-runtime --test thread_safety
    timeout 120 cargo test -p smux_v1 --features metrics --test metrics_e2e

# 分配计数基线（只在 tokio 装配下有意义：它需要一个进程级全局分配器）。
alloc:
    timeout 120 cargo test -p smux_v1 --no-default-features --features test-tokio-runtime --test alloc_count

# 文档测试。
doc:
    timeout 120 cargo test -p smux_v1 --doc

# 静态检查（全部 target；workspace `authors` 提示是既有噪声）。
lint:
    timeout 300 cargo clippy -p smux_v1 --all-targets

# 全量。
all: lib inmem integration alloc doc lint
