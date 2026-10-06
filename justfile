# smux_v1 的测试配方：一条 `just test` 跑完所有 feature 组合。
#
# # 为什么需要 feature 矩阵
#
# 本项目至少支持 tokio 与 compio 两个异步运行时，而两个运行时的 **socket 类型与
# 设备适配 crate 不同**（tokio 走 `buffex_tokio_adapt`，compio 走
# `buffex_compio_adapt`），无法用同一个函数体同时表达。于是：
#
# **缺省只开 `test-compio-runtime`**，与「默认后端 = compio」这条设计一致：
#
# - `cargo test --all-targets`（缺省）⇒ **compio 装配的全量**（含 compio 冒烟与
#   keepalive_compio），一条命令就是一个完整、对称的装配；
# - `--no-default-features --features test-tokio-runtime` ⇒ tokio 装配的全量
#   （含 smoke_tokio 与 keepalive）；
# - `--no-default-features --features test-compio-runtime` ⇒ 与缺省等价（用于确认
#   「不开任何 feature」之外的显式写法）。
#
# 为什么**不**把两个 feature 都放缺省里：`TrConnCfg::Rt` 取的是
# `default_rt_()`——「哪个后端是默认」由 feature 唯一决定。两个都开时缺省解是 compio，
# 于是 `tests/keepalive.rs`（tokio 装配）会在自己的 tokio 上下文里调 compio 的
# `current()` 而 panic；同时 `examples/` 无法开启测试 feature，示例也就跟着坏掉。
# 相关因果见 `dev-notes/timer-mock-clock-and-generic-drop-20261006-1625.md` §6。
#
# `test` 配方把这三类组合都跑一遍，因此不需要任何参数即可覆盖全部 feature 组合。
# 因果与踩过的坑见 `dev-notes/testing-dual-runtime-20261122-0000.md`。
#
# # 两个写法上的约定
#
# 1. **每条 cargo 都带 `--manifest-path {{manifest}}`**：just 1.58 的
#    `set working-directory` 不接受函数调用（`justfile_directory()` 在 const 上下文里
#    不可用），因此改成给 cargo 传绝对 manifest 路径——配方从仓库任何子目录调用都能跑。
# 2. **用「配方互相依赖」而不是 `&&`**：just 同一条配方的多个依赖是**并行**执行的，
#    而 cargo 对同一个 target 目录有独占锁，并行跑几条 cargo 会互相等锁、日志交错。
#    链式依赖（`a: b`）由 just 保证**串行**推进，同时每一步在 `just --list` 里可见。

# 绝对路径的 manifest（`--manifest-path` 必须跟在子命令之后）
manifest := justfile_directory() / "Cargo.toml"

# 列出可用配方
default:
    @{{just_executable()}} --list

# 全部测试的完整验收（串行）：clippy → 编译矩阵 → tokio 装配全量 → compio 装配全量 → 文档
#
# **两种装配各自跑一遍，而不是靠 cfg 把跑不了的格子跳过去**：同一个 cargo 进程里
# 「谁是默认后端」只能有一个答案（bridge 的裸名 `Runtime` 必须唯一），所以
# 全部测试的完整验收：两种装配各自跑一遍。
test: test-doc
    @echo
    @echo "== 全部装配下的测试通过 =="

# 文档测试（不需要运行时 feature）
test-doc: test-compio
    @echo "== 测试：文档 =="
    cargo test --manifest-path {{manifest}} --doc

# compio 装配的全量（缺省 feature；示例也在这个装配下跑）
test-compio: test-tokio
    @echo "== 测试：compio 装配（缺省 feature，--all-targets）=="
    cargo test --manifest-path {{manifest}} --all-targets

# tokio 装配的全量（显式 opt-in）
#
# 刻意**不带 `--all-targets`**：example 是 compio 传输，在 tokio 后端下编得过也
# 没有意义（它是「默认后端」的演示，归 compio 那一趟）。测试目标逐个列出，一个不少。
test-tokio: check-all-features
    @echo "== 测试：tokio 装配（--no-default-features --features test-tokio-runtime）=="
    cargo test --manifest-path {{manifest}} --no-default-features --features test-tokio-runtime \
        --lib --test smoke_tokio --test keepalive --test inmem_mux --test layered_rpc --test alloc_count --test thread_safety
    @echo "注：thread_safety / alloc_count / keepalive 都是 tokio 专属（它们要一个"
    @echo "    `Send + Sync` 的运行时值或 tokio 的 socket），只在本次运行里有意义。"

# 示例：默认后端（compio）的端到端演示，也是 README §2 的落地版本
demo:
    cargo run --manifest-path {{manifest}} --example active_passive

# 编译矩阵：三种非缺省 feature 组合都要能编过
check-all-features: clippy
    @echo "注：这里用 check 而不是 test —— 这几格要的是「能编译」，"
    @echo "    运行覆盖由 test-tokio / test-compio 两步负责。"
    @echo "== 编译：缺省（compio）=="
    cargo check --manifest-path {{manifest}} --all-targets
    @echo "== 编译：只开 tokio feature =="
    cargo check --manifest-path {{manifest}} --all-targets --no-default-features --features test-tokio-runtime
    @echo "== 编译：只开 compio feature =="
    cargo check --manifest-path {{manifest}} --all-targets --no-default-features --features test-compio-runtime

# 静态检查：库 + 所有测试目标（警告即失败）
#
# **两个装配都查**：tokio 专属的测试文件在缺省（compio）装配下被 cfg 掉，
# 只查缺省会漏掉它们的 lint。
clippy:
    @echo "== 静态检查：clippy -D warnings（缺省 compio 装配）=="
    cargo clippy --manifest-path {{manifest}} --all-targets -- -D warnings
    @echo "== 静态检查：clippy -D warnings（tokio 装配）=="
    cargo clippy --manifest-path {{manifest}} --no-default-features --features test-tokio-runtime \
        --lib --test smoke_tokio --test keepalive --test inmem_mux --test layered_rpc \
        --test alloc_count --test thread_safety -- -D warnings

#-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
# 局部自查用（不参与 `just test`）
#-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

# 只查编译，比 clippy 快；改完接口先跑这个
check:
    cargo check --manifest-path {{manifest}} --all-targets

# 单个测试目标，用法：just test-target inmem_mux
test-target target:
    cargo test --manifest-path {{manifest}} --test {{target}}

# 单个用例（按名字模糊匹配，两个运行时下的变体都会跑到），用法：just test-one frame_head
test-one filter:
    cargo test --manifest-path {{manifest}} --all-targets {{filter}}
