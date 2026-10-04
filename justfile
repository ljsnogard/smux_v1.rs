# smux_v1 的测试配方：一条 `just test` 跑完所有 feature 组合。
#
# # 为什么需要 feature 矩阵
#
# 本项目至少支持 tokio 与 compio 两个异步运行时，而两个运行时的 **socket 类型与
# 设备适配 crate 不同**（tokio 走 `buffex_tokio_adapt`，compio 走
# `buffex_compio_adapt`），无法用同一个函数体同时表达。于是：
#
# - `test-tokio-runtime` / `test-compio-runtime` 两个 feature **都在缺省里**；
# - `tests/smoke_common.inc`（两个冒烟 target 共用的场景）按「`test-tokio-runtime`
#   是否打开」二选一分派装配；
# - 两个冒烟 target 各自用 `required-features` 选中自己那一侧。
#
# 因此**一条 cargo 命令跑不完所有格子**：
#
# - `cargo test --all-targets`（缺省）⇒ tokio 侧冒烟 + 其余全部集成用例；
# - `--no-default-features` ⇒ compio 侧冒烟 + 其余全部（tokio 侧冒烟被 cfg 掉）；
# - 两个「只开一侧」组合 ⇒ 验证单侧编译与另一套设备适配。
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

# 全部 feature 组合的完整验收（串行）：clippy → 编译矩阵 → tokio 侧 → compio 侧 → 文档
test: test-doc
    @echo
    @echo "== 全部 feature 组合通过 =="

# 文档测试（不需要运行时 feature）
test-doc: test-compio
    @echo "== 测试：文档 =="
    cargo test --manifest-path {{manifest}} --doc

# compio 侧冒烟（单独指定 target + --no-default-features）
test-compio: test-tokio
    @echo "== 测试：compio 侧冒烟（--no-default-features --features test-compio-runtime）=="
    @echo "注：必须 --no-default-features，否则 test-tokio-runtime 仍会被默认打开、"
    @echo "    共享场景会走 tokio 分支；本 target 在 --all-targets 下被 cfg 掉。"
    cargo test --manifest-path {{manifest}} --test smoke_compio --no-default-features --features test-compio-runtime

# tokio 侧全部集成用例（缺省 feature 下的 `--all-targets`）
test-tokio: check-all-features
    @echo "== 测试：tokio 侧（缺省 feature，--all-targets）=="
    cargo test --manifest-path {{manifest}} --all-targets

# 编译矩阵：三种非缺省 feature 组合都要能编过
check-all-features: clippy
    @echo "注：这里用 check 而不是 test —— 这几格要的是「能编译」，"
    @echo "    运行覆盖由 test-tokio / test-compio 两步负责。"
    @echo "== 编译：无运行时 feature（compio 分支）=="
    cargo check --manifest-path {{manifest}} --all-targets --no-default-features
    @echo "== 编译：只开 tokio feature =="
    cargo check --manifest-path {{manifest}} --all-targets --no-default-features --features test-tokio-runtime
    @echo "== 编译：只开 compio feature =="
    cargo check --manifest-path {{manifest}} --all-targets --no-default-features --features test-compio-runtime

# 静态检查：库 + 所有测试目标（警告即失败）
clippy:
    @echo "== 静态检查：clippy --all-targets -D warnings =="
    cargo clippy --manifest-path {{manifest}} --all-targets -- -D warnings

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
