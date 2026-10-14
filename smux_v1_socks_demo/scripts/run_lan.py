#!/usr/bin/env python3
"""跨机编排：经 ssh 在**两台真实机器**上各起一个 peer 进程，跑完一组配对并回收结果。

与 `run_loopback.py` 共用同一套判据与交叉核对（`cross_check`），区别只在「进程起在哪」：

- 被动端在 B 机上后台跑，stdout 落到本地日志文件，等它打出 `ready` 事件；
- 主动端在 A 机上前台跑，带超时；
- 两边各回收一行 `result` JSON，做跨机对账。

用法：

    python3 scripts/run_lan.py \
        --ssh-a root@192.168.7.29 --ssh-b root@192.168.7.100 \
        --ip-a 192.168.7.29 --ip-b 192.168.7.100 \
        --dir-a /root/projects/me.noli/old-cantare/smux_v1_sock_demo \
        --dir-b /root/projects/me.noli/old-cantare/smux_v1_sock_demo

`--dry-run` 只打印将要执行的命令，便于先核对地址与端口。
"""

from __future__ import annotations

import argparse
import datetime
import json
import os
import pathlib
import shlex
import subprocess
import sys
import time
from typing import Any

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))

from run_loopback import PAIRS, cross_check  # noqa: E402  （同目录复用，路径上面已插入）

DEMO_DIR = pathlib.Path(__file__).resolve().parent.parent
# 远端要能编译，就得连**路径依赖**的子树一起同步：demo 依赖 `../smux_v1` 与
# `../buffex_smol_adapt`，smux_v1 又依赖 `../abs_art/*`。因此同步这四棵树，
# 并且让 demo 目录在远端落在与本地相同的位置（依赖是相对路径）。
REPO_ROOT = DEMO_DIR.parent
SYNC_PATHS = ["smux_v1_sock_demo", "smux_v1", "abs_art", "buffex_smol_adapt"]


# 跨机 ssh 的状态：密码（从环境变量读，避免落到命令行与进程表里）与临时 askpass 脚本。
SSH_STATE: dict[str, Any] = {"password": None, "askpass": None}
# `--ship-binaries`：本机编译、把可执行文件投递过去（远端没有匹配的 nightly 时用）。
SHIP: dict[str, Any] = {"enabled": False, "profile": "release"}

# 这几种写法表示「就在本机跑」，不走 ssh（便于半本机半远端联调）。
LOCAL_HOSTS = {"local", "localhost", "-", ""}


def is_local(host: str) -> bool:
    """`local` / `localhost` / `-` 表示本机侧。"""
    return host.strip() in LOCAL_HOSTS


def ssh_invocation(host: str, remote: str) -> tuple[list[str], dict[str, str]]:
    """返回 `(argv, env)`：本机走 shell，远端走 ssh（有密码时经 `SSH_ASKPASS` 喂）。

    OpenSSH 在**没有 tty** 且设置了 `SSH_ASKPASS` + `SSH_ASKPASS_REQUIRE=force` 时会调用
    askpass 程序取密码，因此 `setsid -w` 把 ssh 从控制终端上摘下来即可，不需要 `sshpass`。
    """
    env = dict(os.environ)
    if is_local(host):
        return ["bash", "-uc", remote], env
    argv = [
        "ssh",
        "-o", "StrictHostKeyChecking=no",
        "-o", "UserKnownHostsFile=/dev/null",
        "-o", "ConnectTimeout=10",
    ]
    if SSH_STATE["password"]:
        env["SSH_ASKPASS"] = str(SSH_STATE["askpass"])
        env["SSH_ASKPASS_REQUIRE"] = "force"
        argv = ["setsid", "-w"] + argv
    else:
        argv += ["-o", "BatchMode=yes"]
    argv += [host, remote]
    return argv, env


def run_ssh(host: str, remote: str, timeout: float, dry: bool) -> tuple[int, str]:
    """同步跑一条远端命令，返回 `(退出码, 合并输出)`。"""
    argv, env = ssh_invocation(host, remote)
    if dry:
        print(f"    [dry-run] {shlex.join(argv)}")
        return 0, ""
    proc = subprocess.run(argv, text=True, capture_output=True, timeout=timeout, env=env)
    return proc.returncode, (proc.stdout or "") + (proc.stderr or "")


def cargo_prefix(host: str) -> str:
    """远端 cargo 常常不在非交互 PATH 里，显式补上 `~/.cargo/bin`。"""
    return "" if is_local(host) else 'export PATH="$HOME/.cargo/bin:$PATH"; '


def check_toolchain(host: str, timeout: float, dry: bool) -> str:
    """看一眼远端工具链：edition 2024 + allocator_api 需要 nightly。"""
    if is_local(host):
        remote = "cargo -V; rustc -V"
    else:
        remote = cargo_prefix(host) + "cargo -V; rustc -V"
    code, out = run_ssh(host, remote, timeout, dry)
    if code != 0:
        return f"（{host} 上取不到 cargo/rustc：{out.strip()[:200]}）"
    return " ; ".join(line.strip() for line in out.splitlines() if line.strip())


def sync_tree(host: str, dir_remote: str, timeout: float, dry: bool) -> bool:
    """把四棵必要的源码树同步到远端（`tar` over `ssh`，不依赖本机有 rsync）。

    远端目录与本地保持**同样的相对位置**（依赖是相对路径），落在 `--dir-*` 的父目录下。
    本机侧（`--ssh-x local`）跳过同步：源码本来就在那儿。
    """
    if is_local(host):
        print(f"   {host} 是本机侧，跳过同步", flush=True)
        return True
    root_remote = str(pathlib.PurePosixPath(dir_remote).parent)
    code, _ = run_ssh(host, f"mkdir -p {shlex.quote(root_remote)}", timeout, dry)
    if code != 0:
        print(f"   远端建目录失败：{root_remote}")
        return False
    tar_argv = [
        "tar",
        "--exclude=target",
        "--exclude=.git",
        "--exclude=results",
        "--exclude=__pycache__",
        "-czf", "-",
        "-C", str(REPO_ROOT),
        *SYNC_PATHS,
    ]
    ssh_argv, env = ssh_invocation(host, f"tar -xzf - -C {shlex.quote(root_remote)}")
    if dry:
        print(f"    [dry-run] {shlex.join(tar_argv)} | {shlex.join(ssh_argv)}")
        return True
    print(f"   同步 {len(SYNC_PATHS)} 棵源码树 -> {host}:{root_remote}/ ...", flush=True)
    tar_proc = subprocess.Popen(tar_argv, stdout=subprocess.PIPE)
    ssh_proc = subprocess.Popen(ssh_argv, stdin=tar_proc.stdout, env=env)
    assert tar_proc.stdout is not None
    tar_proc.stdout.close()
    ssh_proc.wait(timeout=timeout)
    tar_proc.wait(timeout=timeout)
    if ssh_proc.returncode != 0 or tar_proc.returncode != 0:
        print(f"   同步失败（tar {tar_proc.returncode} / ssh {ssh_proc.returncode}）")
        return False
    return True


def peer_opts(args: argparse.Namespace, role: str, listen_port: int, peer_ip: str, peer_port: int) -> str:
    """peer 的参数字符串（除运行时与角色外，两端必须逐字一致）。"""
    parts = [
        f"--role {role}",
        f"--listen 0.0.0.0:{listen_port}",
        f"--peer {peer_ip}:{peer_port}",
        f"--streams {args.streams}",
        f"--bytes {args.bytes}",
        f"--seed {args.seed}",
        f"--chunk {args.chunk}",
        f"--ring-cap {args.ring_cap}",
        f"--max-packet-size {args.max_packet_size}",
        f"--pre-flood-yield {args.pre_flood_yield}",
        f"--base-dock {args.base_dock}",
        f"--service-dock {args.service_dock}",
    ]
    return " ".join(parts)


def build_local_peer(runtime: str, profile: str, offline: bool) -> bool:
    """在本机按运行时 feature 编译一个 peer（`--ship-binaries` 用）。"""
    cmd = [
        "cargo",
        "build",
        "--manifest-path",
        str(DEMO_DIR / "Cargo.toml"),
        "--no-default-features",
        "--features",
        f"rt-{runtime}",
        "--bin",
        f"peer_{runtime}",
    ]
    if offline:
        cmd.append("--offline")
    if profile == "release":
        cmd.append("--release")
    print(f"   本机编译 peer_{runtime}（{profile}）...", flush=True)
    proc = subprocess.run(cmd, cwd=str(DEMO_DIR), text=True, capture_output=True)
    if proc.returncode != 0:
        print((proc.stdout or "")[-1500:])
        print((proc.stderr or "")[-1500:])
        return False
    return True


def ship_peer(host: str, dir_remote: str, runtime: str, timeout: float, dry: bool) -> bool:
    """把本机编译好的 `peer_<runtime>` 投递到远端 `--dir-x` 下。"""
    profile = SHIP["profile"]
    name = f"peer_{runtime}"
    src = DEMO_DIR / "target" / profile / name
    if not src.exists():
        print(f"   本机缺少 {src}")
        return False
    dest_dir = str(pathlib.PurePosixPath(dir_remote) / "target" / profile)
    ssh_argv, env = ssh_invocation(host, f"mkdir -p {shlex.quote(dest_dir)} && tar xzf - -C {shlex.quote(dest_dir)}")
    if dry:
        print(f"    [dry-run] tar czf - -C {src.parent} {name} | {shlex.join(ssh_argv)}")
        return True
    print(f"   投递 {name} -> {host}:{dest_dir}/ ...", flush=True)
    tar_proc = subprocess.Popen(["tar", "czf", "-", "-C", str(src.parent), name], stdout=subprocess.PIPE)
    ssh_proc = subprocess.Popen(ssh_argv, stdin=tar_proc.stdout, env=env)
    assert tar_proc.stdout is not None
    tar_proc.stdout.close()
    ssh_proc.wait(timeout=timeout)
    tar_proc.wait(timeout=timeout)
    return ssh_proc.returncode == 0 and tar_proc.returncode == 0


def ensure_built(host: str, workdir: str, runtime: str, cargo_flags: str, timeout: float, dry: bool) -> bool:
    """在远端按运行时 feature 编译出 peer 可执行文件。"""
    if SHIP["enabled"]:
        # 投递模式：本机编译（或复用已有产物），远端只解包。
        if not dry and not build_local_peer(runtime, SHIP["profile"], "--offline" in cargo_flags):
            return False
        if is_local(host):
            return True
        return ship_peer(host, workdir, runtime, timeout, dry)

    remote = (
        f"{cargo_prefix(host)}cd {shlex.quote(workdir)} && cargo build {cargo_flags} "
        f"--no-default-features --features rt-{runtime} --bin peer_{runtime}"
    )
    print(f"   在 {host} 上编译 peer_{runtime} ...", flush=True)
    code, out = run_ssh(host, remote, timeout, dry)
    if code != 0:
        print(f"   编译失败（{host} / peer_{runtime}）：\n{out[-2000:]}")
        return False
    return True


def parse_result_line(text: str) -> dict[str, Any] | None:
    """从输出里挑出最后一行 `result` 事件。"""
    found: dict[str, Any] | None = None
    for line in text.splitlines():
        line = line.strip()
        if not line.startswith("{"):
            continue
        try:
            obj = json.loads(line)
        except json.JSONDecodeError:
            continue
        if obj.get("event") == "result":
            found = obj
    return found


def run_case(
    pair: tuple[str, str],
    index: int,
    args: argparse.Namespace,
    workdir_a: str,
    workdir_b: str,
) -> dict[str, Any]:
    """跑一组跨机配对。"""
    ra, rb = pair
    port_a = args.port_base + index * 4
    port_b = args.port_base + index * 4 + 2
    tag = f"lan-{ra}-{rb}"
    started = time.monotonic()
    note = ""

    if not args.skip_build:
        if not ensure_built(args.ssh_a, workdir_a, ra, args.cargo_flags, args.build_timeout, args.dry_run):
            return {"event": "case", "pair": list(pair), "ok": False, "note": "A 机编译失败"}
        if not ensure_built(args.ssh_b, workdir_b, rb, args.cargo_flags, args.build_timeout, args.dry_run):
            return {"event": "case", "pair": list(pair), "ok": False, "note": "B 机编译失败"}

    profile = SHIP["profile"] if SHIP["enabled"] else "debug"
    passive_remote = (
        f"cd {shlex.quote(workdir_b)} && exec ./target/{profile}/peer_{rb} "
        f"--runtime {rb} {peer_opts(args, 'passive', port_b, args.ip_a, port_a)} --tag {tag}"
    )
    active_remote = (
        f"cd {shlex.quote(workdir_a)} && exec ./target/{profile}/peer_{ra} "
        f"--runtime {ra} {peer_opts(args, 'active', port_a, args.ip_b, port_b)} --tag {tag}"
    )

    if args.dry_run:
        print(f"== 配对 {ra} / {rb}（dry-run）")
        print(f"   B 机： ssh {args.ssh_b} {shlex.join([passive_remote])}")
        print(f"   A 机： ssh {args.ssh_a} {shlex.join([active_remote])}")
        return {"event": "case", "pair": list(pair), "ok": False, "note": "dry-run"}

    log_dir = pathlib.Path(args.out)
    log_dir.mkdir(parents=True, exist_ok=True)
    passive_log = log_dir / f"lan-{tag}-passive.log"

    passive_argv, passive_env = ssh_invocation(args.ssh_b, passive_remote)
    with passive_log.open("w", encoding="utf-8") as handle:
        passive_proc = subprocess.Popen(
            passive_argv,
            stdout=handle,
            stderr=subprocess.STDOUT,
            text=True,
            env=passive_env,
        )

    # 等被动端报告已监听（避免主动端在第一轮重试之前白等）。
    deadline = time.monotonic() + args.startup_timeout
    while time.monotonic() < deadline:
        if passive_log.exists() and '"event":"ready"' in passive_log.read_text(encoding="utf-8", errors="ignore"):
            break
        if passive_proc.poll() is not None:
            note = f"{rb} 在报告已监听之前就退出了"
            break
        time.sleep(0.2)

    active_out = ""
    if not note:
        active_argv, active_env = ssh_invocation(args.ssh_a, active_remote)
        try:
            proc = subprocess.run(
                active_argv,
                text=True,
                capture_output=True,
                timeout=args.timeout,
                env=active_env,
            )
            active_out = (proc.stdout or "") + (proc.stderr or "")
            if proc.returncode != 0:
                note = f"{ra} 退出码 {proc.returncode}"
        except subprocess.TimeoutExpired:
            note = f"{ra} 超时（{args.timeout}s）"

    try:
        passive_proc.wait(timeout=args.timeout)
    except subprocess.TimeoutExpired:
        passive_proc.kill()
        note = note or f"{rb} 超时（{args.timeout}s）"

    passive_out = passive_log.read_text(encoding="utf-8", errors="ignore") if passive_log.exists() else ""
    active_result = parse_result_line(active_out)
    passive_result = parse_result_line(passive_out)

    cross: list[str] = []
    if active_result and passive_result:
        cross = cross_check(active_result, passive_result, args.streams, args.bytes)
    else:
        if not active_result:
            cross.append("A 机没有 result 行")
        if not passive_result:
            cross.append("B 机没有 result 行")

    ok = (not note) and bool(active_result) and bool(passive_result) and not cross
    if ok:
        ok = bool(active_result.get("ok")) and bool(passive_result.get("ok"))

    return {
        "event": "case",
        "pair": list(pair),
        "tag": tag,
        "ports": {"a": port_a, "b": port_b},
        "ok": ok,
        "note": note,
        "cross_check": cross,
        "elapsed_s": round(time.monotonic() - started, 3),
        "active": active_result,
        "passive": passive_result,
        "active_stdout": active_out.splitlines(),
        "passive_stdout": passive_out.splitlines(),
    }


def render_markdown(cases: list[dict[str, Any]], args: argparse.Namespace) -> str:
    now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    lines = [
        "# smux_v1_sock_demo：真实局域网跨机结果",
        "",
        f"- 生成时间：{now}",
        f"- A 机：`{args.ssh_a}`（{args.ip_a}）；B 机：`{args.ssh_b}`（{args.ip_b}）",
        f"- 负载：每连接 {args.streams} 条子流 × 每方向 {args.bytes} 字节"
        f"（chunk {args.chunk}，环 {args.ring_cap}，max_packet_size {args.max_packet_size}，"
        f"pre-flood-yield {args.pre_flood_yield}，种子 {args.seed}）",
        f"- 运行方式：{'本机编译后投递二进制' if SHIP['enabled'] else '各机自行 cargo build'}（{SHIP['profile']}）",
        "",
        "| 配对（A / B） | 端 | 阶段 | 发出 B | 收到 B | 回执确认 | 接收校验 | 耗时 ms | 吞吐 MiB/s | 结论 |",
        "| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |",
    ]
    for case in cases:
        pair = " / ".join(case["pair"])
        verdict = "通过" if case["ok"] else "失败"
        for key, label in (("active", "A"), ("passive", "B")):
            result = case.get(key)
            if not result:
                lines.append(f"| {pair} | {label} | - | - | - | - | - | - | - | {verdict}（无结果行） |")
                continue
            for phase in result.get("phases", []):
                lines.append(
                    f"| {pair} | {label}（{result.get('runtime')}） | {phase.get('role')} | "
                    f"{phase.get('sent_bytes')} | {phase.get('recv_bytes')} | "
                    f"{phase.get('sent_acked')}/{phase.get('streams')} | "
                    f"{phase.get('recv_verified')}/{phase.get('streams')} | "
                    f"{phase.get('elapsed_ms', 0.0):.1f} | {phase.get('mib_per_s', 0.0):.1f} | {verdict} |"
                )
        for item in case.get("cross_check", []):
            lines.append(f"| {pair} | 对账不通过：{item} | | | | | | | | |")
        if case.get("note"):
            lines.append(f"| {pair} | 备注：{case['note']} | | | | | | | | |")
    lines += [
        "",
        "指标口径与判据见 `README.md` §3 / §4；进程级 metrics 逐项在 `results/lan-*.jsonl` 里。",
        "",
    ]
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description="smux_v1_sock_demo 跨机编排")
    parser.add_argument("--ssh-a", required=True, help="A 机的 ssh 目标（如 root@192.168.7.29）")
    parser.add_argument("--ssh-b", required=True, help="B 机的 ssh 目标")
    parser.add_argument("--ip-a", required=True, help="A 机在局域网上的 IP（写进 --peer）")
    parser.add_argument("--ip-b", required=True, help="B 机在局域网上的 IP")
    parser.add_argument("--dir-a", default=str(DEMO_DIR), help="A 机上 demo 源码目录")
    parser.add_argument("--dir-b", default=str(DEMO_DIR), help="B 机上 demo 源码目录")
    parser.add_argument("--streams", type=int, default=8)
    parser.add_argument("--bytes", type=int, default=8 * 1024 * 1024)
    parser.add_argument("--seed", type=int, default=20261007)
    parser.add_argument("--max-packet-size", type=int, default=4096)
    parser.add_argument("--pre-flood-yield", type=int, default=256)
    parser.add_argument("--chunk", type=int, default=16 * 1024)
    parser.add_argument("--ring-cap", type=int, default=256 * 1024)
    parser.add_argument("--base-dock", type=int, default=4096)
    parser.add_argument("--service-dock", type=int, default=1)
    parser.add_argument("--port-base", type=int, default=47000)
    parser.add_argument("--timeout", type=float, default=180.0, help="单个进程的墙钟上限（秒）")
    parser.add_argument("--build-timeout", type=float, default=900.0)
    parser.add_argument("--startup-timeout", type=float, default=30.0)
    parser.add_argument(
        "--pairs",
        default="all",
        help="all / tokio-compio / tokio-smol / compio-smol（逗号分隔可多选）",
    )
    parser.add_argument("--cargo-flags", default="", help="额外的 cargo 参数（如 --offline）")
    parser.add_argument("--skip-build", action="store_true")
    parser.add_argument(
        "--ship-binaries",
        action="store_true",
        help="本机编译后把 peer 可执行文件投递到远端（远端没有匹配 nightly 工具链时用；"
        "默认 release 档）",
    )
    parser.add_argument(
        "--ssh-password-env",
        default="SMUX_LAN_SSH_PASSWORD",
        help="从哪个环境变量读 ssh 密码（默认 SMUX_LAN_SSH_PASSWORD；不设则用密钥认证）",
    )
    parser.add_argument(
        "--sync",
        action="store_true",
        help="先用 rsync 把四棵源码树同步到两台机器（smux_v1_sock_demo / smux_v1 / abs_art / buffex_smol_adapt）",
    )
    parser.add_argument("--dry-run", action="store_true", help="只打印将执行的命令")
    parser.add_argument("--out", default=str(DEMO_DIR / "results"))
    args = parser.parse_args()

    SHIP["enabled"] = bool(args.ship_binaries)
    SHIP["profile"] = "release"

    password = os.environ.get(args.ssh_password_env) or None
    askpass_path = None
    if password and not (is_local(args.ssh_a) and is_local(args.ssh_b)):
        askpass_path = pathlib.Path(args.out) / ".askpass.sh"
        askpass_path.parent.mkdir(parents=True, exist_ok=True)
        askpass_path.write_text(f"#!/bin/sh\nprintf '%s\\n' {shlex.quote(password)}\n", encoding="utf-8")
        askpass_path.chmod(0o700)
        SSH_STATE["password"] = password
        SSH_STATE["askpass"] = askpass_path

    if args.pairs == "all":
        pairs = PAIRS
    else:
        wanted = {tuple(p.split("-")) for p in args.pairs.split(",") if p}
        pairs = [p for p in PAIRS if p in wanted]
        if not pairs:
            raise SystemExit("没有可跑的配对")

    print(f"A 机工具链：{check_toolchain(args.ssh_a, 30.0, args.dry_run)}", flush=True)
    print(f"B 机工具链：{check_toolchain(args.ssh_b, 30.0, args.dry_run)}", flush=True)
    if args.sync:
        print("== 同步源码 ==", flush=True)
        if not sync_tree(args.ssh_a, args.dir_a, args.build_timeout, args.dry_run):
            raise SystemExit("同步到 A 机失败")
        if not sync_tree(args.ssh_b, args.dir_b, args.build_timeout, args.dry_run):
            raise SystemExit("同步到 B 机失败")

    cases: list[dict[str, Any]] = []
    for index, pair in enumerate(pairs):
        print(f"== 配对 {pair[0]} / {pair[1]}（A={args.ip_a} / B={args.ip_b}）", flush=True)
        case = run_case(pair, index, args, args.dir_a, args.dir_b)
        cases.append(case)
        print(f"   结论：{'通过' if case['ok'] else '失败'}{'（' + case['note'] + '）' if case.get('note') else ''}", flush=True)
        for item in case.get("cross_check", []):
            print(f"   对账：{item}", flush=True)
        for key, label in (("active", "A"), ("passive", "B")):
            result = case.get(key)
            if not result:
                continue
            for phase in result["phases"]:
                print(
                    f"   [{label} {result['runtime']} {phase['role']}] 发出 {phase['sent_bytes']} B / "
                    f"收到 {phase['recv_bytes']} B，吞吐 {phase.get('mib_per_s', 0.0):.1f} MiB/s，"
                    f"回执 {phase['sent_acked']}/{phase['streams']}，校验 {phase['recv_verified']}/{phase['streams']}",
                    flush=True,
                )

    if args.dry_run:
        return 0

    out_dir = pathlib.Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
    md_path = out_dir / f"lan-{stamp}.md"
    jsonl_path = out_dir / f"lan-{stamp}.jsonl"
    md_path.write_text(render_markdown(cases, args), encoding="utf-8")
    with jsonl_path.open("w", encoding="utf-8") as handle:
        for case in cases:
            handle.write(json.dumps(case, ensure_ascii=False) + "\n")

    if askpass_path is not None:
        askpass_path.unlink(missing_ok=True)

    passed = sum(1 for c in cases if c["ok"])
    print(f"\n== 合计：{passed}/{len(cases)} 组通过 ==")
    print(f"报告：{md_path}")
    print(f"原始：{jsonl_path}")
    return 0 if passed == len(cases) else 1


if __name__ == "__main__":
    raise SystemExit(main())
