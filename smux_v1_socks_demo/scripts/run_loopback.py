#!/usr/bin/env python3
"""本机环回编排：三组运行时配对，每组起两个 peer 进程跑一次完整实验。

一次「用例」= 两个进程（不同运行时）+ 两条连接 + 两段角色交替。判据见
`docs`（README §判据）。本脚本负责：

1. 按配对逐格编译 peer（`--no-default-features --features rt-<名字>`）；
2. 各挑一个空闲端口，先起被动端、等它打出 `ready` 事件，再起主动端；
3. 收两边各一行 `result` JSON，做**跨进程交叉核对**（一端的 sent == 另一端的 recv）；
4. 汇总成控制台表格 + `results/loopback-<时间戳>.md` + `.jsonl`，全部通过才退出 0。

用法示例：

    python3 scripts/run_loopback.py                 # 3 组配对，默认负载
    python3 scripts/run_loopback.py --bytes 1048576 --streams 4
    python3 scripts/run_loopback.py --pairs tokio-compio
"""

from __future__ import annotations

import argparse
import datetime
import json
import pathlib
import socket
import subprocess
import sys
import threading
import time
from typing import Any

# 三组「取两个运行时」的配对。
PAIRS: list[tuple[str, str]] = [
    ("tokio", "compio"),
    ("tokio", "smol"),
    ("compio", "smol"),
]

DEMO_DIR = pathlib.Path(__file__).resolve().parent.parent


class Peer:
    """一个 peer 子进程：收它的 stdout JSON 行与 stderr。"""

    def __init__(self, label: str, argv: list[str]) -> None:
        self.label = label
        self.argv = argv
        self.proc: subprocess.Popen[str] | None = None
        self.ready: dict[str, Any] | None = None
        self.result: dict[str, Any] | None = None
        self.stdout_lines: list[str] = []
        self.stderr_text = ""
        self._threads: list[threading.Thread] = []
        self.timed_out = False

    def start(self) -> None:
        self.proc = subprocess.Popen(
            self.argv,
            cwd=str(DEMO_DIR),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
        )
        t_out = threading.Thread(target=self._pump_stdout, daemon=True)
        t_err = threading.Thread(target=self._pump_stderr, daemon=True)
        self._threads = [t_out, t_err]
        for t in self._threads:
            t.start()

    def _pump_stdout(self) -> None:
        assert self.proc is not None and self.proc.stdout is not None
        for line in self.proc.stdout:
            line = line.rstrip("\n")
            self.stdout_lines.append(line)
            try:
                obj = json.loads(line)
            except json.JSONDecodeError:
                continue
            event = obj.get("event")
            if event == "ready":
                self.ready = obj
            elif event == "result":
                self.result = obj

    def _pump_stderr(self) -> None:
        assert self.proc is not None and self.proc.stderr is not None
        self.stderr_text = self.proc.stderr.read()

    def wait_ready(self, timeout_s: float) -> bool:
        deadline = time.monotonic() + timeout_s
        while time.monotonic() < deadline:
            if self.ready is not None:
                return True
            if self.proc is not None and self.proc.poll() is not None:
                return False
            time.sleep(0.05)
        return False

    def wait(self, timeout_s: float) -> bool:
        assert self.proc is not None
        try:
            self.proc.wait(timeout=timeout_s)
        except subprocess.TimeoutExpired:
            self.timed_out = True
            self.proc.kill()
            self.proc.wait(timeout=10)
            return False
        for t in self._threads:
            t.join(timeout=5)
        return True

    @property
    def returncode(self) -> int | None:
        return None if self.proc is None else self.proc.returncode

    def kill(self) -> None:
        if self.proc is not None and self.proc.poll() is None:
            self.proc.kill()
            try:
                self.proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                pass


def free_port(bind_ip: str = "127.0.0.1") -> int:
    """挑一个当前空闲的 TCP 端口（在 `bind_ip` 上）。"""
    sock = socket.socket()
    sock.bind((bind_ip, 0))
    port = sock.getsockname()[1]
    sock.close()
    return port


def build_peer(runtime: str, offline: bool, release: bool) -> None:
    """按运行时 feature 编译一个 peer 可执行文件。"""
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
    if release:
        cmd.append("--release")
    print(f"   编译 peer_{runtime} ...", flush=True)
    proc = subprocess.run(cmd, cwd=str(DEMO_DIR), text=True, capture_output=True)
    if proc.returncode != 0:
        sys.stderr.write(proc.stdout)
        sys.stderr.write(proc.stderr)
        raise SystemExit(f"编译 peer_{runtime} 失败（退出码 {proc.returncode}）")


def peer_argv(
    runtime: str,
    role: str,
    listen_port: int,
    peer_ip: str,
    peer_port: int,
    args: argparse.Namespace,
    tag: str,
) -> list[str]:
    """拼出 peer 的命令行。两端除角色与地址外**必须完全一致**（负载、dock、种子）。"""
    profile = "release" if args.release else "debug"
    binary = DEMO_DIR / "target" / profile / f"peer_{runtime}"
    return [
        str(binary),
        "--runtime",
        runtime,
        "--role",
        role,
        "--listen",
        f"0.0.0.0:{listen_port}",
        "--peer",
        f"{peer_ip}:{peer_port}",
        "--streams",
        str(args.streams),
        "--bytes",
        str(args.bytes),
        "--seed",
        str(args.seed),
        "--chunk",
        str(args.chunk),
        "--ring-cap",
        str(args.ring_cap),
        "--max-packet-size",
        str(args.max_packet_size),
        "--pre-flood-yield",
        str(args.pre_flood_yield),
        "--base-dock",
        str(args.base_dock),
        "--service-dock",
        str(args.service_dock),
        *(
            ["--mix-telegraph", "--tg-frames", str(args.tg_frames), "--tg-bytes", str(args.tg_bytes)]
            if args.mix_telegraph
            else []
        ),
        "--tag",
        tag,
    ]


def cross_check(active_result: dict[str, Any], passive_result: dict[str, Any], streams: int, per_stream: int) -> list[str]:
    """跨进程交叉核对：一端「发出的」必须等于另一端「收到的」，角色必须交替。

    两个进程的角色是互补的：`--role active` 的一端阶段 1 当 initiator、阶段 2 当
    accepter，另一端正好相反。因此这里按**角色互补 + 逐阶段字节对账**来判。
    """
    notes: list[str] = []
    want = {
        "active": ["initiator", "accepter"],
        "passive": ["accepter", "initiator"],
    }
    for label, result, expected in (
        ("active", active_result, want["active"]),
        ("passive", passive_result, want["passive"]),
    ):
        roles = [phase.get("role") for phase in result.get("phases", [])]
        if roles != expected:
            notes.append(f"{label}（{result.get('runtime')}）的阶段角色 {roles} != 期望 {expected}")
        for i, phase in enumerate(result.get("phases", [])):
            tag = f"{label}（{result.get('runtime')}）第 {i + 1} 段"
            if phase.get("sent_acked") != streams:
                notes.append(f"{tag} 被回执确认 {phase.get('sent_acked')} != {streams}")
            if phase.get("recv_verified") != streams:
                notes.append(f"{tag} 校验通过 {phase.get('recv_verified')} != {streams}")
            if phase.get("sent_bytes") != streams * per_stream:
                notes.append(f"{tag} 发出 {phase.get('sent_bytes')} != {streams * per_stream}")
            if phase.get("recv_bytes") != streams * per_stream:
                notes.append(f"{tag} 收到 {phase.get('recv_bytes')} != {streams * per_stream}")
            if phase.get("mix_telegraph"):
                want_frames = phase.get("tg_frames")
                if phase.get("tg_sent_acked") != want_frames:
                    notes.append(
                        f"{tag} telegraph 被 ACK {phase.get('tg_sent_acked')} != {want_frames}"
                    )
                if phase.get("tg_recv_verified") != want_frames:
                    notes.append(
                        f"{tag} telegraph 接收校验 {phase.get('tg_recv_verified')} != {want_frames}"
                    )

    # 同一条连接的两个方向：active 端发的 == passive 端收的，反之亦然。
    a_phases = active_result.get("phases", [])
    b_phases = passive_result.get("phases", [])
    for i in range(min(len(a_phases), len(b_phases))):
        a = a_phases[i]
        b = b_phases[i]
        tag = f"第 {i + 1} 段（active {a.get('role')} / passive {b.get('role')}）"
        if a.get("sent_bytes") != b.get("recv_bytes"):
            notes.append(f"{tag}：一端发出 {a.get('sent_bytes')} B，另一端收到 {b.get('recv_bytes')} B")
        if a.get("recv_bytes") != b.get("sent_bytes"):
            notes.append(f"{tag}：一端收到 {a.get('recv_bytes')} B，另一端发出 {b.get('sent_bytes')} B")
        if a.get("sent_acked") != b.get("recv_verified"):
            notes.append(f"{tag}：回执确认 {a.get('sent_acked')} != 对方校验通过 {b.get('recv_verified')}")
        # telegraph 的跨端对账与子流同形：一端「被 ACK 的帧数」必须等于另一端
        # 「校验通过的帧数」——这正是「两端都真的收到了」的跨进程证据。
        if a.get("mix_telegraph") or b.get("mix_telegraph"):
            if not (a.get("mix_telegraph") and b.get("mix_telegraph")):
                notes.append(f"{tag}：两端对 `--mix-telegraph` 的开启状态不一致")
            elif a.get("tg_sent_acked") != b.get("tg_recv_verified"):
                notes.append(
                    f"{tag}：telegraph 一端被 ACK {a.get('tg_sent_acked')} != 另一端校验通过 {b.get('tg_recv_verified')}"
                )
            elif b.get("tg_sent_acked") != a.get("tg_recv_verified"):
                notes.append(
                    f"{tag}：telegraph 一端被 ACK {b.get('tg_sent_acked')} != 另一端校验通过 {a.get('tg_recv_verified')}"
                )
    return notes


def run_case(pair: tuple[str, str], args: argparse.Namespace) -> dict[str, Any]:
    """跑一组配对，返回一份用例记录。"""
    ra, rb = pair
    port_a, port_b = free_port(args.ip_a), free_port(args.ip_b)
    tag = f"{ra}__{rb}"
    active = Peer(
        f"{ra}(active)",
        peer_argv(ra, "active", port_a, args.ip_b, port_b, args, tag),
    )
    passive = Peer(
        f"{rb}(passive)",
        peer_argv(rb, "passive", port_b, args.ip_a, port_a, args, tag),
    )
    started = time.monotonic()
    note = ""
    try:
        passive.start()
        if not passive.wait_ready(args.startup_timeout):
            note = f"{rb} 未在 {args.startup_timeout}s 内报告已监听"
        active.start()
        active_done = active.wait(args.timeout)
        passive_done = passive.wait(args.timeout)
        if not active_done:
            note = note or f"{ra} 超时（{args.timeout}s）"
        if not passive_done:
            note = note or f"{rb} 超时（{args.timeout}s）"
        cross: list[str] = []
        if active.result and passive.result:
            cross = cross_check(active.result, passive.result, args.streams, args.bytes)
        peers_ok = (
            active.result is not None
            and passive.result is not None
            and bool(active.result.get("ok"))
            and bool(passive.result.get("ok"))
        )
        ok = (not note) and active.returncode == 0 and passive.returncode == 0 and peers_ok and not cross
        return {
            "event": "case",
            "pair": list(pair),
            "tag": tag,
            "ok": ok,
            "elapsed_s": round(time.monotonic() - started, 3),
            "note": note,
            "cross_check": cross,
            "active": active.result,
            "passive": passive.result,
            "active_exit": active.returncode,
            "passive_exit": passive.returncode,
            "active_stdout": active.stdout_lines,
            "passive_stdout": passive.stdout_lines,
            "active_stderr": active.stderr_text,
            "passive_stderr": passive.stderr_text,
        }
    finally:
        active.kill()
        passive.kill()


def fmt_mib_per_s(phase: dict[str, Any] | None) -> str:
    if not phase:
        return "-"
    return f"{phase.get('mib_per_s', 0.0):.1f}"


def render_markdown(cases: list[dict[str, Any]], args: argparse.Namespace) -> str:
    now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    lines = [
        f"# smux_v1_sock_demo：结果（{args.label}）",
        "",
        f"- 生成时间：{now}",
        f"- 拓扑：同一台机器两个进程，真实 TCP（A={args.ip_a} / B={args.ip_b}）；先起被动端，再起主动端",
        f"- 构建档：{'release' if args.release else 'debug'}",
        f"- 负载：每连接 {args.streams} 条子流 × 每方向 {args.bytes} 字节"
        f"（chunk {args.chunk}，环 {args.ring_cap}，max_packet_size {args.max_packet_size}，"
        f"pre-flood-yield {args.pre_flood_yield}）",
        (
            f"- telegraph 混合项：**开启**，每方向 {args.tg_frames} 条报文 × 每帧 {args.tg_bytes} 字节"
            f"（停止等待 + 应用层 ACK，与子流并发）"
            if args.mix_telegraph
            else "- telegraph 混合项：未开启（`--mix-telegraph`）"
        ),
        f"- 种子：{args.seed}；超时：{args.timeout}s",
        "",
        "| 配对（主动 / 被动） | 阶段 | 发出 B | 收到 B | 回执确认 | 接收校验 | 耗时 ms | 吞吐 MiB/s | 结论 |",
        "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |",
    ]
    for case in cases:
        pair = " / ".join(case["pair"])
        ok = "通过" if case["ok"] else "失败"
        for result_key in ("active", "passive"):
            result = case.get(result_key)
            if not result:
                lines.append(f"| {pair} | - | - | - | - | - | - | - | {ok}（无结果行） |")
                continue
            for phase in result.get("phases", []):
                lines.append(
                    f"| {pair}（{result.get('runtime')}） | {phase.get('role')} | "
                    f"{phase.get('sent_bytes')} | {phase.get('recv_bytes')} | "
                    f"{phase.get('sent_acked')}/{phase.get('streams')} | "
                    f"{phase.get('recv_verified')}/{phase.get('streams')} | "
                    f"{phase.get('elapsed_ms', 0.0):.1f} | {fmt_mib_per_s(phase)} | {ok} |"
                )
        if case.get("note"):
            lines.append(f"| {pair} | 备注：{case['note']} | | | | | | | |")
        for item in case.get("cross_check", []):
            lines.append(f"| {pair} | 交叉核对不通过：{item} | | | | | | | |")
    lines += [
        "",
        "## 指标口径",
        "",
        "- 「发出 / 收到 B」是**本体字节**，不含信封（每条子流每方向 32 字节）与帧头。",
        "- 「回执确认」= 本端发出的载荷被对端回执证明的条数；「接收校验」= 本端收到的载荷",
        "  通过 digest 与逐字节比对的条数。两者都必须等于子流条数。",
        "- 吞吐 = 本体字节 / 阶段墙钟。进程级 metrics（帧数、传输字节、WINDOW_UPDATE、",
        "  流控阻塞、子流寿命）在 `*.jsonl` 里逐项可见。",
        "",
    ]
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description="smux_v1_sock_demo 本机环回编排")
    parser.add_argument("--streams", type=int, default=8, help="每条连接的子流条数（默认 8）")
    parser.add_argument("--bytes", type=int, default=8 * 1024 * 1024, help="每方向本体字节数（默认 8 MiB）")
    parser.add_argument("--seed", type=int, default=20261007)
    parser.add_argument("--chunk", type=int, default=16 * 1024)
    parser.add_argument("--ring-cap", type=int, default=256 * 1024)
    parser.add_argument("--max-packet-size", type=int, default=4096)
    parser.add_argument("--pre-flood-yield", type=int, default=256)
    parser.add_argument("--base-dock", type=int, default=4096)
    parser.add_argument("--service-dock", type=int, default=1)
    parser.add_argument(
        "--mix-telegraph",
        action="store_true",
        help="在每条连接上额外开一条 telegraph 端点（与子流并发；telegraph 侧跑「发一帧等 ACK」的停止等待闭环）",
    )
    parser.add_argument("--tg-frames", type=int, default=16, help="telegraph 每个方向的报文条数（默认 16）")
    parser.add_argument("--tg-bytes", type=int, default=256, help="telegraph 每条报文的载荷字节数（默认 256）")
    parser.add_argument("--timeout", type=float, default=180.0, help="每个进程的墙钟上限（秒）")
    parser.add_argument("--startup-timeout", type=float, default=20.0, help="等被动端 ready 的秒数")
    parser.add_argument(
        "--pairs",
        default="all",
        help="要跑的配对：all / tokio-compio / tokio-smol / compio-smol，可用逗号分隔",
    )
    parser.add_argument(
        "--ip-a",
        default="127.0.0.1",
        help="A 端（主动端）用的地址：本端监听 + 写进 B 端的 --peer",
    )
    parser.add_argument(
        "--ip-b",
        default="127.0.0.1",
        help="B 端（被动端）用的地址：本端监听 + 写进 A 端的 --peer",
    )
    parser.add_argument("--out", default=str(DEMO_DIR / "results"), help="结果输出目录")
    parser.add_argument(
        "--label",
        default="loopback",
        help="结果文件名与标题里的标签（如 loopback-local / loopback-lanaddr）",
    )
    parser.add_argument("--skip-build", action="store_true", help="跳过编译（复用已有二进制）")
    parser.add_argument(
        "--release",
        action="store_true",
        help="用 release 档编译与运行（性能数字必须用它；debug 档会低一个数量级）",
    )
    parser.add_argument(
        "--offline",
        action="store_true",
        help="给 cargo 传 --offline（本机 cargo 缓存只读或断网时用）",
    )
    args = parser.parse_args()

    if args.pairs == "all":
        pairs = PAIRS
    else:
        wanted = {tuple(p.split("-")) for p in args.pairs.split(",") if p}
        pairs = [p for p in PAIRS if p in wanted]
        unknown = wanted - set(PAIRS)
        if unknown:
            raise SystemExit(f"无法识别的配对：{sorted('-'.join(u) for u in unknown)}")

    runtimes: list[str] = []
    for pair in pairs:
        for runtime in pair:
            if runtime not in runtimes:
                runtimes.append(runtime)

    if not args.skip_build:
        for runtime in runtimes:
            build_peer(runtime, args.offline, args.release)

    cases: list[dict[str, Any]] = []
    for pair in pairs:
        print(f"== 配对 {pair[0]} / {pair[1]} ==", flush=True)
        case = run_case(pair, args)
        cases.append(case)
        print(f"   结论：{'通过' if case['ok'] else '失败'}{'（' + case['note'] + '）' if case['note'] else ''}", flush=True)
        for item in case["cross_check"]:
            print(f"   交叉核对：{item}", flush=True)
        for key in ("active", "passive"):
            result = case.get(key)
            if not result:
                print(f"   {key}：没有结果行；stderr 尾部：{case[key + '_stderr'][-400:]}", flush=True)
                continue
            for phase in result["phases"]:
                print(
                    f"   [{result['runtime']} {phase['role']}] 发出 {phase['sent_bytes']} B / 收到 "
                    f"{phase['recv_bytes']} B，吞吐 {fmt_mib_per_s(phase)} MiB/s，"
                    f"回执 {phase['sent_acked']}/{phase['streams']}，校验 {phase['recv_verified']}/{phase['streams']}",
                    flush=True,
                )

    out_dir = pathlib.Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
    md_path = out_dir / f"{args.label}-{stamp}.md"
    jsonl_path = out_dir / f"{args.label}-{stamp}.jsonl"
    md_path.write_text(render_markdown(cases, args), encoding="utf-8")
    with jsonl_path.open("w", encoding="utf-8") as handle:
        for case in cases:
            handle.write(json.dumps(case, ensure_ascii=False) + "\n")

    passed = sum(1 for c in cases if c["ok"])
    print(f"\n== 合计：{passed}/{len(cases)} 组通过 ==")
    print(f"报告：{md_path}")
    print(f"原始：{jsonl_path}")
    return 0 if passed == len(cases) else 1


if __name__ == "__main__":
    raise SystemExit(main())
