#!/usr/bin/env python3
"""把各格的 JSONL 结果合并成一张总表（Markdown）。

# 输入 / 输出

- 输入：`results/*/bench-<后端>.jsonl`（由 `bench_<后端>` 逐格写出，一行一个用例）；
- 输出：`results/summary.md`：`后端 × 环境 × 并发度` 的总表，外加 raw ↔ smux 对照。

# 为什么从 JSONL 读而不是从各格的 summary.md 读

JSONL 是**原始记录**（一行一个用例，含全部统计量），而各格的 summary.md 是渲染结果。
从原始记录合并，可以把「跨格取最好」这类口径在合并阶段重新做一遍，而不是在文本层拼表。
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def load_records(results_root: Path) -> list[dict]:
    """读出全部逐格记录。"""
    records: list[dict] = []
    for path in sorted(results_root.glob("*/bench-*.jsonl")):
        for line in path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                continue
            # 目录名形如 `tokio-r1` / `smol-r4`：环境（跑器数）从那里取。
            env = path.parent.name
            record["env"] = env
            records.append(record)
    return records


def best(records: list[dict]) -> list[dict]:
    """每个「环境 × 后端 × 路径 × 并发度」取带宽最好的一轮。"""
    table: dict[tuple, dict] = {}
    for record in records:
        if record.get("failure"):
            continue
        key = (
            record["env"],
            record["runtime"],
            record["mode"],
            record["clients"],
        )
        prev = table.get(key)
        if prev is None or prev.get("bandwidth_mib_s", 0.0) < record.get("bandwidth_mib_s", 0.0):
            table[key] = record
    return [table[k] for k in sorted(table)]


def render(records: list[dict], merged: list[dict]) -> str:
    """渲染总表。"""
    out: list[str] = []
    out.append("# smux_v1 UDS 性能测试总表\n")
    out.append("- 来源：`results/*/bench-*.jsonl`\n")
    out.append(f"- 原始记录条数：{len(records)}；有效（无失败）格数：{len(merged)}\n")
    out.append("- 口径：墙钟 = 栅栏释放 → 最后一个发送任务写完；服务端收齐只作参考量\n")
    out.append("- 带宽单位 MiB/s；任务时长单位 ms，方差单位 ms²\n")

    out.append("\n## 1. 逐格（每个环境 × 后端 × 路径 × N 取最好一轮）\n")
    out.append(
        "| 环境 | 后端 | 路径 | N | 总字节 | 墙钟 s | 带宽 MiB/s | 任务均值 ms | 方差 ms² | 标准差 ms | CV | 并发因子 |\n"
    )
    out.append("| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |\n")
    for record in merged:
        out.append(
            "| {env} | {runtime} | {mode} | {clients} | {total} | {wall:.6f} | {bw:.2f} | "
            "{mean:.3f} | {var:.4f} | {sd:.3f} | {cv:.4f} | {cf:.2f} |\n".format(
                env=record["env"],
                runtime=record["runtime"],
                mode=record["mode"],
                clients=record["clients"],
                total=human_bytes(record["total_bytes"]),
                wall=record["wall_secs"],
                bw=record["bandwidth_mib_s"],
                mean=record["task_mean_ns"] / 1e6,
                var=record["task_var_ns2"] / 1e12,
                sd=record["task_stddev_ns"] / 1e6,
                cv=record["task_cv"],
                cf=record["concurrency_factor"],
            )
        )

    out.append("\n## 2. raw ↔ smux 带宽比（同环境、同后端、同 N）\n")
    out.append("| 环境 | 后端 | N | raw MiB/s | smux MiB/s | smux/raw |\n")
    out.append("| --- | --- | ---: | ---: | ---: | ---: |\n")
    index = {(r["env"], r["runtime"], r["mode"], r["clients"]): r for r in merged}
    keys = sorted({(r["env"], r["runtime"], r["clients"]) for r in merged})
    for env, runtime, clients in keys:
        raw = index.get((env, runtime, "raw", clients))
        smux = index.get((env, runtime, "smux", clients))
        if raw is None or smux is None:
            continue
        ratio = smux["bandwidth_mib_s"] / raw["bandwidth_mib_s"] if raw["bandwidth_mib_s"] else 0.0
        out.append(
            f"| {env} | {runtime} | {clients} | {raw['bandwidth_mib_s']:.2f} | "
            f"{smux['bandwidth_mib_s']:.2f} | {ratio:.3f} |\n"
        )

    out.append("\n## 3. 单线程 ↔ 多线程（同后端、同路径、同 N，各自取最好一轮）\n")
    out.append(
        "| 后端 | 路径 | N | 单跑器 MiB/s | 多跑器 MiB/s | 多/单 | 单跑器并发因子 | 多跑器并发因子 |\n"
    )
    out.append("| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |\n")
    index = {(r["env"], r["runtime"], r["mode"], r["clients"]): r for r in merged}
    for runtime, single_env, multi_env in env_pairs(merged):
        for mode in ("raw", "smux"):
            for clients in (2, 4, 8, 16):
                one = index.get((single_env, runtime, mode, clients))
                many = index.get((multi_env, runtime, mode, clients))
                if one is None or many is None:
                    continue
                ratio = (
                    many["bandwidth_mib_s"] / one["bandwidth_mib_s"]
                    if one["bandwidth_mib_s"]
                    else 0.0
                )
                out.append(
                    f"| {runtime} | {mode} | {clients} | {one['bandwidth_mib_s']:.2f} | "
                    f"{many['bandwidth_mib_s']:.2f} | {ratio:.3f} | "
                    f"{one['concurrency_factor']:.2f} | {many['concurrency_factor']:.2f} |\n"
                )

    out.append("\n## 4. 结论\n\n")
    out.append(conclude(merged))
    return "".join(out)


def conclude(merged: list[dict]) -> str:
    """按「测到的数字」写结论（不写死结论：换机器、换版本都会变）。"""
    out: list[str] = []
    out.append("**真并发判据**：`并发因子 = Σ(任务时长) / 墙钟`——串行时趋近 1.0，")
    out.append("N 个任务真同时推进时趋近 N。逐格数字见上表。\n\n")
    index = {(r["env"], r["runtime"], r["mode"], r["clients"]): r for r in merged}
    for runtime, single, multi in env_pairs(merged):
        for mode in ("raw", "smux"):
            a = index.get((single, runtime, mode, 16))
            b = index.get((multi, runtime, mode, 16))
            if a is None or b is None:
                continue
            ratio = b["bandwidth_mib_s"] / a["bandwidth_mib_s"] if a["bandwidth_mib_s"] else 0.0
            out.append(
                f"- `{runtime}` / `{mode}` N=16：单跑器 **{a['bandwidth_mib_s']:.1f}** MiB/s"
                f"（并发因子 {a['concurrency_factor']:.2f}），"
                f"多跑器 **{b['bandwidth_mib_s']:.1f}** MiB/s"
                f"（并发因子 {b['concurrency_factor']:.2f}），比值 ×{ratio:.3f}。\n"
            )
    out.append("\n读法：`smux/raw` 明显小于 1 是**预期**——复用要把 N 条子流的字节轮流编成帧")
    out.append("写进**同一条** socket，多了一层定序与窗口轮转；`并发因子` 才是「有没有把子流串起来」")
    out.append("的证据：它越接近 N，说明复用路径没有把子流排成一队。\n")
    return "".join(out)


def env_pairs(merged: list[dict]) -> list[tuple[str, str, str]]:
    """把「单跑器环境」与「多跑器环境」按后端配对。

    目录名形如 `compio-r1` / `compio-r4`：`-r1` 是单跑器环境，其余是多跑器环境。
    返回 `(后端, 单跑器环境, 多跑器环境)`，跳过缺一边的后端。
    """
    by_runtime: dict[str, dict[str, str]] = {}
    for record in merged:
        env = record["env"]
        head, _, tail = env.rpartition("-r")
        if not head or not tail.isdigit():
            continue
        slot = by_runtime.setdefault(record["runtime"], {})
        if tail == "1":
            slot["single"] = env
        else:
            slot.setdefault("multi", env)
    pairs: list[tuple[str, str, str]] = []
    for runtime in sorted(by_runtime):
        slot = by_runtime[runtime]
        if "single" in slot and "multi" in slot:
            pairs.append((runtime, slot["single"], slot["multi"]))
    return pairs


def human_bytes(count: int) -> str:
    """字节数转人读形式。"""
    for unit, scale in (("G", 1024**3), ("M", 1024**2), ("K", 1024)):
        if count >= scale and count % scale == 0:
            return f"{count // scale}{unit}"
    return str(count)


def main() -> int:
    """入口。"""
    results_root = ROOT / "results"
    if not results_root.is_dir():
        print(f"找不到结果目录：{results_root}", file=sys.stderr)
        return 2
    records = load_records(results_root)
    if not records:
        print("没有找到任何 JSONL 记录", file=sys.stderr)
        return 2
    merged = best(records)
    text = render(records, merged)
    target = results_root / "summary.md"
    target.write_text(text, encoding="utf-8")
    print(f"已写出 {target}（{len(records)} 条原始记录，{len(merged)} 格）")
    return 0


if __name__ == "__main__":
    sys.exit(main())
