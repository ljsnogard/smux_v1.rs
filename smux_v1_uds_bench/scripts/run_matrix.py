#!/usr/bin/env python3
"""smux_v1 UDS bench 的**编排器**：按「后端 × 环境」逐格调用三个可执行文件。

# 为什么要有编排器

三个运行时后端是**互斥**的编译期选择，因此「对照」只能是**三个可执行文件**之间的
对照；而「单线程 / 多线程」两种环境又是同一个可执行文件的两种参数。本脚本把
「后端 × 环境」这个二维组合编排起来，把每格的结果落到自己的目录里，方便事后合并。

# 用法

```bash
python3 scripts/run_matrix.py --release                    # 全量：3 后端 × {1,4} 跑器
python3 scripts/run_matrix.py --release --only tokio        # 只跑一个后端
python3 scripts/run_matrix.py --runners 1 --bytes 4M --repeat 1   # 快速冒烟
python3 scripts/run_matrix.py --dry-run                     # 只看要跑什么
```

参数与 `bench_*` 的命令行**同名**（`--cases` / `--bytes` / `--chunk` / `--repeat` /
`--timeout`），因此本脚本只负责「多跑几个后端、多跑几种环境」，不重复解释语义。
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path

# 后端 → （feature, 可执行文件名）
BACKENDS = {
    "tokio": ("rt-tokio", "bench_tokio"),
    "compio": ("rt-compio", "bench_compio"),
    "smol": ("rt-smol", "bench_smol"),
}

ROOT = Path(__file__).resolve().parent.parent


def parse_args() -> argparse.Namespace:
    """解析命令行。"""
    ap = argparse.ArgumentParser(description="smux_v1 UDS bench 编排器")
    ap.add_argument("--only", help="只跑一个后端（tokio / compio / smol）")
    ap.add_argument("--runners", default="1,4", help="跑器线程数列表，逗号分隔（缺省 1,4）")
    ap.add_argument("--cases", default="2,4,8,16", help="并发度列表，逗号分隔")
    ap.add_argument("--bytes", default="64M", help="每个客户端的字节数")
    ap.add_argument("--chunk", default="64K", help="分块大小")
    ap.add_argument("--ring-cap", default="64K", help="环容量")
    ap.add_argument("--repeat", default="2", help="每个用例重复轮数")
    ap.add_argument("--timeout", default="120", help="单格墙钟上限（秒）")
    ap.add_argument("--release", action="store_true", help="用 release 构建")
    ap.add_argument("--offline", action="store_true", help="给 cargo 传 --offline")
    ap.add_argument("--out", default="results", help="结果根目录（缺省 results）")
    ap.add_argument("--label", default="", help="给本次运行起个名字（进目录名）")
    ap.add_argument("--skip-build", action="store_true", help="不重新编译")
    ap.add_argument("--dry-run", action="store_true", help="只打印将要执行的命令")
    return ap.parse_args()


def ensure_built(backend: str, release: bool, offline: bool) -> None:
    """按需编译一个后端。"""
    feat, bin_name = BACKENDS[backend]
    cmd = [
        "cargo",
        "build",
        "--manifest-path",
        str(ROOT / "Cargo.toml"),
        "--no-default-features",
        "--features",
        feat,
        "--bin",
        bin_name,
    ]
    if release:
        cmd.append("--release")
    if offline:
        cmd.append("--offline")
    print(f"== 编译 {backend}：{' '.join(cmd)}", flush=True)
    subprocess.run(cmd, check=True)


def run_case(
    backend: str,
    runners: str,
    args: argparse.Namespace,
    out_dir: Path,
) -> tuple[bool, str]:
    """跑一个「后端 × 环境」格，返回 (是否全绿, 结果目录)。"""
    _, bin_name = BACKENDS[backend]
    profile = "release" if args.release else "debug"
    exe = ROOT / "target" / profile / bin_name
    suffix = f"-{args.label}" if args.label else ""
    case_dir = out_dir / f"{backend}-r{runners}{suffix}"
    case_dir.mkdir(parents=True, exist_ok=True)
    cmd = [
        str(exe),
        "--cases",
        args.cases,
        "--bytes",
        args.bytes,
        "--chunk",
        args.chunk,
        "--ring-cap",
        args.ring_cap,
        "--repeat",
        str(args.repeat),
        "--runners",
        str(runners),
        "--timeout",
        str(args.timeout),
        "--out",
        str(case_dir),
    ]
    print(f"\n===== {backend} / runners={runners} =====", flush=True)
    print("$ " + " ".join(cmd), flush=True)
    if args.dry_run:
        return True, str(case_dir)
    proc = subprocess.run(cmd)
    return proc.returncode == 0, str(case_dir)


def summarise(out_dir: Path, results: list[tuple[str, str, bool, str]]) -> None:
    """把本轮的「格 → 结果」写成一份清单，方便事后一眼看出哪格有失败。"""
    manifest = {
        "cases": [
            {"backend": backend, "runners": runners, "ok": ok, "dir": case_dir}
            for backend, runners, ok, case_dir in results
        ]
    }
    (out_dir / "manifest.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print("\n===== 本轮清单 =====")
    for backend, runners, ok, case_dir in results:
        mark = "通过" if ok else "**有失败格**"
        print(f"  {backend:7} runners={runners:<3} {mark:12} {case_dir}")
    print(f"（清单：{out_dir / 'manifest.json'}）")


def main() -> int:
    """入口。"""
    args = parse_args()
    backends = [args.only] if args.only else list(BACKENDS)
    for backend in backends:
        if backend not in BACKENDS:
            print(f"未知后端 `{backend}`；可选 {', '.join(BACKENDS)}", file=sys.stderr)
            return 2
    runners_list = [r.strip() for r in args.runners.split(",") if r.strip()]

    out_root = (ROOT / args.out).resolve() if not Path(args.out).is_absolute() else Path(args.out)
    out_root.mkdir(parents=True, exist_ok=True)

    if not args.skip_build and not args.dry_run:
        for backend in backends:
            ensure_built(backend, args.release, args.offline)

    results: list[tuple[str, str, bool, str]] = []
    for backend in backends:
        for runners in runners_list:
            ok, case_dir = run_case(backend, runners, args, out_root)
            results.append((backend, runners, ok, case_dir))

    summarise(out_root, results)
    failures = [r for r in results if not r[2]]
    if failures:
        print(f"\n有 {len(failures)} 个格存在失败用例；详见各自的 summary.md", file=sys.stderr)
        return 1
    print("\n全部格通过。")
    return 0


if __name__ == "__main__":
    sys.exit(main())
