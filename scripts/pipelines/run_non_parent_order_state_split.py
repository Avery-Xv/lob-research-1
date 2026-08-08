#!/usr/bin/env python3
"""Prepare and run bounded partitions for the full-market non-parent state job."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.factors.order_shape_mechanism.compute_non_parent_order_state_v4 import (
    NonParentOrderStateConfig,
    build_manifest,
    load_inputs,
    validate_batch_dir,
)
from scripts.factors.order_shape_mechanism.reproduce_mechanisms_v4 import file_sha256


RUNNER = REPO_ROOT / "scripts/factors/order_shape_mechanism/compute_non_parent_order_state_v4.py"


def write_json(path: Path, value: dict[str, object]) -> None:
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    os.replace(temporary, path)


def read_symbols(path: Path) -> list[str]:
    symbols = [line.strip() for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
    if symbols != sorted(set(symbols)):
        raise ValueError(f"symbols must be sorted and unique: {path}")
    return symbols


def prepare(args: argparse.Namespace) -> int:
    plan_dir = args.plan_dir.resolve()
    if plan_dir.exists():
        raise SystemExit(f"Refusing to overwrite split plan: {plan_dir}")
    file_list = args.file_list.resolve()
    metadata_path = args.universe_metadata.resolve()
    history_dir = args.history_state_dir.resolve()
    base_dir = args.base_shard_dir.resolve() if args.base_shard_dir else None
    warmup = list(args.warmup_months)
    target = args.target_month
    inputs, metadata = load_inputs(file_list, metadata_path, warmup, target)
    months = set(warmup + [target])
    inputs = {
        symbol: paths for symbol, paths in inputs.items()
        if months.issubset(paths)
    }
    completed: set[str] = set()
    base_manifest_sha256 = None
    if base_dir is not None:
        expected = build_manifest(
            file_list, metadata_path, metadata, inputs, warmup,
            NonParentOrderStateConfig(target_month=target), 1, None, history_dir,
        )
        base_manifest = base_dir / "manifest.json"
        actual = json.loads(base_manifest.read_text(encoding="utf-8"))
        if actual.get("fingerprint") != expected.get("fingerprint"):
            raise ValueError("base shard manifest does not match the certified full-market specification")
        base_manifest_sha256 = file_sha256(base_manifest)
        for batch in sorted(base_dir.glob("batch_*")):
            validate_batch_dir(batch)
            done = json.loads((batch / "done.json").read_text(encoding="utf-8"))
            batch_symbols = [str(symbol) for symbol in done.get("symbols", [])]
            if len(batch_symbols) != 1:
                raise ValueError(f"expected one symbol in completed base shard: {batch}")
            symbol = batch_symbols[0]
            if symbol not in inputs or symbol in completed:
                raise ValueError(f"unexpected or duplicate completed symbol: {symbol}")
            completed.add(symbol)
            if not (history_dir / f"{symbol}.json").is_file():
                raise ValueError(f"missing history snapshot for completed symbol: {symbol}")

    remaining = sorted(set(inputs) - completed)
    plan_dir.mkdir(parents=True, exist_ok=False)
    partitions_dir = plan_dir / "symbols"
    partitions_dir.mkdir()
    output_root = args.partition_output_root.resolve()
    log_root = args.log_root.resolve()
    partitions = []
    for offset in range(0, len(remaining), args.partition_size):
        number = offset // args.partition_size + 1
        partition_id = f"part_{number:04d}"
        symbols = remaining[offset:offset + args.partition_size]
        symbols_file = partitions_dir / f"{partition_id}.txt"
        symbols_file.write_text("\n".join(symbols) + "\n", encoding="utf-8")
        partitions.append({
            "id": partition_id,
            "symbols_file": str(symbols_file.resolve()),
            "symbols_file_sha256": file_sha256(symbols_file),
            "symbol_count": len(symbols),
            "first_symbol": symbols[0],
            "last_symbol": symbols[-1],
            "shard_dir": str((output_root / partition_id).resolve()),
            "log": str((log_root / f"{partition_id}.log").resolve()),
            "completion": str((output_root / partition_id / "partition_complete.json").resolve()),
        })
    completed_digest = hashlib.sha256("\n".join(sorted(completed)).encode()).hexdigest()
    payload = {
        "kind": "non_parent_order_state_split_plan",
        "schema_version": 1,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "factor_run_manifest": str(args.factor_run_manifest.resolve()),
        "factor_run_manifest_sha256": file_sha256(args.factor_run_manifest),
        "preflight_receipt": str(args.preflight_receipt.resolve()),
        "preflight_receipt_sha256": file_sha256(args.preflight_receipt),
        "runner": str(RUNNER),
        "runner_sha256": file_sha256(RUNNER),
        "file_list": str(file_list),
        "file_list_sha256": file_sha256(file_list),
        "universe_metadata": str(metadata_path),
        "universe_metadata_sha256": file_sha256(metadata_path),
        "warmup_months": warmup,
        "target_month": target,
        "history_state_dir": str(history_dir),
        "base_shard_dir": str(base_dir) if base_dir else None,
        "base_manifest_sha256": base_manifest_sha256,
        "full_symbol_count": len(inputs),
        "preserved_completed_symbol_count": len(completed),
        "preserved_completed_symbols_sha256": completed_digest,
        "remaining_symbol_count": len(remaining),
        "partition_size": args.partition_size,
        "partition_count": len(partitions),
        "workers_per_partition": args.workers_per_partition,
        "max_active_partitions": args.max_active_partitions,
        "partitions": partitions,
    }
    write_json(plan_dir / "plan.json", payload)
    print(json.dumps({
        "plan": str(plan_dir / "plan.json"),
        "preserved": len(completed),
        "remaining": len(remaining),
        "partitions": len(partitions),
    }))
    return 0


def validate_plan(plan: dict[str, object], plan_path: Path) -> None:
    if plan.get("kind") != "non_parent_order_state_split_plan":
        raise ValueError("unexpected split plan kind")
    checks = (
        (Path(str(plan["runner"])), str(plan["runner_sha256"])),
        (Path(str(plan["file_list"])), str(plan["file_list_sha256"])),
        (Path(str(plan["universe_metadata"])), str(plan["universe_metadata_sha256"])),
        (Path(str(plan["factor_run_manifest"])), str(plan["factor_run_manifest_sha256"])),
        (Path(str(plan["preflight_receipt"])), str(plan["preflight_receipt_sha256"])),
    )
    for path, expected_hash in checks:
        if not path.is_file() or file_sha256(path) != expected_hash:
            raise ValueError(f"split-plan input changed: {path}")
    for partition in plan["partitions"]:
        symbols_file = Path(str(partition["symbols_file"]))
        if file_sha256(symbols_file) != partition["symbols_file_sha256"]:
            raise ValueError(f"partition symbol list changed: {symbols_file}")
    if plan_path.resolve() != plan_path:
        raise ValueError("plan path must be resolved")


def partition_command(plan: dict[str, object], partition: dict[str, object]) -> list[str]:
    symbols = read_symbols(Path(str(partition["symbols_file"])))
    return [
        str(REPO_ROOT / "conda_lob/bin/python"), str(RUNNER),
        "--file-list", str(plan["file_list"]),
        "--universe-metadata", str(plan["universe_metadata"]),
        "--warmup-months", *[str(month) for month in plan["warmup_months"]],
        "--target-month", str(plan["target_month"]),
        "--workers", str(plan["workers_per_partition"]),
        "--batch-size", "1", "--fetch-rows", "200000", "--memory-limit", "2GB",
        "--shard-dir", str(partition["shard_dir"]),
        "--history-state-dir", str(plan["history_state_dir"]),
        "--audit-symbols", *symbols,
    ]


def run(args: argparse.Namespace) -> int:
    plan_path = args.plan.resolve()
    plan = json.loads(plan_path.read_text(encoding="utf-8"))
    validate_plan(plan, plan_path)
    active: dict[subprocess.Popen[bytes], tuple[dict[str, object], object]] = {}
    pending = [
        partition for partition in plan["partitions"]
        if not Path(str(partition["completion"])).is_file()
    ]
    planned_max_active = int(plan["max_active_partitions"])
    max_active = planned_max_active if args.max_active_partitions is None else args.max_active_partitions
    if max_active <= 0 or max_active > planned_max_active:
        raise ValueError(
            f"max active override must be in [1,{planned_max_active}]"
        )
    while pending or active:
        while pending and len(active) < max_active:
            partition = pending.pop(0)
            log_path = Path(str(partition["log"]))
            log_path.parent.mkdir(parents=True, exist_ok=True)
            log_handle = log_path.open("ab", buffering=0)
            process = subprocess.Popen(
                partition_command(plan, partition), cwd=REPO_ROOT,
                stdout=log_handle, stderr=subprocess.STDOUT,
            )
            active[process] = (partition, log_handle)
            print(f"started {partition['id']} pid={process.pid}", flush=True)
        if not active:
            break
        time.sleep(5)
        for process in list(active):
            status = process.poll()
            if status is None:
                continue
            partition, log_handle = active.pop(process)
            log_handle.close()
            if status != 0:
                raise RuntimeError(f"partition failed: {partition['id']} exit={status}")
            symbols = read_symbols(Path(str(partition["symbols_file"])))
            shard_dir = Path(str(partition["shard_dir"]))
            batches = sorted(shard_dir.glob("batch_*"))
            if len(batches) != len(symbols):
                raise RuntimeError(
                    f"partition incomplete: {partition['id']} {len(batches)}/{len(symbols)}"
                )
            for batch in batches:
                validate_batch_dir(batch)
            write_json(Path(str(partition["completion"])), {
                "kind": "non_parent_order_state_partition_completion",
                "schema_version": 1,
                "partition": partition["id"],
                "symbol_count": len(symbols),
                "completed_at": datetime.now(timezone.utc).isoformat(),
                "symbols_file_sha256": partition["symbols_file_sha256"],
                "runner_sha256": plan["runner_sha256"],
            })
            print(f"completed {partition['id']} symbols={len(symbols)}", flush=True)
    return 0


def parser() -> argparse.ArgumentParser:
    root = argparse.ArgumentParser(description=__doc__)
    commands = root.add_subparsers(dest="command", required=True)
    prepare_parser = commands.add_parser("prepare")
    prepare_parser.add_argument("--file-list", type=Path, required=True)
    prepare_parser.add_argument("--universe-metadata", type=Path, required=True)
    prepare_parser.add_argument("--warmup-months", nargs="+", required=True)
    prepare_parser.add_argument("--target-month", required=True)
    prepare_parser.add_argument("--history-state-dir", type=Path, required=True)
    prepare_parser.add_argument("--base-shard-dir", type=Path)
    prepare_parser.add_argument("--partition-output-root", type=Path, required=True)
    prepare_parser.add_argument("--log-root", type=Path, required=True)
    prepare_parser.add_argument("--factor-run-manifest", type=Path, required=True)
    prepare_parser.add_argument("--preflight-receipt", type=Path, required=True)
    prepare_parser.add_argument("--plan-dir", type=Path, required=True)
    prepare_parser.add_argument("--partition-size", type=int, default=100)
    prepare_parser.add_argument("--workers-per-partition", type=int, default=4)
    prepare_parser.add_argument("--max-active-partitions", type=int, default=2)
    run_parser = commands.add_parser("run")
    run_parser.add_argument("--plan", type=Path, required=True)
    run_parser.add_argument("--max-active-partitions", type=int)
    return root


def main() -> int:
    args = parser().parse_args()
    if args.command == "prepare":
        if min(args.partition_size, args.workers_per_partition, args.max_active_partitions) <= 0:
            raise ValueError("partition and worker values must be positive")
        return prepare(args)
    return run(args)


if __name__ == "__main__":
    raise SystemExit(main())
