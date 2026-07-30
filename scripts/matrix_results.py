#!/usr/bin/env python3
"""Validate and aggregate official Kafka experimental matrix results."""

from __future__ import annotations

import argparse
import csv
import json
import statistics
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Sequence

COMBINATIONS = (("A", "baseline"), ("A", "fault"), ("B", "baseline"), ("B", "fault"))
BASE_ARTIFACTS = (
    "metadata.json", "timeline.jsonl", "producer.jsonl", "consumer.jsonl",
    "summary.json", "integrity.json",
)
FAULT_ARTIFACTS = (
    "fault-plan.json", "recovery.json", "kubernetes/pods-before.json",
    "kubernetes/pods-after.json", "kubernetes/events.jsonl",
    "kafka/topic-before.json", "kafka/topic-during.json", "kafka/topic-after.json",
)
SUMMARY_METRICS = (
    "attempted", "acknowledged", "failed", "consumed", "acknowledged_missing",
    "duplicates", "sequence_regressions", "sequence_gaps", "out_of_order", "final_lag",
)
RECOVERY_METRICS = (
    "infrastructure_seconds", "kafka_seconds", "application_seconds", "performance_seconds",
)
RUN_FIELDS = (
    "run_id", "profile", "scenario", "repetition", "created_at", "status",
    *SUMMARY_METRICS, *RECOVERY_METRICS,
)


@dataclass(frozen=True)
class Run:
    path: Path
    metadata: dict[str, Any]
    summary: dict[str, Any]
    recovery: dict[str, Any] | None

    @property
    def run_id(self) -> str:
        return str(self.metadata.get("run_id", ""))

    @property
    def key(self) -> tuple[str, str, int]:
        return (
            str(self.metadata.get("profile", "")),
            str(self.metadata.get("scenario", "")),
            int(self.metadata.get("repetition", 0)),
        )


def load_json(path: Path, errors: list[str]) -> dict[str, Any] | None:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        errors.append(f"{path}: cannot read valid JSON: {exc}")
        return None
    if not isinstance(value, dict):
        errors.append(f"{path}: expected a JSON object")
        return None
    return value


def discover(raw_dir: Path) -> list[Path]:
    if not raw_dir.exists():
        return []
    return sorted(
        (path for path in raw_dir.iterdir() if path.is_dir() and path.name.startswith("official-")),
        key=lambda path: path.name,
    )


def inspect_run(path: Path, errors: list[str]) -> Run | None:
    metadata_path = path / "metadata.json"
    metadata = load_json(metadata_path, errors)
    if metadata is None:
        return None
    run_id = metadata.get("run_id")
    profile = metadata.get("profile")
    scenario = metadata.get("scenario")
    repetition = metadata.get("repetition")
    valid = True
    checks = (
        (isinstance(metadata.get("versions"), dict) and bool(metadata.get("versions")), "versions must be a non-empty object"),
        (isinstance(metadata.get("config"), dict) and bool(metadata.get("config")), "config must be a non-empty object"),
        (isinstance(run_id, str) and run_id.startswith("official-"), "run_id must start with official-"),
        (run_id == path.name, "run_id must match its directory name"),
        (profile in {"A", "B"}, "profile must be A or B"),
        (scenario in {"baseline", "fault"}, "scenario must be baseline or fault"),
        (isinstance(repetition, int) and not isinstance(repetition, bool) and repetition >= 1, "repetition must be an integer >= 1"),
        (metadata.get("status") == "passed", "status must be passed"),
        (metadata.get("dry_run") is False, "dry_run must be false"),
    )
    for condition, message in checks:
        if not condition:
            errors.append(f"{metadata_path}: {message}")
            valid = False
    artifacts = BASE_ARTIFACTS + (FAULT_ARTIFACTS if scenario == "fault" else ())
    for relative in artifacts:
        if not (path / relative).is_file():
            errors.append(f"{path}: missing required artifact {relative}")
            valid = False
    summary = load_json(path / "summary.json", errors) if (path / "summary.json").is_file() else None
    recovery: dict[str, Any] | None = None
    if scenario == "fault" and (path / "recovery.json").is_file():
        recovery = load_json(path / "recovery.json", errors)
    if summary is None:
        valid = False
    else:
        for metric in SUMMARY_METRICS:
            value = summary.get(metric)
            if not isinstance(value, (int, float)) or isinstance(value, bool):
                errors.append(f"{path / 'summary.json'}: metric {metric} must be numeric")
                valid = False
    if scenario == "fault":
        if recovery is None:
            valid = False
        else:
            for metric in RECOVERY_METRICS:
                value = recovery.get(metric)
                if not isinstance(value, (int, float)) or isinstance(value, bool):
                    errors.append(f"{path / 'recovery.json'}: metric {metric} must be numeric")
                    valid = False
    return Run(path, metadata, summary or {}, recovery) if valid else None


def normalized_config(metadata: dict[str, Any]) -> Any:
    config = metadata.get("config")
    if not isinstance(config, dict):
        return config
    normalized = dict(config)
    normalized.pop("topic", None)
    normalized.pop("consumer_group", None)
    return normalized


def consistency_errors(runs: Sequence[Run]) -> list[str]:
    errors: list[str] = []
    if not runs:
        return errors
    expected_versions = runs[0].metadata.get("versions")
    for run in runs[1:]:
        if run.metadata.get("versions") != expected_versions:
            errors.append(f"{run.path / 'metadata.json'}: experimental versions differ from {runs[0].path / 'metadata.json'}")
    for profile in ("A", "B"):
        profile_runs = [run for run in runs if run.metadata.get("profile") == profile]
        if not profile_runs:
            continue
        expected_config = normalized_config(profile_runs[0].metadata)
        for run in profile_runs[1:]:
            if normalized_config(run.metadata) != expected_config:
                errors.append(f"{run.path / 'metadata.json'}: profile {profile} configuration differs from {profile_runs[0].path / 'metadata.json'}")
    return errors


def check(raw_dir: Path, repetitions: int) -> int:
    errors: list[str] = []
    runs = [run for path in discover(raw_dir) if (run := inspect_run(path, errors)) is not None]
    errors.extend(consistency_errors(runs))
    by_key: dict[tuple[str, str, int], list[Run]] = {}
    for run in runs:
        by_key.setdefault(run.key, []).append(run)
    for profile, scenario in COMBINATIONS:
        combination = [run for run in runs if run.key[:2] == (profile, scenario)]
        if len(combination) != repetitions:
            errors.append(f"{profile}/{scenario}: expected exactly {repetitions} valid runs, found {len(combination)}")
        for repetition in range(1, repetitions + 1):
            found = by_key.get((profile, scenario, repetition), [])
            if not found:
                errors.append(f"{profile}/{scenario}: missing repetition {repetition}")
            elif len(found) > 1:
                errors.append(f"{profile}/{scenario}: duplicate repetition {repetition}: {', '.join(run.run_id for run in found)}")
        unexpected = sorted({run.key[2] for run in combination if run.key[2] > repetitions})
        for repetition in unexpected:
            errors.append(f"{profile}/{scenario}: unexpected repetition {repetition} (expected 1..{repetitions})")
    if errors:
        print("matrix check failed:", file=sys.stderr)
        for error in errors:
            print(f"- {error}", file=sys.stderr)
        return 1
    print("profile scenario  runs")
    for profile, scenario in COMBINATIONS:
        count = sum(run.key[:2] == (profile, scenario) for run in runs)
        print(f"{profile:<7} {scenario:<8} {count:>4}")
    return 0


def run_row(run: Run) -> dict[str, Any]:
    row: dict[str, Any] = {
        "run_id": run.run_id,
        "profile": run.metadata["profile"],
        "scenario": run.metadata["scenario"],
        "repetition": run.metadata["repetition"],
        "created_at": run.metadata.get("created_at", ""),
        "status": run.metadata["status"],
    }
    row.update({metric: run.summary[metric] for metric in SUMMARY_METRICS})
    for metric in RECOVERY_METRICS:
        row[metric] = run.recovery[metric] if run.recovery is not None else ""
    return row


def format_number(value: float) -> str:
    return f"{value:.6f}"


def write_aggregate(rows: Sequence[dict[str, Any]], path: Path) -> None:
    fields = ("profile", "scenario", "metric", "count", "mean", "stddev", "minimum", "maximum")
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        for profile, scenario in COMBINATIONS:
            group = [row for row in rows if (row["profile"], row["scenario"]) == (profile, scenario)]
            for metric in (*SUMMARY_METRICS, *RECOVERY_METRICS):
                values = [float(row[metric]) for row in group if row[metric] != ""]
                if not values:
                    continue
                writer.writerow({
                    "profile": profile, "scenario": scenario, "metric": metric,
                    "count": len(values), "mean": format_number(statistics.fmean(values)),
                    "stddev": format_number(statistics.stdev(values) if len(values) > 1 else 0.0),
                    "minimum": format_number(min(values)), "maximum": format_number(max(values)),
                })


def aggregate(raw_dir: Path, output_dir: Path) -> int:
    errors: list[str] = []
    runs = [run for path in discover(raw_dir) if (run := inspect_run(path, errors)) is not None]
    runs.sort(key=lambda run: (run.key[2], run.key[0], run.key[1], run.run_id))
    rows = [run_row(run) for run in runs]
    output_dir.mkdir(parents=True, exist_ok=True)
    runs_path = output_dir / "runs.csv"
    with runs_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=RUN_FIELDS)
        writer.writeheader()
        writer.writerows(rows)
    aggregate_path = output_dir / "aggregate.csv"
    write_aggregate(rows, aggregate_path)
    for error in errors:
        print(f"warning: ignored invalid official run: {error}", file=sys.stderr)
    print(f"aggregated {len(rows)} valid official runs into {runs_path} and {aggregate_path}")
    return 0


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description=__doc__)
    subparsers = result.add_subparsers(dest="command", required=True)
    check_parser = subparsers.add_parser("check")
    check_parser.add_argument("--raw-dir", type=Path, default=Path("data/raw"))
    check_parser.add_argument("--repetitions", type=int, default=5)
    aggregate_parser = subparsers.add_parser("aggregate")
    aggregate_parser.add_argument("--raw-dir", type=Path, default=Path("data/raw"))
    aggregate_parser.add_argument("--output-dir", type=Path, default=Path("data/processed"))
    return result


def main(argv: Sequence[str] | None = None) -> int:
    args = parser().parse_args(argv)
    if args.command == "check":
        if not 1 <= args.repetitions <= 99:
            print("error: --repetitions must be between 1 and 99", file=sys.stderr)
            return 2
        return check(args.raw_dir, args.repetitions)
    return aggregate(args.raw_dir, args.output_dir)


if __name__ == "__main__":
    raise SystemExit(main())
