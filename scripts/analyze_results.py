#!/usr/bin/env python3

import csv
import statistics
from collections import defaultdict
from pathlib import Path

INPUT = Path("data/processed/runs.csv")

COUNT_METRICS = [
    "attempted",
    "acknowledged",
    "failed",
    "consumed",
    "acknowledged_missing",
    "duplicates",
    "sequence_regressions",
    "sequence_gaps",
    "out_of_order",
    "final_lag",
]

RECOVERY_METRICS = [
    "infrastructure_seconds",
    "kafka_seconds",
    "application_seconds",
    "performance_seconds",
]


def number(value):
    if value is None or value.strip() == "":
        return None
    return float(value)


def describe(values):
    values = [value for value in values if value is not None]

    if not values:
        return "sem dados"

    return (
        f"n={len(values)} | "
        f"mediana={statistics.median(values):.3f} | "
        f"média={statistics.fmean(values):.3f} | "
        f"mín={min(values):.3f} | "
        f"máx={max(values):.3f}"
    )


with INPUT.open(newline="", encoding="utf-8") as file:
    rows = list(csv.DictReader(file))

groups = defaultdict(list)

for row in rows:
    groups[(row["profile"], row["scenario"])].append(row)

print("=== COBERTURA ===")
for group in [
    ("A", "baseline"),
    ("B", "baseline"),
    ("A", "fault"),
    ("B", "fault"),
]:
    print(f"{group[0]}/{group[1]}: {len(groups[group])}")

print("\n=== MÉTRICAS POR GRUPO ===")

for group in [
    ("A", "baseline"),
    ("B", "baseline"),
    ("A", "fault"),
    ("B", "fault"),
]:
    print(f"\n--- {group[0]}/{group[1]} ---")

    for metric in COUNT_METRICS + RECOVERY_METRICS:
        values = [number(row.get(metric)) for row in groups[group]]
        values = [value for value in values if value is not None]

        if values:
            print(f"{metric:26} {describe(values)}")

print("\n=== TAXAS POR EXECUÇÃO ===")

header = [
    "profile",
    "scenario",
    "rep",
    "ack_success_pct",
    "failure_pct",
    "consume_vs_ack_pct",
    "missing_pct",
    "duplicate_pct",
    "gap_pct",
]

print("\t".join(header))

for row in sorted(
    rows,
    key=lambda r: (
        r["profile"],
        r["scenario"],
        int(r["repetition"]),
    ),
):
    attempted = float(row["attempted"])
    acknowledged = float(row["acknowledged"])
    failed = float(row["failed"])
    consumed = float(row["consumed"])
    missing = float(row["acknowledged_missing"])
    duplicates = float(row["duplicates"])
    gaps = float(row["sequence_gaps"])

    ack_success = acknowledged / attempted * 100 if attempted else 0
    failure_rate = failed / attempted * 100 if attempted else 0
    consume_rate = consumed / acknowledged * 100 if acknowledged else 0
    missing_rate = missing / acknowledged * 100 if acknowledged else 0
    duplicate_rate = duplicates / consumed * 100 if consumed else 0
    gap_rate = gaps / consumed * 100 if consumed else 0

    values = [
        row["profile"],
        row["scenario"],
        row["repetition"],
        f"{ack_success:.4f}",
        f"{failure_rate:.4f}",
        f"{consume_rate:.4f}",
        f"{missing_rate:.4f}",
        f"{duplicate_rate:.4f}",
        f"{gap_rate:.4f}",
    ]

    print("\t".join(values))
