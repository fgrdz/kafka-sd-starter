from __future__ import annotations

import csv
import importlib.util
import io
import json
import tempfile
import sys
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path

MODULE_PATH = Path(__file__).parents[1] / "scripts" / "matrix_results.py"
SPEC = importlib.util.spec_from_file_location("matrix_results", MODULE_PATH)
assert SPEC and SPEC.loader
matrix_results = importlib.util.module_from_spec(SPEC)
sys.modules["matrix_results"] = matrix_results
SPEC.loader.exec_module(matrix_results)


class MatrixResultsTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.raw = self.root / "raw"
        self.output = self.root / "processed"
        self.raw.mkdir()

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def create_run(
        self,
        profile: str,
        scenario: str,
        repetition: int,
        *,
        suffix: str = "abcd",
        status: str = "passed",
        dry_run: bool = False,
        official: bool = True,
        versions: dict[str, str] | None = None,
        missing: str | None = None,
        attempted: int = 10,
        recovery_base: float = 1.0,
    ) -> Path:
        prefix = "official" if official else "pilot"
        run_id = f"{prefix}-{profile.lower()}-{scenario}-r{repetition:02d}-20260730190000-{suffix}"
        run = self.raw / run_id
        run.mkdir(parents=True)
        metadata = {
            "run_id": run_id,
            "profile": profile,
            "scenario": scenario,
            "repetition": repetition,
            "created_at": "2026-07-30T19:00:00Z",
            "status": status,
            "dry_run": dry_run,
            "versions": versions or {"KAFKA_VERSION": "4.1.0", "STRIMZI_VERSION": "0.47.0"},
            "config": {
                "profile": profile,
                "topic": run_id,
                "consumer_group": run_id,
                "replication_factor": 1 if profile == "A" else 3,
                "min_insync_replicas": 1 if profile == "A" else 2,
            },
        }
        summary = {
            "attempted": attempted,
            "acknowledged": attempted,
            "failed": 0,
            "consumed": attempted,
            "acknowledged_missing": 0,
            "duplicates": 0,
            "sequence_regressions": 0,
            "sequence_gaps": 0,
            "out_of_order": 0,
            "final_lag": 0,
        }
        artifacts = set(matrix_results.BASE_ARTIFACTS)
        if scenario == "fault":
            artifacts.update(matrix_results.FAULT_ARTIFACTS)
        for relative in artifacts:
            if relative == missing:
                continue
            path = run / relative
            path.parent.mkdir(parents=True, exist_ok=True)
            if relative == "metadata.json":
                value = metadata
            elif relative == "summary.json":
                value = summary
            elif relative == "recovery.json":
                value = {
                    "infrastructure_seconds": recovery_base,
                    "kafka_seconds": recovery_base + 1,
                    "application_seconds": recovery_base + 2,
                    "performance_seconds": recovery_base + 3,
                }
            elif relative.endswith(".json"):
                value = {}
            else:
                path.write_text("{}\n", encoding="utf-8")
                continue
            path.write_text(json.dumps(value), encoding="utf-8")
        return run

    def complete_matrix(self, repetitions: int = 1) -> None:
        for repetition in range(1, repetitions + 1):
            for index, (profile, scenario) in enumerate(matrix_results.COMBINATIONS):
                self.create_run(profile, scenario, repetition, suffix=f"{repetition}{index}aa")

    def run_check(self, repetitions: int = 1) -> tuple[int, str, str]:
        stdout, stderr = io.StringIO(), io.StringIO()
        with redirect_stdout(stdout), redirect_stderr(stderr):
            result = matrix_results.check(self.raw, repetitions)
        return result, stdout.getvalue(), stderr.getvalue()

    def test_complete_valid_matrix(self) -> None:
        self.complete_matrix(2)
        result, stdout, stderr = self.run_check(2)
        self.assertEqual(result, 0, stderr)
        self.assertIn("A       baseline", stdout)
        self.assertIn("B       fault", stdout)

    def test_missing_repetition(self) -> None:
        self.complete_matrix(2)
        target = next(path for path in self.raw.iterdir() if "-a-baseline-r02-" in path.name)
        for child in sorted(target.rglob("*"), reverse=True):
            if child.is_file():
                child.unlink()
            else:
                child.rmdir()
        target.rmdir()
        result, _, stderr = self.run_check(2)
        self.assertEqual(result, 1)
        self.assertIn("A/baseline: missing repetition 2", stderr)

    def test_duplicate_repetition(self) -> None:
        self.complete_matrix()
        self.create_run("A", "baseline", 1, suffix="duplicate")
        result, _, stderr = self.run_check()
        self.assertEqual(result, 1)
        self.assertIn("duplicate repetition 1", stderr)

    def test_failed_official_is_reported_and_not_counted(self) -> None:
        self.complete_matrix()
        failed = next(path for path in self.raw.iterdir() if "-b-fault-" in path.name)
        metadata_path = failed / "metadata.json"
        metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
        metadata["status"] = "failed"
        metadata_path.write_text(json.dumps(metadata), encoding="utf-8")
        result, _, stderr = self.run_check()
        self.assertEqual(result, 1)
        self.assertIn("status must be passed", stderr)
        self.assertIn("B/fault: missing repetition 1", stderr)

    def test_pilot_and_dry_run_are_not_counted(self) -> None:
        self.complete_matrix()
        self.create_run("A", "baseline", 1, official=False)
        self.create_run("B", "baseline", 1, suffix="dryrun", dry_run=True)
        result, _, stderr = self.run_check()
        self.assertEqual(result, 1)
        self.assertNotIn("pilot-", stderr)
        self.assertIn("dry_run must be false", stderr)

    def test_missing_fault_recovery(self) -> None:
        self.complete_matrix()
        fault = next(path for path in self.raw.iterdir() if "-a-fault-" in path.name)
        (fault / "recovery.json").unlink()
        result, _, stderr = self.run_check()
        self.assertEqual(result, 1)
        self.assertIn("missing required artifact recovery.json", stderr)

    def test_versions_divergence(self) -> None:
        self.complete_matrix()
        target = next(path for path in self.raw.iterdir() if "-b-fault-" in path.name)
        metadata_path = target / "metadata.json"
        metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
        metadata["versions"]["KAFKA_VERSION"] = "different"
        metadata_path.write_text(json.dumps(metadata), encoding="utf-8")
        result, _, stderr = self.run_check()
        self.assertEqual(result, 1)
        self.assertIn("experimental versions differ", stderr)

    def test_aggregate_baseline_fault_and_deterministic_order(self) -> None:
        self.create_run("B", "fault", 2, suffix="z", attempted=20)
        self.create_run("A", "baseline", 1, suffix="y", attempted=10)
        self.create_run("A", "fault", 1, suffix="x", attempted=15)
        with redirect_stdout(io.StringIO()), redirect_stderr(io.StringIO()):
            self.assertEqual(matrix_results.aggregate(self.raw, self.output), 0)
        with (self.output / "runs.csv").open(newline="", encoding="utf-8") as handle:
            rows = list(csv.DictReader(handle))
        self.assertEqual(
            [(row["repetition"], row["profile"], row["scenario"]) for row in rows],
            [("1", "A", "baseline"), ("1", "A", "fault"), ("2", "B", "fault")],
        )
        baseline = rows[0]
        fault = rows[1]
        self.assertEqual(baseline["infrastructure_seconds"], "")
        self.assertEqual(fault["infrastructure_seconds"], "1.0")

    def test_sample_standard_deviation(self) -> None:
        self.create_run("A", "baseline", 1, suffix="one", attempted=10)
        self.create_run("A", "baseline", 2, suffix="two", attempted=20)
        matrix_results.aggregate(self.raw, self.output)
        with (self.output / "aggregate.csv").open(newline="", encoding="utf-8") as handle:
            rows = list(csv.DictReader(handle))
        attempted = next(row for row in rows if row["profile"] == "A" and row["scenario"] == "baseline" and row["metric"] == "attempted")
        self.assertEqual(attempted["stddev"], "7.071068")

    def test_single_run_standard_deviation_is_zero(self) -> None:
        self.create_run("B", "fault", 1)
        matrix_results.aggregate(self.raw, self.output)
        with (self.output / "aggregate.csv").open(newline="", encoding="utf-8") as handle:
            rows = list(csv.DictReader(handle))
        self.assertTrue(rows)
        self.assertTrue(all(row["stddev"] == "0.000000" for row in rows))


if __name__ == "__main__":
    unittest.main()
