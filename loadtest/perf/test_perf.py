from contextlib import contextmanager, nullcontext
from io import BytesIO
import json
from pathlib import Path
import subprocess
import sys
from tempfile import TemporaryDirectory
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import runner
import summarize


class PerfFixtures(unittest.TestCase):
    def setUp(self):
        runner.TOOLS.mkdir(parents=True, exist_ok=True)
        work = TemporaryDirectory(dir=runner.TOOLS)
        self.addCleanup(work.cleanup)
        self.case = Path(work.name)
        self.stage = self.case / "01-1MBps"
        self.summary = self.stage / "results" / self.stage.name / "summary.json"
        self.summary.parent.mkdir(parents=True)
        runner.write_json(self.summary, {"metrics": {
            "jammy_k6_ingest_responses": {"values": {"count": 3}},
        }})
        records = [
            {"status": 202, "accepted_events": 2, "batch_id": "first"},
            {"status": 202, "accepted_events": 3, "batch_id": "second"},
            {"status": 503, "accepted_events": None, "batch_id": "rejected"},
        ]
        (self.stage / "k6-console.log").write_text("".join(
            "PERF_REQUEST " + json.dumps({**record, "started": 2000, "duration_ms": 1,
                                           "wire_bytes": 3, "target": "http://ingest-a1:8080"}) + "\n"
            for record in records
        ))
        self.times = {"start": 0, "measure_start": 1, "measure_end": 21, "idle_end": 26, "k6_exit": 0}
        runner.write_json(self.stage / "windows.json", self.times)
        runner.write_json(self.stage / "stage.json", {"targets": ["ingest-a1"]})

    @contextmanager
    def drain_fixture(self, snapshots):
        elapsed = 0
        polls = []
        snapshots = iter(snapshots)

        def sleep(seconds):
            nonlocal elapsed
            elapsed += seconds

        def fetch(url, timeout):
            self.assertGreater(timeout, 0)
            if url == "http://127.0.0.1:8222/jsz?streams=true&consumers=true":
                polls.append(elapsed)
                self.pending, self.in_flight, self.unacked = next(snapshots)
                streams = []
                for name, pending in self.pending.items():
                    streams.append({"name": name, "consumer_detail": [
                        {"name": "processor", "num_pending": 9, "num_ack_pending": 3},
                        {"name": "archiver", "num_pending": pending,
                         "num_ack_pending": self.in_flight.get(name, 0)},
                    ]})
                return BytesIO(json.dumps({"account_details": [{"stream_detail": streams}]}).encode())
            self.assertEqual(url, "http://127.0.0.1:19097/metrics")
            if self.unacked is None:
                return BytesIO(b"# no gauge published yet\n")
            return BytesIO(f'jammy_archiver_unacked_messages{{service="archiver"}} {self.unacked}\n'.encode())

        with (patch.object(runner.time, "monotonic", side_effect=lambda: elapsed),
              patch.object(runner.time, "sleep", side_effect=sleep),
              patch.object(runner, "urlopen", side_effect=fetch)):
            yield polls

    def collect_loss(self, arm, db, parquet, mvp_loss="parquet", snapshots=None, drain_timeout=25):
        def query(_env, sql):
            if arm == "split":
                self.assertEqual(self.pending, {"raw-a1": 0, "raw-a2": 0})
                self.assertFalse(any(self.in_flight.values()))
                self.assertEqual(self.unacked, 0)
                self.assertIn("jammy.summary_1m", sql)
                self.assertIn("jammy.processed_batches", sql)
                return db["jammy.summary_1m"] + "|" + json.dumps(db["jammy.processed_batches"].splitlines())
            for table, rows in db.items():
                if table in sql:
                    return rows
            self.fail(f"Unexpected SQL for {arm}: {sql}")

        if snapshots is None:
            snapshots = [({"raw-a1": 0, "raw-a2": 0}, {}, 0)]
        args = SimpleNamespace(arm=arm, mvp_loss=mvp_loss, archive_timeout=0, s3_endpoint=None,
                               archive_drain_timeout=drain_timeout)
        with (patch.object(runner, "psql", side_effect=query),
              patch.object(runner, "output", return_value=parquet) as parquet_count,
              patch.object(runner, "sleep_until") as partition_wait,
              self.drain_fixture(snapshots) as self.drain_polls):
            runner.loss_check(self.case, args, "helper", {}, [(self.stage, self.times)])
            if arm == "split" and parquet is None:
                parquet_count.assert_not_called()
            if arm == "mvp" and mvp_loss == "db":
                parquet_count.assert_not_called()
                partition_wait.assert_not_called()
        runner.write_json(self.case / "run.json", {
            "id": "fixture", "kind": "repeat-30-1", "arm": arm, "impl": "go",
            "scrape_interval_seconds": 5, "stats_interval_seconds": 5,
        })
        run = summarize.summarize_run(self.case)
        report = {"runs": [run], "repeat_30": summarize.repeat_summary([run])}
        return run["loss"], summarize.markdown(report)

    def test_direct_db_matches_before_and_after_drop(self):
        for raw, dropped in [(5, 0), (2, 3), (0, 5)]:
            with self.subTest(raw=raw, dropped=dropped):
                loss, markdown = self.collect_loss("mvp", {"jammy.raw_events": f"{raw}|1|{dropped}"}, "5")
                self.assertEqual(loss["result"], "matched")
                self.assertIn(f"| fixture | 5 | {raw} | {raw} | {dropped} | 5 | 5 | Parquet | matched |", markdown)

    def test_mvp_cli_defaults_to_db_and_accepts_parquet(self):
        for options, expected in [([], "db"), (["--mvp-loss", "parquet"], "parquet")]:
            with patch.object(runner.sys, "argv", ["runner.py", "--arm", "mvp", "--impl", "go", *options]):
                self.assertEqual(runner.parse_args("sweep").mvp_loss, expected)

    def test_db_loss_checks_live_and_exported_rows_without_parquet(self):
        for raw, exported, verdict in [(5, 0, "matched"), (2, 3, "matched"), (2, 2, "mismatch")]:
            with self.subTest(raw=raw, exported=exported):
                loss, markdown = self.collect_loss(
                    "mvp", {"jammy.raw_events": f"{raw}|{exported}"}, None, mvp_loss="db",
                )
                self.assertEqual(loss["result"], verdict)
                self.assertEqual(loss["parquet_rows"],
                                 "not measured (Arm-1 export runs after hour close; see prereg change log)")
                self.assertIn(f"| DB | {verdict} |", markdown)

    def test_finalize_loss_recounts_saved_requests(self):
        runner.write_json(self.case / "run.json", {"arm": "mvp", "rates": [1]})
        runner.write_json(self.case / "loss.json", {
            "accepted_events": 99, "db_raw_events_rows_first_check": 4,
            "error": "Waiting for loss counts to reconcile",
        })
        with patch.object(runner, "psql") as psql, patch.object(runner, "output") as output:
            runner.finalize_loss(self.case, 2, 3)
            psql.assert_not_called()
            output.assert_not_called()
        loss = json.loads((self.case / "loss.json").read_text())
        self.assertTrue(loss["finalized_offline"])
        self.assertEqual(loss["accepted_events"], 5)
        self.assertEqual(loss["db_raw_events_rows_first_check"], 4)
        self.assertEqual(loss["db_raw_events_rows"], 2)
        self.assertEqual(loss["result"], "matched")
        self.assertNotIn("error", loss)

    def test_finalize_loss_rejects_incomplete_request_evidence(self):
        runner.write_json(self.case / "run.json", {"arm": "mvp", "rates": [1]})
        (self.stage / "k6-console.log").unlink()
        runner.finalize_loss(self.case, 0)
        loss = json.loads((self.case / "loss.json").read_text())
        self.assertTrue(loss["finalized_offline"])
        self.assertIsNone(loss["accepted_events"])
        self.assertEqual(loss["result"], "not measured")

    def test_direct_db_checks_physical_parquet_independently(self):
        loss, _ = self.collect_loss("mvp", {"jammy.raw_events": "0|0|5"}, "4")
        self.assertEqual(loss["result"], "mismatch")

    def test_direct_db_checks_accounted_rows_independently(self):
        loss, _ = self.collect_loss("mvp", {"jammy.raw_events": "2|1|2"}, "5")
        self.assertEqual(loss["result"], "mismatch")

    def test_split_uses_committed_summary_events(self):
        for committed, parquet, verdict in [(5, "5", "matched"), (4, "5", "mismatch"), (5, "4", "mismatch")]:
            with self.subTest(committed=committed, parquet=parquet):
                loss, markdown = self.collect_loss("split", {
                    "jammy.summary_1m": str(committed), "jammy.processed_batches": "first\nsecond",
                }, parquet)
                self.assertEqual(loss["result"], verdict)
                self.assertIn("DB processor events (`sum(jammy.summary_1m.count)`)", markdown)
                self.assertIn(f"| fixture | 5 | {committed} | {parquet} | {verdict} |", markdown)

    def test_split_waits_for_each_stream_and_archiver_unacked(self):
        loss, _ = self.collect_loss("split", {
            "jammy.summary_1m": "5", "jammy.processed_batches": "first\nsecond",
        }, "5", snapshots=[
            ({"raw-a1": 0, "raw-a2": 120}, {}, 0),
            ({"raw-a1": 0, "raw-a2": 0}, {"raw-a2": 2}, 0),
            ({"raw-a1": 0, "raw-a2": 0}, {}, 100),
            ({"raw-a1": 0, "raw-a2": 0}, {}, 0),
        ], drain_timeout=40)
        self.assertEqual(self.drain_polls, [0, 10, 20, 30])
        self.assertEqual(loss["archive_drain_seconds"], 30)
        self.assertEqual(loss["result"], "matched")

    def test_split_drain_timeout_reports_backlog_without_counting(self):
        loss, markdown = self.collect_loss("split", {}, None, snapshots=[
            ({"raw-a1": 0, "raw-a2": 101}, {"raw-a2": 20}, 100),
            ({"raw-a1": 0, "raw-a2": 90}, {"raw-a2": 10}, 80),
        ], drain_timeout=19)
        self.assertEqual(self.drain_polls, [0, 10])
        self.assertEqual(loss["archive_drain_seconds"], 19)
        self.assertEqual(loss["result"], "not measured (archiver backlog)")
        self.assertEqual(loss["archive_backlog"]["streams"]["raw-a2"],
                         {"num_pending": 90, "num_ack_pending": 10})
        self.assertEqual(loss["archive_backlog"]["unacked_messages"], 80)
        self.assertIn("raw-a2", markdown)

    def test_missing_stream_or_metric_cannot_prove_drain(self):
        for pending, unacked in [({"raw-a1": 0}, 0),
                                 ({"raw-a1": 0, "raw-a2": 0}, None)]:
            with self.subTest(pending=pending, unacked=unacked):
                loss, _ = self.collect_loss("split", {}, None, snapshots=[(pending, {}, unacked)],
                                            drain_timeout=5)
                self.assertEqual(loss["result"], "not measured (archiver backlog)")

    def test_split_classifies_only_persisted_503s_and_timeouts(self):
        log = self.stage / "k6-console.log"
        with log.open("a") as stream:
            for status, batch_id, timed_out in [(0, "timeout", True), (0, "transport", False),
                                                 (400, "invalid", False), (503, "not-persisted", False)]:
                stream.write("PERF_REQUEST " + json.dumps({
                    "status": status, "batch_id": batch_id, "timed_out": timed_out,
                    "accepted_events": None, "started": 2000, "duration_ms": 1,
                    "wire_bytes": 3, "target": "http://ingest-a1:8080",
                }) + "\n")
        runner.write_json(self.summary, {"requests_by_status": {"202": 2, "503": 2, "400": 1, "0": 2}})
        matched = "first\nsecond\nrejected\ntimeout"
        for batches, parquet, persisted, verdict in [
            (matched, "12", 2, "matched"), (matched, "11", 2, "mismatch"),
            (matched.replace("second\n", ""), "12", 2, "mismatch"),
            (matched + "\ntransport", "12", 2, "mismatch"),
            (matched + "\ninvalid", "12", 2, "mismatch"),
            (matched + "\nunsent", "12", 2, "mismatch"),
        ]:
            with self.subTest(batches=batches, parquet=parquet):
                loss, markdown = self.collect_loss("split", {
                    "jammy.summary_1m": "12", "jammy.processed_batches": batches,
                }, parquet)
                self.assertEqual(loss["result"], verdict)
                self.assertEqual(loss["persisted_after_error_response_batches"], persisted)
                self.assertIn("persisted after error response (client would retry; idempotent by batch_id)", markdown)

    def test_archive_drain_timeout_cli(self):
        for options, expected in [([], 1800), (["--archive-drain-timeout", "42"], 42)]:
            with patch.object(runner.sys, "argv", ["runner.py", "--arm", "split", "--impl", "go", *options]):
                self.assertEqual(runner.parse_args("sweep").archive_drain_timeout, expected)

    def test_incomplete_receipts_do_not_prove_loss(self):
        self.summary.unlink()
        loss, _ = self.collect_loss("mvp", {"jammy.raw_events": "0|0|5"}, "5")
        self.assertIsNone(loss["accepted_events"])
        self.assertEqual(loss["result"], "not measured")

    def record_drops(self, count):
        data = json.loads(self.summary.read_text())
        data["metrics"]["dropped_iterations"] = {"values": {"count": count}}
        runner.write_json(self.summary, data)

    def test_dropped_sweep_repetition_is_reported_and_excluded(self):
        self.collect_loss("mvp", {"jammy.raw_events": "5|1|0"}, "5")
        valid = summarize.summarize_run(self.case)
        self.record_drops(4)
        invalid = summarize.summarize_run(self.case)
        repeated = summarize.repeat_summary([valid, invalid])
        self.assertEqual(repeated["go-mvp"]["accepted_MB_s"]["n"], 1)
        self.assertEqual(summarize.repeat_summary([invalid]), {})
        self.assertEqual(invalid["steps"][0]["dropped_iterations"], 4)
        report = summarize.markdown({"runs": [invalid], "repeat_30": repeated})
        self.assertIn("invalid: dropped iterations", report)

    def test_runner_flags_dropped_sweep_step_in_run_record(self):
        self.record_drops(4)
        args = SimpleNamespace(
            out=self.case / "output", impl="go", arm="mvp", tag="tag", warmup=0, measure=20,
            idle=5, stats_interval=5, scrape_interval=0, seed=42, events_per_batch=100, assets=1,
        )

        def drive(stage, *_):
            runner.write_json(stage / "windows.json", self.times)
            summary = stage / "results" / stage.name / "summary.json"
            summary.parent.mkdir(parents=True)
            summary.write_bytes(self.summary.read_bytes())
            return self.times

        def loss(case, *_):
            runner.write_json(case / "loss.json", {"result": "matched"})

        with (patch.object(runner, "stack", return_value=nullcontext({})),
              patch.object(runner, "psql"), patch.object(runner, "wait_for_metrics"),
              patch.object(runner, "collect_stats"), patch.object(runner, "export_metrics"),
              patch.object(runner, "drive", side_effect=drive),
              patch.object(runner, "loss_check", side_effect=loss)):
            runner.run_case(args, "helper", "sweep", [1], None)
        record = json.loads(next(args.out.rglob("run.json")).read_text())
        self.assertEqual(record.get("invalid_steps"), {"01-1MBps": "dropped iterations"})

    def test_split_case_starts_drain_before_final_idle_and_flags_backlog(self):
        args = SimpleNamespace(
            out=self.case / "output", impl="go", arm="split", tag="tag", warmup=0, measure=20,
            idle=5, stats_interval=5, scrape_interval=0, seed=42, events_per_batch=100, assets=1,
        )
        phases = []

        def loss(case, *_):
            phases.append("drain")
            runner.write_json(case / "loss.json", {"result": "not measured (archiver backlog)"})

        with (patch.object(runner, "stack", return_value=nullcontext({})),
              patch.object(runner, "psql"), patch.object(runner, "wait_for_metrics"),
              patch.object(runner, "collect_stats"), patch.object(runner, "export_metrics"),
              patch.object(runner, "drive", return_value=self.times),
              patch.object(runner, "sleep_until", side_effect=lambda _: phases.append("idle")),
              patch.object(runner, "loss_check", side_effect=loss)):
            self.assertFalse(runner.run_case(args, "helper", "sweep", [1, 2], None))
        self.assertEqual(phases, ["idle", "drain", "idle"])

    def test_fault_drops_are_reported_alongside_status_ratios(self):
        self.record_drops(4)
        for fault in ("F1", "F2"):
            with self.subTest(fault=fault):
                runner.write_json(self.case / "run.json", {
                    "id": "fixture", "kind": fault.lower(), "fault": fault, "impl": "go",
                    "arm": "mvp", "stats_interval_seconds": 5,
                })
                runner.write_json(self.stage / "fault.json", {"name": fault, "start": 1, "resume": 4})
                run = summarize.summarize_run(self.case)
                report = summarize.markdown({"runs": [run], "repeat_30": {}})
                self.assertEqual(run["steps"][0]["requests_not_sent"], 4)
                self.assertIn("202 66.67%, 503 33.33% | 4 requests not sent because the target stalled", report)
                self.assertNotIn("invalid", report)

    def test_loadgen_summary_flags_fault_starvation_and_separates_cpu(self):
        url = "http://ingest-a1:8080"
        measurement = {
            "achieved_MB_s": 10.1, "wire_bytes": 202000000, "raw_bytes": 230000000,
            "accepted_wire_bytes": 180000000, "requests_by_status": {"202": 2, "503": 1},
            "p50_ms": 5, "p95_ms": 20, "p99_ms": 28,
        }
        runner.write_json(self.stage / "summary.json", {
            "measure": measurement, "per_target": {url: {"measure": measurement}},
            "requests_by_status": {"202": 2, "503": 1},
            "not_sent": 4, "generator_starved": 2,
        })
        (self.stage / "k6-console.log").rename(self.stage / "loadgen.log")
        times = dict(self.times, loadgen_exit=0)
        del times["k6_exit"]
        runner.write_json(self.stage / "windows.json", times)
        runner.write_json(self.case / "run.json", {
            "id": "fixture", "kind": "f1", "fault": "F1", "impl": "go",
            "arm": "mvp", "stats_interval_seconds": 5,
        })
        (self.case / "docker-stats.jsonl").write_text("".join(
            json.dumps({"Name": service, "timestamp": timestamp, "CPUPerc": "50%"}) + "\n"
            for service in ("ingest", "jammy-perf-fixture-123456-loadgen")
            for timestamp in (1, 6, 11, 16, 21)
        ))
        step = summarize.summarize_run(self.case)["steps"][0]
        self.assertEqual(step["invalid_reason"], "generator starved")
        self.assertEqual(step["requests_not_sent"], 4)
        self.assertEqual(step["generator_starved"], 2)
        self.assertEqual(step["per_server"]["ingest-a1"]["achieved_MB_s"], 10.1)
        self.assertEqual(step["traffic"]["accepted_MB_s"], 9.0)
        self.assertEqual(step["traffic"]["p99_ms"], 28)
        self.assertEqual(set(step["resources"]), {"ingest"})
        self.assertEqual(step["loadgen_resources"]["cpu_seconds"], 10)

    def test_cpu_coverage_allows_jitter_but_rejects_large_gaps(self):
        for end, timestamps, expected in [
            (100, [4, 12, 23, 35, 44, 54, 63, 75, 87, 96], 4.6),
            (100, [5, 20, 35, 50, 65, 80, 95], 4.5),
            (100, [6, 20, 35, 50, 65, 80, 94], summarize.MISSING),
            (200, [0, 10, *range(26, 197, 10), 200], summarize.MISSING),
            (200, list(range(16, 197, 10)) + [200], summarize.MISSING),
        ]:
            with self.subTest(timestamps=timestamps):
                samples = [{"Name": "ingest", "timestamp": at, "CPUPerc": "50%"} for at in timestamps]
                result = summarize.resources(samples, 0, end, 5, 10)
                self.assertEqual(result["ingest"]["cpu_seconds_per_accepted_MB"], expected)

    def test_stats_commands_share_one_poll_budget(self):
        elapsed = 0
        timeouts = []

        def docker_output(command, timeout):
            nonlocal elapsed
            timeouts.append(timeout)
            if command[1] == "ps":
                elapsed += 4
                return "container-id"
            elapsed += timeout
            raise subprocess.TimeoutExpired(command, timeout)

        stop = SimpleNamespace(is_set=lambda: len(timeouts) >= 2, wait=lambda _: None)
        with patch.object(runner.time, "monotonic", side_effect=lambda: elapsed):
            with patch.object(runner, "output", side_effect=docker_output):
                runner.collect_stats(stop, "project", self.case / "stats.jsonl", 5)
        self.assertEqual(timeouts, [5, 1])

    def test_legacy_split_counts_cannot_support_a_three_way_verdict(self):
        runner.write_json(self.case / "run.json", {"arm": "split", "stats_interval_seconds": 5})
        runner.write_json(self.case / "loss.json", {
            "accepted_events": 5, "db_accounted_rows": 0, "parquet_rows": 5, "result": "mismatch",
        })
        run = summarize.summarize_run(self.case)
        self.assertEqual(run["loss"]["result"], "not measured")
        self.assertEqual(run["steps"][0]["store_p99_seconds"], "not measured")

    def test_victoriametrics_p99_flows_into_report(self):
        samples = {
            "jammy_ingest_store_duration_seconds_bucket": "0.012",
            "jammy_processor_commit_duration_seconds_bucket": "0.034",
        }
        queries = {
            f'histogram_quantile(0.99, sum by (le) (increase({metric}{{run_id="{self.case.name}"}}[20000ms])))': sample
            for metric, sample in samples.items()
        }

        def fetch(base_url, path, params):
            self.assertEqual(base_url, "http://vm.example:8428")
            if path == "/api/v1/export":
                return b""
            self.assertEqual(path, "/api/v1/query")
            self.assertEqual(params["time"], runner.iso(21))
            self.assertEqual(params["nocache"], 1)
            return json.dumps({"status": "success", "data": {"resultType": "vector", "result": [
                {"metric": {}, "value": [21, queries[params["query"]]]},
            ]}}).encode()

        args = SimpleNamespace(scrape_interval=5, stats_interval=5, vm_url="http://vm.example:8428")
        with patch.object(runner, "vm_request", side_effect=fetch):
            runner.export_metrics(self.case, [(self.stage, self.times)], args)
        _, markdown = self.collect_loss("mvp", {"jammy.raw_events": "0|0|5"}, "5")
        self.assertIn("| Store p99 s | Commit p99 s |", markdown)
        self.assertIn("| 0.012 | 0.034 |", markdown)
        run = summarize.summarize_run(self.case)
        for field, expected in [("store_p99_seconds", 0.012), ("commit_p99_seconds", 0.034)]:
            self.assertEqual(run["steps"][0][field], expected)
            self.assertEqual(summarize.repeat_summary([run])["go-mvp"][field]["median"], expected)

    def test_victoriametrics_null_samples_are_skipped(self):
        runner.write_json(self.case / "run.json", {"arm": "split", "stats_interval_seconds": 5})
        lag = {"__name__": "jammy_processor_lag_seconds"}
        pending = {"__name__": "jammy_processor_consumer_pending"}
        exports = {
            "vm-measure.jsonl": [
                {"metric": lag, "timestamps": [1000, 2000], "values": [None, 2.5]},
                {"metric": pending, "timestamps": [1000], "values": [None]},
            ],
            "vm-boundary.jsonl": [
                {"metric": lag, "timestamps": [22000, 24000], "values": [None, 0]},
            ],
        }
        for filename, rows in exports.items():
            (self.stage / filename).write_text("".join(json.dumps(row) + "\n" for row in rows))

        step = summarize.summarize_run(self.case)["steps"][0]
        self.assertEqual(step["lag_max_seconds"], 2.5)
        self.assertEqual(step["lag_recovery_seconds"], 3)
        self.assertEqual(step["processor_pending_max"], "not measured")

    def test_excluded_runs_are_reported_without_reading_their_data(self):
        raw = self.case / "raw"
        valid = raw / "go-mvp" / "valid"
        invalid = raw / "go-mvp" / "invalid"
        failed_group = raw / "failed-k6-oom"
        failed = failed_group / "nested" / "trial"
        for case in (valid, invalid, failed):
            case.mkdir(parents=True)
        runner.write_json(valid / "run.json", {
            "id": "valid", "kind": "sweep", "arm": "mvp", "impl": "go",
        })
        (invalid / "run.json").write_text("incomplete")
        (invalid / "INVALID.txt").write_text("Host suspended\nMore invalid-run details\n")
        (failed / "run.json").write_text("incomplete")
        (failed_group / "README.txt").write_text("Generator ran out of memory\nMore failure details\n")
        out = self.case / "summary.md"

        subprocess.run([sys.executable, summarize.__file__, str(raw), "--out", str(out)], check=True)

        report = json.loads(out.with_suffix(".json").read_text())
        self.assertEqual([run["id"] for run in report["runs"]], ["valid"])
        self.assertEqual(report["excluded_runs"], [
            {"path": "failed-k6-oom/nested/trial", "reason": "Generator ran out of memory"},
            {"path": "go-mvp/invalid", "reason": "Host suspended"},
        ])
        markdown = out.read_text()
        self.assertIn("## Excluded runs", markdown)
        self.assertIn("`failed-k6-oom/nested/trial` — Generator ran out of memory", markdown)
        self.assertIn("`go-mvp/invalid` — Host suspended", markdown)
        self.assertNotIn("More ", markdown)
