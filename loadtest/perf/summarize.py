#!/usr/bin/env python3
"""Render Markdown and JSON from recorded architecture performance trials."""
import argparse
from collections import Counter, defaultdict
import json
import math
from pathlib import Path
import re
from statistics import median

from runner import (
    LATENCY_BUCKETS, dropped_iterations, generator_exit, invalid_step, request_log,
    requests, response_count, stage_summary,
)

MISSING = "not measured"
MAX_CPU_GAP = 15


def read_json(path):
    return json.loads(path.read_text()) if path.exists() else {}


def json_lines(path):
    return [json.loads(line) for line in path.read_text().splitlines()] if path.exists() else []


def percentile(values, fraction):
    if not values:
        return MISSING
    values = sorted(values)
    rank = (len(values) - 1) * fraction
    low = int(rank)
    return values[low] + (values[math.ceil(rank)] - values[low]) * (rank - low)


def traffic(records, start, end):
    window = [item for item in records if start <= item["started"] / 1000 < end]
    counts = Counter(str(item["status"]) for item in window)
    wire = sum(item["wire_bytes"] for item in window)
    accepted = sum(item["wire_bytes"] for item in window if item["status"] == 202)
    durations = [item["duration_ms"] for item in window]
    return {
        "requests": len(window) if window else MISSING,
        "wire_bytes": wire if window else MISSING,
        "achieved_MB_s": wire / 1e6 / (end - start) if window else MISSING,
        "accepted_wire_MB": accepted / 1e6 if window else MISSING,
        "accepted_MB_s": accepted / 1e6 / (end - start) if window else MISSING,
        "status_ratios": {code: count / len(window) for code, count in sorted(counts.items())} if window else MISSING,
        "p50_ms": percentile(durations, 0.5),
        "p95_ms": percentile(durations, 0.95),
        "p99_ms": percentile(durations, 0.99),
    }


def traffic_with_summary(records, start, end, summary):
    result = traffic(records, start, end)
    if measured := summary.get("measure"):
        for field in ("wire_bytes", "raw_bytes", "achieved_MB_s", "p50_ms", "p95_ms", "p99_ms"):
            result[field] = measured[field] if measured[field] is not None else MISSING
        # Use the generator's monotonic clock for measurement boundaries.
        # Docker VM wall-clock adjustments can move logged receipt timestamps.
        if "requests_by_status" in measured:
            counts = measured["requests_by_status"] or {}
            count = sum(counts.values())
            result["requests"] = count
            result["status_ratios"] = {code: n / count for code, n in counts.items()} if count else MISSING
            accepted = measured.get("accepted_wire_bytes")
            if accepted is None and counts.get("202") == count:
                accepted = measured["wire_bytes"]
            if accepted is not None:
                result["accepted_wire_MB"] = accepted / 1e6
                result["accepted_MB_s"] = accepted / 1e6 / (end - start)
    return result


def series(rows, metric):
    for row in rows:
        if row["metric"]["__name__"] == metric:
            yield row["metric"], [
                (timestamp / 1000, value)
                for timestamp, value in zip(row["timestamps"], row["values"])
                if value is not None and math.isfinite(value)
            ]


def gauge_max(rows, metric):
    values = [value for _, samples in series(rows, metric) for _, value in samples]
    return max(values, default=MISSING)


def recovery(rows, start, end):
    return min((at - start for _, points in series(rows, "jammy_processor_lag_seconds")
                for at, value in points if start <= at <= end and value == 0), default=MISSING)


def memory_bytes(value):
    match = re.fullmatch(r"(\d+(?:\.\d+)?)\s*([kKMGTPE]?i?B)", value.strip())
    if not match:
        raise ValueError(f"Unrecognized Docker memory value {value!r}")
    quantity, units = match.groups()
    magnitude = "BKMGTPE".index(units[0].upper())
    return float(quantity) * (1024 if "i" in units else 1000) ** magnitude


def resources(rows, start, end, interval, accepted_mb):
    containers = defaultdict(list)
    margin = max(interval, MAX_CPU_GAP)
    for row in rows:
        if "Name" in row and start - margin <= row["timestamp"] <= end + margin:
            containers[row["Name"]].append(row)
    result = {}
    for name, samples in sorted(containers.items()):
        samples.sort(key=lambda row: row["timestamp"])
        memory = [memory_bytes(row["MemUsage"].split("/")[0]) for row in samples
                  if start <= row["timestamp"] < end and row.get("MemUsage") not in (None, "--")]
        seconds = coverage = 0
        last_covered = start
        max_gap = 0
        for earlier, later in zip(samples, samples[1:]):
            left, right = earlier["timestamp"], later["timestamp"]
            lo, hi = max(left, start), min(right, end)
            if hi <= lo or right - left > MAX_CPU_GAP:
                continue
            try:
                percent = float(later["CPUPerc"].rstrip("%"))
            except (KeyError, ValueError):
                continue
            if not math.isfinite(percent):
                continue
            seconds += (hi - lo) * percent / 100
            coverage += hi - lo
            max_gap = max(max_gap, lo - last_covered)
            last_covered = hi
        max_gap = max(max_gap, end - last_covered)
        service = re.sub(r"^jammy-perf-.*-[a-f0-9]{6}-", "", name)
        complete = coverage >= 0.9 * (end - start) and max_gap <= MAX_CPU_GAP
        sample = {"memory_max_MiB": max(memory) / 2**20 if memory else MISSING}
        if service == "loadgen":
            sample["cpu_seconds"] = seconds if complete else MISSING
        else:
            sample["cpu_seconds_per_accepted_MB"] = (
                seconds / accepted_mb if complete and isinstance(accepted_mb, (float, int)) and accepted_mb > 0
                else MISSING
            )
        result[service] = sample
    return result


def summarize_run(path):
    run = read_json(path / "run.json")
    loss = read_json(path / "loss.json")
    if run.get("arm") == "mvp" and "mvp_loss" not in loss and "accepted_events_match_parquet" in loss:
        loss["mvp_loss"] = "parquet"
    if run.get("arm") == "split" and "db_accounted_rows" in loss and "db_processor_events" not in loss:
        loss.update(recorded_result=loss["result"], result=MISSING,
                    detail="This split run predates collection of DB processor event counts.")
    result = {**run, "loss": loss or {"result": MISSING}, "steps": []}
    if failure := read_json(path / "failure.json"):
        result["failure"] = failure
    stats = json_lines(path / "docker-stats.jsonl")
    for stage in sorted(path.glob("*MBps")):
        meta = read_json(stage / "stage.json")
        times = read_json(stage / "windows.json")
        summary = stage_summary(stage)
        dropped = dropped_iterations(summary)
        fault = read_json(stage / "fault.json")
        step = {"name": stage.name, "rate_MB_s_per_server": meta.get("target_MB_s_per_server", MISSING),
                "generator_exit": generator_exit(times) if generator_exit(times) is not None else MISSING,
                "dropped_iterations": MISSING if dropped is None else dropped,
                "generator_starved": summary.get("generator_starved", MISSING)}
        is_fault = run.get("fault") or run.get("kind") in ("f1", "f2") or fault
        if is_fault:
            step["requests_not_sent"] = step["dropped_iterations"]
        if reason := run.get("invalid_steps", {}).get(stage.name) or invalid_step(summary, is_fault):
            step["invalid_reason"] = reason
        result["steps"].append(step)
        if not times:
            continue
        start, end = times["measure_start"], times["measure_end"]
        measured = json_lines(stage / "vm-measure.jsonl")
        boundary = json_lines(stage / "vm-boundary.jsonl")
        latency = read_json(stage / "vm-latency.json")
        records = list(requests(request_log(stage)))
        expected = response_count(summary)
        if expected != len(records):
            records = []
        step.update(
            traffic=traffic_with_summary(records, start, end, summary),
            per_server={target: traffic_with_summary(
                [item for item in records if item["target"] == f"http://{target}:8080"], start, end,
                summary.get("per_target", {}).get(f"http://{target}:8080", {}),
            ) for target in meta["targets"]},
            lag_max_seconds=gauge_max(measured, "jammy_processor_lag_seconds"),
            lag_recovery_seconds=recovery(boundary, end, times["idle_end"]),
            processor_pending_max=gauge_max(measured, "jammy_processor_consumer_pending") if run["arm"] == "split" else MISSING,
            archiver_unacked_max=gauge_max(measured, "jammy_archiver_unacked_messages") if run["arm"] == "split" else MISSING,
        )
        step.update({field: latency.get(field) if latency.get(field) is not None else MISSING
                     for field in LATENCY_BUCKETS})
        step["resources"] = resources(stats, start, end, run["stats_interval_seconds"],
                                      step["traffic"]["accepted_wire_MB"])
        if generator := step["resources"].pop("loadgen", None):
            step["loadgen_resources"] = generator
        if "start" in fault and "resume" in fault:
            step["fault"] = {
                **fault,
                "traffic": traffic(records, fault["start"], fault["resume"]),
                "lag_max_seconds": gauge_max(json_lines(stage / "vm-interruption.jsonl"),
                                             "jammy_processor_lag_seconds"),
                "lag_recovery_seconds": recovery(boundary, fault["resume"], times["idle_end"]),
            }
    return result


def repeat_summary(runs):
    repeated = defaultdict(list)
    for run in runs:
        if not run["kind"].startswith("repeat-30-") or not run["steps"]:
            continue
        step = run["steps"][0]
        if not step.get("invalid_reason"):
            repeated[f"{run['impl']}-{run['arm']}"].append(step)
    result = {}
    for combo, steps in sorted(repeated.items()):
        measurements = defaultdict(list)
        status_codes = {"202"}
        for step in steps:
            ratios = step.get("traffic", {}).get("status_ratios")
            if isinstance(ratios, dict):
                status_codes.update(ratios)
        fields = ("accepted_MB_s", "p50_ms", "p99_ms", *LATENCY_BUCKETS, "lag_max_seconds",
                  "lag_recovery_seconds", "processor_pending_max", "archiver_unacked_max")
        for step in steps:
            traffic_data = step.get("traffic", {})
            ratios = traffic_data.get("status_ratios")
            values = {key: traffic_data.get(key, step.get(key, MISSING)) for key in fields}
            for status in sorted(status_codes):
                values[f"status_{status}_ratio"] = ratios.get(status, 0) if isinstance(ratios, dict) else MISSING
            for service, metrics in step.get("resources", {}).items():
                values.update({f"{service} {name}": value for name, value in metrics.items()})
            for key, value in values.items():
                measured = measurements[key]
                if step["generator_exit"] == 0 and isinstance(value, (float, int)):
                    measured.append(value)
        result[combo] = {
            key: {"n": len(values), "median": median(values), "min": min(values), "max": max(values)}
            if values else MISSING for key, values in measurements.items()
        }
    return result


def fmt(value):
    if isinstance(value, float):
        return f"{value:.5g}"
    if isinstance(value, dict):
        return ", ".join(f"{status} {ratio:.2%}" for status, ratio in value.items())
    return MISSING if value is None else str(value)


def line(*columns):
    return "| " + " | ".join(map(fmt, columns)) + " |"


def first_line(path):
    if path.is_file():
        with path.open() as source:
            return source.readline().strip()
    return ""


def exclusion_reason(case):
    invalid = case / "INVALID.txt"
    if invalid.is_file():
        return first_line(invalid) or "Marked invalid by INVALID.txt"

    case = case.absolute()
    failed_dirs = [directory for directory in (case, *case.parents)
                   if directory.name.startswith("failed-")]
    if failed_dirs:
        for directory in failed_dirs:
            if reason := first_line(directory / "README.txt"):
                return reason
        return f"Under failed directory {failed_dirs[0].name}"
    return None


def markdown(report):
    lines = ["# Local architecture performance", ""]
    grouped = defaultdict(list)
    for run in report["runs"]:
        grouped[f"{run['impl']}-{run['arm']}"].append(run)
    for combo, runs in sorted(grouped.items()):
        lines.extend([f"## {combo}", "",
                      "| Run / target | MB/s per server offered | Accepted MB/s | Status ratios | Requests not sent (warmup + measure) | Generator starved | Ingest p50 ms | Ingest p99 ms | Store p99 s | Commit p99 s | Lag max s | Lag recovery s | Processor pending max | Archiver unacked max | Loss check |",
                      "|---|---:|---:|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|"])
        for run in runs:
            loss = run["loss"]
            for step in run["steps"]:
                t = step.get("traffic", {})
                label = f"{run['id']} / {step['name']}"
                if step["generator_exit"] != 0:
                    label += " (load generator incomplete)"
                if reason := step.get("invalid_reason"):
                    label += f" (invalid: {reason})"
                dropped = step["dropped_iterations"]
                if isinstance(step.get("requests_not_sent"), int):
                    if step["generator_starved"] == MISSING:
                        dropped = f"{dropped} requests not sent because the target stalled"
                    else:
                        dropped = f"{dropped} requests not sent"
                lines.append(line(label, step["rate_MB_s_per_server"], t.get("accepted_MB_s"),
                                  t.get("status_ratios"), dropped, step["generator_starved"], t.get("p50_ms"), t.get("p99_ms"),
                                  step.get("store_p99_seconds"), step.get("commit_p99_seconds"),
                                  step.get("lag_max_seconds"),
                                  step.get("lag_recovery_seconds"), step.get("processor_pending_max"),
                                  step.get("archiver_unacked_max"), loss["result"]))
                if fault := step.get("fault"):
                    traffic_data = fault["traffic"]
                    lines.append(line(f"{run['id']} / {fault['name']} interruption",
                                      step["rate_MB_s_per_server"], traffic_data["accepted_MB_s"],
                                      traffic_data["status_ratios"], None, None, traffic_data["p50_ms"], traffic_data["p99_ms"],
                                      None, None, fault["lag_max_seconds"], fault["lag_recovery_seconds"],
                                      None, None, loss["result"]))
            if not run["steps"]:
                lines.append(line(f"{run['id']} (no load window)", *([MISSING] * 14)))
        lines.extend(["", "| Run / target | Ingest server | Achieved wire MB/s | Accepted MB/s | Status ratios | Ingest p50 ms | Ingest p95 ms | Ingest p99 ms |",
                      "|---|---|---:|---:|---|---:|---:|---:|"])
        for run in runs:
            for step in run["steps"]:
                for server, t in step.get("per_server", {}).items():
                    lines.append(line(f"{run['id']} / {step['name']}", server, t["achieved_MB_s"], t["accepted_MB_s"],
                                      t["status_ratios"], t["p50_ms"], t["p95_ms"], t["p99_ms"]))
        lines.extend(["", "| Run / target | Container | CPU-s / accepted MB | Max memory MiB |",
                      "|---|---|---:|---:|"])
        for run in runs:
            for step in run["steps"]:
                for name, sample in step.get("resources", {}).items():
                    lines.append(line(f"{run['id']} / {step['name']}", name,
                                      sample["cpu_seconds_per_accepted_MB"], sample["memory_max_MiB"]))
        if any("loadgen_resources" in step for run in runs for step in run["steps"]):
            lines.extend(["", "| Run / target | Load generator CPU-s | Max memory MiB |",
                          "|---|---:|---:|"])
            for run in runs:
                for step in run["steps"]:
                    if sample := step.get("loadgen_resources"):
                        lines.append(line(f"{run['id']} / {step['name']}", sample["cpu_seconds"],
                                          sample["memory_max_MiB"]))
        if runs[0]["arm"] == "mvp":
            lines.extend(["", "The DB rule compares 202 receipt events with live DB rows plus rows in dropped exports. "
                          "The Parquet rule also requires the Parquet row count to match.", "",
                          "| Run | 202 receipt events | Live DB rows on first check | Live DB rows on final check | DB rows in dropped exports | DB rows accounted | Parquet rows | Arm-1 loss rule | Result |",
                          "|---|---:|---:|---:|---:|---:|---:|---|---|"])
            db_fields = ("db_raw_events_rows_first_check", "db_raw_events_rows",
                         "db_exported_rows", "db_accounted_rows")
        else:
            lines.extend(["", "| Run | 202 receipt events | DB processor events (`sum(jammy.summary_1m.count)`) | Parquet rows | Result |",
                          "|---|---:|---:|---:|---|"])
            db_fields = ("db_processor_events",)
        for run in runs:
            loss = run["loss"]
            rule = {"db": "DB", "parquet": "Parquet"}.get(loss.get("mvp_loss"), MISSING)
            rule_column = (rule,) if run["arm"] == "mvp" else ()
            lines.append(line(run["id"], loss.get("accepted_events"),
                              *(loss.get(field) for field in db_fields), loss.get("parquet_rows"),
                              *rule_column, loss["result"]))
        for run in runs:
            loss = run["loss"]
            if detail := loss.get("error") or loss.get("detail"):
                lines.extend(["", f"{run['id']} — {detail}", ""])
            if failure := run.get("failure"):
                lines.extend(["", f"{run['id']} — {failure['type']} {failure['reason']}", ""])
            if "archive_drain_seconds" in loss:
                lines.extend(["", f"{run['id']} — Archive drain wait {fmt(loss['archive_drain_seconds'])} s.", ""])
            if loss["result"] == "not measured (archiver backlog)" and (backlog := loss.get("archive_backlog")):
                for stream, counts in backlog["streams"].items():
                    lines.append(f"- `{stream}` pending {fmt(counts['num_pending'])}, "
                                 f"awaiting acknowledgment {fmt(counts['num_ack_pending'])}")
                lines.append(f"- Archiver unacked messages {fmt(backlog['unacked_messages'])}")
                lines.extend(f"- {error}" for error in backlog.get("errors", []))
            if "db_processor_batches" in loss:
                matched = loss.get("processor_batches_matched")
                verdict = MISSING if matched is None else "matched" if matched else "mismatch"
                lines.extend(["", f"{run['id']} — Processor batch IDs {verdict}. "
                              f"202 receipts with accepted events covered {fmt(loss.get('accepted_batches_with_events'))} distinct batches "
                              f"and DB recorded {fmt(loss['db_processor_batches'])} processor batches.", ""])
            if "persisted_after_error_response_batches" in loss:
                lines.extend(["", f"{run['id']} — {loss['persisted_after_error_response_batches']} batches "
                              "persisted after error response (client would retry; idempotent by batch_id). "
                              f"Missing accepted batches {loss['missing_accepted_batches']}. "
                              f"Unexplained processor batches {loss['unexplained_processor_batches']}.", ""])
        if combo in report["repeat_30"]:
            lines.extend(["", "### Fresh 30 MB/s repetitions", "",
                          "| Metric | Observations | Median | Min | Max |", "|---|---:|---:|---:|---:|"])
            for key, values in report["repeat_30"][combo].items():
                lines.append(line(key, *(values[field] for field in ("n", "median", "min", "max")))
                             if isinstance(values, dict) else line(key, MISSING, MISSING, MISSING, MISSING))
        lines.append("")
    lines.extend(["## Excluded runs", ""])
    if excluded := report.get("excluded_runs"):
        lines.extend(f"- `{run['path']}` — {run['reason']}" for run in excluded)
    else:
        lines.append("No runs excluded.")
    return "\n".join(lines) + "\n"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("raw", type=Path, help="run, combination, or raw results directory")
    parser.add_argument("--out", type=Path, required=True, help="Markdown path with a sibling JSON file")
    args = parser.parse_args()
    paths = sorted(args.raw.rglob("run.json"))
    if not paths:
        parser.error(f"no run.json under {args.raw}")
    runs, excluded_runs = [], []
    for path in paths:
        case = path.parent
        if reason := exclusion_reason(case):
            excluded_runs.append({"path": case.relative_to(args.raw).as_posix(), "reason": reason})
        else:
            runs.append(summarize_run(case))
    report = {"runs": runs, "repeat_30": repeat_summary(runs), "excluded_runs": excluded_runs}
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(markdown(report))
    args.out.with_suffix(".json").write_text(json.dumps(report, indent=2) + "\n")


if __name__ == "__main__":
    main()
