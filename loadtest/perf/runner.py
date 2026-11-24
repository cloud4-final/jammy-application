"""Run a local architecture performance sweep or fault trial."""
import argparse
from contextlib import contextmanager
from datetime import datetime, timezone
import json
import math
import os
from pathlib import Path
import platform
import re
import shutil
import signal
import subprocess
import sys
import tempfile
import threading
import time
from urllib.parse import urlencode
from urllib.request import urlopen

ROOT = Path(__file__).resolve().parents[2]
PERF = ROOT / "loadtest/perf"
TOOLS = Path(os.environ.get("JAMMY_TOOLS_DIR") or Path.home() / ".cache/jammy-tools").expanduser().resolve()
COMPOSE = ["docker", "compose", "--project-directory", "deploy/compose"]
LOADGEN = "jammy-perf-loadgen:local"
DROPPED_ITERATIONS = "dropped iterations"
MVP_PARQUET_NOT_MEASURED = "not measured (Arm-1 export runs after hour close; see prereg change log)"
INCOMPLETE_RECEIPTS = "Incomplete load generator request records or invalid 202 receipts"
ARCHIVER_BACKLOG = "not measured (archiver backlog)"
# The split Compose archiver subscribes to both streams under this durable.
ARCHIVER_STREAMS = ("raw-a1", "raw-a2")
ARCHIVER_CONSUMER = "archiver"
ARCHIVE_DRAIN_POLL_SECONDS = 10
ARCHIVER_UNACKED = "jammy_archiver_unacked_messages"
METRICS = (
    "jammy_ingest_store_duration_seconds(_bucket|_count|_sum)?",
    "jammy_processor_commit_duration_seconds(_bucket|_count|_sum)?",
    "jammy_processor_lag_seconds",
    "jammy_processor_consumer_pending",
    ARCHIVER_UNACKED,
    "jammy_archiver_files_total",
    "up",
)
LATENCY_BUCKETS = {
    "store_p99_seconds": "jammy_ingest_store_duration_seconds_bucket",
    "commit_p99_seconds": "jammy_processor_commit_duration_seconds_bucket",
}


def iso(timestamp=None):
    return datetime.fromtimestamp(
        time.time() if timestamp is None else timestamp, timezone.utc
    ).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def utc_timestamp(value):
    # Docker emits nanoseconds, while the system Python on macOS reads microseconds.
    value = re.sub(r"(\.\d{6})\d+(?=Z$)", r"\1", value)
    return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()


def write_json(path, value):
    path.write_text(json.dumps(value, indent=2) + "\n")


def stage_summary(stage):
    summary = stage / "summary.json"
    if not summary.exists():
        summary = stage / "results" / stage.name / "summary.json"
    return json.loads(summary.read_text()) if summary.exists() else {}


def dropped_iterations(summary):
    if "not_sent" in summary:
        return summary["not_sent"]
    if summary:
        return summary.get("metrics", {}).get("dropped_iterations", {}).get("values", {}).get("count", 0)
    return None


def response_count(summary):
    if "requests_by_status" in summary:
        return sum(summary["requests_by_status"].values())
    return summary.get("metrics", {}).get("jammy_k6_ingest_responses", {}).get("values", {}).get("count")


def generator_exit(times):
    return times.get("loadgen_exit", times.get("k6_exit"))


def invalid_step(summary, fault):
    if summary.get("generator_starved", 0):
        return "generator starved"
    if not fault and (dropped_iterations(summary) or 0) > 0:
        return DROPPED_ITERATIONS
    return None


def request_log(stage):
    log = stage / "loadgen.log"
    return log if log.exists() else stage / "k6-console.log"


def command(args, env=None, **kwargs):
    return subprocess.run(
        list(map(str, args)), cwd=ROOT, env=env, check=True, text=True, **kwargs
    )


def output(args, env=None, timeout=None):
    return command(args, env, stdout=subprocess.PIPE, timeout=timeout).stdout.strip()


def bounded_int(value):
    number = int(value)
    if number < 0:
        raise argparse.ArgumentTypeError("must be nonnegative")
    return number


def positive_int(value):
    number = bounded_int(value)
    if not number:
        raise argparse.ArgumentTypeError("must be positive")
    return number


def rate(value):
    number = float(value)
    if not math.isfinite(number) or number <= 0:
        raise argparse.ArgumentTypeError("must be a finite positive MB/s value")
    return number


def parse_args(mode):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--impl", choices=("go", "py"), required=True)
    parser.add_argument("--arm", choices=("mvp", "split"), required=True)
    parser.add_argument("--out", type=Path)
    parser.add_argument("--tag", default=os.environ.get("TAG"))
    parser.add_argument("--vm-url", default=os.environ.get("VM_URL") or "http://127.0.0.1:8428",
                        help="VictoriaMetrics query URL (overrides VM_URL)")
    parser.add_argument("--s3-endpoint", help="S3 URL for the Parquet loss check (overrides S3_ENDPOINT)")
    parser.add_argument("--mvp-loss", choices=("db", "parquet"), default="db",
                        help="Arm-1 loss rule: reconcile DB rows (default) or also count Parquet rows")
    parser.add_argument("--warmup", type=bounded_int, default=60, metavar="SECONDS")
    parser.add_argument("--measure", type=positive_int, default=180, metavar="SECONDS")
    parser.add_argument("--idle", type=bounded_int, default=60, metavar="SECONDS")
    parser.add_argument("--grace", type=bounded_int, default=30, metavar="SECONDS")
    parser.add_argument("--stats-interval", type=positive_int, default=5, metavar="SECONDS")
    parser.add_argument("--scrape-interval", type=positive_int, default=5, metavar="SECONDS")
    parser.add_argument("--startup-timeout", type=positive_int, default=120, metavar="SECONDS")
    parser.add_argument("--archive-timeout", type=positive_int, default=300, metavar="SECONDS")
    parser.add_argument("--archive-poll", type=positive_int, default=5, metavar="SECONDS")
    parser.add_argument("--archive-drain-timeout", type=positive_int, default=1800, metavar="SECONDS",
                        help="maximum wait for the split archiver to drain (default: 1800)")
    parser.add_argument("--wait-for-idle", type=bounded_int, default=0, metavar="SECONDS")
    parser.add_argument("--wait-poll", type=positive_int, default=60, metavar="SECONDS")
    parser.add_argument("--seed", type=bounded_int, default=42)
    parser.add_argument("--events-per-batch", type=positive_int, default=1000)
    parser.add_argument("--assets", type=positive_int, default=16)
    parser.add_argument("--max-in-flight", type=positive_int, default=os.environ.get("MAX_IN_FLIGHT", "64"),
                        help="maximum in-flight requests per ingest target")
    parser.add_argument("--payload-anchor", help="RFC3339 payload timestamp anchor (default: invocation start)")
    if mode == "sweep":
        parser.add_argument("--steps", default="10 20 30 40")
        parser.add_argument("--repeat-30", type=bounded_int, default=3)
    else:
        parser.add_argument("--fault", choices=("F1", "F2", "both"), default="both")
        parser.add_argument("--rate", type=rate, default=30)
        parser.add_argument("--pause", type=positive_int, default=30, metavar="SECONDS")
        parser.add_argument("--stop", type=positive_int, default=60, metavar="SECONDS")
        parser.add_argument("--fault-offset", type=bounded_int, default=0, metavar="SECONDS")
        parser.add_argument("--stop-grace", type=bounded_int, default=10, metavar="SECONDS")
    args = parser.parse_args()
    try:
        if mode == "sweep":
            args.steps = [rate(item) for item in args.steps.split()]
            if not args.steps or len(set(args.steps)) != len(args.steps):
                parser.error("--steps must list distinct positive rates")
        else:
            longest = args.pause if args.fault == "F1" else args.stop
            if args.fault == "both":
                longest = max(args.pause, args.stop)
            if args.fault_offset + longest >= args.measure:
                parser.error("the selected fault must leave recovery time in the measure window")
    except (ValueError, argparse.ArgumentTypeError) as exc:
        parser.error(str(exc))
    return args


def wait_for_host(args):
    k3d = shutil.which("k3d") or str(TOOLS / "k3d")
    deadline = time.monotonic() + args.wait_for_idle
    while True:
        containers = output(["docker", "ps", "--format", "{{json .}}"])
        busy = []
        for line in containers.splitlines():
            container = json.loads(line)
            if re.search(
                r"jammy|bp-e2e|Final-Project/[^/]+/deploy/compose",
                " ".join(container[key] for key in ("Names", "Image", "Labels")),
                re.IGNORECASE,
            ):
                busy.append(container["Names"])
        clusters = json.loads(output([k3d, "cluster", "list", "-o", "json"]) or "[]")
        for cluster in clusters or []:
            fields = {key.lower(): value for key, value in cluster.items()}
            if (
                fields.get("serversrunning", 0)
                or fields.get("agentsrunning", 0)
                or "serversrunning" not in fields
            ):
                busy.append(f"k3d/{fields['name']}")
        if not busy:
            print("Host check passed: no running Jammy Compose workload or k3d cluster", flush=True)
            return
        remaining = deadline - time.monotonic()
        print("Waiting for local workloads: " + ", ".join(busy), flush=True)
        if remaining <= 0:
            raise RuntimeError("Local workloads still running")
        time.sleep(min(args.wait_poll, remaining))


def build_helper(directory):
    TOOLS.mkdir(parents=True, exist_ok=True)
    go = shutil.which("go") or str(TOOLS / "go/bin/go")
    env = os.environ.copy()
    env.setdefault("GOCACHE", str(TOOLS / "perf-go-build"))
    helper = directory / "perf-helper"
    command([go, "build", "-o", helper, "./loadtest/perf"], env)
    return helper


def image_tag():
    try:
        version = output(
            ["git", "describe", "--tags", "--abbrev=0", "--match", "v[0-9]*"]
        ).lstrip("v")
    except subprocess.CalledProcessError:
        version = "0.1.0"
    return version + "-" + output(["git", "rev-parse", "--short=7", "HEAD"])


def build_loadgen():
    command(["docker", "build", "-t", LOADGEN, "-f", PERF / "loadgen/Dockerfile", ROOT])


def vm_request(base_url, path, parameters):
    with urlopen(base_url.rstrip("/") + path + "?" + urlencode(parameters, doseq=True), timeout=30) as response:
        return response.read()


def wait_for_metrics(run_id, targets, args):
    deadline = time.monotonic() + args.startup_timeout
    required = set(targets + ["processor", "archiver"])
    query = f'up{{run_id="{run_id}"}}'
    while time.monotonic() < deadline:
        try:
            samples = vm_request(args.vm_url, "/api/v1/export", {
                "match[]": query, "start": iso(time.time() - 2 * args.scrape_interval),
            })
            ready = {
                row["metric"]["instance"]
                for row in map(json.loads, samples.splitlines())
                if row["values"][-1] == 1
            }
            if required <= ready:
                return
        except (OSError, ValueError, KeyError):
            pass
        time.sleep(1)
    raise RuntimeError(f"Alloy has not scraped {', '.join(sorted(required))}")


def histogram_p99(vm_url, run_id, metric, start, end):
    window = f"{round((end - start) * 1000)}ms"
    query = f'histogram_quantile(0.99, sum by (le) (increase({metric}{{run_id="{run_id}"}}[{window}])))'
    # Short idle windows can otherwise trigger VM's recent-query time adjustment.
    response = json.loads(vm_request(vm_url, "/api/v1/query", {"query": query, "time": iso(end), "nocache": 1}))
    if response["status"] != "success":
        raise ValueError(response.get("error", "VictoriaMetrics query failed"))
    results = response["data"]["result"]
    if not results:
        return None
    value = float(results[0]["value"][1])
    return value if math.isfinite(value) else None


def export_metrics(case, windows, args):
    selector = '{__name__=~"' + "|".join(METRICS) + f'",run_id="{case.name}"' + '}'
    for stage, times in windows:
        start, finish, idle_end = (times[key] for key in ("start", "measure_end", "idle_end"))
        ranges = {
            "warmup": (start, times["measure_start"]),
            "measure": (times["measure_start"], finish),
            "idle": (finish, idle_end),
            "boundary": (start - 2 * args.scrape_interval, idle_end + args.stats_interval),
        }
        fault_path = stage / "fault.json"
        if fault_path.exists():
            fault = json.loads(fault_path.read_text())
            if "start" in fault and "resume" in fault:
                ranges["interruption"] = (fault["start"], fault["resume"])
                ranges["recovery"] = (fault["resume"], idle_end)
        for name, (lo, hi) in ranges.items():
            filepath = stage / f"vm-{name}.jsonl"
            try:
                filepath.write_bytes(vm_request(args.vm_url, "/api/v1/export", {
                    "match[]": [selector], "start": iso(lo), "end": iso(hi),
                }))
            except OSError as exc:
                with (stage / "errors.log").open("a") as errors:
                    print(f"VictoriaMetrics export ({name}): {exc}", file=errors)
        latency = {}
        for field, metric in LATENCY_BUCKETS.items():
            try:
                latency[field] = histogram_p99(args.vm_url, case.name, metric, times["measure_start"], finish)
            except (OSError, ValueError, KeyError) as exc:
                with (stage / "errors.log").open("a") as errors:
                    print(f"VictoriaMetrics query ({metric}): {exc}", file=errors)
        write_json(stage / "vm-latency.json", latency)


def collect_stats(stop, project, path, interval):
    with path.open("w", buffering=1) as log:
        while not stop.is_set():
            deadline = time.monotonic() + interval
            try:
                ids = output(["docker", "ps", "-q", "--filter", f"label=com.docker.compose.project={project}"], timeout=interval)
                remaining = deadline - time.monotonic()
                if ids and remaining > 0 and not stop.is_set():
                    capture = output(["docker", "stats", "--no-stream", "--format", "{{json .}}", *ids.split()],
                                     timeout=remaining)
                    at = time.time()
                    for line in capture.splitlines():
                        print(json.dumps(dict(json.loads(line), timestamp=at)), file=log)
            except (OSError, ValueError, subprocess.SubprocessError) as exc:
                print(json.dumps({"timestamp": time.time(), "error": str(exc)}), file=log)
                deadline = time.monotonic() + interval
            stop.wait(max(0, deadline - time.monotonic()))


@contextmanager
def record_failure(case):
    try:
        yield
    except (OSError, ValueError, RuntimeError, subprocess.SubprocessError, KeyboardInterrupt) as exc:
        write_json(case / "failure.json", {"type": type(exc).__name__, "reason": str(exc)})
        raise


@contextmanager
def stack(case, args, project):
    alloy = case / "alloy-config.alloy"
    original = (ROOT / "observability/alloy/compose.alloy").read_text()
    alloy.write_text(original.replace(
        'prometheus.scrape "services" {',
        f'prometheus.scrape "services" {{\n\tscrape_interval = "{args.scrape_interval}s"'
        f'\n\tscrape_timeout = "{min(4, args.scrape_interval)}s"',
        1,
    ))
    override = case / "compose.override.json"
    write_json(override, {"services": {"alloy": {"volumes": [
        f"{alloy}:/etc/alloy/jammy/compose.alloy:ro"
    ]}}})
    env = dict(
        os.environ, COMPOSE_PROJECT_NAME=project,
        COMPOSE_FILE=f"{ROOT / 'deploy/compose/docker-compose.yml'}:{override}",
        IMPL=args.impl, ARM=args.arm, TAG=args.tag, APPS="1",
        RUN_ID=case.name, PROFILE="mixed",
    )
    with (case / "stack.log").open("w") as log:
        try:
            command([*COMPOSE, "--profile", "*", "down", "-v"], env, stdout=log, stderr=subprocess.STDOUT)
            command(["make", f"up-{args.arm}"], env, stdout=log, stderr=subprocess.STDOUT)
            command(["make", "up-obs"], env, stdout=log, stderr=subprocess.STDOUT)
            command([sys.executable, "tests/e2e/wait_ready.py", args.arm], env,
                    stdout=log, stderr=subprocess.STDOUT)
            yield env
        finally:
            try:
                with (case / "services.log").open("w") as services:
                    command([*COMPOSE, "--profile", "*", "logs", "--no-color", "--tail", "200"], env,
                            stdout=services, stderr=subprocess.STDOUT, timeout=45)
            except (OSError, subprocess.SubprocessError) as exc:
                print(f"Could not collect Compose logs ({exc})", file=log)
            try:
                command(["make", "down"], env, stdout=log, stderr=subprocess.STDOUT)
            finally:
                command([*COMPOSE, "--profile", "*", "down", "--volumes"], env,
                        stdout=log, stderr=subprocess.STDOUT)


def psql(env, query):
    return output([
        *COMPOSE, "--profile", "mvp", "exec", "-T", "pg-primary",
        "psql", "-XAt", "-v", "ON_ERROR_STOP=1", "-U", "jammy", "-d", "jammy",
        "-c", query,
    ], env, timeout=60)


def sleep_until(when):
    time.sleep(max(0, when - time.time()))


def read_marker(path, process, timeout):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if path.exists():
            with path.open() as log:
                for line in log:
                    marker = re.search(r"PERF_START (\d+)", line)
                    if marker:
                        return int(marker[1]) / 1000
        if process.poll() is not None:
            raise RuntimeError(f"loadgen exited before it started (status {process.returncode})")
        time.sleep(0.1)
    raise RuntimeError("loadgen never emitted its start timestamp")


def do_fault(stage, fault, env, args, planned):
    service = "pg-primary" if fault == "F1" else "processor"
    cid = output([*COMPOSE, "--profile", args.arm, "ps", "-q", service], env)
    if not cid:
        raise RuntimeError(f"{service} container unavailable")
    event = {"name": fault, "service": service, "planned_start": planned}
    path = stage / "fault.json"
    sleep_until(planned)
    event["action_requested"] = time.time()
    restore = False
    try:
        if fault == "F1":
            command(["docker", "pause", cid], timeout=args.startup_timeout)
            restore = True
            event["start"] = time.time()
            duration = args.pause
        else:
            command(["docker", "stop", "--time", args.stop_grace, cid], timeout=args.startup_timeout)
            restore = True
            finished = output(["docker", "inspect", "--format", "{{.State.FinishedAt}}", cid])
            event["start"] = utc_timestamp(finished)
            duration = args.stop
        write_json(path, event)
        sleep_until(event["start"] + duration)
    finally:
        if restore:
            event["resume_requested"] = time.time()
            command(["docker", "unpause" if fault == "F1" else "start", cid],
                    timeout=args.startup_timeout)
            event["resume"] = time.time()
        write_json(path, event)


def drive(stage, args, env, network, targets, load, fault):
    write_json(stage / "stage.json", {
        "target_MB_s_per_server": load, "targets": targets,
        "seed": args.seed, "payload_anchor": args.payload_anchor, "max_in_flight": args.max_in_flight,
    })
    project = env["COMPOSE_PROJECT_NAME"]
    name = f"{project}-loadgen"
    loadgen = [
        "docker", "run", "--rm", "--name", name,
        "--label", f"com.docker.compose.project={project}",
        "--network", network,
        LOADGEN, "--summary", "-",
        "--targets", ",".join(f"http://{target}:8080" for target in targets),
        "--wire-mb-s", load, "--duration", f"{args.measure}s", "--warmup", f"{args.warmup}s",
        "--seed", args.seed, "--events-per-batch", args.events_per_batch, "--assets", args.assets,
        "--anchor", args.payload_anchor, "--max-in-flight", args.max_in_flight,
        "--grace", f"{args.grace}s", "--linger", f"{args.stats_interval + 2}s",
    ]
    times = {}
    log_path = stage / "loadgen.log"
    with log_path.open("w") as log:
        with subprocess.Popen(list(map(str, loadgen)), cwd=ROOT, stdout=log, stderr=subprocess.STDOUT) as process:
            try:
                start = read_marker(log_path, process, args.startup_timeout)
                measure_start = start + args.warmup
                measure_end = measure_start + args.measure
                times = {
                    "start": start, "measure_start": measure_start,
                    "measure_end": measure_end, "idle_end": measure_end + args.idle,
                }
                write_json(stage / "windows.json", times)
                if fault:
                    do_fault(stage, fault, env, args, measure_start + args.fault_offset)
                finish = measure_end + args.grace + args.stats_interval + 2
                status = process.wait(timeout=max(1, finish - time.time()) + args.startup_timeout)
                times["loadgen_exit"] = status
                write_json(stage / "windows.json", times)
            finally:
                if process.poll() is None:
                    subprocess.run(["docker", "rm", "-f", name], check=False, stdout=log, stderr=log)
                    process.terminate()
    # Docker multiplexes one stream from the non-root container. Keep raw
    # request records in the log and materialize its single summary record.
    with log_path.open() as log:
        for line in log:
            if line.startswith("PERF_SUMMARY "):
                write_json(stage / "summary.json", json.loads(line.removeprefix("PERF_SUMMARY ")))
                break
        else:
            if status == 0:
                raise RuntimeError(f"loadgen did not emit a summary; see {log_path}")
    return times


def requests(path):
    if path.exists():
        with path.open() as log:
            for line in log:
                if "PERF_REQUEST " in line:
                    yield json.loads(line.split("PERF_REQUEST ", 1)[1])


def receipt_counts(windows):
    accepted_events = 0
    accepted_batch_ids = set()
    error_batch_ids = set()
    invalid = 0
    accounted = bool(windows)
    for stage, times in windows:
        summary = stage_summary(stage)
        if not summary or generator_exit(times) != 0:
            accounted = False
            continue
        expected = response_count(summary)
        observed = 0
        for item in requests(request_log(stage)):
            observed += 1
            if item["status"] == 202:
                if item["accepted_events"] is None:
                    invalid += 1
                else:
                    accepted_events += item["accepted_events"]
                    if item["accepted_events"] > 0:
                        accepted_batch_ids.add(item["batch_id"])
            elif (item["status"] == 503 or
                  (item["status"] == 0 and item.get("timed_out") is True)):
                if batch_id := item.get("batch_id"):
                    error_batch_ids.add(batch_id)
        accounted &= bool(observed and expected == observed)
    loss = {"accepted_events": accepted_events if accounted and not invalid else None,
            "observed_accepted_events": accepted_events, "invalid_202_receipts": invalid,
            "accepted_batches_with_events": len(accepted_batch_ids) if accounted and not invalid else None}
    return loss, accepted_batch_ids, error_batch_ids


def mvp_db_loss(loss, raw, exported, error=None):
    loss.update(mvp_loss="db", parquet_rows=MVP_PARQUET_NOT_MEASURED,
                accepted_events_match_parquet=None, checked_at=iso())
    if error is None:
        loss.update(db_raw_events_rows=raw, db_exported_rows=exported, db_accounted_rows=raw + exported)
    if loss["accepted_events"] is None:
        error = f"{error}. {INCOMPLETE_RECEIPTS}" if error else INCOMPLETE_RECEIPTS
    if error:
        loss.update(result="not measured", error=error)
    else:
        loss["result"] = "matched" if loss["accepted_events"] == loss["db_accounted_rows"] else "mismatch"
    return loss


def finalize_loss(case, raw, exported=0):
    run = json.loads((case / "run.json").read_text())
    if run.get("arm") != "mvp":
        raise ValueError("finalize-loss requires an Arm-1 (mvp) case")
    stages = set(case.glob("*MBps"))
    stages.update(case / f"{index:02d}-{load:g}MBps"
                  for index, load in enumerate(run.get("rates", []), 1))
    windows = []
    for stage in sorted(stages):
        path = stage / "windows.json"
        windows.append((stage, json.loads(path.read_text()) if path.exists() else {}))
    loss, _, _ = receipt_counts(windows)
    path = case / "loss.json"
    if path.exists():
        previous = json.loads(path.read_text())
        if "db_raw_events_rows_first_check" in previous:
            loss["db_raw_events_rows_first_check"] = previous["db_raw_events_rows_first_check"]
    loss["finalized_offline"] = True
    write_json(path, mvp_db_loss(loss, raw, exported))
    return loss


def archiver_pending(timeout):
    with urlopen("http://127.0.0.1:8222/jsz?streams=true&consumers=true", timeout=timeout) as response:
        info = json.load(response)
    counters = ("num_pending", "num_ack_pending")
    pending = {stream: dict.fromkeys(counters) for stream in ARCHIVER_STREAMS}
    for account in info.get("account_details", []):
        for stream in account.get("stream_detail", []):
            if stream["name"] not in pending:
                continue
            for consumer in stream.get("consumer_detail", []):
                if consumer["name"] == ARCHIVER_CONSUMER:
                    pending[stream["name"]] = {field: consumer.get(field) for field in counters}
    return pending


def archiver_unacked(env, timeout):
    port = env.get("ADMIN_PORT_ARCHIVER") or "19097"
    with urlopen(f"http://127.0.0.1:{port}/metrics", timeout=timeout) as response:
        metrics = response.read().decode()
    samples = re.findall(rf"^{ARCHIVER_UNACKED}(?:\{{[^\n]*\}})?[ \t]+(\S+)", metrics, re.MULTILINE)
    values = [float(value) for value in samples]
    if not values or not all(value >= 0 and value.is_integer() for value in values):
        raise ValueError(f"Missing or invalid {ARCHIVER_UNACKED} samples")
    return sum(map(int, values))


def wait_for_archive_drain(env, timeout):
    start = time.monotonic()
    deadline = start + timeout
    next_poll = start
    backlog = {}
    while (poll_start := time.monotonic()) < deadline:
        errors = []
        try:
            streams = archiver_pending(min(5, deadline - poll_start))
        except (OSError, ValueError, KeyError, TypeError) as exc:
            streams = {stream: {"num_pending": None, "num_ack_pending": None} for stream in ARCHIVER_STREAMS}
            errors.append(f"JetStream consumer status failed ({exc})")
        unacked = None
        if (remaining := deadline - time.monotonic()) > 0:
            try:
                unacked = archiver_unacked(env, min(5, remaining))
            except (OSError, ValueError) as exc:
                errors.append(f"Archiver metrics failed ({exc})")
        backlog = {"streams": streams, "unacked_messages": unacked}
        if errors:
            backlog["errors"] = errors
        now = time.monotonic()
        if now <= deadline and unacked == 0 and all(
            all(value == 0 for value in counts.values()) for counts in streams.values()
        ):
            return True, round(now - start, 3), backlog
        next_poll += ARCHIVE_DRAIN_POLL_SECONDS
        time.sleep(max(0, min(next_poll, deadline) - now))
    return False, round(time.monotonic() - start, 3), backlog


def split_loss_check(loss, accepted_batch_ids, error_batch_ids, args, parquet_command, env):
    drained, elapsed, backlog = wait_for_archive_drain(env, args.archive_drain_timeout)
    loss.update(archive_drain_seconds=elapsed, archive_backlog=backlog)
    if not drained:
        loss.update(result=ARCHIVER_BACKLOG, checked_at=iso())
        return
    errors = []
    try:
        # The ledger stores IDs; summary_1m records event counts in the batch
        # claim transaction. Read both from the same PostgreSQL snapshot.
        result = psql(env, "SELECT coalesce(sum(count), 0), "
                      "(SELECT coalesce(json_agg(batch_id), '[]') FROM jammy.processed_batches "
                      "WHERE consumer='processor') FROM jammy.summary_1m")
        events, batch_ids = result.split("|", 1)
        committed = set(json.loads(batch_ids))
        loss.update(db_processor_events=int(events), db_processor_batches=len(committed))
        if loss["accepted_batches_with_events"] is not None:
            missing = accepted_batch_ids - committed
            extra = committed - accepted_batch_ids
            persisted = extra & error_batch_ids
            unexplained = extra - persisted
            loss.update(processor_batches_matched=not missing and not unexplained,
                        missing_accepted_batches=len(missing),
                        unexplained_processor_batches=len(unexplained),
                        persisted_after_error_response_batches=len(persisted))
    except (OSError, ValueError, subprocess.SubprocessError) as exc:
        errors.append(f"PostgreSQL count failed ({exc})")
    try:
        loss["parquet_rows"] = int(output(parquet_command, timeout=305))
    except (OSError, ValueError, subprocess.SubprocessError) as exc:
        errors.append(f"Parquet count failed ({exc})")
    if loss["accepted_events"] is None:
        errors.append(INCOMPLETE_RECEIPTS)
    loss["checked_at"] = iso()
    if errors:
        loss.update(result="not measured", error=". ".join(errors))
    else:
        loss["accepted_events_match_parquet"] = loss["accepted_events"] == loss["parquet_rows"]
        loss["processor_events_match_parquet"] = loss["db_processor_events"] == loss["parquet_rows"]
        loss["result"] = (
            "matched" if loss["processor_batches_matched"] and loss["processor_events_match_parquet"] else "mismatch"
        )


def loss_check(case, args, helper, env, windows):
    loss, accepted_batch_ids, error_batch_ids = receipt_counts(windows)
    if args.arm == "mvp" and args.mvp_loss == "db":
        try:
            result = psql(env, "SELECT count(*), "
                          "(SELECT coalesce(sum(rows),0) FROM jammy.archive_exports WHERE dropped_at IS NOT NULL) "
                          "FROM jammy.raw_events")
            raw, exported = map(int, result.split("|"))
        except (OSError, ValueError, subprocess.SubprocessError) as exc:
            mvp_db_loss(loss, None, None, error=f"PostgreSQL count failed ({exc})")
        else:
            loss["db_raw_events_rows_first_check"] = raw
            mvp_db_loss(loss, raw, exported)
        write_json(case / "loss.json", loss)
        return
    cache = case / "parquet-objects.json"
    parquet_command = [helper, "parquet-rows"]
    if args.s3_endpoint:
        parquet_command.extend(["--s3-endpoint", args.s3_endpoint])
    parquet_command.append(cache)
    if args.arm == "split":
        split_loss_check(loss, accepted_batch_ids, error_batch_ids, args, parquet_command, env)
        write_json(case / "loss.json", loss)
        return
    loss["mvp_loss"] = "parquet"
    deadline = time.time() + (args.archive_timeout if loss["accepted_events"] is not None else 0)
    partition_close = None
    waiting_logged = False
    while True:
        counts = {}
        errors = []
        try:
            result = psql(env, "SELECT count(*), coalesce(extract(epoch FROM max(received_at)), 0), "
                          "(SELECT coalesce(sum(rows),0) FROM jammy.archive_exports WHERE dropped_at IS NOT NULL) "
                          "FROM jammy.raw_events")
            raw_text, received_text, exported_text = result.split("|")
            raw, exported = int(raw_text), int(exported_text)
            if loss["accepted_events"] is not None and raw and partition_close is None:
                partition_close = (int(float(received_text)) // 3600 + 1) * 3600
                deadline = max(deadline, partition_close + args.archive_timeout)
            counts.update(db_raw_events_rows=raw, db_exported_rows=exported, db_accounted_rows=raw + exported)
            loss.setdefault("db_raw_events_rows_first_check", raw)
        except (OSError, ValueError, subprocess.SubprocessError) as exc:
            errors.append(f"PostgreSQL count failed ({exc})")
        try:
            counts["parquet_rows"] = int(output(parquet_command, timeout=305))
        except (OSError, ValueError, subprocess.SubprocessError) as exc:
            errors.append(f"Parquet count failed ({exc})")
        expected_rows = loss["accepted_events"]
        counts["accepted_events_match_parquet"] = (
            counts["parquet_rows"] == expected_rows
            if expected_rows is not None and "parquet_rows" in counts else None
        )
        if expected_rows is None:
            errors.append(INCOMPLETE_RECEIPTS)
        equal = not errors and counts["accepted_events_match_parquet"] and counts["db_accounted_rows"] == expected_rows
        if equal or expected_rows is None or time.time() >= deadline:
            break
        if not waiting_logged:
            print(f"Waiting for direct DB partition export ({case.name}) until {iso(deadline)}",
                  flush=True)
            waiting_logged = True
        write_json(case / "loss.json", {**loss, **counts, "result": "not measured",
                                        "error": "Waiting for loss counts to reconcile"})
        if partition_close and time.time() < partition_close:
            sleep_until(partition_close)
            continue
        time.sleep(min(args.archive_poll, max(0, deadline - time.time())))
    loss.update(counts, checked_at=iso())
    if errors:
        loss.update(result="not measured", error=". ".join(errors))
    else:
        loss["result"] = "matched" if equal else "mismatch"
    write_json(case / "loss.json", loss)


def run_case(args, helper, label, rates, fault):
    case_id = f"{datetime.now(timezone.utc):%Y%m%dT%H%M%S}-{label}-{os.urandom(3).hex()}"
    case = args.out / f"{args.impl}-{args.arm}" / case_id
    case.mkdir(parents=True)
    targets = ["ingest-a1"] if args.arm == "mvp" else ["ingest-a1", "ingest-a2"]
    run = {
        "id": case_id, "impl": args.impl, "arm": args.arm, "tag": args.tag,
        "kind": label, "fault": fault, "rates": rates, "warmup_seconds": args.warmup,
        "measure_seconds": args.measure, "idle_seconds": args.idle,
        "stats_interval_seconds": args.stats_interval, "scrape_interval_seconds": args.scrape_interval,
        "seed": args.seed, "events_per_batch": args.events_per_batch, "assets": args.assets,
        "invalid_steps": {},
        "host": {"os": platform.system(), "arch": platform.machine()},
    }
    write_json(case / "run.json", run)
    print(f"Running {case}", flush=True)
    windows = []
    project = f"jammy-perf-{case_id.lower()}"
    with record_failure(case), stack(case, args, project) as env:
        network = f"{project}_default"
        seed_sql = "\n".join(
            f"INSERT INTO jammy.assets VALUES ('solar-{asset:06d}', 'tenant-000001', 'solar', 1000);"
            for asset in range(1, args.assets + 1)
        )
        psql(env, seed_sql)
        wait_for_metrics(case_id, targets, args)
        stop = threading.Event()
        sampler = threading.Thread(
            target=collect_stats, args=(stop, project, case / "docker-stats.jsonl", args.stats_interval)
        )
        sampler.start()
        try:
            for index, load in enumerate(rates, 1):
                stage = case / f"{index:02d}-{load:g}MBps"
                stage.mkdir()
                times = drive(stage, args, env, network, targets, load, fault)
                windows.append((stage, times))
                if reason := invalid_step(stage_summary(stage), fault):
                    run["invalid_steps"][stage.name] = reason
                    write_json(case / "run.json", run)
                if generator_exit(times) != 0:
                    raise RuntimeError(f"loadgen failed with status {generator_exit(times)}; see {stage / 'loadgen.log'}")
                if index < len(rates):
                    sleep_until(times["idle_end"])
        finally:
            try:
                if windows:
                    if args.arm == "split":
                        loss_check(case, args, helper, env, windows)
                    sleep_until(windows[-1][1]["idle_end"])
            finally:
                stop.set()
                sampler.join()
            if windows:
                # Let the final idle samples move through Alloy's remote-write queue.
                time.sleep(args.scrape_interval)
                export_metrics(case, windows, args)
                if args.arm == "mvp":
                    loss_check(case, args, helper, env, windows)
    loss = json.loads((case / "loss.json").read_text())
    print(f"Loss check ({case_id}): {loss['result']}", flush=True)
    return loss["result"] not in ("not measured", ARCHIVER_BACKLOG)


def main(mode):
    if mode == "finalize-loss":
        parser = argparse.ArgumentParser(description="Recompute an Arm-1 loss check from saved request logs.")
        parser.add_argument("case", type=Path)
        parser.add_argument("--db-raw-events-rows", type=bounded_int, required=True)
        parser.add_argument("--db-exported-rows", type=bounded_int, default=0,
                            help="rows in dropped exports at the same snapshot (default: 0)")
        args = parser.parse_args()
        loss = finalize_loss(args.case, args.db_raw_events_rows, args.db_exported_rows)
        print(f"Loss check ({args.case.name}): {loss['result']}")
        return int(loss["result"] == "not measured")

    args = parse_args(mode)
    date = datetime.now().date().isoformat()
    args.out = (args.out or ROOT / "docs/measurements/raw" / f"arch-perf-{date}").resolve()
    args.tag = args.tag or image_tag()
    if not re.fullmatch(r"[0-9]+\.[0-9]+\.[0-9]+-[a-f0-9]{7}", args.tag):
        raise RuntimeError("--tag must be a version followed by a seven-character Git SHA")
    args.out.mkdir(parents=True, exist_ok=True)
    cases = (
        [("sweep", args.steps, None)]
        + [(f"repeat-30-{i + 1}", [30.0], None) for i in range(args.repeat_30)]
        if mode == "sweep"
        else [(name.lower(), [args.rate], name) for name in ("F1", "F2") if args.fault in ("both", name)]
    )
    wait_for_host(args)
    args.payload_anchor = args.payload_anchor or iso()
    build_loadgen()
    TOOLS.mkdir(parents=True, exist_ok=True)
    failed = False
    with tempfile.TemporaryDirectory(prefix="perf-tools-", dir=TOOLS) as directory:
        helper = build_helper(Path(directory))
        for label, rates, fault in cases:
            try:
                wait_for_host(args)
                if not run_case(args, helper, label, rates, fault):
                    failed = True
            except (OSError, ValueError, RuntimeError, subprocess.SubprocessError) as exc:
                print(f"{label} failed: {exc}", file=sys.stderr, flush=True)
                failed = True
    return int(failed)


if __name__ == "__main__":
    def interrupt(_signum, _frame):
        raise KeyboardInterrupt

    signal.signal(signal.SIGTERM, interrupt)
    try:
        sys.exit(main(sys.argv.pop(1)))
    except KeyboardInterrupt:
        sys.exit(130)
    except (OSError, ValueError, RuntimeError, subprocess.SubprocessError) as error:
        print(error, file=sys.stderr)
        sys.exit(1)
