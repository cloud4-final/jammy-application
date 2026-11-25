"""Regression tests for .github/workflows/backend.yml delivery checks."""

from __future__ import annotations

import json
import os
import re
import subprocess
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
WORKFLOW_PATH = REPO_ROOT / ".github" / "workflows" / "backend.yml"


def _extract_step_script(step_name: str) -> str:
    lines = WORKFLOW_PATH.read_text(encoding="utf-8").splitlines()
    for idx, line in enumerate(lines):
        if re.match(rf"^\s*-\s*name:\s*{re.escape(step_name)}\s*$", line):
            indent = len(line) - len(line.lstrip())
            script_lines: list[str] = []
            in_run = False
            run_indent = 0
            for sub in lines[idx + 1 :]:
                if not in_run:
                    if re.match(r"^\s*run:\s*\|\s*$", sub):
                        in_run = True
                        run_indent = len(sub) - len(sub.lstrip()) + 2
                    elif sub.strip() and len(sub) - len(sub.lstrip()) <= indent:
                        break
                    continue
                if sub.strip() == "":
                    script_lines.append("")
                    continue
                if len(sub) - len(sub.lstrip()) < run_indent:
                    break
                script_lines.append(sub[run_indent:])
            return "\n".join(script_lines) + "\n"
    raise AssertionError(f"Step {step_name!r} not found in {WORKFLOW_PATH}")


def test_service_discovery_and_publish_matrix(tmp_path: Path) -> None:
    workflow = WORKFLOW_PATH.read_text(encoding="utf-8")
    script = _extract_step_script("Discover service Dockerfiles")

    def run_discovery(files: list[str]) -> list[dict[str, str]]:
        work = tmp_path / f"case_{len(files)}"
        work.mkdir()
        for rel in files:
            target = work / rel
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text("FROM scratch\n", encoding="utf-8")
        out = tmp_path / f"out_{len(files)}.txt"
        out.write_text("", encoding="utf-8")
        subprocess.run(
            ["bash", "-eo", "pipefail", "-c", script],
            cwd=work,
            env={**os.environ, "GITHUB_OUTPUT": str(out)},
            check=True,
        )
        line = out.read_text(encoding="utf-8").strip()
        assert line.startswith("services=")
        return json.loads(line.removeprefix("services="))

    assert run_discovery([]) == []
    assert run_discovery(["apps/instruction-api/Dockerfile", "tools/plantsim/Dockerfile"]) == [
        {"service": "instruction-api", "dockerfile": "apps/instruction-api/Dockerfile"},
        {"service": "plantsim", "dockerfile": "tools/plantsim/Dockerfile"},
    ]

    full_services = run_discovery(
        [
            "apps/instruction-api/Dockerfile",
            "apps/dispatcher/Dockerfile",
            "apps/portal-publisher/Dockerfile",
            "apps/ingest-go/Dockerfile",
            "apps/ingest-py/Dockerfile",
            "apps/processor-go/Dockerfile",
            "apps/processor-py/Dockerfile",
            "apps/archiver-go/Dockerfile",
            "apps/archiver-py/Dockerfile",
            "apps/migrate/Dockerfile",
            "tools/plantsim/Dockerfile",
        ]
    )
    assert len(full_services) == 11

    assert workflow.count("include: ${{ fromJSON(needs.changes.outputs.services) }}") == 2
    publish_section = workflow.split("\n  publish:", 1)[1]
    assert "needs: [changes, images]" in publish_section
    assert "needs.changes.outputs.services != '[]'" in publish_section
    assert "continue-on-error" not in publish_section


def test_check_go_packages_fails_on_error(tmp_path: Path) -> None:
    script = _extract_step_script("Check Go packages")
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    fake_go = bin_dir / "go"

    def run_check(go_body: str, name: str) -> tuple[int, str, str]:
        fake_go.write_text(f"#!/usr/bin/env bash\n{go_body}\n", encoding="utf-8")
        fake_go.chmod(0o755)
        out = tmp_path / f"{name}.out"
        out.write_text("", encoding="utf-8")
        proc = subprocess.run(
            ["bash", "-eo", "pipefail", "-c", script],
            env={
                **os.environ,
                "PATH": f"{bin_dir}:{os.environ['PATH']}",
                "GITHUB_OUTPUT": str(out),
            },
            capture_output=True,
            text=True,
            check=False,
        )
        return proc.returncode, out.read_text(encoding="utf-8").strip(), proc.stderr

    rc, out, _ = run_check("exit 0", "empty")
    assert (rc, out) == (0, "run=false")

    rc, out, _ = run_check("echo github.com/cloud4-final/jammy-application/apps/ingest-go", "nonempty")
    assert (rc, out) == (0, "run=true")

    rc, out, stderr = run_check("echo 'broken go.mod' >&2; exit 2", "error")
    assert rc == 2
    assert out == ""
    assert "broken go.mod" in stderr


def test_smoke_test_matches_ingest_go_and_all_config_errors() -> None:
    smoke = _extract_step_script("Smoke-test the native image")
    assert "INGEST_STREAM is required" not in smoke
    match = re.search(r"\[\[ \$\{smoke_lines\[0\]\} =~ \^(.+)\$ \]\]", smoke)
    assert match is not None
    pattern = re.compile("^" + match.group(1).replace(r"\ ", " ") + "$")
    for line in (
        "HMAC_KEYS_FILE is required",
        "PLANT_GATEWAY_URL is required",
        "S3_BUCKET is required",
        "NATS_URL is required",
        "DATABASE_URL is required",
        "invalid NATS_STREAMS: expected comma-separated nonempty items without spaces",
    ):
        assert pattern.match(line), line
    assert not pattern.match("Traceback (most recent call last):")


def test_python_dockerfiles_use_conditional_manifests_pattern() -> None:
    for pkg in (
        "instruction-api",
        "dispatcher",
        "portal-publisher",
        "ingest-py",
        "processor-py",
        "archiver-py",
    ):
        text = (REPO_ROOT / "apps" / pkg / "Dockerfile").read_text(encoding="utf-8")
        assert "AS manifests" in text, pkg
        assert "COPY --from=manifests /manifests/ ./" in text, pkg
        assert f"uv sync --locked --no-dev --no-editable --package {pkg}" in text, pkg
        assert f"uv sync --frozen --no-dev --no-editable --package {pkg}" in text, pkg
        assert "COPY tests/e2e/" not in text, pkg
    dispatcher_df = (REPO_ROOT / "apps" / "dispatcher" / "Dockerfile").read_text(encoding="utf-8")
    assert "apps/instruction-api/src" not in dispatcher_df
