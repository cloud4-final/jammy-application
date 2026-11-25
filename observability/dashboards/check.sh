#!/usr/bin/env bash
set -euo pipefail

root=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)

python3 - "$root" <<'PY'
import json
from pathlib import Path
import re
import sys

root = Path(sys.argv[1])
dashboard_dir = root / "observability/dashboards"
dashboards = ("platform-overview", "data-pipeline", "control-path", "experiment-run")
variables = {"run_id", "arm", "site", "service"}
contract = (root / "contracts/METRICS.md").read_text()
metric_pattern = r"[a-zA-Z_:][a-zA-Z0-9_:]*"
label_pattern = r"[a-zA-Z_][a-zA-Z0-9_]*"
label_allowlist = re.search(r"Application label allowlist is (.+?)\. ", contract).group(1)
general_labels, http_label_clause = label_allowlist.split("on the HTTP histogram only")
http_labels = set(re.findall(r"`([^`]+)`", http_label_clause))
allowed_labels = set(re.findall(r"`([^`]+)`", general_labels)) | variables | {"le"}
http_histogram = "http_server_request_duration_seconds"
http_metrics = {http_histogram + suffix for suffix in ("", "_sum", "_count", "_bucket")}
result_values = {
    match[1]: set(re.findall(r"`([^`]+)`", match[2]))
    for match in re.finditer(r"`(jammy_\w+)` \| `result` \(([^)]*)\)", contract)
}
selector_pattern = re.compile(rf"({metric_pattern})?\s*\{{([^{{}}]*)\}}")
matcher_pattern = re.compile(rf'({label_pattern})\s*(=~|!~|!=|=)\s*("(?:\\.|[^"\\])*")')
grouping_pattern = r"\b(?:by|without|on|ignoring|group_left|group_right)\s*\(([^()]*)\)"
allowed_functions = {"rate", "sum", "histogram_quantile"}
metrics = set(re.findall(r"`(jammy_[a-z0-9_]+|http_server_request_duration_seconds)`", contract))
if not metrics:
    raise SystemExit("No application metrics found in METRICS.md")
allowed_metrics = metrics | {"up"}
for metric in metrics:
    if metric.endswith("_duration_seconds"):
        allowed_metrics.update(metric + suffix for suffix in ("_bucket", "_count", "_sum"))

# Remove strings, label selectors, grouping labels, ranges, Grafana macros
# and numeric literals. Restrict the functions to those used by the dashboards
# so functions taking label names as strings cannot bypass the label check.
# Reject __name__ selectors because they hide metric names in strings.
def metric_names(expression):
    expression = re.sub(r'"(?:\\.|[^"\\])*"', '""', expression)
    if re.search(r"\b__name__\b", expression):
        raise ValueError("use explicit metric names instead of __name__ selectors")
    expression = re.sub(r"\$\{[^{}]+\}|\$[a-zA-Z_]\w*", "", expression)
    expression = re.sub(r"\{[^{}]*\}|\[[^\[\]]*\]", "", expression)
    expression = re.sub(grouping_pattern, "", expression)
    function_pattern = rf"({metric_pattern})\s*(?=\()"
    if extra := set(re.findall(function_pattern, expression)) - allowed_functions:
        raise ValueError(f"unsupported PromQL functions {sorted(extra)}")
    expression = re.sub(function_pattern, "", expression)
    expression = re.sub(
        r"(?<![\w:])(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?(?:ms|s|m|h|d|w|y)?",
        "", expression,
    )
    return set(re.findall(metric_pattern, expression)) - {
        "and", "or", "unless", "bool", "offset", "group_left", "group_right", "Inf", "NaN",
    }

def validate_labels(expression, names):
    # Mask quoted strings before finding selectors so value text cannot supply label names.
    masked = re.sub(r'"(?:\\.|[^"\\])*"', lambda match: " " * len(match[0]), expression)
    for selector in selector_pattern.finditer(masked):
        metric = selector[1]
        if metric is None:
            raise ValueError("use an explicit metric before every selector")
        labels = allowed_labels.copy()
        if metric in http_metrics:
            labels |= http_labels
        value = expression[selector.start(2):selector.end(2)]
        matchers = list(matcher_pattern.finditer(value))
        separators = re.sub(r"\s+", "", matcher_pattern.sub("", value))
        if separators != "," * (len(matchers) - 1) and separators != "," * len(matchers):
            raise ValueError(f"unsupported label selector {{{value}}}")
        for matcher in matchers:
            label, operator, quoted = matcher.groups()
            if label not in labels:
                raise ValueError(f"uncontracted label {label!r} for {metric}")
            if label == "result":
                if operator not in {"=", "!="} or json.loads(quoted) not in result_values.get(metric, set()):
                    raise ValueError(f"uncontracted result matcher {matcher[0]!r} for {metric}")
            if label == "le" and not metric.endswith("_bucket"):
                raise ValueError(f"{metric} has no histogram bucket label")

    grouping_labels = {
        label
        for grouping in re.finditer(grouping_pattern, masked)
        for label in re.findall(label_pattern, grouping[1])
    }
    labels = allowed_labels | http_labels if names <= http_metrics else allowed_labels
    if extra := grouping_labels - labels:
        raise ValueError(f"uncontracted grouping labels {sorted(extra)}")


def panel_expressions(item):
    if isinstance(item, dict):
        for key, value in item.items():
            if key == "expr":
                yield value
            else:
                yield from panel_expressions(value)
    elif isinstance(item, list):
        for value in item:
            yield from panel_expressions(value)

count = 0
for uid in dashboards:
    path = dashboard_dir / f"{uid}.json"
    if not path.is_file():
        raise SystemExit(f"Missing dashboard: {path.relative_to(root)}")
    data = json.loads(path.read_text())
    if data.get("uid") != uid:
        raise SystemExit(f"{path.name}: expected uid={uid}")
    template = data["templating"]["list"]
    if {variable["name"] for variable in template} != variables:
        raise SystemExit(f"{path.name}: expected run_id, arm, site and service variables")

    expressions = list(panel_expressions(data["panels"]))
    if not expressions:
        raise SystemExit(f"{path.name}: no panel PromQL expressions")
    for variable in template:
        query = variable["query"]
        if isinstance(query, dict):
            query = query["query"]
        match = re.fullmatch(r"label_values\((.+),\s*" + variable["name"] + r"\)", query)
        if not match:
            raise SystemExit(f"{path.name}: invalid query for variable {variable['name']}")
        expressions.append(match.group(1))

    for expression in expressions:
        try:
            names = metric_names(expression)
            if not names or names - allowed_metrics:
                raise ValueError(f"missing or uncontracted metrics {sorted(names - allowed_metrics)}")
            validate_labels(expression, names)
        except ValueError as error:
            raise SystemExit(f"{path.name}: {error} in {expression}") from error
    count += len(expressions)
    print(f"PASS {path.name}: {len(expressions)} PromQL expressions")

print(f"PASS {len(dashboards)} dashboards, {count} expressions")
PY

alloy_dir="$root/observability/alloy"
alloy_image=grafana/alloy:v1.20.1

for file in shared compose k8s; do
    docker run --rm --network none \
        -v "$alloy_dir:/config:ro" "$alloy_image" fmt --test "/config/$file.alloy"
done
printf '%s\n' 'PASS Alloy configuration formatting'

for deployment in compose k8s; do
    docker run --rm --network none \
        -v "$alloy_dir/shared.alloy:/config/shared.alloy:ro" \
        -v "$alloy_dir/$deployment.alloy:/config/$deployment.alloy:ro" \
        "$alloy_image" validate /config
    printf 'PASS Alloy %s configuration validation\n' "$deployment"
done
