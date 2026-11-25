# JAMMY observability

Run `make up-obs` to start Alloy, VictoriaMetrics, Grafana, Loki and Tempo on the Compose network. Grafana provisions four dashboards in the **JAMMY** folder at `http://127.0.0.1:3000`. VictoriaMetrics exposes the Prometheus query API at `http://127.0.0.1:8428`.

To test split with the application images present, use the same image implementation and environment for both Make targets:

```sh
APPS=1 IMPL=go ARM=split RUN_ID=example-split PROFILE=solar \
  OTEL_EXPORTER_OTLP_ENDPOINT=http://alloy:4317 make up-split up-obs
curl -fsSG http://127.0.0.1:8428/api/v1/query \
  --data-urlencode 'query=up{service="ingest",arm="split"} == 1'
observability/dashboards/check.sh
```

Alloy uses Compose service DNS names and scrapes each listed service at `/metrics` on port 9090. A service absent from the running Compose profile can show `up=0`. Alloy forwards metrics to `/api/v1/write`. The `RUN_ID`, `ARM` and `PROFILE` environment variables supply the `run_id`, `arm` and `profile` external labels. An application sample that already carries a `profile` keeps its value. The OTLP receiver listens on ports 4317 (gRPC) and 4318 (HTTP) within the deployment network and forwards traces to Tempo and logs to Loki.

The dashboard service filter uses the same role names for scrape health and application metrics in Compose and Kubernetes. Both implementations appear as `ingest`, `processor`, and `archiver`. Use `run_id` to select a measured trial.

Alloy v1.20.1 loads the two mounted files from the configuration directory at its default stability level. Both environments use `shared.alloy` for remote write, external labels, and OTLP forwarding.

| Environment | Files mounted in `/etc/alloy/jammy` |
| --- | --- |
| Compose | `shared.alloy`, `compose.alloy` |
| Kubernetes | `shared.alloy`, `k8s.alloy` |

For Kubernetes, mount the two Kubernetes files from a ConfigMap, pass `/etc/alloy/jammy` to `alloy run`, and grant the Alloy service account permission to list and watch pods in the `jammy` namespace. Discovery keeps running pods annotated with `jammy.io/scrape: "true"`. It selects the `admin` container port and applies the `jammy.io/port` annotation when present. Set `VICTORIAMETRICS_WRITE_URL`, `TEMPO_OTLP_ENDPOINT` and `LOKI_OTLP_ENDPOINT` to the reachable backend endpoints. The Loki endpoint should include `/otlp`. Set the experiment environment variables in the Alloy workload when running a measured trial.

The receipt SLO counts POST observations for the `/v1/instructions` route template. It divides the rate in the 1-second bucket, matching both `le="1"` and the normalized `le="1.0"` by the total observation rate and subtracts the quotient from 1. During periods without receipts the ratio is undefined. The RED error panel shows HTTP 5xx and above and operation `result="error"` in separate series so an HTTP observation and its corresponding operation result are visible without summing them.
