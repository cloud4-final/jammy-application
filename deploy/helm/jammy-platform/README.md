# JAMMY platform Helm chart

Use the tool versions in [VERSIONS.md](../../../docs/backend/VERSIONS.md). The chart installs into the existing `jammy` namespace by default. Run Helm with `--namespace jammy` so the release and its resources use the same namespace.

| Values | Data path | Required node labels |
| --- | --- | --- |
| `values-mvp.yaml` | Direct PostgreSQL, one replica per enabled service | `role=main` |
| `values-split.yaml` | JetStream, two ingest instances and two dispatchers | Two `role=control` nodes. Ingest nodes use `role=ingest` and `jammy.io/ingest-instance=a1` or `a2`. |

Split control workloads tolerate `role=control:NoSchedule`. Dispatcher replicas require distinct hostnames. The ingest instances create and use `raw-a1` and `raw-a2`; processor and archiver subscribe to both. Keep one replica per ingest instance for per-instance stream ownership. The profiles set `global.siteId: jammy-a`. Supply a different site ID in the overlay when deploying another site.

## Site values and secrets

Provide an overlay outside version control, for example `/path/to/site-values.yaml`.

| Value | Input |
| --- | --- |
| `global.image.tag` | Published release tag or `<version>-<sha7>`. |
| `global.existingSecret` | Name of the platform Secret in `jammy`. |
| `global.s3.bucket`, `global.s3.endpoint` | Bucket and optional endpoint for archiver and portal-publisher. Leave the endpoint empty to use the AWS default endpoint for the configured region. Set `region` and `forcePathStyle` as needed. |
| `global.nats.url` | NATS endpoint for JetStream mode. Set `global.nats.existingSecret` with key `NATS_CREDS` when credentials are required. |
| `dispatcher.env.PLANT_GATEWAY_URL` | Plant gateway base URL. Enable `plantsim` and point the gateway URL at its Service for a simulator deployment. |
| `dispatcher.env.LEASE_MODE` | Choose `disabled` or `dynamodb`. DynamoDB mode also requires `LEASE_TABLE` and `LEASE_AUTO_ACQUIRE`. |
| `global.otlpEndpoint` | Optional OTLP exporter endpoint. |

Create the namespace and prepare the Secret before installation. The Secret uses `DATABASE_URL` for database users, `HMAC_KEYS` for the instruction API key map, and `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` when supplying AWS credentials through the chart. AWS keys may be omitted when the SDK obtains credentials through another provider. Prepare the `ghcr-pull` dockerconfigjson Secret in the same namespace for private image pulls.

The chart supplies the mode, stream names, probe ports, and standard Secret references. Use each service's `env` map for settings in [ENV.md](../../../contracts/ENV.md), and its `resources` map to size each workload for the site. Default Services are ClusterIP. To expose an ingest API, set that instance's `service.type` to `NodePort` and optionally set `service.nodePort`. Admin probes and Alloy scrapes use pod port 9090. The ingress NetworkPolicy allows API port 8080 and allows in-cluster pods to reach admin port 9090.

## Install or render

From the repository root:

```sh
helm upgrade --install jammy-platform deploy/helm/jammy-platform --namespace jammy \
  -f deploy/helm/jammy-platform/values-split.yaml -f /path/to/site-values.yaml --wait=watcher
kubectl -n jammy get deployments,pods,services
```

Helm runs the migration Job before install and upgrade. Allow the Job to complete before starting the app deployments when using Kustomize. Render the Job and workloads separately:

```sh
helm template jammy-platform deploy/helm/jammy-platform --namespace jammy \
  -f deploy/helm/jammy-platform/values-split.yaml -f /path/to/site-values.yaml \
  --show-only templates/job-migrate.yaml > /tmp/jammy-migrate.yaml
make helm-render ARM=split HELM_VALUES=/path/to/site-values.yaml
kubectl kustomize deploy/helm/rendered/split
```

Run the rendered Job and wait for completion before applying the rendered Kustomization. The Kustomization contains the long-running resources; the Job is a separate operation. Set `migrate.enabled: false` when the schema is already managed separately. Use the same arm and site settings for the migration and app renders.

## Verification

`make helm-test` runs the unit tests, lints both profiles, and validates the full rendered resources against Kubernetes 1.37.2 schemas. For a local cluster smoke, make the T3 Compose stack and the selected images available in Docker, then run `IMPL=go TAG=<published-tag> deploy/helm/k3d-smoke.sh`. It starts isolated Compose dependencies and k3d clusters, installs both profiles and waits for all app pods to become Ready. The smoke uses fixture credentials and deletes its own clusters and Compose volumes on exit.
