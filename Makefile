BUF_VERSION := 1.68.4
ifeq ($(shell buf --version 2>/dev/null),$(BUF_VERSION))
BUF := buf
else ifneq ($(shell command -v go 2>/dev/null),)
BUF := go run github.com/bufbuild/buf/cmd/buf@v$(BUF_VERSION)
else
BUF := docker run --rm --user "$$(id -u):$$(id -g)" -e HOME=/tmp \
	-v "$(CURDIR):/workspace" -w /workspace bufbuild/buf:$(BUF_VERSION)
endif
POSTGRES_IMAGE := postgres:17.12-trixie
MIGRATE_IMAGE := migrate/migrate:v4.20.1
REDOCLY_IMAGE := redocly/cli:2.54.3
PROTO_OUT ?= .

GO_PROTO_DIR := libs/go/pb/jammyv1
PY_PROTO_DIR := libs/py/jammy_common/pb
E2E_PROTO_DIR := tests/e2e/pb

.PHONY: proto contracts-check proto-lint proto-check migrations-check openapi-check
.PHONY: py-test go-test up-mvp up-split up-obs down seed e2e helm-test

proto:
	$(BUF) generate --output "$(PROTO_OUT)"
	@mv "$(PROTO_OUT)/$(PY_PROTO_DIR)"/jammy/v1/*_pb2.py "$(PROTO_OUT)/$(PY_PROTO_DIR)/"
	@rmdir "$(PROTO_OUT)/$(PY_PROTO_DIR)"/jammy/v1 "$(PROTO_OUT)/$(PY_PROTO_DIR)"/jammy
	@mkdir -p "$(PROTO_OUT)/$(E2E_PROTO_DIR)"
	@cp "$(PROTO_OUT)/$(PY_PROTO_DIR)"/*_pb2.py "$(PROTO_OUT)/$(E2E_PROTO_DIR)/"

contracts-check:
	$(MAKE) proto-lint
	$(MAKE) proto-check
	$(MAKE) migrations-check
	$(MAKE) openapi-check

proto-lint:
	$(BUF) lint

# Keep regeneration output visible through the Docker workspace mount.
proto-check:
	@set -eu; \
	tmp=$$(mktemp -d .proto-check.XXXXXXXX); \
	trap 'rm -rf "$$tmp"' EXIT; \
	$(MAKE) proto PROTO_OUT="$$tmp"; \
	for pair in "$(GO_PROTO_DIR):*.pb.go" "$(PY_PROTO_DIR):*_pb2.py" "$(E2E_PROTO_DIR):*_pb2.py"; do \
		dir=$${pair%%:*}; pattern=$${pair#*:}; \
		[ -d "$$dir" ] || continue; \
		for file in "$$tmp"/"$$dir"/$$pattern; do \
			diff -u "$${file#"$$tmp"/}" "$$file"; \
		done; \
		for file in "$$dir"/$$pattern; do \
			test -f "$$tmp/$$file" || { echo "Stale generated file: $$file" >&2; exit 1; }; \
		done; \
	done

migrations-check:
	@set -eu; \
	network=$$(docker network create --label jammy.use=contracts-check "jammy-contracts-$$$$"); \
	pg=; \
	trap 'if [ -n "$$pg" ]; then docker rm -f "$$pg" >/dev/null; fi; docker network rm "$$network" >/dev/null' EXIT; \
	pg=$$(docker run --detach --rm --network "$$network" --network-alias postgres \
		-e POSTGRES_HOST_AUTH_METHOD=trust -e POSTGRES_DB=jammy "$(POSTGRES_IMAGE)"); \
	for attempt in $$(seq 1 60); do \
		if docker exec "$$pg" pg_isready -h 127.0.0.1 -U postgres -d jammy >/dev/null; then break; fi; \
		if [ "$$attempt" -eq 60 ]; then docker logs "$$pg"; exit 1; fi; \
		sleep 1; \
	done; \
	for operation in up 'down -all' up; do \
		echo "migrate $$operation"; \
		docker run --rm --network "$$network" \
			-v "$(CURDIR)/contracts/db/migrations:/migrations:ro" "$(MIGRATE_IMAGE)" \
			-path /migrations -database 'postgres://postgres@postgres:5432/jammy?sslmode=disable' $$operation; \
	done; \
	docker exec -i "$$pg" psql -U postgres -d jammy < contracts/testdata/direct_db_cases.sql

openapi-check:
	docker run --rm -v "$(CURDIR)/contracts/openapi:/specs:ro" -w /specs "$(REDOCLY_IMAGE)" \
		lint instruction-api.yaml ingest.yaml

# The local infrastructure can come up before the application images exist.
COMPOSE_LOCAL = docker compose --project-directory deploy/compose
LOCAL_DEPENDENCIES := pg-primary seaweedfs elasticmq dynamodb-local
LOCAL_APPS := ingest-a1 instruction-api dispatcher-1 processor archiver portal-publisher plantsim
LOCAL_HMAC_KEYS := deploy/compose/.local-hmac-keys.json

up-mvp: profile := mvp
up-mvp: pipeline := direct-db
up-mvp: lease := disabled
up-mvp: dependencies := $(LOCAL_DEPENDENCIES)
up-mvp: apps := $(LOCAL_APPS)
up-split: profile := split
up-split: pipeline := jetstream
up-split: lease := dynamodb
up-split: sync_standby := ANY 1 (standby1)
up-split: dependencies := $(LOCAL_DEPENDENCIES) pg-standby nats
up-split: apps := $(LOCAL_APPS) ingest-a2 dispatcher-2

up-mvp up-split:
	@set -eu; \
	version=$$(git describe --tags --abbrev=0 --match 'v[0-9]*' 2>/dev/null || printf v0.1.0); \
	export JAMMY_LOCAL_TAG="$${version#v}-$$(git rev-parse --short=7 HEAD)"; \
	export PIPELINE_MODE="$(pipeline)" LEASE_MODE="$(lease)" JAMMY_SYNC_STANDBY="$(sync_standby)"; \
	if [ "$(profile)" = mvp ]; then \
		$(COMPOSE_LOCAL) --profile split stop ingest-a2 dispatcher-2 pg-standby nats; \
	fi; \
	$(COMPOSE_LOCAL) --profile "$(profile)" up --wait --wait-timeout 120 $(dependencies); \
	if [ "$(profile)" = split ]; then \
		curl -fsS --retry 30 --retry-connrefused --retry-delay 1 --max-time 3 \
			-o /dev/null http://127.0.0.1:8222/jsz; \
	fi; \
	$(COMPOSE_LOCAL) --profile "$(profile)" up --build --no-deps --force-recreate --detach migrate; \
	migrate_id=$$($(COMPOSE_LOCAL) --profile "$(profile)" ps --all --quiet migrate); \
	if [ "$$(docker wait "$$migrate_id")" != 0 ]; then \
		$(COMPOSE_LOCAL) --profile "$(profile)" logs migrate; \
		exit 1; \
	fi; \
	deploy/compose/aws/init-aws.sh init; \
	if [ "$(APPS)" = 1 ]; then \
		compose_env=$$($(COMPOSE_LOCAL) --profile "$(profile)" config --environment); \
		secret=$$(printf '%s\n' "$$compose_env" | sed -n 's/^JAMMY_HMAC_SECRET=//p'); \
		if [ -z "$$secret" ]; then \
			secret=$$(sed -n 's/^JAMMY_HMAC_SECRET=//p' deploy/compose/.env.example); \
		fi; \
		keys_tmp=$$(mktemp "$(LOCAL_HMAC_KEYS).XXXXXXXX"); \
		trap 'rm -f "$$keys_tmp"' EXIT; \
		printf '%s' "$$secret" | python3 -c 'import json, sys; print(json.dumps({"key-a": sys.stdin.read()}))' > "$$keys_tmp"; \
		if ! cmp -s "$$keys_tmp" "$(LOCAL_HMAC_KEYS)"; then \
			chmod 644 "$$keys_tmp"; \
			mv -f "$$keys_tmp" "$(LOCAL_HMAC_KEYS)"; \
			if [ -n "$$($(COMPOSE_LOCAL) --profile "$(profile)" ps --all --quiet instruction-api)" ]; then \
				$(COMPOSE_LOCAL) --profile "$(profile)" up --detach --no-deps --force-recreate instruction-api; \
			fi; \
		fi; \
		$(COMPOSE_LOCAL) --profile "$(profile)" up --detach $(apps); \
		python3 tests/e2e/wait_ready.py "$(profile)"; \
	fi

up-obs:
	$(COMPOSE_LOCAL) --profile obs up --detach grafana victoriametrics alloy loki tempo

down:
	$(COMPOSE_LOCAL) --profile '*' down

SEED_SQL ?= seed.sql
seed:
	$(COMPOSE_LOCAL) --profile mvp exec -T -e PGOPTIONS= pg-primary sh -ec \
		'exec psql -X -v ON_ERROR_STOP=1 --single-transaction -U "$$POSTGRES_USER" -d "$$POSTGRES_DB" -f -' \
		< "$(SEED_SQL)"

IMPL ?= go
ARM ?= mvp

py-test go-test:
	@echo "not implemented yet"
	@exit 1

HELM_CHART := deploy/helm/jammy-platform
HELM_TEST_VALUES := $(HELM_CHART)/tests/site-values.yaml
HELM_SCHEMA_URL := https://raw.githubusercontent.com/yannh/kubernetes-json-schema/0c4dd99d9873f7b4c2da24c008476f5841adbe2d/{{.NormalizedKubernetesVersion}}-standalone{{.StrictSuffix}}/{{.ResourceKind}}{{.KindSuffix}}.json
HELM_VALUES ?= deploy/helm/site-values.yaml

.PHONY: helm-render

helm-test:
	helm unittest "$(HELM_CHART)"
	@set -eu; \
	tmp=$$(mktemp -d); trap 'rm -rf "$$tmp"' EXIT; \
	for arm in mvp split; do \
		echo "Validating $$arm"; \
		helm lint --strict "$(HELM_CHART)" -f "$(HELM_CHART)/values-$$arm.yaml" -f "$(HELM_TEST_VALUES)"; \
		helm template jammy-platform "$(HELM_CHART)" --namespace jammy \
			-f "$(HELM_CHART)/values-$$arm.yaml" -f "$(HELM_TEST_VALUES)" > "$$tmp/$$arm.yaml"; \
		kubeconform -strict -summary -kubernetes-version 1.37.2 \
			-schema-location '$(HELM_SCHEMA_URL)' "$$tmp/$$arm.yaml"; \
	done

helm-render:
	@set -eu; \
	case "$(ARM)" in mvp|split) ;; *) echo 'Set ARM=mvp or ARM=split' >&2; exit 2;; esac; \
	out="deploy/helm/rendered/$(ARM)"; \
	mkdir -p "$$out"; \
	tmp=$$(mktemp -d "$$out/.render.XXXXXXXX"); trap 'rm -rf "$$tmp"' EXIT; \
	helm template jammy-platform "$(HELM_CHART)" --namespace jammy --no-hooks \
		-f "$(HELM_CHART)/values-$(ARM).yaml" -f "$(HELM_VALUES)" > "$$tmp/manifests.yaml"; \
	printf '%s\n' 'apiVersion: kustomize.config.k8s.io/v1beta1' 'kind: Kustomization' \
		'resources:' '  - manifests.yaml' > "$$tmp/kustomization.yaml"; \
	mv "$$tmp/manifests.yaml" "$$tmp/kustomization.yaml" "$$out/"; \
	echo "Rendered $$out"

e2e:
	@case "$(IMPL)" in go|py) ;; *) echo "invalid IMPL=$(IMPL) (expected go or py)" >&2; exit 2;; esac
	@case "$(ARM)" in mvp|split) ;; *) echo "invalid ARM=$(ARM) (expected mvp or split)" >&2; exit 2;; esac
	@if [ -f uv.lock ]; then \
		E2E_IMPL="$(IMPL)" E2E_ARM="$(ARM)" uv run --package jammy-e2e pytest tests/e2e -v $(PYTEST_ARGS); \
	else \
		uv venv --allow-existing --quiet .venv && \
		uv pip install --quiet -r tests/e2e/pyproject.toml && \
		E2E_IMPL="$(IMPL)" E2E_ARM="$(ARM)" uv run --no-sync --package jammy-e2e pytest tests/e2e -v $(PYTEST_ARGS); \
	fi
