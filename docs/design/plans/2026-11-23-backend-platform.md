# JAMMY 백엔드 플랫폼 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** ①②④⑤⑥⑧ 서비스(④⑤⑥은 Go·Python 두 벌)와 migrate·plantsim·payloadgen·k6·Helm·compose·CI·Grafana를 만든다. 같은 이미지로 Arm-1(direct-db)과 Arm-2(jetstream)를 돌리고, 2026-11-24에 k8s 담당에게 인계한다.

**Architecture:** 계약(`contracts/`: proto, OpenAPI, SQL, env, metric, 시험 사례)을 먼저 고정하고, 4개 구현 track이 이 계약만 보고 병렬로 만든다. 아키텍처 차이는 `PIPELINE_MODE`와 Helm values로만 바꾼다. 정합성 경계: ④는 내구 저장 후 202, ⑤는 DB commit 후 ACK, ⑥은 S3 업로드 후 ACK.

**Tech Stack:** (정확한 버전은 `docs/backend/VERSIONS.md`가 유일한 원본) Python 3.14 + FastAPI + uvicorn + psycopg 3 + nats-py + pyarrow + boto3 + zstandard + OTel SDK, `uv` workspace. Go 1.27 + net/http + pgx/v5 + nats.go/jetstream + parquet-go + aws-sdk-go-v2 + klauspost/compress/zstd + OTel Go. buf(protobuf 생성), golang-migrate, PostgreSQL 17, NATS 2.15(JetStream), SeaweedFS, ElasticMQ, amazon/dynamodb-local, k6, Helm 4, k3d, Grafana 13.2.5, Loki 3.7.7, VictoriaMetrics, Alloy, Tempo.

**Spec:** `docs/design/specs/2026-11-23-backend-platform-design.md` (이하 "spec §N")

## Global Constraints

- 모든 값·이름·응답 코드는 spec §5를 그대로 쓴다. 계약을 바꿔야 하면 구현을 멈추고 지휘자에게 보고한다. 계약 변경은 T0 소유자(설계자)만 한다.
- 이미지 이름 `ghcr.io/cloud4-final/jammy-application/<service>`, 태그 `<version>-<sha7>`, `latest` 금지(spec §6).
- 이미지는 `linux/amd64`와 `linux/arm64` 둘 다 빌드되어야 한다. 아키텍처별 코드 분기 금지.
- 컨테이너 non-root, read-only rootfs에서 동작(쓰기는 `/tmp`와 `STAGE_DIR` emptyDir만).
- 모든 서비스: admin 포트 9090의 `/livez`(프로세스 생존), `/readyz`(의존성 연결됨), `/metrics`. SIGTERM을 받으면 `/readyz`를 503으로 바꾸고, 진행 중 작업을 마친 뒤 `SHUTDOWN_TIMEOUT_SECONDS` 안에 종료한다.
- metric 이름·label·bucket은 spec §5.10. Go와 Python은 같은 이름을 쓴다. label에 `event_id`, `instruction_id`, `asset_id`, `batch_id` 금지.
- 로그는 JSON 한 줄(spec §5.10 필드). payload 원문과 secret은 로그에 쓰지 않는다.
- 이미지 태그·의존성 버전은 `docs/backend/VERSIONS.md`의 값으로 정확히 고정한다(`:latest`, `^`, 범위 지정 금지). lock 파일 커밋. 표에 없는 의존성을 추가하면 같은 규칙으로 고르고 표에 행을 추가한다.
- 기존 `apps/collector/`, `.github/workflows/ci.yml`, `.github/workflows/publish-image.yml`은 수정하지 않는다.
- 측정하지 않은 성능 수치를 문서·주석에 쓰지 않는다.
- commit은 각 task 끝에 한다. push와 PR 머지는 사용자 검토 뒤 지휘자가 한다.

## Review Focus

1. 압축 폭탄: 작은 zstd 본문이 64MiB를 넘게 풀리면 메모리를 다 쓰기 전에 413을 줘야 한다. → T7 `test_zstd_bomb_rejected_413`, T11 `TestZstdBombRejected413`
2. SIGTERM 중 진행 중인 배치: 202를 받은 배치는 반드시 저장되어 있고, 저장 전에 끊긴 요청은 202를 받지 않아야 한다. → T8 `test_sigterm_drains_inflight`, T11 `TestSigtermDrainsInflight`
3. ACK 제한 시간보다 처리가 느려 재전송될 때: 요약이 두 번 더해지면 안 된다. → T9 `test_redelivery_after_ackwait_no_double_count`, T12 같은 이름의 Go 시험
4. 시간 경계와 늦게 온 이벤트: 봉인 뒤 도착한 이벤트는 `raw-late/`로 가야 하고, 봉인 파일의 행 수가 바뀌면 안 된다. → T10 `test_late_event_goes_to_raw_late`, T13 같은 이름의 Go 시험
5. 기동 시 NATS나 PG가 없을 때: crash loop 대신 `/readyz` 503으로 재연결을 계속하고, 의존성이 돌아오면 200이 되어야 한다. → T1 `test_readyz_tracks_dependency`, T2 `TestReadyzTracksDependency`

## 실행 구조

모델: Claude Opus 5.5 medium(지휘·설계·최종 평가), codex GPT-6.2 Sol medium(구현), agy Gemini 4 Hydrogen high(검토·시험).

분업 원칙(사용자, 2026-11-23): 토큰을 많이 쓰는 일(코드·문서 작성, 긴 파일 읽기, 시험 실행, diff 검토, 충돌 해결)은 codex와 agy가 한다. 판단하는 일(설계 결정, 계약 변경 승인, 검토 의견 충돌 판정, 범위·우선순위 결정, 실패 원인 판정)은 Claude가 한다. Claude는 원본 diff·로그를 직접 읽지 않고, worker가 낸 요약 보고(판정에 필요한 증거 경로 포함)를 입력으로 받는다. 근거가 모자랄 때만 해당 파일의 해당 부분을 읽는다.

| Phase | Task | 담당 | 선행 |
|---|---|---|---|
| 0 | T0 계약 + 공유 파일 뼈대(루트 `Makefile` target, `pyproject.toml` workspace, `go.mod`, compose 서비스 이름) 작성 | codex#4 (spec §5를 그대로 옮김, 새 결정 금지) | — |
| 0 | T0 검토 관문 | agy#2, agy#3 각자 독립 검토. 의견이 갈리거나 spec에 없는 결정이 필요하면 Claude가 판정 | T0 |
| 1 | T1 py-common(가장 먼저), T4 instruction-api, T5 dispatcher, T6 portal-publisher | codex#1 (track A, `feat/bp-control`) | 관문 |
| 1 | T7 py gate·codec, T8 ingest-py, T9 processor-py, T10 archiver-py | codex#2 (track B, `feat/bp-data-py`) | 관문, T1(T8부터) |
| 1 | T2 go-common, T11 ingest-go, T12 processor-go, T13 archiver-go | codex#3 (track C, `feat/bp-data-go`) | 관문 |
| 1 | T3 compose·migrate, T14 payloadgen·plantsim, T15 k6 | codex#4 (track D1, `feat/bp-foundation`) | 관문 |
| 1 | T16 Helm, T17 CI, T18 관측 | codex#5 (track D2, `feat/bp-delivery`) | 관문 |
| 1 | T19 E2E 시험 작성(계약만 보고, 구현 코드는 보지 않음) | agy#1 | 관문 |
| 2 | T19 E2E 실행 | agy#1 | T3, T14, 서비스 전체 |
| 2 | T20 인계 문서 작성 | codex#5 (Claude가 목차와 독자 기준만 정함) | T16, T19 |
| 3 | 증거 묶음 작성: 완성 기준 6개별 통과 여부, 시험 결과 경로, 동등성 표, 미해결 검토 의견 | agy#1 | 전체 |
| 3 | 최종 평가 | 새 context의 Claude(진행 기록 없이 spec·계획·증거 묶음만 받음) | 증거 묶음 |

검토 담당(서로 독립, 결과를 공유하지 않은 채 각자 보고):

| 검토자 | 범위 |
|---|---|
| agy#1 | E2E 작성·실행, T0 `cases.jsonl`의 HTTP 수준 재현 |
| agy#2 | 데이터 track B·C. **동등성 검토**: 같은 서비스의 Go·Py를 나란히 놓고 알고리즘, 배치 크기, pool 크기, 동시성, 재시도 값이 같은지 확인. 다른 점은 언어 차이로만 남긴다 |
| agy#3 | 제어 track A, 배포 track D1·D2(보안 설정, chart, CI 공급망 포함) |

- 기준 branch: `feat/backend-platform`. track마다 worktree를 따로 쓴다.
- 공유 파일(루트 `Makefile`, `pyproject.toml`, `go.mod`, compose)은 T0가 뼈대를 만든다. 각 track은 자기 target·member·service 항목만 추가하고, 다른 track의 항목은 고치지 않는다.
- 모든 task는 담당 agy 검토를 통과한 뒤 병합한다. 병합 전 rebase와 충돌 해결은 해당 track의 codex가 한다. 차단 이슈가 있으면 같은 구현자에게 돌려보낸다.
- 지휘자(Claude)는 worker 보고만 보고 판단한다: 병합 승인, 검토 의견 충돌 판정, 계약 변경 승인, 일정·범위 조정. 코드 작성과 원본 diff 검토는 하지 않는다.
- worker 보고 형식: 결과(통과/실패), 판단이 필요한 질문, 증거 경로(파일:줄, 명령, 로그 경로). 원본 출력을 그대로 붙이지 않는다.
- 공통 시험 도구: Python `pytest` + `testcontainers`(버전은 `VERSIONS.md`, nats는 `-js`), Go `testing` + `testcontainers-go`. Docker가 필요하다.

---

### Task 0: 계약 고정

**Files:**
- Create: `contracts/proto/jammy/v1/envelope.proto`, `contracts/proto/jammy/v1/profiles.proto`, `buf.yaml`, `buf.gen.yaml`
- Create: `contracts/openapi/instruction-api.yaml`, `contracts/openapi/ingest.yaml`
- Create: `contracts/db/migrations/0001_init.up.sql`, `0001_init.down.sql`
- Create: `contracts/ENV.md`, `contracts/METRICS.md`, `contracts/BATCH_HANDOFF.md`
- Create: `contracts/testdata/gate_cases/cases.jsonl`, `contracts/testdata/gate_cases/*.bin`, `contracts/testdata/hmac_cases.json`, `contracts/testdata/allocation_cases.json`, `contracts/testdata/parquet_schema.json`(spec §5.8 열 이름·Arrow 타입·zstd)
- Create: `libs/go/pb/jammyv1/*.pb.go`, `libs/py/jammy_common/pb/*_pb2.py` (생성물, 커밋)
- Create: `Makefile` (target `proto`, `contracts-check`)

**Interfaces:**
- Produces: spec §5의 모든 계약. Go 패키지 `github.com/cloud4-final/jammy-5th/libs/go/pb/jammyv1`, Python 모듈 `jammy_common.pb.envelope_pb2`, `jammy_common.pb.profiles_pb2`.
- `cases.jsonl` 한 줄 = `{"name", "input_file", "content_type", "content_encoding", "now_unix_ms", "expect_status", "expect_accepted", "expect_rejected", "expect_reasons": {...}}`. 최소 사례: 전부 정상(3 profile 각각), §5.2의 9개 사유 각각, 413 본문 초과, 413 압축 폭탄, 400 빈 배치, 400 10001개, JSON 본문 정상.
- `hmac_cases.json`: `{key_id, secret, timestamp, method, path, body, expected_signature}` 5개 이상.
- `allocation_cases.json`: `{target_kw, assets: [{asset_id, power_kw}], expected: {asset_id: kw}, expected_status}`. 비례 배분, 잔차 처리, 총출력 부족, 출력 0 자산 포함.

- [ ] **Step 1:** proto 두 파일을 spec §5.1대로 작성하고 `make proto`로 Go·Python 코드를 생성한다. 확인: `buf lint` 통과, 생성 파일 존재.
- [ ] **Step 2:** `0001_init.up.sql`에 spec §5.6 테이블 전부와 `jammy.ensure_partitions(ahead interval)`를 작성한다. 확인: `docker run postgres:17` 위에서 `migrate up` → `migrate down` → `migrate up` 이 모두 성공한다.
- [ ] **Step 3:** OpenAPI 두 파일 작성. 확인: `npx @redocly/cli@<고정 버전> lint` 통과.
- [ ] **Step 4:** `ENV.md`(공통 + 서비스별 env, 기본값, 필수 여부), `METRICS.md`, `BATCH_HANDOFF.md`를 spec §5.8~5.10대로 작성한다.
- [ ] **Step 5:** 시험 사례 파일을 생성한다. 생성 스크립트 `contracts/testdata/gen.py`도 커밋한다. 기대값은 spec 규칙에서 손으로 계산한 값이다.
- [ ] **Step 6:** `make contracts-check`(buf lint, 생성물 diff 없음, migrate up/down, OpenAPI lint) 통과 확인 후 commit `feat(contracts): fix backend platform contracts`.

---

### Task 1: Python 공통 라이브러리

**Files:**
- Create: `pyproject.toml`(uv workspace, members: `libs/py`, `apps/instruction-api`, `apps/dispatcher`, `apps/portal-publisher`, `apps/ingest-py`, `apps/processor-py`, `apps/archiver-py`, `tests/e2e`), `uv.lock`
- Create: `libs/py/pyproject.toml`, `libs/py/jammy_common/{config.py,logging.py,obs.py,admin.py,lifecycle.py,db.py,nats.py,s3.py}`
- Test: `libs/py/tests/test_config.py`, `test_admin.py`, `test_lifecycle.py`

**Interfaces:**
- Produces:
  - `config.env(name: str, default: str | None = None, required: bool = False) -> str`. `<NAME>_FILE`이 있으면 파일 내용을 우선 쓴다.
  - `config.CommonConfig.from_env() -> CommonConfig`. 필드 `site_id, instance_id, http_port, admin_port, log_level, shutdown_timeout_s, pipeline_mode`.
  - `logging.setup(service: str, cfg: CommonConfig) -> None` (spec §5.10 JSON 필드).
  - `obs.setup(service: str, cfg: CommonConfig) -> Meter`. OTel MeterProvider + Prometheus reader, OTLP trace exporter(endpoint가 비면 끔). `obs.HTTP_BUCKETS`.
  - `admin.AdminServer(port: int, checks: dict[str, Callable[[], Awaitable[bool]]])`. 메서드 `start()`, `set_draining()`. `/readyz`는 모든 check가 True이고 draining이 아닐 때 200.
  - `lifecycle.run(main: Callable[[asyncio.Event], Awaitable[None]], cfg) -> None`. SIGTERM·SIGINT를 받으면 stop event를 set하고, `shutdown_timeout_s`가 넘으면 강제 종료(exit 1).
  - `db.pool(cfg) -> psycopg_pool.AsyncConnectionPool`, `nats.connect(cfg) -> nats.aio.client.Client`(무한 재연결), `s3.client(cfg) -> boto3 S3 client`.

- [ ] **Step 1: 시험 작성**
  - `test_env_file_overrides_value`: `X=a`, `X_FILE=<파일 "b">` → `env("X") == "b"`
  - `test_required_missing_raises`: 없는 필수 env → `ConfigError`
  - `test_readyz_tracks_dependency`: check가 False → 503. True로 바꾸면 200. `set_draining()` 후 503.
  - `test_livez_always_200`
  - `test_metrics_exposes_http_histogram_buckets`: `/metrics`에 `le="0.005"`와 `le="1.0"`이 있다.
  - `test_sigterm_sets_stop_and_exits_zero`: 하위 프로세스에 SIGTERM → 종료 코드 0, `shutdown_timeout_s`보다 빨리 끝남.
  - `test_shutdown_timeout_forces_exit_one`: stop을 무시하는 main → 종료 코드 1.
- [ ] **Step 2:** `uv run pytest libs/py -q` → 실패 확인.
- [ ] **Step 3:** 위 Interfaces를 구현한다.
- [ ] **Step 4:** `uv run pytest libs/py -q` → 통과. `uv run ruff check libs/py` → 통과.
- [ ] **Step 5:** commit `feat(py-common): config, admin, lifecycle, obs`.

---

### Task 2: Go 공통 라이브러리 (gate·codec 포함)

**Files:**
- Create: `go.mod`(module `github.com/cloud4-final/jammy-5th`), `go.sum`
- Create: `libs/go/{config,logx,obs,admin,lifecycle,codec,gate,dbx,natsx,s3x}/*.go`
- Test: 각 패키지 `*_test.go`, `libs/go/gate/cases_test.go`

**Interfaces:**
- Produces (T1과 같은 의미):
  - `config.Env(name, def string) string`, `config.MustEnv(name string) string`, `config.LoadCommon() (Common, error)`
  - `admin.New(port int, checks map[string]func(context.Context) bool) *Server`, `(*Server).Start()`, `(*Server).SetDraining()`
  - `lifecycle.Run(main func(ctx context.Context) error, timeout time.Duration)`
  - `obs.Setup(service string, c config.Common) (metric.Meter, func(context.Context) error, error)`
  - `codec.DecodeBatch(body []byte, contentType, contentEncoding string, maxBody, maxDecompressed int64) (*jammyv1.EventBatch, []byte /*decompressed*/, error)`. 오류 형식 `*codec.HTTPError{Status int, Msg string}`.
  - `gate.Config{MaxPast, MaxFuture time.Duration}`, `gate.Validate(b *jammyv1.EventBatch, now time.Time, c gate.Config) gate.Result`. `Result{Accepted []*jammyv1.Event, Rejected []Rejection{Event, Reason string}, PrimaryValues []float64}`.
- Python 쪽 짝은 T7이 만든다. 두 구현이 `cases.jsonl`에서 같은 결과를 내야 한다.

- [ ] **Step 1: 시험 작성**
  - `TestGateCases`: `contracts/testdata/gate_cases/cases.jsonl` 전부. status, accepted, rejected, reasons가 기대값과 같다.
  - `TestZstdBombRejected413`: 1KiB zstd가 100MiB로 풀림 → `HTTPError.Status == 413`. 측정 중 힙 증가 < 80MiB.
  - `TestReadyzTracksDependency`, `TestLivezAlways200`, `TestMetricsHTTPBuckets`(T1과 같은 단언)
  - `TestRunSigtermExitsCleanly`, `TestRunTimeoutForcesExit`
  - `TestEnvFileOverrides`
- [ ] **Step 2:** `go test ./libs/go/...` → 실패 확인.
- [ ] **Step 3:** 구현. zstd는 `zstd.NewReader` + `io.LimitReader(maxDecompressed+1)`로 읽고 초과하면 413을 준다.
- [ ] **Step 4:** `go test ./libs/go/... -race` 통과, `golangci-lint run ./libs/go/...` 통과.
- [ ] **Step 5:** commit `feat(go-common): config, admin, lifecycle, codec, gate`.

---

### Task 3: 로컬 스택, migrate 이미지, seed

**Files:**
- Create: `apps/migrate/Dockerfile`(base `migrate/migrate:<고정 v4 tag>`, `contracts/db/migrations`를 `/migrations`에 복사)
- Create: `deploy/compose/docker-compose.yml`, `deploy/compose/.env.example`, `deploy/compose/pg/{primary-init.sh,standby-entrypoint.sh}`, `deploy/compose/nats/js.conf`, `deploy/compose/aws/{elasticmq.conf,init-aws.sh}`
- Create: `deploy/compose/README.md`
- Modify: `Makefile` (target `up-mvp`, `up-split`, `up-obs`, `down`, `seed`)

**Interfaces:**
- Consumes: T0 migrations. 앱 이미지는 `IMPL`과 `TAG` env로 고른다. 아직 없는 앱 서비스는 compose에 정의만 하고, 해당 track 병합 뒤 동작한다.
- Produces: 호스트 포트 고정. PG primary 5432, standby 5433, NATS 4222, monitor 8222, SeaweedFS S3 8333, ElasticMQ 9324, dynamodb-local 8000, ingest-a1 18081, ingest-a2 18082, instruction-api 18080, plantsim 18090, Grafana 3000, VictoriaMetrics 8428. 버킷 `jammy-local`, 큐 `jammy-instructions`, lease 테이블 `jammy-site-lease`.

- [ ] **Step 1:** 확인 스크립트 `deploy/compose/smoke.sh` 작성. 단언: migrate exit 0, `SELECT count(*) FROM jammy.assets` 성공, split에서 `pg_stat_replication.sync_state`가 `sync` 또는 `quorum`, NATS `/jsz` 200, `aws s3 ls s3://jammy-local --endpoint-url` 성공, SQS 큐와 Dynamo 테이블 존재.
- [ ] **Step 2:** `make up-split && deploy/compose/smoke.sh split` → 실패 확인.
- [ ] **Step 3:** compose 작성. PG는 공식 `postgres:17` 이미지로 primary init 스크립트(복제 사용자, `synchronous_standby_names='ANY 1 (standby1)'`)와 standby(`pg_basebackup -R`)를 구성한다. Bitnami 이미지는 쓰지 않는다. `version:` 헤더는 쓰지 않는다.
- [ ] **Step 4:** `make up-mvp && smoke.sh mvp`, `make up-split && smoke.sh split` 둘 다 통과.
- [ ] **Step 5:** commit `feat(deploy): local compose stack and migrate image`.

---

### Task 4: ① instruction-api

**Files:**
- Create: `apps/instruction-api/{pyproject.toml,Dockerfile,README.md}`, `apps/instruction-api/src/instruction_api/{main.py,hmac_auth.py,models.py,repo.py}`
- Test: `apps/instruction-api/tests/{test_hmac.py,test_api.py}`

**Interfaces:**
- Consumes: T1 `config`, `admin`, `lifecycle`, `obs`, `db`. T0 `instruction-api.yaml`, `hmac_cases.json`.
- Produces: `hmac_auth.sign(secret: bytes, ts: int, method: str, path: str, body: bytes) -> str` (T15 k6와 T19가 같은 규칙 사용). env `HMAC_KEYS_FILE`, `HMAC_MAX_SKEW_SECONDS=300`. `instructions` 행 status `accepted`, source `https`.

- [ ] **Step 1: 시험 작성**
  - `test_sign_matches_cases`: `hmac_cases.json` 전부 일치
  - `test_new_instruction_202` / `test_same_id_same_body_200` / `test_same_id_diff_body_409`
  - `test_bad_signature_401`, `test_stale_timestamp_401`(301초 차이), `test_unknown_key_401`
  - `test_invalid_body_422`: `valid_until <= valid_from`, `target_reduction_kw <= 0`
  - `test_get_status_returns_commands`: commands 2행 삽입 후 GET → 2개 반환
  - `test_accept_p99_under_1s_local`: 200건 순차 → p99 < 1s(로컬 testcontainers, 기능 확인용이며 성능 근거로 쓰지 않음)
- [ ] **Step 2:** `uv run pytest apps/instruction-api -q` → 실패.
- [ ] **Step 3:** FastAPI 구현. `INSERT ... ON CONFLICT (instruction_id) DO NOTHING RETURNING` 다음 body_sha256을 비교한다. 서명 검증은 raw body로 한다.
- [ ] **Step 4:** 시험 통과, `docker buildx build --platform linux/amd64,linux/arm64 apps/instruction-api -f apps/instruction-api/Dockerfile .` 성공(context는 레포 루트).
- [ ] **Step 5:** commit `feat(instruction-api): signed idempotent instruction intake`.

---

### Task 5: ② dispatcher

**Files:**
- Create: `apps/dispatcher/{pyproject.toml,Dockerfile,README.md}`, `apps/dispatcher/src/dispatcher/{main.py,leader.py,lease.py,allocate.py,commands.py,sqs_drain.py,compliance.py,cli.py}`
- Test: `apps/dispatcher/tests/{test_allocate.py,test_leader.py,test_lease.py,test_dispatch.py,test_sqs.py,test_compliance.py}`

**Interfaces:**
- Consumes: T1, T0 `allocation_cases.json`, spec §5.4·§5.5. 시험에서 plantsim 대신 로컬 fake HTTP 서버를 쓴다(T14 plantsim과 같은 계약).
- Produces:
  - `allocate.allocate(target_kw: Decimal, assets: list[tuple[str, Decimal]]) -> tuple[dict[str, Decimal], str]` (status `dispatched|partially_dispatched`)
  - `leader.Leader(pool, name="dispatcher")`: `async try_acquire() -> int | None`(generation), `async is_held() -> bool`
  - `lease.Lease` (`LEASE_MODE`, `LEASE_TABLE`, `LEASE_AUTO_ACQUIRE`, `LEASE_TTL_SECONDS=15`, `LEASE_RENEW_SECONDS=5`, `DYNAMODB_ENDPOINT`): `async tick() -> LeaseState(active: bool, epoch: int)`, `async takeover(confirm: bool) -> int`
  - CLI `python -m dispatcher.cli lease takeover --confirm`
  - env: `PLANT_GATEWAY_URL`, `COMMAND_TIMEOUT_SECONDS=10`, `SQS_QUEUE_URL`(비면 끔), `SQS_ENDPOINT`, `DISPATCH_POLL_SECONDS=1`, `COMPLIANCE_GRACE_SECONDS=120`
- 루프 순서(leader이고 lease active일 때만): SQS 수거 → `accepted` 지시를 `FOR UPDATE SKIP LOCKED`로 집어 배분 → 명령 insert(같은 tx) → 전송 → 상태 갱신 → 만료 지시 `expired` → 이행 확인.

- [ ] **Step 1: 시험 작성**
  - `test_allocation_cases`: `allocation_cases.json` 전부
  - `test_allocation_never_exceeds_power`, `test_allocation_sum_equals_target_when_enough`
  - `test_only_one_leader`: 인스턴스 2개 → 정확히 1개만 `try_acquire`가 generation을 받음
  - `test_generation_increments_on_takeover`: leader 연결 종료 → 다른 인스턴스 generation = 이전 + 1
  - `test_lease_renew_and_lose`: dynamodb-local container. 다른 holder로 덮어쓰기 → 다음 tick에서 `active=False`
  - `test_lease_no_auto_acquire_on_site_b`: 만료 lease + `LEASE_AUTO_ACQUIRE=false` → active False. `takeover(confirm=True)` → epoch +1
  - `test_dispatch_sends_and_records_ack`: fake plant 200 → commands 전부 `acked`, 지시 `dispatched`
  - `test_dispatch_timeout_marks_timeout`: fake plant 지연 11s(시험에서는 timeout 0.5s로 설정) → `timeout`
  - `test_no_duplicate_commands_on_leader_switch`: 전송 중 leader 교체 → `UNIQUE(instruction_id, asset_id)` 덕분에 commands 중복 0
  - `test_lease_lost_stops_sending`
  - `test_sqs_drain_inserts_idempotently_and_deletes`: ElasticMQ container, 같은 메시지 2번 → instructions 1행, 큐 비어 있음
  - `test_compliance_verdict`: summary_1m 시드 → verdict `met|not_met`
- [ ] **Step 2:** 실패 확인.
- [ ] **Step 3:** 구현.
- [ ] **Step 4:** 시험 통과, multi-arch 빌드 성공.
- [ ] **Step 5:** commit `feat(dispatcher): leader-fenced allocation, lease, sqs drain`.

---

### Task 6: ⑧ portal-publisher

**Files:**
- Create: `apps/portal-publisher/{pyproject.toml,Dockerfile,README.md}`, `apps/portal-publisher/src/portal_publisher/{main.py,publish.py}`
- Test: `apps/portal-publisher/tests/test_publish.py`

**Interfaces:**
- Consumes: T1, `summary_1m`, `assets`.
- Produces: S3 키 `portal/<tenant_id>/latest.json`, `portal/<tenant_id>/1m/<YYYYMMDDHHMM>.json`. 본문 `{tenant_id, minute, assets: [{asset_id, count, avg_kw, min_kw, max_kw}], generated_at}`. env `PORTAL_INTERVAL_SECONDS=60`, `PORTAL_PREFIX=portal`.

- [ ] **Step 1: 시험 작성**: `test_publishes_per_tenant`(tenant 2개 → 키 4개), `test_republish_same_minute_overwrites`(같은 minute 2번 → 객체 수 그대로), `test_no_data_skips`, `test_s3_failure_counts_error_and_retries`
- [ ] **Step 2:** 실패 확인.
- [ ] **Step 3:** 구현. 마지막으로 닫힌 minute(`now - 1m`의 내림) 기준.
- [ ] **Step 4:** 시험 통과, multi-arch 빌드 성공.
- [ ] **Step 5:** commit `feat(portal-publisher): per-tenant minute JSON to S3`.

---

### Task 7: Python gate·codec

**Files:**
- Create: `libs/py/jammy_common/{codec.py,gate.py,profiles.py}`
- Test: `libs/py/tests/{test_gate_cases.py,test_codec.py}`

**Interfaces:**
- Consumes: T0 pb 모듈과 `cases.jsonl`.
- Produces (T2와 같은 의미):
  - `codec.decode_batch(body: bytes, content_type: str, content_encoding: str | None, max_body: int, max_decompressed: int) -> tuple[EventBatch, bytes]`. 실패 시 `codec.HTTPError(status, msg)`.
  - `gate.GateConfig(max_past: timedelta, max_future: timedelta)`, `gate.validate(batch, now: datetime, cfg) -> GateResult(accepted: list[Event], rejected: list[tuple[Event, str]], primary_values: list[float])`
  - `profiles.primary_value(event) -> float`

- [ ] **Step 1: 시험 작성**: `test_gate_cases`(cases.jsonl 전부, T2와 같은 단언), `test_zstd_bomb_rejected_413`(스트리밍 해제, 피크 RSS 증가 < 80MiB), `test_json_body_equivalent_to_protobuf`
- [ ] **Step 2:** 실패 확인.
- [ ] **Step 3:** 구현. `zstandard.ZstdDecompressor().stream_reader`로 `max_decompressed + 1`까지만 읽는다.
- [ ] **Step 4:** 통과.
- [ ] **Step 5:** commit `feat(py-common): contract gate and codec`.

---

### Task 8: ④ ingest-py

**Files:**
- Create: `apps/ingest-py/{pyproject.toml,Dockerfile,README.md}`, `apps/ingest-py/src/ingest/{main.py,sink_jetstream.py,sink_db.py}`
- Test: `apps/ingest-py/tests/{test_ingest_jetstream.py,test_ingest_db.py,test_shutdown.py}`

**Interfaces:**
- Consumes: T1, T7, spec §4·§5.2.
- Produces: `POST /v1/batches`(spec §5.2). env `INGEST_STREAM`, `INGEST_MAX_BODY_BYTES`, `INGEST_MAX_DECOMPRESSED_BYTES`, `INGEST_MAX_INFLIGHT`, `GATE_MAX_PAST`, `GATE_MAX_FUTURE`, `JS_MAX_AGE=45m`, `JS_MAX_BYTES=60GiB`, `WEB_CONCURRENCY`(uvicorn 워커 수, 기본 CPU 수).
- `Sink` 프로토콜: `async store(batch_id: str, raw_compressed: bytes | None, result: GateResult) -> None`. 실패 시 `StoreUnavailable` → 503. jetstream sink는 시작할 때 stream과 `quarantine-<INGEST_STREAM>` stream(1GiB, 7일)을 create-or-update한다. db sink는 `COPY`로 `raw_events`와 `quarantine`에 넣고, 시작할 때와 10분마다 `SELECT jammy.ensure_partitions('3 hours')`를 호출한다.

- [ ] **Step 1: 시험 작성**
  - `test_valid_batch_202_and_in_stream`: 202, stream 메시지 1개, header `Nats-Msg-Id == batch_id`
  - `test_fully_valid_batch_passes_bytes_through`: stream 메시지 데이터 == 받은 압축 본문(바이트 동일)
  - `test_partial_batch_reencodes_valid_only`, `test_rejected_go_to_quarantine_with_reason`
  - `test_duplicate_batch_id_single_message`
  - `test_stream_full_returns_503`: `JS_MAX_BYTES=1MiB`로 채운 뒤 → 503 + `Retry-After`
  - `test_inflight_limit_429`
  - `test_db_mode_copies_rows`: direct-db → raw_events 행 수 == accepted, 같은 batch 재전송 시 행 증가 0
  - `test_nats_down_readyz_503_then_recovers`
  - `test_sigterm_drains_inflight`: 큰 배치 요청 중 SIGTERM → 그 요청은 202이고 stream에 있음. 이후 새 요청은 연결 거부 또는 503.
- [ ] **Step 2:** 실패 확인.
- [ ] **Step 3:** 구현.
- [ ] **Step 4:** 통과, multi-arch 빌드 성공.
- [ ] **Step 5:** commit `feat(ingest-py): contract-gated batch intake`.

---

### Task 9: ⑤ processor-py

**Files:**
- Create: `apps/processor-py/{pyproject.toml,Dockerfile,README.md}`, `apps/processor-py/src/processor/{main.py,source_jetstream.py,source_db.py,aggregate.py,anomaly.py,state.py}`
- Test: `apps/processor-py/tests/{test_aggregate.py,test_anomaly.py,test_processor_jetstream.py,test_processor_db.py}`

**Interfaces:**
- Consumes: T1, T7(`primary_value`), T8의 stream 메시지 형식.
- Produces: `summary_1m`, `latest_state`(`STATE_FLUSH_SECONDS=1`마다 upsert), `incidents`, `processed_batches`. env `NATS_STREAMS`, `CONSUMER_NAME=processor`, `ACK_WAIT_SECONDS=30`, `MAX_ACK_PENDING=1000`, `FETCH_BATCH=64`, `ANOMALY_DROP_RATIO=0.5`.
- 시작할 때와 10분마다 `jammy.ensure_partitions('3 hours')`를 호출한다(summary_1m 파티션).
- `aggregate.fold(events) -> dict[(profile, routing_key, minute), Agg(count, sum, min, max)]`. upsert는 `count = count + EXCLUDED.count, sum = sum + EXCLUDED.sum, min = LEAST, max = GREATEST`.

- [ ] **Step 1: 시험 작성**
  - `test_fold_minute_buckets`, `test_anomaly_drop_detected`, `test_anomaly_over_capacity`
  - `test_summary_counts_match_published`: 이벤트 10,000개 publish → `sum(count) == 10000`
  - `test_ack_only_after_commit`: DB를 commit 직전에 끊음 → 메시지 미ACK, 재연결 후 count 정확
  - `test_redelivery_after_ackwait_no_double_count`: `ACK_WAIT_SECONDS=1`, 처리 2초 지연 주입 → 최종 count 정확
  - `test_kill9_restart_exact_counts`: 처리 중 프로세스 SIGKILL → 재시작 후 count 정확
  - `test_db_mode_cursor_progress_exact`: direct-db, 재시작 2번 → count 정확
  - `test_processed_batches_pruned_after_1d`
- [ ] **Step 2:** 실패 확인.
- [ ] **Step 3:** 구현. 한 fetch 묶음 = 한 tx(processed_batches 확인과 insert, 요약 upsert, incidents). commit 후 메시지별 ACK.
- [ ] **Step 4:** 통과, multi-arch 빌드 성공.
- [ ] **Step 5:** commit `feat(processor-py): commit-then-ack aggregation`.

---

### Task 10: ⑥ archiver-py

**Files:**
- Create: `apps/archiver-py/{pyproject.toml,Dockerfile,README.md}`, `apps/archiver-py/src/archiver/{main.py,source_jetstream.py,export_db.py,parquet_writer.py,seal.py,ratelimit.py}`
- Test: `apps/archiver-py/tests/{test_parquet.py,test_seal.py,test_archiver_jetstream.py,test_archiver_db.py,test_ratelimit.py}`

**Interfaces:**
- Consumes: T1, T7, spec §5.8.
- Produces: spec §5.8의 S3 객체, manifest, 봉인 파일. env `NATS_STREAMS`, `CONSUMER_NAME=archiver`, `ARCHIVE_FLUSH_SECONDS=10`, `ARCHIVE_FLUSH_BYTES=67108864`, `ARCHIVE_ZSTD_LEVEL=1`, `ARCHIVE_MAX_UPLOAD_BPS=0`(0 = 제한 없음), `SEAL_LATENESS=10m`, `STAGE_DIR=/stage`, `ACK_WAIT_SECONDS`(기본 `ARCHIVE_FLUSH_SECONDS × 3 + 60`).
- direct-db: 봉인된 `raw_events` 시간 파티션 → Parquet → 업로드 → S3 행 수와 sha256 확인 → `archive_exports` 기록 → `DETACH PARTITION` → `DROP TABLE`.

- [ ] **Step 1: 시험 작성**
  - `test_parquet_schema_matches_contract`(`contracts/testdata/parquet_schema.json`과 열 이름·타입·zstd 비교)
  - `test_manifest_rows_and_sha256`
  - `test_s3_rows_equal_unique_published`: 이벤트 20,000개 → S3 Parquet 고유 event_id 수 == 20000
  - `test_kill9_mid_flush_no_loss_no_extra_keys`: 업로드 전 SIGKILL → 재시작 후 같은 키로 덮어쓰기, 고유 수 정확
  - `test_seal_written_after_watermark`, `test_late_event_goes_to_raw_late`
  - `test_stage_file_removed_after_upload`
  - `test_upload_rate_limit_respected`: 1MiB/s 제한 → 5MiB 업로드 ≥ 4.5초
  - `test_db_mode_export_then_drop_partition`: 파티션 DROP 뒤 `archive_exports.rows` == S3 행 수
- [ ] **Step 2:** 실패 확인.
- [ ] **Step 3:** 구현. flush 단위로 메시지를 모았다가 업로드 성공 뒤 그 메시지들을 ACK한다.
- [ ] **Step 4:** 통과, multi-arch 빌드 성공.
- [ ] **Step 5:** commit `feat(archiver-py): parquet to s3 with seal`.

---

### Task 11: ④ ingest-go

**Files:**
- Create: `apps/ingest-go/{Dockerfile,README.md,main.go}`, `apps/ingest-go/internal/{server,sinkjs,sinkdb}/*.go`
- Test: 같은 패키지 `*_test.go`

**Interfaces:**
- Consumes: T2. env·응답·메시지 형식은 T8과 같다(spec §5.2).
- Produces: T8과 같은 시험 단언을 Go 이름으로 만든다.

- [ ] **Step 1: 시험 작성**: T8의 시험 10개를 Go로(`TestValidBatch202AndInStream`, `TestFullyValidBatchPassesBytesThrough`, `TestPartialBatchReencodesValidOnly`, `TestRejectedGoToQuarantineWithReason`, `TestDuplicateBatchIDSingleMessage`, `TestStreamFullReturns503`, `TestInflightLimit429`, `TestDBModeCopiesRows`, `TestNATSDownReadyzThenRecovers`, `TestSigtermDrainsInflight`)와 `TestZstdBombRejected413`(HTTP 수준)
- [ ] **Step 2:** `go test ./apps/ingest-go/...` 실패 확인.
- [ ] **Step 3:** 구현. DB COPY는 `pgx.CopyFrom`.
- [ ] **Step 4:** `go test -race` 통과, multi-arch 빌드 성공.
- [ ] **Step 5:** commit `feat(ingest-go): contract-gated batch intake`.

---

### Task 12: ⑤ processor-go

**Files:**
- Create: `apps/processor-go/{Dockerfile,README.md,main.go}`, `apps/processor-go/internal/{aggregate,anomaly,sourcejs,sourcedb,state}/*.go`
- Test: 같은 패키지 `*_test.go`

**Interfaces:**
- Consumes: T2. 동작·env·테이블 쓰기 규칙은 T9와 같다.

- [ ] **Step 1: 시험 작성**: T9의 9개 시험을 Go로(`TestRedeliveryAfterAckwaitNoDoubleCount` 포함).
- [ ] **Step 2:** 실패 확인.
- [ ] **Step 3:** 구현.
- [ ] **Step 4:** `go test -race` 통과, multi-arch 빌드 성공.
- [ ] **Step 5:** commit `feat(processor-go): commit-then-ack aggregation`.

---

### Task 13: ⑥ archiver-go

**Files:**
- Create: `apps/archiver-go/{Dockerfile,README.md,main.go}`, `apps/archiver-go/internal/{sourcejs,exportdb,pq,seal,ratelimit}/*.go`
- Test: 같은 패키지 `*_test.go`

**Interfaces:**
- Consumes: T2. 동작·env·S3 키는 T10과 같다. Python이 쓴 Parquet와 Go가 쓴 Parquet는 같은 스키마여야 한다.

- [ ] **Step 1: 시험 작성**: T10의 9개 시험을 Go로 만들고, `TestParquetSchemaMatchesContract`를 추가한다(T0의 `contracts/testdata/parquet_schema.json`과 열 정의 비교).
- [ ] **Step 2:** 실패 확인.
- [ ] **Step 3:** 구현(`parquet-go`, zstd).
- [ ] **Step 4:** `go test -race` 통과, multi-arch 빌드 성공.
- [ ] **Step 5:** commit `feat(archiver-go): parquet to s3 with seal`.

---

### Task 14: payloadgen, plantsim

**Files:**
- Create: `tools/payloadgen/main.go`, `tools/payloadgen/gen/*.go`, `tools/plantsim/{main.go,Dockerfile,README.md}`
- Test: `tools/payloadgen/gen/gen_test.go`, `tools/plantsim/main_test.go`

**Interfaces:**
- Produces:
  - `payloadgen assets --profile solar --count N --tenants M > seed.sql` (`assets` INSERT, `ON CONFLICT DO NOTHING`)
  - `payloadgen batches --profile <p> --events-per-batch K --batches B --assets N --out DIR --seed S`. 결과물: `DIR/batch-000001.bin`(protobuf+zstd), `DIR/index.json`(`[{file, batch_id, events, wire_bytes, raw_bytes}]`). event_time은 k6가 보낼 때 덮어쓰지 않는다. 대신 `--time-mode relative`이면 payload 생성 시각 기준 상대값을 쓰고, 이 경우 batch는 재생 직전에 생성한다.
  - 같은 `--seed`면 같은 바이트가 나온다.
  - plantsim: `POST /v1/commands`(spec §5.4), 자산별 최대 epoch 기억, env `PLANTSIM_DELAY_MS=0`, `PLANTSIM_FAIL_RATIO=0`. metric `jammy_plantsim_commands_total{result}`.
- [ ] **Step 1: 시험 작성**: `TestDeterministicWithSeed`, `TestBatchesPassGate`(T2 `gate.Validate`로 전부 accepted), `TestIndexBytesMatchFiles`, `TestStaleEpochRejected409`, `TestAckRecordsMaxEpoch`
- [ ] **Step 2:** 실패 확인.
- [ ] **Step 3:** 구현.
- [ ] **Step 4:** 통과, plantsim multi-arch 빌드 성공.
- [ ] **Step 5:** commit `feat(tools): payloadgen and plantsim`.

---

### Task 15: k6 시나리오

**Files:**
- Create: `loadtest/k6/{ingest.js,instruction.js,lib/hmac.js,README.md}`
- Test: `loadtest/k6/smoke.sh`

**Interfaces:**
- Consumes: T14 `index.json`과 `.bin`, T4 서명 규칙(`hmac_cases.json`으로 확인).
- Produces:
  - `ingest.js` env: `TARGETS`(쉼표 목록, 수신 주소별 분산), `TARGET_WIRE_MBPS_PER_SERVER=30`, `DURATION`, `PAYLOAD_DIR`, `RUN_ID`. `index.json`의 `wire_bytes`로 초당 필요한 요청 수를 계산한다(`constant-arrival-rate`). 커스텀 metric `jammy_k6_wire_bytes`, `jammy_k6_raw_bytes`, 상태 코드별 counter. 종료 요약을 `results/<RUN_ID>/summary.json`에 저장한다.
  - `instruction.js` env: `TARGET`, `HMAC_KEY_ID`, `HMAC_SECRET`, `RATE`. threshold `http_req_duration{name:instruction}` p(99) < 1000.
  - k6 버전은 `grafana/k6:<고정 tag>`.
- [ ] **Step 1:** `smoke.sh` 작성. 단언: compose split에서 `ingest.js`를 10초, `TARGET_WIRE_MBPS_PER_SERVER=1`로 돌려 202 비율 100%, `hmac.js`가 `hmac_cases.json` 전부와 일치.
- [ ] **Step 2:** 실패 확인.
- [ ] **Step 3:** 구현.
- [ ] **Step 4:** `smoke.sh` 통과.
- [ ] **Step 5:** commit `feat(loadtest): k6 ingest and instruction scenarios`.

---

### Task 16: Helm chart

**Files:**
- Create: `deploy/helm/jammy-platform/{Chart.yaml,values.yaml,values-mvp.yaml,values-split.yaml,values.schema.json,README.md}`
- Create: `deploy/helm/jammy-platform/templates/{_helpers.tpl,deployment-app.yaml,deployment-ingest.yaml,service.yaml,job-migrate.yaml,pdb.yaml,networkpolicy.yaml,configmap.yaml,NOTES.txt}`
- Test: `deploy/helm/jammy-platform/tests/*.yaml`(helm-unittest), `deploy/helm/k3d-smoke.sh`

**Interfaces:**
- Consumes: 모든 서비스의 env(`contracts/ENV.md`), spec §6.
- Produces: values 키 `global.image.registry`, `global.image.tag`, `global.siteId`, `global.pipelineMode`, `global.existingSecret`(키 `DATABASE_URL`, `HMAC_KEYS`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`), `global.nats.url`, `global.s3.*`, `global.otlpEndpoint`. 서비스별 `<svc>.enabled`, `.replicas`, `.resources`, `.nodeSelector`, `.tolerations`, `.env`. `ingest.impl|processor.impl|archiver.impl: go|py`. `ingest.instances: [{name, stream, nodeSelector, service: {type, nodePort}}]`. migrate Job은 `helm.sh/hook: pre-install,pre-upgrade`.
- 현장 정합(origin/main `k8s/sites/a/README.md` 기준): 기본 namespace `jammy`, `global.imagePullSecrets` 기본값 `[ghcr-pull]`(GHCR private). Traefik·ServiceLB가 꺼져 있으므로 Ingress와 `LoadBalancer`를 기본으로 만들지 않는다(Service 기본 ClusterIP, ④만 NodePort 선택). k8s 담당은 kustomize를 쓰므로 `make helm-render ARM=<mvp|split>`이 `deploy/helm/rendered/<arm>/`에 평문 manifest와 `kustomization.yaml`을 만들어 `k8s/sites/a`에서 참조할 수 있게 한다. `k8s/` 아래 파일은 이 task에서 수정하지 않는다.
- [ ] **Step 1: 시험 작성** (helm-unittest, 아래에 `namespace_default_jammy`, `image_pull_secret_ghcr_pull`, `no_ingress_or_loadbalancer_by_default` 추가)
  - `impl_go_selects_go_image`: `ingest.impl=go` → image `.../ingest-go:<tag>`
  - `split_renders_two_ingest_instances`: 두 Deployment, 각각 `INGEST_STREAM`가 `raw-a1`·`raw-a2`
  - `mvp_sets_direct_db`: 모든 ④⑤⑥ env `PIPELINE_MODE=direct-db`
  - `security_context_hardened`: runAsNonRoot, readOnlyRootFilesystem, `capabilities.drop: [ALL]`
  - `probes_on_admin_port`: liveness `/livez:9090`, readiness `/readyz:9090`
  - `no_latest_tag`: tag가 비면 렌더 실패
  - `dispatcher_split_two_replicas_antiaffinity`
- [ ] **Step 2:** `helm unittest deploy/helm/jammy-platform` 실패 확인.
- [ ] **Step 3:** 구현.
- [ ] **Step 4:** unittest 통과, `helm lint` 두 values 통과, `helm template ... | kubeconform -strict` 통과, `k3d-smoke.sh`(k3d 클러스터 + 클러스터 밖 compose PG·NATS·SeaweedFS 연결, 두 values 각각 install 후 모든 pod Ready) 통과.
- [ ] **Step 5:** commit `feat(helm): jammy-platform chart with mvp and split values`.

---

### Task 17: CI·이미지 배포

**Files:**
- Create: `.github/workflows/backend.yml`, `scripts/image-tag.sh`
- Test: `scripts/tests/test_image_tag.sh`

**Interfaces:**
- Produces:
  - `scripts/image-tag.sh` → stdout `<version>-<sha7>`. version은 `git describe --tags --abbrev=0 --match 'v*'`에서 `v`를 뺀 값, 없으면 `0.1.0`. HEAD가 tag 커밋이면 두 번째 줄에 `<version>`.
  - workflow job: `changes`(paths-filter) → `contracts`(make contracts-check) → `py`(ruff, pytest matrix) → `go`(golangci-lint, `go test -race`) → `helm`(unittest, lint, kubeconform) → `images`(matrix: instruction-api, dispatcher, portal-publisher, ingest-go, ingest-py, processor-go, processor-py, archiver-go, archiver-py, migrate, plantsim. buildx `linux/amd64,linux/arm64`, Trivy HIGH·CRITICAL이면 실패, SBOM SPDX, main push일 때만 GHCR push와 cosign keyless 서명).
  - action 버전은 기존 `ci.yml`이 쓰는 major와 같게 고정한다.
- [ ] **Step 1:** `test_image_tag.sh`: tag 없음 → `0.1.0-<sha7>`. `v1.2.0` tag 커밋 → `1.2.0-<sha7>`과 `1.2.0`. tag 뒤 커밋 → `1.2.0-<새 sha7>`.
- [ ] **Step 2:** 실패 확인.
- [ ] **Step 3:** 스크립트와 workflow 구현.
- [ ] **Step 4:** 시험 통과, `actionlint .github/workflows/backend.yml` 통과. PR에서 push 없이 workflow가 끝까지 green인지는 지휘자가 PR을 올릴 때 확인한다.
- [ ] **Step 5:** commit `ci(backend): multi-arch build, scan, sign, push`.

---

### Task 18: 관측 (③ 대시보드, Alloy)

**Files:**
- Create: `observability/dashboards/{platform-overview.json,data-pipeline.json,control-path.json,experiment-run.json}`
- Create: `observability/alloy/config.alloy`, `observability/alloy/README.md`
- Modify: `deploy/compose/docker-compose.yml`(profile `obs`에 마운트)
- Test: `observability/dashboards/check.sh`

**Interfaces:**
- Consumes: `contracts/METRICS.md`.
- Produces: Alloy가 compose와 k8s에서 pod annotation `jammy.io/scrape="true"`인 대상을 scrape한다. external label `run_id`, `arm`, `profile`(env `RUN_ID`, `ARM`, `PROFILE`)을 붙여 `prometheus.remote_write`로 VictoriaMetrics `/api/v1/write`에 보낸다. OTLP receiver 4317·4318 → Tempo, Loki.
- 대시보드 변수 `run_id`, `arm`, `site`, `service`. 패널: RED(서비스별), ingest 이벤트·바이트(와이어·해제), 거부 사유, JetStream pending·lag, processor commit p99, archiver 업로드 대역, 지시 접수 p99와 1초 SLO 위반 비율, 명령 결과, leader·lease 상태.
- [ ] **Step 1:** `check.sh` 작성. 대시보드 JSON의 모든 PromQL에서 metric 이름을 뽑아 `METRICS.md` 목록에 있는지 확인한다. `alloy fmt` 통과도 확인한다.
- [ ] **Step 2:** 실패 확인.
- [ ] **Step 3:** 구현.
- [ ] **Step 4:** `check.sh` 통과. `make up-split up-obs` 후 Grafana에서 4개 대시보드가 로드되고, `up{service="ingest-go"}`가 VictoriaMetrics에서 조회된다.
- [ ] **Step 5:** commit `feat(observability): dashboards and alloy scrape to victoriametrics`.

---

### Task 19: E2E 시험 (agy)

**Files:**
- Create: `tests/e2e/{pyproject.toml,conftest.py,test_pipeline.py,test_faults.py,test_control.py}`
- Modify: `Makefile`(target `e2e`: `IMPL=go|py ARM=mvp|split`)

**Interfaces:**
- Consumes: T3 compose 포트, T14 payloadgen·plantsim, T4 `sign`.
- Produces: `make e2e IMPL=<go|py> ARM=<mvp|split>`. 4개 조합 모두 통과해야 완성이다.
- [ ] **Step 1: 시험 작성**
  - `test_three_profiles_end_to_end`: profile 3종 × 배치 50개 → `sum(summary_1m.count)` == accepted, S3 고유 event_id == accepted(봉인 대기 포함, 제한 시간 5분)
  - `test_duplicate_batch_resend_no_double_count`
  - `test_kill9_processor_exact` / `test_kill9_archiver_exact` / `test_kill9_ingest_acked_not_lost`(202 받은 batch_id 전부가 최종 저장됨)
  - `test_jetstream_full_503`(split만)
  - `test_instruction_to_commands_acked`: 서명된 지시 → plantsim ACK → status `dispatched`
  - `test_dispatcher_leader_failover_no_duplicate_commands`(dispatcher 2개 실행, leader SIGKILL)
  - `test_stale_epoch_command_rejected`
  - `test_direct_db_partition_archived_and_dropped`(mvp만, `SEAL_LATENESS=0s`와 짧은 시간 창으로 가속)
  - `test_gate_cases_over_http`: `cases.jsonl` 전부를 실제 ingest에 HTTP로 보내 기대 결과와 비교
- [ ] **Step 2:** 실행 → 실패 항목 목록을 각 track 구현자에게 돌려보낸다.
- [ ] **Step 3:** 수정 반영 뒤 재실행.
- [ ] **Step 4:** `make e2e` 4개 조합 통과. 결과 로그를 `docs/measurements/raw/e2e-<날짜>/`에 저장(성능 수치로 해석하지 않음).
- [ ] **Step 5:** commit `test(e2e): pipeline, fault and control scenarios`.

---

### Task 20: 인계 문서 (지휘자)

**Files:**
- Create: `docs/backend/HANDOVER.md`

**Interfaces:**
- Consumes: T16 values, `contracts/ENV.md`, T17 태그 규칙, T19 결과.
- Produces: k8s 담당이 이 문서만 보고 설치할 수 있는 절차서. 내용: 이미지 목록과 태그 읽는 법(표), 사전 조건(PG·NATS·S3·secret 키), `helm install` 명령(mvp·split), 노드 label·taint 규칙(`role=main|control|ingest`), 포트·probe, 자원 요청값, migrate 순서, 확인 명령(`kubectl get pods`, `/readyz`), 롤백(`helm rollback`), 문제 해결 5개. 값은 복사하지 않고 `ENV.md`와 `values*.yaml`을 링크한다.
- [ ] **Step 1:** 작성.
- [ ] **Step 2:** agy가 문서만 보고 새 k3d 클러스터에 설치해 본다. 막힌 지점을 기록한다.
- [ ] **Step 3:** 막힌 지점을 반영해 수정한다.
- [ ] **Step 4:** 재시험 통과.
- [ ] **Step 5:** commit `docs(backend): k8s handover guide`.
