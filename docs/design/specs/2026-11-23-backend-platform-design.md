# JAMMY 백엔드 플랫폼 설계 (측정기 겸 인계 이미지)

목적: 같은 이미지로 두 아키텍처를 비교 측정하고, 그 이미지를 GHCR과 Helm chart로 k8s 담당에게 넘긴다. 아키텍처 차이는 Helm values와 env로만 바꾼다. 코드가 바뀌면 비교가 무효다.

- 상위 근거: Confluence 28344332 "0. JAMMY 아키텍처(11-19)" v16, 28934153 "6. [백엔드] 아키텍처 성능평가" v12.
- 실행 계획: `docs/design/plans/2026-11-23-backend-platform.md`
- 결정 출처: 2026-11-23 사용자와의 grilling 세션. 아래 "확정 결정"은 사용자의 명시적 답이고, "설계 선택"은 Claude의 제안이다.

## 1. 확정 결정 (사용자, 2026-11-23)

| 항목 | 결정 |
|---|---|
| 비교군 | Arm-1: 본체 단독, 수신 이벤트를 DB에 직접 저장(`PIPELINE_MODE=direct-db`). Arm-2: v3.12 역할 분리(본체 = DB·제어, A1·A2 = 수신·JetStream, `PIPELINE_MODE=jetstream`) |
| 우선순위 | 이미지, Helm, 인계 문서가 먼저. 실험 하네스는 그 다음 |
| 서비스 모델 | 범용 코어 + 태양광 실제 profile + 합성 profile 2종 |
| 담당 범위 | ①②③④⑤⑥⑧ 와 부하 발생기. ③은 Grafana 대시보드 코드. ⑨ Airflow는 별도 담당자 소관이고, 우리는 인계 계약만 만든다 |
| 저장소 | 이 레포의 `apps/<service>/`. 서비스당 이미지 1개 |
| 언어 | Python 3.14, Go 1.27, Helm 4 등 버전은 `docs/backend/VERSIONS.md`(사용자 승인 2026-11-23). ①②⑧ Python FastAPI. ④⑤⑥ Go와 Python 두 벌, 계약은 같다. 오늘 둘 다 완성 |
| 플랫폼 | `linux/arm64`(Mac에서 먼저 비교)와 `linux/amd64`(Intel Linux에서 재시험) 둘 다 |
| 배포물 | Helm chart, `values-mvp.yaml`·`values-split.yaml`. secret은 `existingSecret` |
| stateful | VM의 PG·Patroni·NATS 설치는 infra 담당. 로컬은 compose |
| 압축 | 발생기가 zstd로 압축해서 보낸다. ④는 다시 압축하지 않는다. JetStream 압축 끔 |
| 부하 | 서버당 와이어(압축 후) 30MB/s 기본. k6 사용, 발생기 병목은 PRE에서 확인 |
| 관측 | VictoriaMetrics(Prometheus 아님) |
| 태그 | `<service>:<마지막 릴리스>-<커밋 7자리>`, 릴리스 커밋에는 `<버전>`도. `latest` 금지 |
| 일정 | VM 2026-11-23 점심 완료, 앱 2026-11-23 완성, 인계 2026-11-24 |
| 역할 | 지휘·설계·최종 평가 Claude Opus 5.5 medium(최종 평가는 새 context), 구현 codex GPT-6.2 Sol medium 5명 병렬, 검토·시험 agy Gemini 4 Hydrogen high 3명이 서로 독립적으로 |

## 2. 설계 선택 (Claude 제안, 사용자가 "추천대로"로 승인)

- 수신 프로토콜: HTTP 배치, `application/x-protobuf` + `Content-Encoding: zstd` 기본, `application/json` 디버그 허용.
- DB 마이그레이션: plain SQL + `golang-migrate`, 앱과 분리한 `migrate` Job 이미지.
- 인증: ① 외부 입구만 HMAC-SHA256 + `instruction_id` 재전송 방지. 내부는 NetworkPolicy.
- 관측: OTel SDK. metric은 admin 포트 `/metrics`를 Alloy가 scrape해 VictoriaMetrics로 remote write. trace·log는 OTLP로 Alloy를 거쳐 Tempo·Loki. 실험 label(`run_id`, `arm`, `profile`)은 scrape 설정에서 붙인다.
- 로컬 AWS 대체: S3는 SeaweedFS, SQS는 ElasticMQ, DynamoDB는 `amazon/dynamodb-local`. grilling에서 LocalStack을 합의했으나 라이선스·인증 정책 변동이 확인되지 않아 단일 기능 대체품으로 바꿨다. 사용자 확인 필요.

## 3. 구성 요소

| # | 서비스(이미지) | 언어 | 포트 | 역할 |
|---|---|---|---|---|
| ① | `instruction-api` | Py | 8080, 9090 | 지시 접수 ≤ 1초, HMAC 검증, 멱등 저장, 상태 조회 |
| ② | `dispatcher` | Py | 9090 | leader 1개(advisory lock + generation), lease 갱신, SQS 수거, 배분·명령·ACK, 이행 확인 |
| ③ | Grafana JSON | — | — | 플랫폼·파이프라인·제어·실험 대시보드 |
| ④ | `ingest-go`, `ingest-py` | Go/Py | 8080, 9090 | 배치 수신, Contract Gate, JetStream publish 또는 DB COPY |
| ⑤ | `processor-go`, `processor-py` | Go/Py | 9090 | 최신 상태, 1분 요약, 이상 감지. DB commit 후 ACK |
| ⑥ | `archiver-go`, `archiver-py` | Go/Py | 9090 | Parquet(zstd) → S3, manifest, 봉인, direct-db 파티션 이관 |
| ⑧ | `portal-publisher` | Py | 9090 | tenant별 1분 JSON → S3 |
| — | `migrate` | SQL | — | 스키마 적용 Job |
| — | `plantsim` | Go | 8080, 9090 | 발전소 명령 수신·ACK, 옛 epoch 거부(시험용) |
| — | `payloadgen` | Go | CLI | 자산 seed SQL과 protobuf+zstd 배치 파일 생성(k6 입력) |

모든 서비스는 admin 포트 9090에 `/livez`, `/readyz`, `/metrics`를 둔다. API 포트 8080은 ①④와 plantsim만 연다.

## 4. 데이터 경로

### 4.1 Arm-2 `PIPELINE_MODE=jetstream`

```
발생기 ─HTTP(pb+zstd)→ ④ ─PubAck 후 202→ JetStream stream(호스트별, R1)
                          └ 거부 이벤트 → quarantine stream
JetStream ─durable "processor"→ ⑤ ─tx(요약 upsert + processed_batches) commit → ACK
JetStream ─durable "archiver"──→ ⑥ ─Parquet 업로드 성공 → ACK
```

- JetStream 메시지 1개 = 검증 통과 배치 1개. `Nats-Msg-Id = batch_id`(stream 중복 창 2분).
- 배치 전체가 통과하면 받은 압축 바이트를 그대로 publish한다. 일부만 통과하면 통과분만 다시 인코딩·압축한다.
- stream 설정: `max_age=45m`, `max_bytes=60GiB`, `discard=new`, `compression=none`, `storage=file`, `replicas=1`, `duplicate_window=2m`. 와이어 30MB/s에서는 `max_bytes`에 먼저 닿아 보관 시간이 약 33분이 된다(추론, 측정 대상).
- stream 이름은 수신 인스턴스마다 다르다(`INGEST_STREAM`, 예: `raw-a1`). ⑤⑥은 `NATS_STREAMS` 목록을 모두 구독한다.

JetStream 메시지 계약 (④ → ⑤⑥, Go·Py 공통):
- raw: stream `<INGEST_STREAM>`, subject `jammy.raw.<INGEST_STREAM>`. 본문은 항상 protobuf `EventBatch`. 받은 본문이 protobuf이고 전부 통과하면 받은 바이트를 그대로(zstd면 zstd 그대로) 싣는다. JSON 본문이거나 일부만 통과하면 통과분을 protobuf + zstd로 다시 인코딩한다.
- raw header: `Nats-Msg-Id = batch_id`, `Content-Type: application/x-protobuf`, 압축이면 `Content-Encoding: zstd`, `Jammy-Ingest-Id = INSTANCE_ID`, `Jammy-Received-At-Ms`(④가 요청을 받은 시각, unix ms). `received_at`은 이 header로 정하고 JetStream metadata 시각은 쓰지 않는다.
- quarantine: stream `quarantine-<INGEST_STREAM>`, subject `jammy.quarantine.<INGEST_STREAM>`. 거부 이벤트마다 메시지 1개, 본문은 protobuf `Event`(압축 없음). header `Nats-Msg-Id = <batch_id>:q:<원 배치 안의 index>`, `Jammy-Batch-Id`, `Jammy-Ingest-Id`, `Jammy-Reason`, `Jammy-Received-At-Ms`. quarantine을 먼저 publish하고 raw를 나중에 publish한다.
- 202의 `duplicate`: raw를 publish했으면 raw PubAck의 duplicate 값. 전부 거부돼 raw가 없으면 모든 quarantine PubAck가 duplicate일 때 true. 중복 판정은 stream `duplicate_window`(2분) 안에서만 보장한다.

### 4.2 Arm-1 `PIPELINE_MODE=direct-db`

```
발생기 → ④ ─COPY into jammy.raw_events commit 후 202
⑤ ─raw_events를 (ingest_xid, event_id) 커서로 읽음 → 요약 upsert + 커서 저장(같은 tx)
⑥ ─봉인된 시간 파티션 → Parquet → S3 → 검증 → DETACH + DROP PARTITION
```

- 행 단위 `DELETE` 금지(WAL과 VACUUM 부담). 파티션 단위 DROP만 쓴다.
- 커서 규칙: `received_at` 순서는 commit 순서와 다를 수 있어 커서로 쓰지 않는다. `raw_events.ingest_xid xid8 DEFAULT pg_current_xact_id()`를 두고, ⑤는 `ingest_xid < pg_snapshot_xmin(pg_current_snapshot())`인 행만 `(ingest_xid, event_id)` 순서로 읽는다. 그 아래 xid는 모두 commit 또는 abort가 끝났으므로 놓치는 행이 없다. ④는 테이블 lock을 쓰지 않는다(Arm-1 처리량을 인위로 제한하지 않음). 긴 트랜잭션이 있으면 ⑤가 그만큼 늦어지며, 이는 `jammy_processor_lag_seconds`로 드러난다.

### 4.3 정합성 규칙

- ④는 저장이 내구화된 뒤에만 202를 준다(JetStream PubAck 또는 DB commit). 이것이 SLO-4 "ACK 받은 메시지 손실 0"의 경계다.
- ④ direct-db 멱등: `processed_batches(consumer='ingest', batch_id)`를 `ON CONFLICT DO NOTHING`으로 먼저 잡고, 같은 tx에서 `raw_events`·`quarantine` COPY를 한다. 충돌이면 둘 다 건너뛴다. 중복 판정 범위는 두 모드 모두 batch_id 단위다. 서로 다른 배치에 같은 event_id가 있으면 ④에서 걸러지지 않는다(알려진 한계).
- ⑤ 멱등: `processed_batches(consumer, batch_id)`를 요약 upsert와 같은 tx에 넣는다. 재전송은 건너뛴다.
- ⑥ 멱등: S3 키를 stream sequence 범위로 정한다(`<ingest_id>-<first_seq>-<last_seq>.parquet`). 재업로드는 같은 키를 덮어쓴다.
- ② 단일 활성: `pg_try_advisory_lock`을 쥔 인스턴스만 동작한다. 획득할 때마다 `leader_generation`을 +1하고, 명령에 generation과 lease epoch를 싣는다.
- 옛 epoch 명령은 plantsim이 거부한다(자산별로 본 최대 epoch 기준).

## 5. 계약 (구현 전 고정, `contracts/`)

### 5.1 이벤트 envelope (`contracts/proto/jammy/v1/envelope.proto`)

```proto
message Event {
  string event_id = 1;          // 1..128 bytes, [A-Za-z0-9._:-]
  string routing_key = 2;       // 1..128 bytes, 같은 문자 집합
  string profile = 3;           // "solar" | "synth-small" | "synth-large"
  uint32 schema_version = 4;    // profile별 지원 버전, 현재 1
  int64 event_time_unix_ms = 5;
  bytes payload = 6;            // profile 메시지의 protobuf 인코딩
  map<string, string> attrs = 7;// 최대 16개, 키·값 각 64 bytes
}
message EventBatch {
  string batch_id = 1;          // 1..128 bytes, [A-Za-z0-9._:-], 위반 시 배치 400
  string producer_id = 2;       // 0..128 bytes, 같은 문자 집합, 위반 시 배치 400
  repeated Event events = 3;    // 1..10000
}
```

profile payload (`profiles.proto`, 괄호 안은 field 번호와 타입). primary value는 요약·이상 감지에 쓰는 값이다.

| profile | 메시지 | 필드와 범위 | primary value |
|---|---|---|---|
| `solar` | `SolarReading` | `asset_id`(1, string, routing_key와 같아야 함, 다르면 `bad_payload`), `power_kw`(2, double) [0, 100000], `irradiance_wm2`(3, double) [0, 1500], `temperature_c`(4, double) [-40, 90], `status`(5, enum `Status`: `STATUS_UNSPECIFIED=0`, `STATUS_OK=1`, `STATUS_DEGRADED=2`, `STATUS_OFFLINE=3`. 0이나 모르는 값은 `out_of_range`) | `power_kw` |
| `synth-small` | `SynthSmall` | `value`(1, double, 유한값), `pad`(2, bytes) ≤ 256 bytes | `value` |
| `synth-large` | `SynthLarge` | `value`(1, double, 유한값), `blob`(2, bytes) ≤ 65536 bytes | `value` |

### 5.2 Contract Gate (④, Go·Py 동일 결과)

검사 순서와 거부 사유 코드(`reason`)는 아래로 고정한다.

1. `event_id` 형식 → `bad_event_id`
2. `routing_key` 형식 → `bad_routing_key`
3. `profile` 미지원 → `unknown_profile`
4. `schema_version` 미지원 → `unsupported_version`
5. `event_time` 범위 [now − `GATE_MAX_PAST`(기본 24h), now + `GATE_MAX_FUTURE`(기본 5m)] → `bad_event_time`
6. payload 디코드 실패 → `bad_payload`
7. 필드 범위 위반 → `out_of_range`
8. 배치 안 `event_id` 중복 → `duplicate_in_batch`(두 번째부터)
9. attrs 한도 초과 → `bad_attrs`

중복 판정(8번)은 형식 검사(1번)를 통과한 모든 event_id를 기록한다. 앞선 같은 event_id가 다른 사유로 거부됐더라도 두 번째부터는 `duplicate_in_batch`다. event_time 비교는 ms 단위 정수로 경계 포함 `[now − max_past, now + max_future]`.

배치 수준 오류(이벤트 단위 아님):

| 조건 | 응답 |
|---|---|
| 본문 > `INGEST_MAX_BODY_BYTES`(기본 16MiB) 또는 압축 해제 > `INGEST_MAX_DECOMPRESSED_BYTES`(기본 64MiB) | 413 |
| 디코드 불가(zstd 본문은 정확히 frame 1개여야 함: 잘린 frame, checksum 오류, 뒤에 붙은 데이터나 두 번째 frame은 400), 이벤트 0개 또는 10000개 초과, batch_id 형식 위반 | 400 |
| 동시 처리 중 요청 > `INGEST_MAX_INFLIGHT`(기본 64) | 429 + `Retry-After: 1` |
| 저장소 거절(JetStream 용량 초과, NATS/PG 불가) | 503 + `Retry-After: 2` |

JSON 본문(`application/json`)은 `EventBatch`의 표준 ProtoJSON이다. lowerCamelCase와 snake_case 이름을 모두 받고, int64는 숫자나 문자열, bytes(`payload`)는 base64(profile 메시지의 protobuf 인코딩)다. 모르는 필드와 형식 오류는 배치 400. JSON에도 `Content-Encoding: zstd`를 쓸 수 있다. 한 객체에 같은 필드를 두 표기(camelCase와 snake_case)로 함께 쓰면 400이다. 202 본문의 `duplicate`는 항상 포함한다(필수).

정상 응답: `202 {"batch_id", "accepted", "rejected", "reasons": {"<reason>": n}, "duplicate": bool}`. 이미 처리한 batch_id면 `duplicate: true`, `accepted: 0`, `rejected: 0`(jetstream은 PubAck의 duplicate 표시, direct-db는 `processed_batches` 충돌로 판정). 전부 거부돼도 202이고 `accepted: 0`이다. 같은 `batch_id` 재전송도 202다(JetStream dedup 또는 DB `ON CONFLICT DO NOTHING`).

시험 사례는 `contracts/testdata/gate_cases/`의 입력과 기대 결과다. Go와 Python이 같은 결과를 내야 한다.

### 5.3 지시 API (①, `contracts/openapi/instruction-api.yaml`)

- `POST /v1/instructions` 본문: `instruction_id`, `target_reduction_kw`(> 0), `valid_from`, `valid_until`(RFC 3339, `valid_until > valid_from`), `routing_keys`(선택, 없으면 전체 solar 자산).
- 서명 헤더: `X-Jammy-Key-Id`, `X-Jammy-Timestamp`(unix 초), `X-Jammy-Signature` = hex(HMAC-SHA256(key, `timestamp + "\n" + method + "\n" + path + "\n" + hex(sha256(body))`)). 허용 시각 차이 ±300초. 키는 `HMAC_KEYS_FILE`(JSON `{key_id: secret}`)에서 읽는다.
- 응답: 신규 202, 같은 id·같은 본문 200(기존 상태), 같은 id·다른 본문 409, 서명 오류 401, 본문 오류 422.
- POST 202/200 본문: `{instruction_id, status, received_at}`.
- `GET /v1/instructions/{id}` 200: `{instruction_id, status, received_at, target_reduction_kw, valid_from, valid_until, commands: [{command_id, asset_id, reduction_kw, status, generation, epoch, sent_at, acked_at}]}`. status는 `accepted|dispatching|dispatched|partially_dispatched|expired`. 없는 id는 404.
- `numeric(14,3)` 수량은 JSON에서 소수 3자리 문자열(예: `"12.500"`)로 표현한다.
- 오류 본문(①과 ④의 모든 4xx·5xx): RFC 9457 `application/problem+json` `{type, title, status, detail}`. `type = https://jammy.dev/problems/<code>`, code ∈ {bad_signature, stale_timestamp, unknown_key, invalid_body, conflict, not_found, payload_too_large, bad_request, too_many_requests, unavailable}. OpenAPI 공통 component 하나를 두 파일이 같이 쓴다. ④의 202 본문은 이 형식이 아니다.

### 5.4 명령 (② → plantsim)

`POST {PLANT_GATEWAY_URL}/v1/commands`, 본문 `command_id, instruction_id, asset_id, reduction_kw, generation, epoch, valid_until`. 응답 200 `acked`, 409 `stale_epoch`. 제한 시간 `COMMAND_TIMEOUT_SECONDS`(기본 10) 안에 응답이 없으면 `timeout`.

배분(최대 잉여 방식, 정수 milli-kW로 계산):
- `c_i = floor(power_i × 1000)`. `power_i`는 대상 solar 자산의 `latest_state.primary_value`. 행이 없거나 음수이거나 `event_time`이 `ALLOCATION_STATE_MAX_AGE`(기본 120초)보다 오래되면 0.
- `T` = 목표(milli-kW). `T ≥ Σc`이면 각 자산에 `c_i`를 주고, `T = Σc`이면 `dispatched`, 아니면 `partially_dispatched`. `Σc = 0`이면 모두 0, `partially_dispatched`.
- `T < Σc`이면 `base_i = floor(T·c_i / Σc)`, 나머지 `r_i = T·c_i mod Σc`, `R = T − Σbase`. `r_i`가 큰 자산 R개에 1씩 더한다(동률은 `c_i`가 큰 쪽, 그다음 `asset_id` 오름차순). 상태 `dispatched`. 이 방식은 `c_i`를 넘거나 음수가 되지 않는다.
- 배분량 0인 자산은 시험 데이터에서 `"0.000"`으로 표시하고 명령 행을 만들지 않는다.

명령·지시 상태 규칙:
- 명령 `command_id`(uuid)는 처음 insert할 때 정하고 leader가 바뀌어도 그대로 다시 보낸다. plantsim은 같은 `command_id` 재수신에 같은 응답을 준다.
- `timeout`, `stale_epoch`, `error`는 최종 상태이고 재시도하지 않는다. 하나라도 있으면 지시는 `partially_dispatched`.
- SQS 메시지 본문은 POST 본문과 같은 JSON이다. insert가 commit됐거나 같은 id·같은 본문의 중복이면 삭제한다. 같은 id·다른 본문이나 형식 오류는 삭제하지 않고 metric(`jammy_dispatcher_sqs_total{result}`)만 올린다. 반복 실패는 SQS redrive 정책으로 DLQ로 간다(infra 설정).
- 이행 확인: 기준 = `valid_from` 직전의 마지막 완전한 1분 bucket 평균(sum/count), 관측 = `valid_from + COMPLIANCE_GRACE_SECONDS` 이후 시작하는 첫 완전한 1분 bucket 평균(유효 기간 안). `observed_reduction_kw = max(0, 기준 − 관측)`. `observed ≥ expected × (1 − COMPLIANCE_TOLERANCE)`(기본 0.05)이면 `met`, 아니면 `not_met`, bucket이 없으면 `no_data`.

### 5.5 lease (②, `LEASE_MODE=dynamodb|disabled`)

- 테이블 `LEASE_TABLE`, 키 `lease_name = "active-site"`, 속성 `holder, epoch, expires_at`.
- lease의 `holder`는 인스턴스가 아니라 거점(`SITE_ID`)이다. 같은 거점의 어느 dispatcher든 leader가 되면 같은 lease를 이어 갱신한다(epoch 그대로). leader가 5초마다 조건부 갱신(`holder = SITE_ID AND epoch = 현재 epoch`), TTL 15초. dispatcher-1·2는 같은 거점(A)의 leader 후보다(v3.12: 본체 leader, A1 대기).
- 만료된 lease 획득은 `LEASE_AUTO_ACQUIRE=true`(거점 A)일 때만 자동이다. 거점 B는 `dispatcher lease takeover --confirm`으로 사람이 실행하고, epoch +1이 된다. 수동 인수도 lease가 없거나 만료됐을 때만 성공한다.
- lease를 잃으면 새 명령 전송과 SQS 수거를 멈춘다. `disabled`이면 항상 active, epoch 0.

### 5.6 DB 스키마 (`contracts/db/migrations/`, schema `jammy`)

| 테이블 | 키·분할 | 쓰는 쪽 |
|---|---|---|
| `assets(asset_id, tenant_id, profile, capacity_kw)` | PK asset_id | seed |
| `raw_events(event_id, routing_key, profile, schema_version, event_time, received_at, ingest_id, primary_value, payload)` | `received_at` 시간 단위 range 파티션, PK(event_id, received_at) | ④ direct-db |
| `quarantine(id, received_at, ingest_id, batch_id, reason, raw)` | PK id | ④ direct-db |
| `processed_batches(consumer, batch_id, processed_at)` | PK(consumer, batch_id). 각 소유자가 자기 consumer 행 중 1일 지난 것을 10분마다 정리 | ④ direct-db(`ingest`), ⑤ |
| `consumer_cursor(consumer, last_xid xid8, last_event_id)` | PK consumer. `raw_events`에는 `ingest_xid xid8` 열과 파티션별 `(ingest_xid, event_id)` 인덱스(migration 0002) | ⑤ direct-db |
| `latest_state(routing_key, profile, primary_value, event_time, updated_at)` | PK routing_key | ⑤ |
| `summary_1m(profile, routing_key, minute, count, sum, min, max)` | `minute` 일 단위 range 파티션, PK(profile, routing_key, minute) | ⑤ |
| `incidents(id, routing_key, profile, kind, window_start, window_end, details)` | UNIQUE(routing_key, kind, window_start) | ⑤ |
| `instructions(instruction_id, body, body_sha256, target_reduction_kw, valid_from, valid_until, status, source, received_at)` | PK instruction_id | ①, ② |
| `commands(command_id, instruction_id, asset_id, reduction_kw, generation, epoch, status, sent_at, acked_at)` | UNIQUE(instruction_id, asset_id) | ② |
| `leader_generation(name, generation, holder, acquired_at)` | PK name | ② |
| `compliance(instruction_id, asset_id, expected_kw, observed_reduction_kw, verdict, checked_at)` | PK(instruction_id, asset_id) | ② |
| `archive_exports(partition_name, s3_key, rows, sha256, exported_at, dropped_at)` | PK partition_name | ⑥ direct-db |

- DDL 규칙: ID·label은 `text`(`commands.command_id`만 `uuid`, `quarantine.id`·`incidents.id`는 `bigint GENERATED BY DEFAULT AS IDENTITY`), 시각은 `timestamptz`, generation·epoch·count·rows는 `bigint`, `schema_version`은 `integer`, 측정값과 요약 집계는 `double precision`, 제어 수량(`capacity_kw`, `target_reduction_kw`, `reduction_kw`, `expected_kw`, `observed_reduction_kw`)은 `numeric(14,3)`, payload·raw·지시 원문은 `bytea`, hash는 소문자 hex `text`, incident details는 `jsonb`.
- NULL 허용은 `commands.sent_at`, `commands.acked_at`, `archive_exports.dropped_at`뿐이다. `DEFAULT now()`는 `received_at, processed_at, updated_at, checked_at, exported_at, acquired_at`에만 둔다.
- CHECK: `instructions.status` ∈ {accepted, dispatching, dispatched, partially_dispatched, expired}, `instructions.source` ∈ {https, sqs}, `commands.status` ∈ {pending, acked, stale_epoch, timeout, error}, `incidents.kind` ∈ {drop, over_capacity}, `compliance.verdict` ∈ {met, not_met, no_data}, `profile` ∈ {solar, synth-small, synth-large}.
- FK는 `commands.instruction_id`, `compliance.instruction_id` → `instructions`만 둔다. 측정 테이블은 `assets`에 의존하지 않는다.
- 파티션 생성: `jammy.ensure_partitions(ahead interval, behind interval DEFAULT '24 hours') RETURNS integer`(만든 파티션 수). `raw_events_YYYYMMDDHH`(UTC 시간 단위)를 현재 시간부터 `now()+ahead`가 속한 시간까지, `summary_1m_YYYYMMDD`(UTC 일 단위)를 `[now()-behind, now()+ahead]`의 모든 날짜에 만든다. half-open 범위, `pg_advisory_xact_lock` 아래 멱등, DEFAULT 파티션 없음(범위 밖 insert는 실패해야 함). migration이 `('3 hours', '24 hours')`로 한 번 호출하고, ④⑤는 시작할 때와 10분마다 `behind = GATE_MAX_PAST`로 호출한다.
- 요약 보관 90일 이후 DROP(v8 값)은 범위 밖이다. 운영 정리 Job은 후속.
- DB 역할과 권한 생성은 infra 담당이다. migrate는 소유자 계정으로 실행한다.

### 5.7 이상 감지 (⑤)

- 5분 창 평균 대비 마지막 1분 평균이 `ANOMALY_DROP_RATIO`(기본 0.5) 이하로 떨어지면 `kind=drop`.
- solar에서 `power_kw > capacity_kw × 1.1`이면 `kind=over_capacity`(⑤가 `assets`를 60초마다 캐시).

### 5.8 ⑨ 인계 계약 (`contracts/BATCH_HANDOFF.md`)

- 경로: `s3://<bucket>/raw/profile=<p>/dt=YYYY-MM-DD/hour=HH/<ingest_id>-<first_seq>-<last_seq>.parquet` (`event_time` UTC 기준).
- Parquet 열: `event_id string, routing_key string, profile string, schema_version int32, event_time timestamp(ms, UTC), received_at timestamp(ms, UTC), ingest_id string, primary_value double, payload binary`. 압축 zstd, 레벨은 `ARCHIVE_ZSTD_LEVEL`(기본 1, 실험에서 1과 3을 비교).
- 파일마다 `<같은 이름>.manifest.json`: `rows, sha256(소문자 hex), min_event_time, max_event_time(UTC RFC 3339 ms), source("jetstream"|"direct-db"), stream, first_seq, last_seq, partition_name, schema_version`. jetstream은 `partition_name`이 null, direct-db는 `stream, first_seq, last_seq`가 null.
- 봉인 파일 본문: `{files: [버킷 기준 key], rows: 합계, watermark, sealed_at}`(시각은 UTC RFC 3339 ms). watermark는 jetstream이면 그 인스턴스가 처리한 `event_time` 최대값, direct-db면 빈틈없이 이관을 마친 마지막 `received_at` 시간의 끝 시각이다.
- manifest의 `schema_version`은 이벤트 schema_version이다. 파일 하나에는 한 schema_version만 담는다(다르면 파일을 나눈다). manifest 형식 버전은 별도 필드 `manifest_version: 1`.
- direct-db: `raw_events` 파티션은 `received_at` 기준이므로, 파티션 하나를 (profile, event_time 시간)마다 파일 하나로 나눠 `raw/profile=<p>/dt=YYYY-MM-DD/hour=HH/db-<partition_name>.parquet`에 쓴다. 이벤트 시간 H는 `received_at` 시간이 `H + ceil(SEAL_LATENESS)` 이하인 파티션이 모두 이관되면 `_SEALED-db.json`으로 봉인한다. 이미 봉인된 시간의 행이 나중에 이관되면 `raw-late/`에 같은 이름 규칙으로 쓴다. 파티션 단위 manifest는 `exports/direct-db/<partition_name>.json`(`raw/` 밖, B1 입력 아님)에 `{partition_name, files: [{key, rows, sha256}], rows, exported_at}`로 쓰고, `archive_exports`의 `s3_key`·`sha256`·`rows`는 이 manifest를 가리킨다. 목록의 모든 파일이 S3 행 수와 sha256 검증을 통과한 뒤에만 DROP한다.
- 봉인: 한 인스턴스가 처리한 `event_time` 최대값(watermark) − `SEAL_LATENESS`(기본 10m)가 그 시간 끝을 넘으면 `_SEALED-<ingest_id>.json`을 쓴다. 내용은 해당 prefix의 파일 목록과 행 수 합계다. 봉인 이후 도착한 이벤트는 `raw-late/` 아래에 같은 규칙으로 쓴다.
- B1은 봉인 파일만 입력으로 쓴다.

### 5.9 공통 env (`contracts/ENV.md`)

| env | 기본 | 설명 |
|---|---|---|
| `SITE_ID` | 필수 | 예: `site-a` |
| `INSTANCE_ID` | hostname | metric·로그·ingest_id |
| `HTTP_PORT` / `ADMIN_PORT` | 8080 / 9090 | |
| `LOG_LEVEL` | `info` | JSON 로그 |
| `SHUTDOWN_TIMEOUT_SECONDS` | 25 | SIGTERM 후 drain 한도. k8s `terminationGracePeriodSeconds`는 30 |
| `PIPELINE_MODE` | `jetstream` | ④⑤⑥ |
| `DATABASE_URL` 또는 `DATABASE_URL_FILE` | — | `_FILE`이 있으면 우선 |
| `DB_POOL_MIN` / `DB_POOL_MAX` | 1 / 10 | 컨테이너당 PG 연결 수. 기동 시 DB를 기다리지 않음. Python 다중 워커는 `DB_POOL_MAX`를 워커 수로 나눠 씀(올림, 최소 1). Go·Py 동등성 비교의 기준 값. site A DB 연결 예산은 k8s 담당 문서 기준으로 values에서 조정 |
| `NATS_URL`, `NATS_CREDS_FILE` | — | |
| `INGEST_STREAM`, `NATS_STREAMS` | — | ④ / ⑤⑥ |
| `S3_BUCKET`, `S3_ENDPOINT`, `S3_REGION`, `S3_FORCE_PATH_STYLE` | —, AWS, `ap-northeast-2`, `false` | 자격 증명은 AWS SDK 표준 체인 |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | 비면 trace·log export 끔 | |
| `OTEL_RESOURCE_ATTRIBUTES` | chart가 설정 | `service.version`, `deployment.environment` |

서비스별 env는 각 서비스 README와 `ENV.md`의 서비스 절에 둔다. `ENV.md`가 유일한 원본이다.

env 값 문법(Go·Py 공통): 이름이 `_SECONDS`로 끝나는 env는 단위 없는 정수 초(`^[0-9]+$`, 예: `25`). 그 밖의 시간은 `^[0-9]+(s|m|h)$`(예: `120s`, `45m`, `24h`), 크기는 `^[0-9]+(KiB|MiB|GiB)?$`(접미사 없으면 bytes, 1024 단위), 목록(`NATS_STREAMS` 등)은 쉼표 구분이고 공백 없음, 불리언은 `true|false`. 형식이 틀리면 기동 시 실패한다.

### 5.10 관측 (`contracts/METRICS.md`)

- HTTP: OTel semantic conventions `http.server.request.duration`(Prometheus 이름 `http_server_request_duration_seconds`). bucket: `0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10`.
- 커스텀 metric 이름은 `jammy_<service>_<name>`. Go와 Python의 이름·label·bucket이 같다.
- 필수 metric: ④ `jammy_ingest_events_total{result,reason}`, `jammy_ingest_bytes_total{kind=wire|decompressed}`, `jammy_ingest_store_duration_seconds`. ⑤ `jammy_processor_batches_total{result}`, `jammy_processor_commit_duration_seconds`, `jammy_processor_consumer_pending`, `jammy_processor_lag_seconds`. ⑥ `jammy_archiver_files_total`, `jammy_archiver_upload_bytes_total`, `jammy_archiver_upload_duration_seconds`, `jammy_archiver_unacked_messages`. ① `jammy_instruction_requests_total{result}`. ② `jammy_dispatcher_is_leader`, `jammy_dispatcher_lease_active`, `jammy_dispatcher_commands_total{result}`, `jammy_dispatcher_dispatch_duration_seconds`. ⑧ `jammy_portal_publish_total{result}`.
- label 허용: `service, instance, site, result, reason, kind, profile`. `event_id`, `instruction_id`, `asset_id`, `batch_id`는 label 금지. trace attribute와 로그에만 쓴다.
- 로그: JSON 한 줄, 필드 `ts, level, msg, service, instance, site, trace_id, span_id`.

## 5.11 Go·Python 구현 동등성 규칙 (Claude 판정 2026-11-23, parity 검토 결과)

언어·런타임 자체의 차이(프로세스 모델, GC, 라이브러리 구현)는 남기고, 그 밖의 차이는 없앤다. 아래가 두 구현의 공통 기준이다.

공통:
- env 검증: 포트 1~65535, `INSTANCE_ID` 비어 있으면 안 됨(기본 hostname), 개수·한도 env는 1 이상, 비율 env는 (0, 1]. 이름이 비면 안 되는 env(`CONSUMER_NAME` 등)는 빈 값 거부.
- Python 다중 워커(`WEB_CONCURRENCY = W`)의 컨테이너 전체 한도는 Go 단일 프로세스와 정확히 같아야 한다: `DB_POOL_MAX`를 W개로 나눌 때 나머지는 앞 워커부터 1씩 더해 합계가 정확히 `DB_POOL_MAX`가 되게 하고, `DB_POOL_MIN`도 같은 방식으로 나눈다(워커당 0 허용). `INGEST_MAX_INFLIGHT`는 컨테이너 전체 공유. 실험 values는 `WEB_CONCURRENCY`를 명시한다.
- trace: HTTP 요청마다 server span, 모든 워커 프로세스에서 tracer 초기화. 로그의 trace_id·span_id 채움.
- 오류 뒤 재시도 대기 1초, 지수 증가 없음. readiness는 PG·NATS를 실제로 ping해서 판단.
- JetStream ACK는 비동기 `ack()`(commit 뒤). 중복은 `processed_batches`가 막는다.

④ ingest:
- DB 저장은 binary COPY(Go `CopyFrom`, Python psycopg binary COPY).
- readiness 확인은 본문을 읽기 전에 한다(의존성이 없으면 본문을 받지 않고 503).
- 준비 단계 SQL(`ensure_partitions`, 정리)은 1초 제한, 저장 시 DB 연결 대기 2초, JetStream publish 5초, HTTP header 수신 5초.

⑤ processor:
- stream마다 소비 루프 1개(Go goroutine, Python asyncio task). fetch 대기 1초, 빈 결과면 0.2초 대기.
- ⑤는 Contract Gate를 다시 돌리지 않는다(④가 이미 검증). 메시지 header의 `Nats-Msg-Id`가 본문 `batch_id`와 같은지만 확인하고, 다르면 오류로 센다. 디코드 상한은 `INGEST_MAX_DECOMPRESSED_BYTES`와 같은 64MiB.
- 한 tx에서 `processed_batches`를 넣을 때는 `batch_id` 정렬 순서로 넣는다(lock 순서 고정).
- `latest_state`는 tx마다 쓰지 않고 메모리에 모았다가 `STATE_FLUSH_SECONDS`마다 한 번 upsert한다.
- drop 이상 감지는 `unnest` 단일 쿼리. incident details 키는 `{last_mean, window_mean, ratio}`.
- over_capacity는 정수 milli-kW로 계산(`power_mkw × 10 > capacity_mkw × 11`). `assets` 캐시는 60초마다 백그라운드 갱신.
- `jammy_processor_batches_total{result="error"}`는 실패한 메시지(배치) 수만큼 센다. `jammy_processor_lag_seconds`는 마지막으로 처리한 행·메시지의 `received_at` 기준이고, 처리할 것이 없으면 0.
- consumer는 `FilterSubject = jammy.raw.<stream>`을 설정한다.

⑥ archiver도 같은 공통 규칙과 stream별 루프·비동기 ACK·재시도 대기 규칙을 따른다.

## 6. 이미지·배포

- 이미지: `ghcr.io/cloud4-final/jammy-application/<service>:<version>-<sha7>`, 릴리스 커밋은 `:<version>`도. `latest` 금지. 버전 원본은 git tag `vX.Y.Z`, 아직 tag가 없으면 `0.1.0`.
- Go: distroless static, non-root(65532). Python: `python:3.14-slim` 계열 multi-stage(정확한 태그는 `docs/backend/VERSIONS.md`), non-root, `uv`로 잠근 의존성. 공통: read-only rootfs 가능, `HEALTHCHECK` 없음(k8s probe 사용), OCI label(`org.opencontainers.image.source`, `revision`, `version`).
- CI: lint, test, 계약 시험, multi-arch 빌드, Trivy(HIGH·CRITICAL 실패), SBOM(SPDX), cosign keyless 서명. main 머지 때만 push.
- Helm chart `deploy/helm/jammy-platform`: 서비스별 `enabled`, ④⑤⑥은 `impl: go|py`로 이미지를 고른다. ④는 `ingest.instances` 목록(이름, nodeSelector, stream)마다 Deployment와 Service를 하나씩 만든다. 공통: requests·limits, probe, PDB, `securityContext`(runAsNonRoot, readOnlyRootFilesystem, drop ALL), NetworkPolicy, ServiceMonitor는 쓰지 않는다(VictoriaMetrics는 Alloy scrape 사용, pod annotation `jammy.io/scrape: "true"`, `jammy.io/port: "9090"`).
- `values-mvp.yaml`: 모든 pod를 `role=main` 노드에, `PIPELINE_MODE=direct-db`, 각 1 replica.
- `values-split.yaml`: ①②는 `role=control`(taint 허용), ②는 2 replica(anti-affinity), ④는 A1·A2 인스턴스 2개(`raw-a1`, `raw-a2`), ⑤⑥⑧은 `role=ingest`, `PIPELINE_MODE=jetstream`.

## 7. 로컬 스택 (`deploy/compose/`)

- profile `mvp`: PG 1대, 앱, plantsim, SeaweedFS, ElasticMQ, dynamodb-local.
- profile `split`: PG primary + 동기 standby(`synchronous_standby_names = 'ANY 1 (standby1)'`), NATS JetStream, ingest 2개, 앱.
- profile `obs`: VictoriaMetrics, Grafana 13.2.5, Alloy, Loki 3.7.7, Tempo.
- `IMPL=go|py`로 ④⑤⑥ 구현을 고른다.

## 8. 범위 밖

- Patroni, PG HA, NATS 클러스터 설치(infra 담당), Airflow DAG(Airflow 담당), 사업자 포털 웹 UI, CloudFront 설정, quarantine의 S3 보관, 요약 보관 기한 정리 Job, mTLS, Java·Rust 구현.

## 9. 열린 질문

- `SITE_ID` 값 형식: 앱은 검증 없이 문자열로 받는다. chart 기본값은 없고, `values-mvp.yaml`·`values-split.yaml`은 k8s 담당의 `k8s/sites/a/README.md`에 맞춰 `jammy-a`로 둔다(Claude 판정 2026-11-23, 팀 합의 시 변경).
- 로컬 AWS 대체품을 LocalStack에서 SeaweedFS·ElasticMQ·dynamodb-local로 바꾼 것에 대한 사용자 확인.
- 30MB/s에서 JetStream 60GiB 상한과 실제 보관 시간: 측정해 공개한다.
