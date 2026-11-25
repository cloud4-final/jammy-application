# JAMMY 백엔드 기술 기본기 체크리스트

JAMMY 백엔드 코드·인프라·CI 리뷰 시 프로덕션 장애(리소스 누수, 무제한 큐/메모리, 타임아웃 누락, 멱등성·펜싱 결함, 공급망 취약점)를 사전에 차단하기 위한 실무 체크리스트다. 채택 버전과 예외 사항은 [VERSIONS.md](./VERSIONS.md) 및 `go.mod`, `pyproject.toml`, `.github/workflows/backend.yml`을 기준으로 맞췄다.

## 1. Go 1.27 (`go1.27.4`)

- [ ] **`http.Server` 타임아웃과 요청 본문 상한 강제**: `ReadHeaderTimeout`을 반드시 설정하고, 핸들러에서 `http.MaxBytesReader` 또는 크기 제한으로 본문을 읽어 Slowloris와 메모리 고갈을 막는다.
  - 이유: 기본 `http.Server`는 헤더/본문 타임아웃이 없어 느린 연결이나 대용량 페이로드로 고루틴과 힙이 고갈된다.
  - 검증: `rg -n "ReadHeaderTimeout|MaxBytesReader|INGEST_MAX_BODY_BYTES" apps/ libs/go/` (`apps/ingest-go/main.go`, `libs/go/admin/admin.go` 참조).
  - 출처: [Go `net/http.Server`](https://pkg.go.dev/net/http#Server)
- [ ] **`context.Context` 전파와 정리 작업 분리**: 모든 I/O·DB·HTTP 호출 첫 인자로 `ctx`를 전달하고, 취소된 요청의 사후 정리(`AbortMultipartUpload`, telemetry `Shutdown`)는 `context.WithoutCancel(ctx)` + 별도 timeout으로 보호한다.
  - 이유: 종료 시그널이나 요청 취소가 전파되지 않으면 고루틴이 영구 대기하고, 반대로 취소된 `ctx`를 쓰면 S3 멀티파트 중단이 실패해 미완성 파트가 남는다.
  - 검증: `rg -n "context\.Background\(\)|context\.WithoutCancel|signal\.NotifyContext" apps/ libs/go/` (`apps/archiver-go/internal/pq/storage.go`, `libs/go/lifecycle/lifecycle.go`).
  - 출처: [Go `context.WithoutCancel`](https://pkg.go.dev/context#WithoutCancel)
- [ ] **동시성 상한과 오류 전파(`errgroup` / 세마포어)**: 무제한 `go func()` 생성을 금지하고 `errgroup.WithContext`·`SetLimit` 또는 `INGEST_MAX_INFLIGHT` 세마포어로 동시 실행 수를 제한한다.
  - 이유: 스파이크 트래픽이나 백엔드 지연 시 고루틴이 무한 증식해 OOMKilled로 이어진다.
  - 검증: `rg -n "errgroup|SetLimit|Inflight|UploadSlot" apps/ libs/go/`.
  - 출처: [Go `errgroup`](https://pkg.go.dev/golang.org/x/sync/errgroup)
- [ ] **`pgx/v5` (`v5.9.2`) 단일 풀과 트랜잭션·커서 정리**: 프로세스당 `pgxpool.Pool` 1개(`MinConns`/`MaxConns` 명시)를 열고, `Begin` 뒤 `defer tx.Rollback(ctx)`, `Query` 뒤 `defer rows.Close()`를 누락하지 않는다.
  - 이유: 풀 미제한이나 `rows.Close()` 누락은 DB 커넥션 고갈과 풀 데드락을 일으킨다.
  - 검증: `rg -n "pgxpool\.NewWithConfig|Rollback\(|rows\.Close\(\)" apps/ libs/go/dbx/` (`libs/go/dbx/dbx.go`).
  - 출처: [pgxpool Docs](https://pkg.go.dev/github.com/jackc/pgx/v5/pgxpool)
- [ ] **`nats.go` (`v1.53.1`) 재연결과 JetStream Pull 타임아웃**: `nats.RetryOnFailedConnect(true)`와 `nats.MaxReconnects(-1)`로 초기·일시 단절에 대비하고, `consumer.Fetch(..., jetstream.FetchMaxWait(...))`로 블로킹을 제한한다.
  - 이유: 기동 시 NATS 미준비로 프로세스가 crash-loop에 빠지거나 `Fetch`가 종료 시그널을 무시하고 멈추는 문제를 막는다.
  - 검증: `rg -n "RetryOnFailedConnect|MaxReconnects|FetchMaxWait" apps/ libs/go/natsx/` (`libs/go/natsx/natsx.go`).
  - 출처: [nats.go `jetstream`](https://pkg.go.dev/github.com/nats-io/nats.go/jetstream)
- [ ] **`parquet-go` (`v0.32.1`) 스키마·RowGroup·Close 순서**: UTC millisecond timestamp 태그, `MaxRowsPerRowGroup` 상한, `SkipPageBounds("payload")`를 지정하고, 업로드 전 `writer.Close()`로 footer 기록을 완료한다.
  - 이유: `Close()` 전 읽은 Parquet 파일은 magic/footer가 누락돼 손상되며, 대용량 binary 컬럼의 page bounds는 메모리와 메타데이터를 폭증시킨다.
  - 검증: `rg -n "NewGenericWriter|MaxRowsPerRowGroup|SkipPageBounds" apps/archiver-go/`.
  - 출처: [parquet-go Docs](https://pkg.go.dev/github.com/parquet-go/parquet-go)
- [ ] **`aws-sdk-go-v2` (`v1.47.8`) 자격 증명 체인과 S3 호환 옵션**: 정적 키 하드코딩 없이 `config.LoadDefaultConfig`를 쓰고, 로컬 S3 호환 스토리지(SeaweedFS)를 위해 `UsePathStyle`과 `RequestChecksumCalculationWhenRequired`를 설정한다.
  - 이유: AWS SDK v2 기본 체크섬·가상 호스팅 방식은 SeaweedFS 등 S3 에뮬레이터에서 서명 불일치 오류를 유발한다.
  - 검증: `rg -n "LoadDefaultConfig|UsePathStyle|RequestChecksumCalculation" apps/ libs/go/s3x/` (`libs/go/s3x/s3x.go`, `apps/archiver-go/internal/pq/storage.go`).
  - 출처: [AWS SDK for Go v2](https://docs.aws.amazon.com/sdk-for-go/v2/developer-guide/welcome.html)
- [ ] **`klauspost/compress/zstd` (`v1.20.5`) 압축 해제 폭탄 방어**: `WithDecoderConcurrency(1)`, `WithDecoderLowmem(true)`, `WithDecoderMaxMemory`, `io.LimitReader`로 해제 크기를 제한하고 단일 프레임 검증과 `defer decoder.Close()`를 수행한다.
  - 이유: 작은 악성 zstd 프레임(zip bomb)이나 다중 프레임 트레일러가 워커 메모리를 즉시 고갈시킬 수 있다.
  - 검증: `rg -n "WithDecoderMaxMemory|WithDecoderLowmem|LimitReader|singleFrame" libs/go/codec/` (`libs/go/codec/zstd.go`).
  - 출처: [klauspost zstd](https://pkg.go.dev/github.com/klauspost/compress/zstd)
- [ ] **OpenTelemetry Go (`v1.47.0` / `v0.69.0`) 뷰 필터와 종료 플러시**: `sdkmetric.WithView`로 허용 라벨만 남기고 `AlwaysOffFilter`로 exemplar 라벨 누출을 막으며, 종료 시 `metrics`/`traces`/`logs.Shutdown(ctx)`를 호출한다.
  - 이유: 미등록 라벨과 exemplar는 시계열 카디널리티를 폭발시키고, `Shutdown` 누락은 종료 직전 스팬·로그를 유실시킨다.
  - 검증: `rg -n "WithView|AlwaysOffFilter|Shutdown\(" libs/go/obs/` (`libs/go/obs/obs.go`).
  - 출처: [OpenTelemetry Go](https://opentelemetry.io/docs/languages/go/instrumentation/)

## 2. Python 3.14 (`3.14.10`)

- [ ] **FastAPI (`0.141.1`) / uvicorn (`0.52.4`) 워커와 DB 풀 분배**: 멀티워커 시 `WEB_CONCURRENCY <= DB_POOL_MAX`를 검증해 파드 전체 연결 상한을 워커 수로 분배하고, 백엔드 준비·정리는 `lifespan`에서 관리한다.
  - 이유: 워커마다 `DB_POOL_MAX`를 통째로 열면 PostgreSQL `max_connections`가 워커 수 배수로 초과된다.
  - 검증: `rg -n "WEB_CONCURRENCY|worker_pool_limits|lifespan" apps/ libs/py/` (`libs/py/jammy_common/db.py`, `apps/ingest-py/src/ingest/main.py`).
  - 출처: [FastAPI Server Workers](https://fastapi.tiangolo.com/deployment/server-workers/)
- [ ] **`asyncio` 블로킹 호출 격리와 타임아웃 보호**: 동기 SDK(`boto3`, `pyarrow`, 파일 I/O)는 `asyncio.to_thread`로 오프로드하고 `asyncio.timeout`을 적용하되, 락 보호 작업이 타임아웃에 취소돼도 상태가 꼬이지 않게 `asyncio.shield`와 done callback을 쓴다.
  - 이유: 이벤트 루프 블로킹은 `/livez`·`/readyz` 응답까지 멈추게 해 불필요한 파드 재시작을 유발한다.
  - 검증: `rg -n "asyncio\.to_thread|asyncio\.timeout|asyncio\.shield" apps/ libs/py/` (`apps/dispatcher/src/dispatcher/lease.py`).
  - 출처: [Python 3.14 `asyncio`](https://docs.python.org/3.14/library/asyncio-task.html)
- [ ] **`psycopg` (`3.3.9`) + `psycopg-pool` (`3.3.3`) 비차단 풀과 헬스 타임아웃**: `AsyncConnectionPool(..., open=False)` 뒤 `await pool.open(wait=False)`로 기동 차단을 피하고, readiness `ping`은 `SET LOCAL statement_timeout`을 트랜잭션 안에 둔다.
  - 이유: DB 일시 장애 시 기동이 무한 대기하거나 readiness 검사가 오래 걸려 kubelet 타임아웃이 누적되는 현상을 막는다.
  - 검증: `rg -n "AsyncConnectionPool|open\(wait=False\)|statement_timeout" libs/py/jammy_common/db.py apps/`.
  - 출처: [psycopg 3 Pool](https://www.psycopg.org/psycopg3/docs/advanced/pool.html)
- [ ] **`nats-py` (`2.13.1`) 연결 정리와 `flush()` 취소 회피**: 연결 실패/취소 시 `is_connecting`/`is_connected`/`is_reconnecting`을 확인한 뒤 `close()`하고, PONG 오류 이슈(`#1030`) 회피를 위해 헬스 체크는 `flush()` 대신 `is_connected` + `stream_info`를 쓴다.
  - 이유: `nats-py 2.13.1`은 미연결 상태 `close()` 시 AttributeError가 나고, 타임아웃된 `flush()`는 후속 PONG Future 충돌을 남긴다.
  - 검증: `rg -n "is_connecting|stream_info|flush\(" libs/py/jammy_common/nats.py apps/`.
  - 출처: [nats.py Docs](https://nats-io.github.io/nats.py/)
- [ ] **`pyarrow` (`25.0.1`) 스키마 고정과 임시 파일 정리**: Parquet 쓰기 시 계약 스키마(`parquet_schema.json`)와 `timestamp("ms", tz="UTC")`, `compression="zstd"`를 강제하고, 스테이징 임시 파일은 업로드 완료·기동 시 정리한다.
  - 이유: 추론 스키마에 의존하면 빈 배치나 타입 차이로 Go/Python 아카이버 간 Parquet 컬럼 호환성이 깨진다.
  - 검증: `rg -n "pyarrow\.parquet|parquet_schema|compression" apps/archiver-py/ contracts/testdata/`.
  - 출처: [PyArrow Parquet](https://arrow.apache.org/docs/python/parquet.html)
- [ ] **`boto3` (`1.43.127`) 명시적 타임아웃과 재시도 제어**: `botocore.config.Config(connect_timeout=..., read_timeout=..., retries=...)`를 명시해 기본 긴 타임아웃·재시도가 워커 풀과 리스 갱신 주기를 넘기지 않게 한다.
  - 이유: 리스 TTL(15s)보다 긴 기본 boto3 타임아웃/재시도는 펜싱 실패 감지를 지연시켜 split-brain 위험을 키운다.
  - 검증: `rg -n "botocore\.config|Config\(|endpoint_url" apps/ libs/py/`.
  - 출처: [botocore Config](https://botocore.amazonaws.com/v1/documentation/api/latest/reference/config.html)
- [ ] **`compression.zstd` (Python 3.14 표준 라이브러리) 스트리밍 상한**: `ZstdDecompressor().decompress(data, max_length=...)` 루프로 해제 바이트를 `INGEST_MAX_DECOMPRESSED_BYTES` 이내로 제한하고, `decoder.eof` 뒤 `unused_data`가 남으면 400으로 거절한다.
  - 이유: 일괄 `decompress()`는 압축 폭탄 한 방에 프로세스를 OOM으로 죽이고, 다중 프레임 트레일러는 검증 우회를 허용한다.
  - 검증: `rg -n "compression\.zstd|ZstdDecompressor|unused_data" libs/py/jammy_common/codec.py apps/`.
  - 출처: [Python 3.14 `compression.zstd`](https://docs.python.org/3.14/library/compression.zstd.html)
- [ ] **OpenTelemetry Python (`1.45.2` / `0.66b2`) 뷰 격리와 `DropAggregation`**: `MeterProvider`에 기본 `View(instrument_name="*", aggregation=DropAggregation())`와 허용 라벨 뷰(`_FallbackView`)를 둬 외부 인스트루멘테이션의 고카디널리티 메트릭을 차단한다.
  - 이유: FastAPI/psycopg 자동 계측이 기본으로 내보내는 비허용 속성이 VictoriaMetrics 시계열 수를 급증시킬 수 있다.
  - 검증: `rg -n "DropAggregation|ExplicitBucketHistogramAggregation|BatchSpanProcessor" libs/py/jammy_common/obs.py`.
  - 출처: [OpenTelemetry Python](https://opentelemetry.io/docs/languages/python/instrumentation/)

## 3. PostgreSQL 17 (`17.12` / site `17.7`)

- [ ] **Range 파티셔닝 사전 생성과 부모 인덱스 상속**: `raw_events`(`received_at` 시간별)와 `summary_1m`(`minute` 일별) 파티션을 `jammy.ensure_partitions(ahead, behind)`로 선행 생성하고, 인덱스는 부모 테이블에 선언해 자동 전파되게 한다.
  - 이유: 미래/과거 허용 범위(`GATE_MAX_PAST`, `GATE_MAX_FUTURE`) 파티션이 없으면 `INSERT`가 즉시 실패한다.
  - 검증: `rg -n "PARTITION BY RANGE|ensure_partitions|CREATE INDEX" contracts/db/migrations/ apps/`.
  - 출처: [PostgreSQL 17 Partitioning](https://www.postgresql.org/docs/17/ddl-partitioning.html)
- [ ] **`xid8` 스냅샷 커서 기반 증분 읽기**: direct-db 소비 커서는 시계가 아니라 `pg_current_xact_id()` 기반 `ingest_xid`와 `event_id` 복합 커서(`(ingest_xid, event_id) > ($1, $2)`)를 사용한다.
  - 이유: 동시 트랜잭션이 커밋되는 순서와 `received_at` 타임스탬프 순서는 어긋날 수 있어 타임스탬프 커서는 행을 영구 누락한다.
  - 검증: `rg -n "xid8|pg_current_xact_id|ingest_xid|last_xid" contracts/db/migrations/ apps/`.
  - 출처: [PostgreSQL 17 Snapshot Functions](https://www.postgresql.org/docs/17/functions-info.html#FUNCTIONS-PG-SNAPSHOT)
- [ ] **대량 적재에 `COPY` 프로토콜 사용**: 원시 이벤트와 격리 행 적재는 단건 `INSERT` 루프 대신 `pgx.CopyFrom` / `psycopg` `copy()`를 단일 트랜잭션에서 사용한다.
  - 이유: 배치당 수천 건을 단건 `INSERT`로 왕복하면 네트워크·WAL 플러시 지연으로 인제스트 처리량이 급락한다.
  - 검증: `rg -n "CopyFrom|copy\(" apps/ingest-go/ apps/ingest-py/`.
  - 출처: [PostgreSQL 17 `COPY`](https://www.postgresql.org/docs/17/sql-copy.html)
- [ ] **트랜잭션 범위 Advisory Lock 사용**: 파티션 생성·단일 리더 조율처럼 세션 경계를 넘지 않아야 하는 잠금은 `pg_advisory_lock` 대신 커밋/롤백 시 자동 해제되는 `pg_advisory_xact_lock`을 쓴다.
  - 이유: 커넥션 풀 환경에서 세션 단위 advisory lock은 반환 후에도 남아 다른 요청을 영구 차단한다.
  - 검증: `rg -n "pg_advisory_xact_lock|pg_advisory_lock" contracts/db/ apps/`.
  - 출처: [PostgreSQL 17 Advisory Locks](https://www.postgresql.org/docs/17/explicit-locking.html#ADVISORY-LOCKS)
- [ ] **커넥션 풀 상한과 쿼리 타임아웃 관리**: 모든 파드의 `DB_POOL_MAX` 합계(× 워커/레플리카)가 DB `max_connections` 여유분 내에 들게 산정하고, 긴 조회·헬스체크에는 `statement_timeout`을 건다.
  - 이유: idle-in-transaction이나 풀 합계 초과는 신규 연결 거부와 운영자 접속 불가 장애를 초래한다.
  - 검증: `rg -n "DB_POOL_MAX|DB_POOL_MIN|statement_timeout" libs/ deploy/helm/ deploy/compose/`.
  - 출처: [PostgreSQL 17 Connection Settings](https://www.postgresql.org/docs/17/runtime-config-connection.html)
- [ ] **동기/비동기 복제 모드와 전용 복제 계정 분리**: 스탠바이 복제는 최소 권한 `REPLICATION` 전용 롤 + `scram-sha-256`으로 제한하고, `split` 모드처럼 RPO=0이 필요할 때만 `synchronous_standby_names`를 활성화하며 스탠바이 단절 시 쓰기 지연 영향을 인지한다.
  - 이유: 동기 복제는 스탠바이 다운 시 프라이머리 커밋을 블로킹할 수 있고, 슈퍼유저 복제 자격 증명은 탈취 시 피해를 극대화한다.
  - 검증: `rg -n "REPLICATION|scram-sha-256|JAMMY_SYNC_STANDBY|synchronous_standby" deploy/compose/pg/ Makefile`.
  - 출처: [PostgreSQL 17 Synchronous Replication](https://www.postgresql.org/docs/17/warm-standby.html#SYNCHRONOUS-REPLICATION)
- [ ] **멱등 클레임 테이블과 소유자별 TTL 정리**: `processed_batches(consumer, batch_id)`에 `ON CONFLICT DO NOTHING`을 같은 트랜잭션에 묶고, 만료 행 삭제는 각 `consumer`가 자기 행만 주기적으로 정리한다.
  - 이유: 커밋 후 별도 트랜잭션으로 클레임을 기록하면 크래시 시 중복 처리가 발생하고, 타 소유자 행 삭제는 경합·중복을 유발한다.
  - 검증: `rg -n "processed_batches|ON CONFLICT" contracts/db/ apps/`.
  - 출처: [PostgreSQL 17 `INSERT ... ON CONFLICT`](https://www.postgresql.org/docs/17/sql-insert.html)

## 4. NATS JetStream (`2.15.3`)

- [ ] **스트림 보존 상한과 `DiscardNew` 백프레셔**: 모든 스트림에 `FileStorage`, `MaxAge`(`JS_MAX_AGE`), `MaxBytes`(`JS_MAX_BYTES`), `DiscardNew`를 설정해 디스크 상한 초과 시 조용한 유실 대신 게시자에게 거절 오류를 반환한다.
  - 이유: 기본 `DiscardOld`나 무제한 스트림은 장애 중 데이터를 조용히 덮어쓰거나 노드의 디스크를 100% 채워 브로커를 멈춘다.
  - 검증: `rg -n "DiscardNew|MaxAge|MaxBytes|FileStorage" apps/ingest-go/ apps/ingest-py/ deploy/compose/nats/`.
  - 출처: [NATS JetStream Streams](https://docs.nats.io/nats-concepts/jetstream/streams)
- [ ] **중복 제거 윈도우(`Duplicates`)와 결정적 `MsgID`**: 스트림의 `Duplicates`(2분)와 퍼블리시 시 결정적 `Nats-Msg-Id`(`batch_id` 기반)를 함께 사용해 재시도 중복 저장을 막는다.
  - 이유: `Duplicates`가 설정돼도 퍼블리셔가 매번 새 ID를 만들거나 헤더를 빼면 중복 제거가 전혀 동작하지 않는다.
  - 검증: `rg -n "Duplicates|WithMsgID|Nats-Msg-Id|msg_id" apps/ingest-go/ apps/ingest-py/`.
  - 출처: [NATS JetStream Model](https://docs.nats.io/using-nats/developer/develop_jetstream/model_deep_dive)
- [ ] **내구성 Pull Consumer와 배치 Fetch**: 워커는 `Durable` 이름을 지정한 Pull Consumer로 `Fetch(FETCH_BATCH)`를 호출해 처리 속도에 맞춰 메시지를 당겨온다.
  - 이유: 무제한 Push 구독은 느린 소비자(DB/S3 지연)에게 메시지를 폭주시켜 클라이언트 메모리 OOM을 유발한다.
  - 검증: `rg -n "Durable|CreateOrUpdateConsumer|Fetch\(|pull_subscribe" apps/processor-go/ apps/processor-py/ apps/archiver-go/ apps/archiver-py/`.
  - 출처: [NATS JetStream Consumers](https://docs.nats.io/nats-concepts/jetstream/consumers)
- [ ] **`MaxAckPending`과 `AckWait`의 파이프라인 정합성**: `MaxAckPending`(`1000`)으로 미확인 인플라이트 수를 묶고, `AckWait`는 최소 `3 * flush_interval + margin`(archiver 기본 90s) 이상으로 잡아 정상 플러시 도중 재전송되지 않게 한다.
  - 이유: `AckWait`가 플러시·S3 업로드 시간보다 짧으면 동일 배치 중복 처리와 재전송 폭풍이 발생한다.
  - 검증: `rg -n "MaxAckPending|AckWait|ACK_WAIT_SECONDS|MAX_ACK_PENDING" apps/ contracts/ENV.md`.
  - 출처: [JetStream Consumer Tuning](https://docs.nats.io/using-nats/developer/develop_jetstream/consumers)
- [ ] **영속 쓰기 완료 후에만 Ack(`AckSync`) 수행**: 퍼블리셔는 JetStream `Publish` ack(`AckSync`) 후 HTTP 202를 반환하고, 컨슈머는 PostgreSQL 커밋 또는 S3 Parquet + manifest 업로드 검증이 끝난 뒤에만 `msg.Ack()`를 호출한다.
  - 이유: 버퍼에 넣자마자 먼저 Ack하면 프로세스 크래시 시 해당 배치가 영구 유실된다.
  - 검증: `rg -n "\.Ack\(|\.AckSync\(" apps/processor-go/ apps/processor-py/ apps/archiver-go/ apps/archiver-py/`.
  - 출처: [JetStream Consumers Dev](https://docs.nats.io/using-nats/developer/develop_jetstream/consumers)
- [ ] **`Nak` vs `Term`과 `MaxDeliver` 독성 메시지 차단**: 일시 오류(DB/S3 타임아웃)는 `Nak`(또는 백오프 `NakWithDelay`)로 재시도하고, 영구 역직렬화/스키마 불량 독성 메시지는 `Term` 또는 `MaxDeliver` + 알림으로 격리한다.
  - 이유: 파싱 불가 메시지를 무한 `Nak`하면 소비자 슬롯(`MaxAckPending`)이 독성 메시지로 가득 차 파이프라인이 멈춘다.
  - 검증: `rg -n "\.Nak\(|\.Term\(|MaxDeliver" apps/`.
  - 출처: [JetStream Consumers Dev](https://docs.nats.io/using-nats/developer/develop_jetstream/consumers)
- [ ] **이중 압축 회피와 `max_payload` 일치**: 페이로드가 이미 zstd로 압축된 경우 스트림 압축(`NoCompression`)을 끄고, 브로커 `max_payload`(`64MB`)가 인제스트 상한(`INGEST_MAX_DECOMPRESSED_BYTES`) 이상인지 확인한다.
  - 이유: 이미 압축된 데이터 재압축은 CPU만 낭비하고, 브로커 `max_payload`보다 큰 메시지는 퍼블리시 단계에서 거절된다.
  - 검증: `rg -n "max_payload|Compression|NoCompression" deploy/compose/nats/ apps/ingest-go/ apps/ingest-py/`.
  - 출처: [NATS Resource Management](https://docs.nats.io/running-a-nats-service/configuration/resource_management)

## 5. S3 + Parquet (`s3 v1.113.4`, `boto3 1.43.127`, `SeaweedFS 4.54`)

- [ ] **충돌 없는 결정적 객체 키와 Manifest 쌍**: 객체 키를 `<profile>/<YYYY>/<MM>/<DD>/<HH>/<stream>-<first_seq>-<last_seq>.parquet`처럼 소스·시퀀스 범위로 결정하고, 동반 `.manifest.json`으로 봉인(seal) 완결성을 추적한다.
  - 이유: 랜덤 UUID 키는 재시도 시 동일 데이터 중복 업로드를 만들고, 시퀀스 기반 키는 멱등 재실행을 가능하게 한다.
  - 검증: `rg -n "manifest\.json|seal\.Prefix|\.parquet" apps/archiver-go/ apps/archiver-py/`.
  - 출처: [Amazon S3 Prefixes](https://docs.aws.amazon.com/AmazonS3/latest/userguide/using-prefixes.html)
- [ ] **조건부 쓰기(`IfNoneMatch: "*"`)로 덮어쓰기 방지**: `PutObject`와 `CompleteMultipartUpload` 모두 `IfNoneMatch: "*"`를 전달해 기존 Parquet/Manifest를 실수로 덮어쓰지 않는다.
  - 이유: 아카이버 재시작이나 이중 실행 시 동일 키를 덮어쓰면 이미 참조 중인 SHA-256·행 수 정합성이 깨진다.
  - 검증: `rg -n "IfNoneMatch" apps/archiver-go/ apps/archiver-py/ apps/portal-publisher/`.
  - 출처: [S3 Conditional Writes](https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-writes.html)
- [ ] **멀티파트 파트 수 제한과 실패 시 `AbortMultipartUpload`**: 파일 크기에 따라 파트 크기를 `max(64MiB, ceil(size/10000))`로 계산해 10,000 파트 한도를 넘기지 않고, 오류 시 반드시 `AbortMultipartUpload`를 실행한다.
  - 이유: 중단되지 않은 미완료 멀티파트 업로드는 버킷에 숨은 스토리지 비용과 고아 파트를 계속 누적시킨다.
  - 검증: `rg -n "CreateMultipartUpload|AbortMultipartUpload|CompleteMultipartUpload" apps/archiver-go/ apps/archiver-py/`.
  - 출처: [S3 Multipart Upload](https://docs.aws.amazon.com/AmazonS3/latest/userguide/mpuoverview.html)
- [ ] **로컬 스테이징 SHA-256 계산 및 업로드 전 파켓 검증**: `io.MultiWriter`로 스테이징 파일과 SHA-256 해시를 동시에 계산하고, 업로드 직전 `parquet.OpenFile` / `pq.ParquetFile`로 footer·컬럼 스키마를 열어 손상 여부를 확인한다.
  - 이유: 디스크 오류나 버퍼 절단으로 손상된 Parquet를 S3에 올린 뒤 NATS를 Ack하면 복구가 불가능하다.
  - 검증: `rg -n "MultiWriter|OpenFile|ParquetFile|sha256" apps/archiver-go/ apps/archiver-py/`.
  - 출처: [S3 Object Integrity](https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html)
- [ ] **Parquet 먼저, Manifest 나중 커밋과 고아 파일 탐지**: `.parquet` 객체 업로드가 성공한 뒤에만 `.manifest.json`을 쓰고, `seal` 작업에서 `.manifest.json` 없는 고아 `.parquet`를 탐지한다.
  - 이유: Manifest가 먼저 쓰이면 아직 존재하지 않거나 불완전한 Parquet를 다운스트림 리더가 정상 봉인된 데이터로 오인한다.
  - 검증: `rg -n "ManifestKey|manifest_key|manifest\.json" apps/archiver-go/ apps/archiver-py/`.
  - 출처: [Parquet Format Configurations](https://parquet.apache.org/docs/file-format/configurations/)
- [ ] **S3 엔드포인트 및 Path-Style 설정 일관성**: `S3_ENDPOINT`, `S3_REGION`, `S3_FORCE_PATH_STYLE`를 Go(`s3x`)와 Python(`jammy_common.s3`) 모두 동일 계약으로 읽어 AWS S3와 SeaweedFS를 코드 변경 없이 전환한다.
  - 이유: 가상 호스팅 스타일 기본값은 사내/로컬 S3 호환 서버에서 DNS 해석 실패를 일으킨다.
  - 검증: `rg -n "S3_FORCE_PATH_STYLE|S3_ENDPOINT|addressing_style|UsePathStyle" libs/ apps/`.
  - 출처: [S3 Virtual Hosting & Path Style](https://docs.aws.amazon.com/AmazonS3/latest/userguide/VirtualHosting.html)

## 6. DynamoDB 조건부 쓰기 (`dynamodb v1.69.1`, `boto3 1.43.127`, `DynamoDB Local 3.3.2`)

- [ ] **조건부 쓰기(`ConditionExpression`)로 리스 획득·갱신 원자화**: 획득은 `attribute_not_exists(lease_name) OR expires_at <= :now`, 갱신은 `holder = :holder AND epoch = :epoch AND expires_at > :now` 조건을 걸어 단 하나의 홀더만 승격되게 한다.
  - 이유: Get 후 무조건 Put/Update하는 코드는 네트워크 지연·동시 실행 시 두 컨트롤 사이트가 동시에 리더가 되는 split-brain을 만든다.
  - 검증: `rg -n "ConditionExpression|ConditionalCheckFailedException" apps/dispatcher/`.
  - 출처: [DynamoDB Conditional Writes](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.ConditionExpressions.html)
- [ ] **단조 증가 `epoch` 펜싱 토큰과 다운스트림 검증**: 리스 획득 시 `ADD epoch :one`과 `ReturnValues="UPDATED_NEW"`로 원자적 `epoch`를 발급하고, 명령 페이로드와 DB·`plantsim`에서 `stale_epoch`를 거부한다.
  - 이유: TTL 만료 직전 멈췄던 구 리더(GC/네트워크 단절)가 뒤늦게 명령을 보내도 펜싱 토큰으로 무효화할 수 있다.
  - 검증: `rg -n "ADD epoch|UPDATED_NEW|stale_epoch" apps/dispatcher/ tools/plantsim/ contracts/`.
  - 출처: [DynamoDB Conditional Update](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/WorkingWithItems.html#WorkingWithItems.ConditionalUpdate)
- [ ] **리스 상태 조회 시 `ConsistentRead=True` 강제**: 리스 테이블 `get_item` 호출에는 항상 `ConsistentRead=True`를 명시해 최종 일관성(eventually consistent)으로 인한 만료·홀더 오판을 차단한다.
  - 이유: 기본 eventual read는 방금 다른 사이트가 획득한 최신 리스나 갱신된 `epoch`를 놓칠 수 있다.
  - 검증: `rg -n "ConsistentRead" apps/dispatcher/`.
  - 출처: [DynamoDB Read Consistency](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/HowItWorks.ReadConsistency.html)
- [ ] **리스 갱신 주기 < TTL 여유분과 짧은 SDK 타임아웃**: `0 < LEASE_RENEW_SECONDS < LEASE_TTL_SECONDS`(예: 5s vs 15s)를 기동 시 검증하고, DynamoDB 클라이언트는 짧은 timeout(`connect=2s`, `read=3s`, `total_max_attempts=1`)으로 설정한다.
  - 이유: SDK 재시도가 리스 TTL을 넘기면 만료 이후에도 스스로를 활성 리더로 착각한다.
  - 검증: `rg -n "LEASE_RENEW_SECONDS|LEASE_TTL_SECONDS|total_max_attempts" apps/dispatcher/src/dispatcher/lease.py`.
  - 출처: [DynamoDB Error Handling](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Programming.Errors.html)
- [ ] **`ConditionalCheckFailedException` 정상 강등 처리**: 조건부 쓰기 실패 시 프로세스를 크래시시키지 않고 즉시 `LeaseState(False, epoch)`로 강등(standby)하며, 그 외 예외만 에러로 전파한다.
  - 이유: 정상적인 리더 경합 패배를 치명 오류로 취급하면 불필요한 파드 재시작 루프가 발생한다.
  - 검증: `rg -n "ConditionalCheckFailedException" apps/dispatcher/src/dispatcher/lease.py`.
  - 출처: [DynamoDB Error Handling](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Programming.Errors.html)
- [ ] **테이블 접근 직렬화와 수동 takeover 가드**: `asyncio.Lock`으로 리스 tick과 readiness 프로브의 테이블 접근을 직렬화하고, 스탠바이 사이트의 강제 인수(`takeover`)는 `--confirm` 플래그 없이는 거부한다.
  - 이유: 동시 tick/프로브 경합과 실수에 의한 스탠바이 자동 인수는 이중 제어를 초래한다.
  - 검증: `rg -n "_lock|takeover|--confirm|LEASE_AUTO_ACQUIRE" apps/dispatcher/src/dispatcher/lease.py`.
  - 출처: [Google SRE Managing Critical State](https://sre.google/sre-book/managing-critical-state/)

## 7. SQS (`sqs v1.52.11`, `boto3 1.43.127`, `ElasticMQ 1.7.1`)

- [ ] **DB 영속 커밋 후 `delete_message` 호출**: SQS 메시지는 PostgreSQL 트랜잭션이 `inserted` 또는 `duplicate`를 확정한 뒤에만 `delete_message`를 호출하고, 일시 DB 오류 시 삭제하지 않는다.
  - 이유: 처리 전에 삭제하면 DB 장애 시 명령이 영구 유실된다.
  - 검증: `rg -n "receive_message|delete_message" apps/dispatcher/src/dispatcher/sqs_drain.py`.
  - 출처: [SQS Visibility Timeout](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-visibility-timeout.html)
- [ ] **Visibility Timeout > 최대 처리 시간**: 큐의 `VisibilityTimeout`을 수신 배치(`MaxNumberOfMessages <= 10`)의 최악 DB 커밋 시간보다 충분히 길게 설정한다.
  - 이유: 처리 도중 visibility가 만료되면 다른 워커가 동일 메시지를 재수신해 동시 경합이 생긴다.
  - 검증: `rg -n "VisibilityTimeout|MaxNumberOfMessages|WaitTimeSeconds" deploy/compose/aws/ apps/dispatcher/`.
  - 출처: [SQS Visibility Timeout](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-visibility-timeout.html)
- [ ] **Dead-Letter Queue(DLQ)와 `maxReceiveCount` 구성**: 스키마 오류(`invalid`)나 본문 충돌(`conflict`)처럼 재시도로 해결 안 되는 메시지는 무한 재전송 대신 DLQ(`RedrivePolicy`)로 격리한다.
  - 이유: 독성 메시지가 큐 선두에 계속 복귀하면 정상 명령 소비가 지연되고 비용·로그가 폭증한다.
  - 검증: `rg -n "RedrivePolicy|maxReceiveCount|dlq" deploy/compose/aws/ apps/dispatcher/`.
  - 출처: [SQS Dead-Letter Queues](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-dead-letter-queues.html)
- [ ] **At-Least-Once 전제 멱등성 + 해시 충돌 검증**: Standard SQS의 중복·순서 역전을 가정해 `ON CONFLICT (instruction_id) DO NOTHING`으로 멱등 삽입하고, 기존 `body_sha256`과 다르면 `conflict`로 기록한다.
  - 이유: 동일 `instruction_id`에 다른 페이로드가 들어온 경우 조용히 무시하면 제어 의도가 왜곡된다.
  - 검증: `rg -n "body_sha256|ON CONFLICT \(instruction_id\)" apps/dispatcher/ apps/instruction-api/`.
  - 출처: [SQS Standard Queues](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/standard-queues.html)
- [ ] **배치 처리 루프 내 리더 상태 재확인**: `receive_message`로 10건을 가져온 뒤에도 각 메시지 처리 직전 `active()`(리스 활성 여부)를 재확인해 비활성 사이트 전환 시 즉시 중단한다.
  - 이유: 수신 직후 리스를 잃은 스탠바이 디스패처가 잔여 메시지를 처리하는 구간을 최소화한다.
  - 검증: `rg -n "active\(\)|drain_once" apps/dispatcher/src/dispatcher/sqs_drain.py`.
  - 출처: [SQS Batch Actions](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-batch-api-actions.html)
- [ ] **FIFO vs Standard 의미 구분과 로컬 에뮬레이터 분기**: 순서·중복 제거가 큐 수준에서 필요할 때만 `.fifo`(`MessageGroupId`/`MessageDeduplicationId`)를 선택하고, `SQS_ENDPOINT` 오버라이드로 ElasticMQ와 실 AWS를 동일 코드로 테스트한다.
  - 이유: Standard 큐에 FIFO 순서를 기대하거나 반대로 단일 `MessageGroupId`를 남용하면 처리량 병목이나 순서 오류가 발생한다.
  - 검증: `rg -n "SQS_ENDPOINT|SQS_QUEUE_URL|elasticmq" apps/dispatcher/ deploy/compose/aws/`.
  - 출처: [SQS FIFO Queues](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/FIFO-queues.html)

## 8. Protobuf + OpenAPI 계약 (`buf v1.68.4`, `protobuf v1.36.12 / 7.36.2`, `@redocly/cli 2.54.3`)

- [ ] **단일 진실 공급원과 코드 생성 드리프트 차단**: 스키마는 `contracts/`에만 두고 CI `make contracts-check`(`proto-check`)로 생성된 `.pb.go`, `_pb2.py`가 최신인지 `diff` 검증한다.
  - 이유: 수동 편집이나 미재생 stub이 커밋되면 Go·Python 서비스 간 직렬화 불일치가 프로덕션에서 터진다.
  - 검증: `rg -n "proto-check|contracts-check" Makefile .github/workflows/backend.yml buf.gen.yaml`.
  - 출처: [Buf Docs](https://buf.build/docs/lint/overview/)
- [ ] **`buf lint` + `buf breaking` 호환성 검사**: `buf.yaml`의 `STANDARD` 린트와 `FILE` 단위 breaking change 규칙을 유지하며 필드 번호·타입 재사용을 금지한다.
  - 이유: 기존 필드 번호 재사용이나 타입 변경은 구버전 에이전트/배치 데이터의 조용한 오독을 일으킨다.
  - 검증: `cat buf.yaml && rg -n "buf lint|buf breaking" Makefile`.
  - 출처: [Buf Breaking Change Detection](https://buf.build/docs/breaking/overview/)
- [ ] **Generator와 Runtime 버전 1:1 고정**: `buf.gen.yaml`의 원격 플러그인 버전(`protocolbuffers/go:v1.36.12`, `python:v36.2`)과 `go.mod`/`uv.lock`의 런타임 패키지(`v1.36.12`, `7.36.2`)를 같은 릴리스 쌍으로 맞춘다.
  - 이유: 생성 코드 버전이 런타임 지원 범위를 넘으면 런타임 import 오류나 ABI 불일치가 발생한다.
  - 검증: `cat buf.gen.yaml && rg -n "protobuf" go.mod pyproject.toml uv.lock`.
  - 출처: [Protobuf Cross-Version Guarantee](https://protobuf.dev/support/cross-version-runtime-guarantee/)
- [ ] **엄격한 ProtoJSON 파싱(중복 키·미지 필드·int64 정밀도)**: JSON 입력 시 중복 키와 알 수 없는 필드를 거부하고, `int64` 필드의 부동소수점 정밀도 손실을 막기 위해 Python에서는 `Decimal` 파서를 사용한다.
  - 이유: 기본 `json.loads`는 중복 키를 덮어쓰고 큰 정수를 float로 손실시킬 수 있다.
  - 검증: `rg -n "_unique_object|ParseDict|Decimal|DiscardUnknown" libs/py/jammy_common/codec.py libs/go/codec/`.
  - 출처: [Protobuf Proto3 JSON](https://protobuf.dev/programming-guides/proto3/)
- [ ] **경계 크기·수량 검증과 교차 언어 골든 픽스처**: 배치 크기(`1..10000`), 본문 상한(`INGEST_MAX_DECOMPRESSED_BYTES`, `INSTRUCTION_MAX_BODY_BYTES`)을 검증하고 `contracts/testdata/gate_cases` 픽스처로 Go/Python 동일 판정을 검증한다.
  - 이유: 언어별 검증 로직이 미세하게 어긋나면 한 구현에서만 통과한 배치가 다른 구현에서 거절된다.
  - 검증: `ls contracts/testdata/gate_cases/ && rg -n "gate_cases" libs/go/gate/ libs/py/tests/`.
  - 출처: [OWASP Input Validation](https://cheatsheetseries.owasp.org/cheatsheets/Input_Validation_Cheat_Sheet.html)
- [ ] **OpenAPI (`redocly lint`)와 메트릭 라우트 템플릿 일치**: `contracts/openapi/*.yaml`을 `redocly lint`로 검증하고, 경로 파라미터 템플릿(`/v1/instructions/{id}`)을 OTel `http_route` 라벨 규약과 일치시킨다.
  - 이유: 스펙과 실제 라우트가 어긋나면 클라이언트 SDK 오작동 및 HTTP 메트릭 카디널리티 오염이 발생한다.
  - 검증: `rg -n "openapi-check|redocly" Makefile contracts/openapi/`.
  - 출처: [OpenAPI 3.1 Specification](https://spec.openapis.org/oas/v3.1.0.html)

## 9. golang-migrate (`v4.20.1`)

- [ ] **Up/Down 왕복 검증(`up` → `down -all` → `up`)**: 모든 마이그레이션에 `.up.sql`과 `.down.sql`을 쌍으로 두고, CI `make migrations-check`에서 실제 PostgreSQL 17 컨테이너에 왕복 적용해 검증한다.
  - 이유: 롤백 스크립트 누락이나 비멱등 SQL은 배포 실패 시 긴급 복구를 가로막는다.
  - 검증: `ls contracts/db/migrations/ && rg -n "migrations-check" Makefile`.
  - 출처: [golang-migrate Migrations](https://github.com/golang-migrate/migrate/blob/v4.20.1/MIGRATIONS.md)
- [ ] **스키마 명시와 `x-migrations-table` 고정**: SQL 내부 객체는 `jammy.<table|function>`으로 스키마를 명시하고, 실행 래퍼는 `x-migrations-table-quoted=true&x-migrations-table="public"."schema_migrations"`를 주입한다.
  - 이유: 롤 이름과 같은 스키마(`$user=jammy`)가 생기면 `search_path` 해석이 바뀌어 2회차 마이그레이션이 `schema_migrations`를 못 찾는다.
  - 검증: `rg -n "x-migrations-table|CREATE SCHEMA" apps/migrate/main.go contracts/db/migrations/`.
  - 출처: [golang-migrate Postgres Driver](https://github.com/golang-migrate/migrate/tree/v4.20.1/database/postgres)
- [ ] **앱 기동과 분리된 단일 마이그레이션 Job**: 마이그레이션은 앱 컨테이너가 아니라 Helm `pre-install,pre-upgrade` Job(`backoffLimit: 0`, `restartPolicy: Never`)으로 배포 전 1회만 실행한다.
  - 이유: 다수 파드가 동시에 마이그레이션을 시도하면 잠금 경합이 나고, 앱 롤백과 스키마 상태가 얽힌다.
  - 검증: `cat deploy/helm/jammy-platform/templates/job-migrate.yaml`.
  - 출처: [Helm Chart Hooks](https://helm.sh/docs/topics/charts_hooks/)
- [ ] **무중단 Expand-and-Contract DDL 원칙**: 운영 테이블에 파괴적 `DROP COLUMN`·타입 변경이나 장시간 `ACCESS EXCLUSIVE` 락을 거는 DDL을 피하고, 하위 호환 컬럼 추가 → 코드 전환 → 정리 순서를 따른다.
  - 이유: `ALTER TABLE` 락 대기는 뒤에 오는 모든 읽기/쓰기 쿼리를 줄줄이 대기시켜 전체 API 장애를 일으킨다.
  - 검증: `rg -n "ALTER TABLE|DROP COLUMN|LOCK TABLE" contracts/db/migrations/`.
  - 출처: [PostgreSQL 17 ALTER TABLE](https://www.postgresql.org/docs/17/ddl-alter.html)
- [ ] **Dirty 상태 자동 강제 복구 금지**: 마이그레이션 실패 시 자동 `force` 스크립트를 두지 말고 원인 SQL과 DB 상태를 확인한 뒤 수동 개입하도록 구성한다.
  - 이유: 부분 적용된 마이그레이션을 맹목적으로 `force`하면 스키마 불일치가 영구 고착된다.
  - 검증: `rg -n "force" apps/migrate/ Makefile deploy/`.
  - 출처: [golang-migrate Getting Started](https://github.com/golang-migrate/migrate/blob/v4.20.1/GETTING_STARTED.md)
- [ ] **최소 정적 빌드(`-tags postgres`, stub 제거)**: Go `migrate` 바이너리는 `CGO_ENABLED=0 -trimpath -tags postgres`로 빌드하고 테스트용 `database/stub` 등록을 제거해 Distroless nonroot에 담는다.
  - 이유: 불필요한 드라이버와 동적 링크 의존성을 제거해야 공격 표면과 취약점(CVE)이 최소화된다.
  - 검증: `cat apps/migrate/Dockerfile`.
  - 출처: [golang-migrate v4.20.1 CLI](https://github.com/golang-migrate/migrate/tree/v4.20.1/cmd/migrate)

## 10. Docker 이미지 (`golang:1.27.4-trixie`, `python:3.14.10-slim-trixie`, `distroless/static-debian13:nonroot`, `uv 0.12.24`)

- [ ] **멀티스테이지 빌드와 최소 런타임 산출물**: 빌드 스테이지의 컴파일러·`uv`·캐시를 런타임 스테이지로 넘기지 않고, Go 정적 바이너리 또는 `uv sync --locked --no-dev --no-editable`로 만든 `.venv`만 복사한다.
  - 이유: 빌드 도구와 패키지 매니저가 프로덕션 컨테이너에 남으면 이미지 크기와 RCE/공급망 공격 표면이 커진다.
  - 검증: `rg -n "^FROM |COPY --from=|uv sync --locked" apps/*/Dockerfile tools/*/Dockerfile`.
  - 출처: [Docker Multi-Stage Builds](https://docs.docker.com/build/building/multi-stage/)
- [ ] **베이스 이미지 OCI Index `sha256` 다이제스트 고정**: 모든 `FROM` 문은 가변 태그 대신 `linux/amd64`·`linux/arm64` 멀티아키텍처 OCI index `sha256` 다이제스트까지 고정한다.
  - 이유: 태그만 고정하면 upstream 재푸시 시 예고 없이 라이브러리나 취약점이 바뀌어 재현 빌드가 깨진다.
  - 검증: `rg -n "^FROM .*@sha256:" apps/*/Dockerfile tools/*/Dockerfile`.
  - 출처: [Docker Best Practices](https://docs.docker.com/build/building/best-practices/)
- [ ] **Go Distroless `nonroot` 및 Python 런타임 패키지 제거**: Go 앱은 `gcr.io/distroless/static-debian13:nonroot`를 쓰고, Python slim 런타임은 `pip`/`ensurepip`/`setuptools`를 제거하며 `PYTHONDONTWRITEBYTECODE=1`, `PYTHONUNBUFFERED=1`을 둔다.
  - 이유: 셸과 패키지 관리자가 없는 최소 런타임은 침투 후 횡적 이동을 어렵게 만들고, `PYTHONUNBUFFERED=1`은 크래시 직전 로그 유실을 막는다.
  - 검증: `rg -n "distroless|ensurepip|PYTHONUNBUFFERED" apps/*/Dockerfile`.
  - 출처: [GoogleContainerTools Distroless](https://github.com/GoogleContainerTools/distroless)
- [ ] **비루트(`USER 65532:65532`) 실행과 `.dockerignore` 비밀 차단**: 모든 `Dockerfile`에 명시적 `USER 65532:65532`를 두고 `.dockerignore`로 `.git`, `.env`, 로컬 키 파일이 빌드 컨텍스트에 들어가지 않게 한다.
  - 이유: 컨테이너 탈출 취약점 발생 시 root 권한 보유는 호스트 침해 위험을 높이고, 빌드 컨텍스트 오염은 이미지 레이어에 비밀을 남긴다.
  - 검증: `cat .dockerignore && rg -n "^USER 65532:65532" apps/*/Dockerfile tools/*/Dockerfile`.
  - 출처: [OWASP Docker Security](https://cheatsheetseries.owasp.org/cheatsheets/Docker_Security_Cheat_Sheet.html)
- [ ] **의존성 매니페스트 우선 복사(레이어 캐싱)**: `go.mod`/`go.sum` 또는 `pyproject.toml`/`uv.lock`을 먼저 `COPY`해 의존성을 다운로드한 뒤 앱 소스를 복사한다.
  - 이유: 코드 한 줄 바뀔 때마다 전체 의존성을 다시 받으면 CI 빌드 시간과 외부 레지스트리 실패율이 급증한다.
  - 검증: `head -n 15 apps/ingest-go/Dockerfile apps/ingest-py/Dockerfile`.
  - 출처: [uv Docker Integration](https://docs.astral.sh/uv/guides/integration/docker/)
- [ ] **재현 가능한 타임스탬프와 OCI 메타데이터 라벨**: BuildKit `SOURCE_DATE_EPOCH` + `rewrite-timestamp=true`와 `org.opencontainers.image.{title,description,source,version,revision}` 라벨을 부여한다.
  - 이유: 배포된 이미지가 어느 커밋과 버전에서 빌드됐는지 추적하고 불필요한 레이어 다이제스트 변동을 없앤다.
  - 검증: `rg -n "org\.opencontainers\.image|SOURCE_DATE_EPOCH|rewrite-timestamp" apps/*/Dockerfile .github/workflows/backend.yml`.
  - 출처: [OCI Image Annotations](https://github.com/opencontainers/image-spec/blob/main/annotations.md)
- [ ] **네이티브 스모크 테스트와 Trivy 취약점 게이트**: 빌드된 이미지를 `--network none`으로 기동해 필수 env 누락 시 1줄 오류 종료(exit 1) 또는 `/livez`를 확인하고, 양쪽 아키텍처에 대해 Trivy `HIGH,CRITICAL`(`--ignore-unfixed`) 스캔을 통과해야만 게시한다.
  - 이유: 엔트리포인트 오타나 누락된 동적 라이브러리, 알려진 수정 가능 CVE가 레지스트리에 올라가는 것을 원천 차단한다.
  - 검증: `rg -n "trivy-action|ignore-unfixed|Smoke-test" .github/workflows/backend.yml`.
  - 출처: [Trivy Docs](https://aquasecurity.github.io/trivy/latest/)

## 11. Helm / Kubernetes (`Helm v4.3.2`, `k3s v1.37.2`, `kubeconform v0.6.7`, `helm-unittest v1.1.2`)

- [ ] **`/livez`와 `/readyz` 프로브 의미 분리**: 전용 `ADMIN_PORT`(`9090`)에서 `/livez`는 프로세스 생존만(외부 의존성 없음), `/readyz`는 DB/NATS/SQS/리스 연결 상태를 짧은 타임아웃으로 검사한다.
  - 이유: `/livez`에 DB·NATS 체크를 넣으면 일시적 백엔드 장애 시 모든 파드가 연쇄 재시작(cascading restart)한다.
  - 검증: `rg -n "readinessProbe|livenessProbe|/readyz|/livez" deploy/helm/jammy-platform/ libs/go/admin/ libs/py/jammy_common/admin.py`.
  - 출처: [Kubernetes Probes](https://kubernetes.io/docs/tasks/configure-pod-container/configure-liveness-readiness-startup-probes/)
- [ ] **리소스 `requests`/`limits`와 종료 유예 시간 정합성**: 모든 워크로드에 `cpu`/`memory`의 `requests`와 `limits`를 명시하고, `terminationGracePeriodSeconds: 30`이 앱 `SHUTDOWN_TIMEOUT_SECONDS: 25`보다 크게 유지한다.
  - 이유: 유예 시간이 앱 드레인 타임아웃보다 짧으면 SIGTERM 후 진행 중이던 DB 커밋·NATS Ack가 SIGKILL로 강제 종료된다.
  - 검증: `rg -n "requests:|limits:|terminationGracePeriodSeconds|SHUTDOWN_TIMEOUT_SECONDS" deploy/helm/jammy-platform/`.
  - 출처: [Kubernetes Pod Termination](https://kubernetes.io/docs/concepts/workloads/pods/pod-lifecycle/#pod-termination)
- [ ] **강력한 `securityContext`와 읽기 전용 루트 파일시스템**: 파드에 `runAsNonRoot: true`, `seccompProfile: RuntimeDefault`, `automountServiceAccountToken: false`를, 컨테이너에 `readOnlyRootFilesystem: true`, `allowPrivilegeEscalation: false`, `capabilities.drop: [ALL]`을 적용하고 `/tmp`·`/stage`만 `emptyDir`로 마운트한다.
  - 이유: 런타임 내 악성 바이너리 기록, 권한 상승, 불필요한 K8s API 토큰 탈취를 동시에 차단한다.
  - 검증: `rg -n "readOnlyRootFilesystem|allowPrivilegeEscalation|automountServiceAccountToken|seccompProfile" deploy/helm/jammy-platform/templates/`.
  - 출처: [Kubernetes Security Context](https://kubernetes.io/docs/tasks/configure-pod-container/security-context/)
- [ ] **PDB(`PodDisruptionBudget`)와 워크로드별 롤아웃 전략**: 각 서비스에 `PDB`(`maxUnavailable: 1`)를 두고, 스트림을 독점 점유하는 `ingest`는 `Recreate`, HA `dispatcher`는 `maxSurge: 0` + `podAntiAffinity`를 적용한다.
  - 이유: 노드 유지보수 시 동시 중단을 막고, 스트림·리스 소유 충돌이 나는 워크로드의 이중 기동을 방지한다.
  - 검증: `cat deploy/helm/jammy-platform/templates/pdb.yaml && rg -n "Recreate|maxSurge|podAntiAffinity" deploy/helm/jammy-platform/templates/_helpers.tpl`.
  - 출처: [Kubernetes PDB](https://kubernetes.io/docs/tasks/run-application/configure-pdb/)
- [ ] **최소 권한 `NetworkPolicy`로 인그레스 격리**: `Ingress` `NetworkPolicy`로 `http`(8080)와 `admin`(9090, 모니터링 네임스페이스 스크랩) 외 모든 인바운드 포트를 차단한다.
  - 이유: 파드 간 무단 접근이나 외부에서의 직접 관리자(`/metrics`) 노출을 막는다.
  - 검증: `cat deploy/helm/jammy-platform/templates/networkpolicy.yaml`.
  - 출처: [Kubernetes Network Policies](https://kubernetes.io/docs/concepts/services-networking/network-policies/)
- [ ] **ConfigMap/Secret 분리와 설정 변경 해시 롤아웃**: 비비밀 설정만 `ConfigMap`에 넣고 `checksum/config` 어노테이션으로 자동 롤아웃을 트리거하며, `DATABASE_URL`·AWS 키·`HMAC_KEYS`·`NATS_CREDS`는 `secretKeyRef` 또는 read-only 볼륨 마운트로만 주입한다.
  - 이유: 시크릿을 `values.yaml`이나 `ConfigMap`에 평문 저장하면 Git·etcd·Helm release 시크릿에 평문 노출된다.
  - 검증: `rg -n "secretKeyRef|checksum/config|existingSecret|unset \$env" deploy/helm/jammy-platform/templates/_helpers.tpl`.
  - 출처: [Kubernetes Secrets](https://kubernetes.io/docs/concepts/configuration/secret/)
- [ ] **가변 이미지 태그 금지와 템플릿 필수값 검증**: `jammy.image` 헬퍼에서 `required`와 정규식(`^[0-9]+\.[0-9]+\.[0-9]+(-[a-f0-9]{7})?$`)으로 `:latest` 등 가변 태그를 차단하고, 모드별 필수 변수를 `fail`로 즉시 거른다.
  - 이유: `:latest` 태그는 노드 캐시에 따라 서로 다른 버전이 섞여 롤백을 불가능하게 만든다.
  - 검증: `rg -n "jammy\.image|required |fail " deploy/helm/jammy-platform/templates/_helpers.tpl`.
  - 출처: [Helm Best Practices](https://helm.sh/docs/chart_best_practices/templates/)
- [ ] **렌더링·유닛 테스트·스키마 검증 파이프라인**: CI `make helm-test`에서 `helm lint`, `helm-unittest`, `kubeconform -kubernetes-version 1.37.2 -strict`를 모두 실행해 API 스키마 오류를 사전 차단한다.
  - 이유: 템플릿 문법 오류나 폐기된 Kubernetes API 필드가 실제 클러스터 배포 시점에 발견되는 것을 막는다.
  - 검증: `rg -n "helm-test|kubeconform|helm-unittest" Makefile .github/workflows/backend.yml`.
  - 출처: [Kubernetes Pod Resources](https://kubernetes.io/docs/concepts/configuration/manage-resources-containers/)

## 12. GitHub Actions 공급망 (`.github/workflows/backend.yml`)

- [ ] **모든 서드파티 Action의 40자 커밋 SHA 고정**: `uses:` 참조는 버전 태그(`@v4`) 대신 검증된 40자 SHA 해시 + 주석(`# v0.36.0`)으로 고정한다.
  - 이유: 과거 `trivy-action` 태그 변조 사례처럼 태그 포인터가 탈취돼도 SHA 고정으로 악성 코드 실행을 차단한다.
  - 검증: `rg -n "uses: " .github/workflows/ | rg -v "@[0-9a-f]{40}"` (매치 없어야 함).
  - 출처: [GitHub Actions Security Hardening](https://docs.github.com/en/actions/security-for-github-actions/security-guides/security-hardening-for-github-actions)
- [ ] **최소 권한 `permissions`와 `persist-credentials: false`**: 워크플로 최상단은 `permissions: {contents: read}`로 묶고 `packages: write`, `id-token: write`는 `publish` job에만 부여하며 `checkout`은 `persist-credentials: false`를 지정한다.
  - 이유: 빌드·테스트 job의 서드파티 도구나 의존성 스크립트가 침해되더라도 레포지토리 쓰기나 패키지 푸시 권한을 얻지 못하게 한다.
  - 검증: `rg -n "permissions:|persist-credentials:" .github/workflows/`.
  - 출처: [Controlling `GITHUB_TOKEN` Permissions](https://docs.github.com/en/actions/writing-workflows/choosing-what-your-workflow-does/controlling-permissions-for-github_token)
- [ ] **단일 빌드 → 스캔 통과 후 게시(Scan-Before-Push) 아티팩트 전달**: `images` job에서 로컬 OCI layout으로 빌드해 양쪽 아키텍처를 Trivy 스캔하고, 통과한 `image.tar`만 `upload-artifact` → `publish` job으로 넘겨 재빌드 없이 푸시한다.
  - 이유: 스캔과 푸시 사이에 재빌드하면 스캔하지 않은 바이너리가 레지스트리에 배포될 수 있다(TOCTOU).
  - 검증: `rg -n "type=oci|upload-artifact|download-artifact" .github/workflows/backend.yml`.
  - 출처: [SLSA Build Levels](https://slsa.dev/spec/v1.0/levels)
- [ ] **BuildKit SLSA Provenance 생성 및 매니페스트 검증**: 릴리스 빌드 시 `provenance: mode=min`을 켜고 `jq`로 각 플랫폼 다이제스트에 `attestation-manifest`가 연결됐는지 확인한다.
  - 이유: 출처 증명(provenance) 누락 시 누가 어느 커밋과 워크플로에서 이미지를 만들었는지 검증할 수 없다.
  - 검증: `rg -n "provenance:|attestation-manifest" .github/workflows/backend.yml`.
  - 출처: [Docker SLSA Provenance](https://docs.docker.com/build/metadata/attestations/slsa-provenance/)
- [ ] **SPDX SBOM 아티팩트 발행 (`anchore/sbom-action` + `syft v1.51.1`)**: `linux/amd64`와 `linux/arm64` 각각에 대해 SPDX JSON SBOM을 생성해 워크플로 아티팩트로 보존한다.
  - 이유: 신규 zero-day CVE 발생 시 재스캔 없이도 영향받는 서비스와 패키지 버전을 즉시 식별할 수 있다.
  - 검증: `rg -n "sbom-action|spdx-json|syft-version" .github/workflows/backend.yml`.
  - 출처: [anchore/sbom-action](https://github.com/anchore/sbom-action)
- [ ] **Cosign (`v3.1.4`) Keyless OIDC 다이제스트 서명**: 장기 개인키 없이 GitHub OIDC `id-token: write`로 `cosign sign --yes "$IMAGE@$digest"`를 실행해 불변 다이제스트에 서명한다.
  - 이유: 태그가 아닌 다이제스트 서명으로 이미지 변조를 막고, 장기 서명 키 유출 위험을 제거한다.
  - 검증: `rg -n "cosign-installer|cosign sign" .github/workflows/backend.yml`.
  - 출처: [Sigstore Cosign](https://docs.sigstore.dev/cosign/signing/signing_with_containers/)
- [ ] **정적 워크플로 검사(`actionlint`)와 릴리스 저장소·브랜치 가드**: 포크 PR에서 실 배포가 돌지 않도록 `RELEASE_BUILD`를 `push && refs/heads/main && github.repository == 'cloud4-final/jammy-5th'`로 제한하고 `actionlint`/`shellcheck`를 적용한다.
  - 이유: 워크플로 표현식 인젝션이나 포크 환경에서의 잘못된 게시 시도를 사전에 차단한다.
  - 검증: `rg -n "RELEASE_BUILD|actionlint" .github/workflows/ docs/backend/VERSIONS.md`.
  - 출처: [actionlint](https://github.com/rhysd/actionlint)

## 13. 관측성 (`VictoriaMetrics v1.156.0`, `Grafana 13.3.1`, `Loki 3.7.10`, `Tempo 3.1.1`, `Alloy v1.20.1`)

- [ ] **Prometheus 표준 명명과 Go/Python 버킷 일치**: 커스텀 메트릭은 `jammy_<service>_<name>`(`_total`, `_seconds` 접미사)을 따르고 모든 `*_duration_seconds` 히스토그램은 `contracts/METRICS.md`의 13개 고정 버킷(`0.001..10`)을 공유한다.
  - 이유: 언어 구현체마다 이름이나 버킷 경계가 다르면 p95/p99 비교와 동일 대시보드 쿼리가 깨진다.
  - 검증: `rg -n "durationBuckets|HTTP_BUCKETS|jammy_" contracts/METRICS.md libs/go/obs/ libs/py/jammy_common/obs.py`.
  - 출처: [Prometheus Metric Naming](https://prometheus.io/docs/practices/naming/)
- [ ] **라벨 허용 목록과 고카디널리티 차단**: 메트릭 라벨은 `service`, `instance`, `site`, `result`, `reason`, `kind`, `profile` 및 HTTP 템플릿 `http_route`(절대 raw URL·`event_id`·`asset_id` 금지)로 제한한다.
  - 이유: 무한 카디널리티 ID를 라벨에 넣으면 VictoriaMetrics/Prometheus 메모리가 고갈돼 스크랩이 중단된다.
  - 검증: `rg -n "applicationLabel|METRIC_LABELS|http_route" contracts/METRICS.md libs/go/obs/ libs/py/jammy_common/obs.py`.
  - 출처: [Prometheus Label Cardinality](https://prometheus.io/docs/practices/instrumentation/#do-not-overuse-labels)
- [ ] **닫힌 집합 `result`와 RED 대시보드 지표 완성**: 모든 핵심 경로가 요청률(Rate)·오류(`http_response_status_code >= 500` 또는 닫힌 집합 `result="error"`)·지연(`*_duration_seconds`)을 노출한다.
  - 이유: 임의 문자열 `result`는 알람과 SLI 집계를 깨뜨리고, RED 중 하나라도 빠지면 장애 탐지가 늦어진다.
  - 검증: `cat contracts/METRICS.md && rg -n '"result"' apps/`.
  - 출처: [OTel HTTP Metrics SemConv](https://opentelemetry.io/docs/specs/semconv/http/http-metrics/)
- [ ] **USE(Utilization, Saturation, Errors) 포화 지표 노출**: `consumer_pending`, `lag_seconds`, `unacked_messages`, `is_leader`, `lease_active`, 인플라이트 세마포어를 게이지로 노출한다.
  - 이유: 지연이나 오류가 터지기 전 큐 적체·포화 징후를 조기에 포착해야 백프레셔·스케일링 대응이 가능하다.
  - 검증: `rg -n "consumer_pending|lag_seconds|unacked_messages|is_leader" contracts/METRICS.md apps/`.
  - 출처: [Google SRE Monitoring](https://sre.google/sre-book/monitoring-distributed-systems/)
- [ ] **단일 라인 JSON 구조화 로그와 민감 정보 배제**: 모든 로그는 `ts`, `level`, `msg`, `service`, `instance`, `site`, `trace_id`, `span_id`를 포함한 1줄 JSON으로 stdout에 출력하고, raw payload·자격 증명·시크릿을 남기지 않는다.
  - 이유: 멀티라인 스택트레이스나 비밀값 출력은 Loki 파싱 실패와 보안 사고로 직결된다.
  - 검증: `rg -n "trace_id|span_id" libs/go/logx/ libs/py/jammy_common/logging.py`.
  - 출처: [OTel Logs Data Model](https://opentelemetry.io/docs/specs/otel/logs/)
- [ ] **W3C `traceparent` 전파와 비차단 배치 전송**: HTTP·NATS 경계에서 `TraceContext`를 추출/주입하고 `BatchSpanProcessor`/`BatchLogRecordProcessor`로 비동기 전송하며, `OTEL_EXPORTER_OTLP_ENDPOINT`가 비어 있으면 안전하게 비활성화한다.
  - 이유: 요청 경로에서 동기 OTLP export를 하면 관측 백엔드 지연이 비즈니스 API 장애로 전이된다.
  - 검증: `rg -n "TraceContext|BatchSpanProcessor|OTEL_EXPORTER_OTLP_ENDPOINT" libs/ apps/`.
  - 출처: [W3C Trace Context](https://www.w3.org/TR/trace-context/)
- [ ] **스크랩 최적화(`/metrics` OpenMetrics + `WithoutTargetInfo`)**: 어드민 포트 `/metrics`에서 `WithoutTargetInfo()`·`WithoutScopeInfo()`(Python: `disable_target_info=True`, `scope_info_enabled=False`)로 불필요한 메타 시계열을 억제한다.
  - 이유: OTel SDK가 기본 생성하는 `target_info`/`otel_scope_info`가 스크랩 대상 수만큼 불필요한 카디널리티를 늘린다.
  - 검증: `rg -n "WithoutTargetInfo|WithoutScopeInfo|disable_target_info" libs/go/obs/obs.go libs/py/jammy_common/obs.py`.
  - 출처: [Prometheus Histograms](https://prometheus.io/docs/practices/histograms/)

## 14. 재사용성과 장기 운영 (Reuse and Long-Term Operations)

- [ ] **공유 라이브러리 vs 복붙 기준 (`libs/go`, `libs/py/jammy_common`)**: 2개 이상의 서비스가 공유하는 인프라·계약 로직(`config`, `admin`, `lifecycle`, `obs`, `logx`, `dbx`, `natsx`, `s3x`, `codec`, `gate`)은 반드시 공유 패키지로 통합하고, 단일 서비스의 도메인 상태 머신만 `apps/<service>`에 둔다.
  - 이유: 검증·메트릭·타임아웃 코드가 서비스마다 복붙되면 버그 수정 시 일부 서비스만 누락되는 드리프트가 발생한다.
  - 검증: `ls libs/go/ libs/py/jammy_common/` 및 앱 내부 중복 구현 여부 검색(`rg -n "ReadHeaderTimeout|ZstdDecompressor" apps/`).
  - 출처: [Google SRE Service Best Practices](https://sre.google/sre-book/service-best-practices/)
- [ ] **환경변수·계약 기반 설정과 `_FILE` 시크릿 우선 규약**: 모든 설정은 `contracts/ENV.md`의 문법(`_SECONDS`, duration, size)과 `<NAME>_FILE` 우선 규약을 따르며, 잘못된 값은 기동 즉시 단일 오류 메시지로 fail-fast 종료한다.
  - 이유: 런타임 도중 뒤늦게 설정 오류로 실패하거나 시크릿을 CLI 인자/평문 env로만 받으면 장애 탐지와 보안이 약해진다.
  - 검증: `rg -n "_FILE|LoadCommon|CommonConfig" contracts/ENV.md contracts/testdata/env_cases.json libs/`.
  - 출처: [The Twelve-Factor App: Config](https://12factor.net/config)
- [ ] **버전 관리 계약과 하위 호환성 유지**: `contracts/proto`, `contracts/openapi`, `contracts/db/migrations` 변경 시 `make contracts-check`와 `buf breaking`으로 기존 클라이언트·파티션·모드(`mvp`/`split`)와의 호환성을 검증한다.
  - 이유: 현장 에이전트와 컨트롤 플레인이 순차 업그레이드되더라도 계약 깨짐 없이 공존할 수 있어야 한다.
  - 검증: `rg -n "contracts-check|breaking" Makefile buf.yaml .github/workflows/backend.yml`.
  - 출처: [Buf Breaking Rules](https://buf.build/docs/breaking/rules/)
- [ ] **테스트 피라미드와 계층별 실행 분리**: 빠른 단위·골든 픽스처 테스트(`gate_cases`, `env_cases`, `helm-unittest`)를 기본으로 두고, Testcontainers 기반 DB/NATS/S3 통합 테스트와 Compose/k3d E2E(`tests/e2e`)를 계층별로 분리해 유지한다.
  - 이유: 모든 검증을 무거운 E2E에만 의존하면 피드백 루프가 느려져 회귀 결함이 머지되기 쉽다.
  - 검증: `ls tests/e2e/ libs/py/tests/ deploy/helm/jammy-platform/tests/`.
  - 출처: [Google Testing Blog: Just Say No to More End-to-End Tests](https://testing.googleblog.com/2015/04/just-say-no-to-more-end-to-end-tests.html)
- [ ] **Go/Python 이중 구현 간 관측 완전성 패리티**: 동일 역할의 Go·Python 서비스(`ingest`, `processor`, `archiver`)가 동일한 메트릭 이름·라벨·히스토그램 버킷·JSON 로그 필드·트레이스 속성을 방출하는지 대조한다.
  - 이유: 구현체 전환(`impl: go` ↔ `py`)이나 비교 실험 시 대시보드와 알람이 깨지지 않아야 한다.
  - 검증: `rg -n "jammy_" contracts/METRICS.md apps/ libs/ observability/dashboards/`.
  - 출처: [OpenTelemetry Signals](https://opentelemetry.io/docs/concepts/signals/)
- [ ] **일관된 오류 분류체계(Error Taxonomy)**: 게이트 거절(`reason`), HTTP 4xx(클라이언트 수정 필요) vs 5xx(재시도 대상), 제어 경로 `result`(`acked`, `stale_epoch`, `timeout`, `error`), 독성 메시지 격리(`quarantine`, `Term`, DLQ)를 명확히 구분한다.
  - 이유: 영구 오류와 일시 오류를 구분하지 못하면 불필요한 재시도 폭풍이나 데이터 조용한 유실이 발생한다.
  - 검증: `rg -n "quarantine|stale_epoch|reason|HTTPError" contracts/METRICS.md libs/ apps/`.
  - 출처: [Google SRE Handling Overload](https://sre.google/sre-book/handling-overload/)
- [ ] **런북 친화성과 안전 가드(Runbook-Friendliness)**: 기동 실패 시 명확한 1줄 원인 출력, `/readyz` 컴포넌트별 JSON 상태 반환, 운영 수동 개입(`takeover --confirm`) 가드, `HANDOVER.md`·`VERSIONS.md` 문서화를 유지한다.
  - 이유: 온콜 엔지니어가 새벽 장애 시 소스 코드를 읽지 않고도 로그·프로브·런북만으로 즉시 진단·조치할 수 있어야 한다.
  - 검증: `ls docs/backend/ && rg -n "readyz|confirm" libs/go/admin/ libs/py/jammy_common/admin.py apps/dispatcher/`.
  - 출처: [Google SRE Postmortem Culture](https://sre.google/sre-book/postmortem-culture/)
- [ ] **의존성 위생(Dependency Hygiene)과 60일 성숙 정책**: 신규/업그레이드 버전은 [VERSIONS.md](./VERSIONS.md)의 60일 경과·패치·EOL 6개월 기준과 보안 패치 예외 기록을 준수하고, `go.mod`·`uv.lock`·이미지 다이제스트로 재현 가능하게 고정한다.
  - 이유: 검증되지 않은 `.0` 최신판 채택이나 미고정 의존성은 예기치 않은 회귀와 공급망 사고를 부른다.
  - 검증: `head -n 10 docs/backend/VERSIONS.md && rg -n "예외|보안 업데이트" docs/backend/VERSIONS.md`.
  - 출처: [SLSA Dependency Hygiene](https://slsa.dev/spec/v1.0/threats)
- [ ] **폐기(Deprecation)와 수명 주기 관리**: 런타임·라이브러리·Helm·DB의 EOL 6개월 전 전환 계획을 세우고, 과도기 모드(`PIPELINE_MODE=direct-db`, 구버전 proto 필드)에는 제거 조건과 유예 기간을 명시한다.
  - 이유: 기한 없는 레거시 모드 방치는 테스트 매트릭스와 유지보수 비용을 기하급수적으로 늘린다.
  - 검증: `rg -n "EOL|deprecated|PIPELINE_MODE" docs/backend/VERSIONS.md contracts/ENV.md`.
  - 출처: [PostgreSQL Versioning Policy](https://www.postgresql.org/support/versioning/)
- [ ] **코드 소유권 경계와 변경 영향 범위 통제**: `contracts/`, `libs/`, `deploy/helm/` 같은 공유 영역은 `.github/workflows/backend.yml`의 `paths-filter`(`shared`)와 명확한 소유권 경계(`CODEOWNERS` 등)로 보호해 양쪽 언어·전체 서비스 검증을 강제한다.
  - 이유: 한쪽 서비스 편의로 공유 계약이나 라이브러리를 고쳤을 때 다른 서비스가 깨지는 일을 방지한다.
  - 검증: `rg -n "shared:|paths-filter" .github/workflows/backend.yml`.
  - 출처: [GitHub CODEOWNERS](https://docs.github.com/en/repositories/managing-your-repositorys-settings-and-features/customizing-your-repository/about-code-owners)
