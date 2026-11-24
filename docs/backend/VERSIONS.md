JAMMY 백엔드의 런타임, 라이브러리, 도구, 서비스 이미지와 CI 버전 후보를 2026-11-23 기준으로 정한다. LTS를 우선하고, 최초 출시 후 약 60일이 지났고 .0 이후 패치가 나온 최신 계열의 마지막 패치를 선택하며, 조건을 못 채우면 이전 계열을 검토하고 EOL이 지났거나 6개월 이내인 계열은 제외한다.

설계 [§2·3·6·7](../design/specs/2026-11-23-backend-platform-design.md)과 [구현 계획 Tech Stack](../design/plans/2026-11-23-backend-platform.md)을 기준으로 검토했다. 59일은 약 60일에 포함했고, 6개월 기준일은 2027-05-23이다. pre-release와 yanked 릴리스는 최신판 비교에서 제외했다. Python OTel의 upstream 동반 beta는 별도 표기했다. 날짜는 upstream 공개일을 우선하고, PyPI 최초 업로드 또는 Go module proxy의 태그 소스 시간만 확인한 경우 그 날짜를 썼다. 이하 **양쪽 이미지**는 해당 태그 또는 digest에서 `linux/amd64`와 `linux/arm64`를 확인했다는 뜻이다.

2026-11-23 사용자 결정으로 Python 3.14를 사용한다. 필요한 라이브러리가 대상 아키텍처 중 하나에서 CPython 3.14 호환 wheel을 제공하지 않으면 Python 3.13을 fallback으로 사용한다. 같은 날 Go 1.27.4, Helm 4.3.2, NATS 2.15.3, PostgreSQL 17.12, VictoriaMetrics Community v1.156.0과 Grafana 13.3.1·Tempo 3.1.1·Python OTel 1.45.2/0.66b2의 선택 예외를 사용자가 승인했다.

## 런타임·베이스

| component | chosen version (exact) | newest available | chosen release date | why (rule clause) | compatibility notes | source URL |
|---|---|---|---|---|---|---|
| CPython | `3.14.10` | `3.15.2` | 2026-10-25 | 2026-11-23 사용자 결정 3.14 · 계열 최신 보안 패치 | 3.14.0은 2025-10-07 출시. 보안 지원 2030-10까지. | [릴리스](https://www.python.org/downloads/release/python-31410/) · [수명](https://peps.python.org/pep-0745/) · [최신](https://www.python.org/downloads/) |
| Python slim | `python:3.14.10-slim-trixie` | `3.14.10-slim-trixie / 3.14.10-slim-bookworm` | 2026-10-25 | 3.14 채택 · 같은 패치의 Debian 13 선택 | 양쪽 이미지. Bookworm도 양쪽 제공. Debian 13 LTS 종료 2030-06, Debian 12는 2028-06. glibc 기반 manylinux wheel 사용. | [이미지](https://hub.docker.com/_/python/tags?name=3.14.10-slim) · [Debian 수명](https://endoflife.date/debian) |
| Go toolchain | `go1.27.4` | `go1.27.4` | 2026-11-10 | 1.27.0 2026-08-19 출시 · 패치 | 최신 두 Go 계열을 지원하는 정책. 빌드 이미지 `golang:1.27.4-trixie`도 양쪽 이미지. parquet-go의 Go ≥1.26 충족. | [릴리스](https://go.dev/doc/devel/release#go1.27.4) · [이미지](https://hub.docker.com/_/golang/tags?name=1.27.4-trixie) |
| Distroless static nonroot | `gcr.io/distroless/static-debian13:nonroot@sha256:8ce6dd87e9823c5b48dcfb2c8604714ccb351ce59607327bcc4957448da279b5` | `static-debian13:nonroot` | 확인 못 함 | SemVer 예외 · Debian 13 계열과 OCI index digest 고정 | 양쪽 이미지 · UID 65532. `CGO_ENABLED=0` 정적 바이너리, Kubernetes HTTP probe 사용. | [공식 이미지 목록](https://github.com/GoogleContainerTools/distroless#what-images-are-available) · [레지스트리](https://gcr.io/v2/distroless/static-debian13/manifests/nonroot) |
| uv | `0.12.24` | `0.13.9` | 2026-10-08 | 상위 계열은 기간 또는 패치 조건 미달 · 2026-07-28 시작 | Python 3.14 workspace와 lock. CI에서 setup-uv의 `version`에 같은 값 지정. | [PyPI](https://pypi.org/project/uv/0.12.24/) · [이력](https://pypi.org/project/uv/#history) |

- **2026-11-24 Go 이미지 고정.** `ingest-go`, `processor-go`, `archiver-go`, `plantsim`의 기존 `golang:1.27.4-trixie` 빌드 태그를 amd64·arm64가 포함된 OCI index `sha256:c393c00864b56f4404b3fe4d029e47742fbb53ee11506521d2f3377009ad7a3a`로 고정했다. 실행 단계에는 고정된 Distroless static nonroot와 명시적인 UID·GID `65532:65532`를 사용한다. [Go 빌드 이미지](https://hub.docker.com/_/golang/tags?name=1.27.4-trixie)

### 2026-11-24 Python 이미지 보안 업데이트

- `instruction-api`, `dispatcher`, `portal-publisher`, `ingest-py`, `processor-py`, `archiver-py`의 빌드 및 런타임 베이스를 `python:3.14.10-slim-trixie@sha256:970b9cf6bb33bc54e34bff9263de57460841813dd5a3d83d3a4e183b21c54f8a`로 고정했다. CPython 3.14.10과 Debian 13 계열을 유지하며 Debian 포인트 릴리스는 13.7에서 13.8로 갱신됐다. OCI 인덱스에 `linux/amd64`, `linux/arm64`가 포함된다.
- `instruction-api`, `dispatcher`, `portal-publisher`의 uv 빌드 이미지도 기존 `0.12.24` 태그와 `sha256:3af4716e991d6956a41e573eab705d0ee08500cd829ed30293eb8472f372c65a`로 고정했다. OCI 인덱스에 `linux/amd64`, `linux/arm64`가 포함된다.
- 런타임에서 Debian 13의 보안 패치 `zlib1g` `1:1.3.dfsg+really1.3.1-1+deb13u1`을 적용하고, 가상환경을 빌드 단계에서 설치하므로 런타임의 pip, ensurepip, setuptools를 제거했다. [Debian CVE-2026-85091](https://security-tracker.debian.org/tracker/CVE-2026-85091)

### 2026-11-24 Known unfixed base CVEs

Trivy로 `0.1.0-secfix` 이미지의 `linux/arm64` Debian 13.8 런타임을 검사했다. HIGH/CRITICAL 미수정 항목은 패키지와 CVE 조합 기준 이미지당 53건이며, 고유 CVE는 13개다.

| CVE | Debian 패키지 |
| --- | --- |
| CVE-2025-69720 | `libncursesw6`, `libtinfo6`, `ncurses-base`, `ncurses-bin` |
| CVE-2026-9538 | `perl-base` |
| CVE-2026-16742 | `libsystemd0`, `libudev1` |
| CVE-2026-54369 | `libacl1` |
| CVE-2026-76642 | `bsdutils`, `libblkid1`, `liblastlog2-2`, `libmount1`, `libsmartcols1`, `libuuid1`, `login`, `mount`, `util-linux` |
| CVE-2026-78408 | `bsdutils`, `libblkid1`, `liblastlog2-2`, `libmount1`, `libsmartcols1`, `libuuid1`, `login`, `mount`, `util-linux` |
| CVE-2026-78409 | `bsdutils`, `libblkid1`, `liblastlog2-2`, `libmount1`, `libsmartcols1`, `libuuid1`, `login`, `mount`, `util-linux` |
| CVE-2026-78410 | `bsdutils`, `libblkid1`, `liblastlog2-2`, `libmount1`, `libsmartcols1`, `libuuid1`, `login`, `mount`, `util-linux` |
| CVE-2026-107494 | `libssl3t64`, `openssl`, `openssl-provider-legacy` |
| CVE-2026-119373 | `libsqlite3-0` |
| CVE-2026-119374 | `libsqlite3-0` |
| CVE-2026-119379 | `libsqlite3-0` |
| CVE-2026-124709 | `libssl3t64`, `openssl`, `openssl-provider-legacy` |

## Python 라이브러리

| component | chosen version (exact) | newest available | chosen release date | why (rule clause) | compatibility notes | source URL |
|---|---|---|---|---|---|---|
| fastapi | `0.141.1` | `0.165.0` | 2026-07-29 | 상위 계열은 기간 또는 패치 조건 미달 · 2026-07-29 시작 | Python ≥3.10. Pydantic ≥2.9 및 HTTPX 0.28.1과 해석 성공. | [PyPI](https://pypi.org/project/fastapi/0.141.1/) · [이력](https://pypi.org/project/fastapi/#history) |
| uvicorn | `0.52.4` | `0.54.0` | 2026-08-19 | 상위 계열은 기간 또는 패치 조건 미달 · 2026-07-29 시작 | Python ≥3.10. ASGI 앱 서버. 기본 패키지 기준으로 해석. | [PyPI](https://pypi.org/project/uvicorn/0.52.4/) · [이력](https://pypi.org/project/uvicorn/#history) |
| pydantic | `2.13.5` | `2.14.4` | 2026-08-28 | 상위 계열은 기간 또는 패치 조건 미달 · 2026-04-13 시작 | Python ≥3.9. `pydantic-core==2.46.5`가 양쪽 `cp314` wheel 제공. | [PyPI](https://pypi.org/project/pydantic/2.13.5/) · [이력](https://pypi.org/project/pydantic/#history) |
| psycopg | `3.3.9` | `3.3.9` | 2026-11-12 | 2025-12-01 시작 · 패치 | Python ≥3.10 · PostgreSQL 17 지원. `psycopg[binary]==3.3.9`는 동일 binary 패치 요구. | [PyPI](https://pypi.org/project/psycopg/3.3.9/) · [이력](https://pypi.org/project/psycopg/#history) |
| psycopg-binary | `3.3.9` | `3.3.9` | 2026-11-12 | 2025-12-01 시작 · 패치 | 양쪽 `cp314` manylinux wheel · 번들 libpq. | [PyPI](https://pypi.org/project/psycopg-binary/3.3.9/) · [이력](https://pypi.org/project/psycopg-binary/#history) |
| psycopg-pool | `3.3.3` | `3.3.3` | 2026-09-22 | 2025-12-01 시작 · 패치 | Python ≥3.10 · 독립 릴리스. psycopg 3.3.9와 해석 성공. | [PyPI](https://pypi.org/project/psycopg-pool/3.3.3/) · [이력](https://pypi.org/project/psycopg-pool/#history) |
| nats-py | `2.13.1` | `2.17.0` | 2026-02-05 | 상위 계열은 기간 또는 패치 조건 미달 · 2026-02-04 시작 | Python ≥3.7 · NATS 2.15.3 기본 JetStream API 사용 범위. 2.15 신규 기능·동작은 별도 검증. | [PyPI](https://pypi.org/project/nats-py/2.13.1/) · [이력](https://pypi.org/project/nats-py/#history) |
| pyarrow | `25.0.1` | `26.0.1` | 2026-08-10 | 상위 계열은 기간 또는 패치 조건 미달 · 2026-07-10 시작 | Python ≥3.10 · 양쪽 `cp314` manylinux_2_28 wheel · Parquet/ZSTD. | [PyPI](https://pypi.org/project/pyarrow/25.0.1/) · [이력](https://pypi.org/project/pyarrow/#history) |
| boto3 | `1.43.127` | `1.44.9` | 2026-11-09 | 상위 계열은 기간 또는 패치 조건 미달 · 2026-04-29 시작 | Python ≥3.10 · botocore ≥1.43.127,<1.44.0. 로컬 서비스별 endpoint URL 지정. | [PyPI](https://pypi.org/project/boto3/1.43.127/) · [이력](https://pypi.org/project/boto3/#history) |
| zstandard | `0.25.0` | `0.25.0` | 2025-09-14 | 예외 · 최근 0.x 계열에 패치 없음 · 3.14 wheel 우선 | Python ≥3.9 · 양쪽 `cp314` manylinux_2_17 wheel. 0.15.2에는 CPython 3.14 호환 wheel이 없다. | [PyPI](https://pypi.org/project/zstandard/0.25.0/) · [이력](https://pypi.org/project/zstandard/#history) |
| protobuf | `7.36.2` | `7.36.2` | 2026-09-17 | 2026-08-20 시작 · 패치 | Python ≥3.10 · 양쪽 `cp310-abi3` manylinux wheel로 CPython 3.14 지원. Buf Python/pyi v36.2 gencode와 대응. | [PyPI](https://pypi.org/project/protobuf/7.36.2/) · [이력](https://pypi.org/project/protobuf/#history) |
| opentelemetry-api | `1.45.2` | `1.45.2` | 2026-10-29 | 2026-09-25 시작(59일) · 패치 · 동일 릴리스 묶음 | Python ≥3.10 · SDK와 동일 안정판. | [PyPI](https://pypi.org/project/opentelemetry-api/1.45.2/) · [이력](https://pypi.org/project/opentelemetry-api/#history) |
| opentelemetry-sdk | `1.45.2` | `1.45.2` | 2026-10-29 | 2026-09-25 시작(59일) · 패치 · 동일 릴리스 묶음 | Python ≥3.10 · API 1.45.2와 semantic-conventions 0.66b2 요구. | [PyPI](https://pypi.org/project/opentelemetry-sdk/1.45.2/) · [이력](https://pypi.org/project/opentelemetry-sdk/#history) |
| opentelemetry-exporter-prometheus | `0.66b2` | `0.66b2` | 2026-10-29 | 2026-09-25 시작(59일) · 동반 beta b2 · 동일 릴리스 묶음 | SDK 1.45.x와 prometheus-client <1.0.0. Alloy scrape용. | [PyPI](https://pypi.org/project/opentelemetry-exporter-prometheus/0.66b2/) · [이력](https://pypi.org/project/opentelemetry-exporter-prometheus/#history) |
| opentelemetry-exporter-otlp | `1.45.2` | `1.45.2` | 2026-10-29 | 2026-09-25 시작(59일) · 패치 · 동일 릴리스 묶음 | gRPC/HTTP와 `opentelemetry-proto==1.45.2` 동반. protobuf ≥5,<8. | [PyPI](https://pypi.org/project/opentelemetry-exporter-otlp/1.45.2/) · [이력](https://pypi.org/project/opentelemetry-exporter-otlp/#history) |
| opentelemetry-instrumentation-fastapi | `0.66b2` | `0.66b2` | 2026-10-29 | 2026-09-25 시작(59일) · 동반 beta b2 · 동일 릴리스 묶음 | ASGI·semantic conventions 0.66b2. FastAPI와 의존성 해석 통과. | [PyPI](https://pypi.org/project/opentelemetry-instrumentation-fastapi/0.66b2/) · [이력](https://pypi.org/project/opentelemetry-instrumentation-fastapi/#history) |
| opentelemetry-instrumentation-psycopg | `0.66b2` | `0.66b2` | 2026-10-29 | 2026-09-25 시작(59일) · 동반 beta b2 · 동일 릴리스 묶음 | DB API instrumentation 0.66b2 · psycopg 3.3.9. | [PyPI](https://pypi.org/project/opentelemetry-instrumentation-psycopg/0.66b2/) · [이력](https://pypi.org/project/opentelemetry-instrumentation-psycopg/#history) |
| prometheus-client | `0.24.1` | `0.26.0` | 2026-01-14 | 상위 계열은 기간 또는 패치 조건 미달 · 2026-01-12 시작 | Python ≥3.9 · exporter 의존성. 앱 자체 metric 등록 시에도 같은 버전 사용. | [PyPI](https://pypi.org/project/prometheus-client/0.24.1/) · [이력](https://pypi.org/project/prometheus-client/#history) |
| pytest | `9.1.1` | `9.2.0` | 2026-06-19 | 상위 계열은 기간 또는 패치 조건 미달 · 2026-06-13 시작 | Python ≥3.10 · Testcontainers 연동 테스트. | [PyPI](https://pypi.org/project/pytest/9.1.1/) · [이력](https://pypi.org/project/pytest/#history) |
| testcontainers | `4.14.2` | `4.15.0` | 2026-03-18 | 상위 계열은 기간 또는 패치 조건 미달 · 2026-01-07 시작 | Python ≥3.10 · Docker Desktop ARM64는 Docker API와 소켓 접근 필요. Ryuk 별도 확인 항목 참조. | [PyPI](https://pypi.org/project/testcontainers/4.14.2/) · [이력](https://pypi.org/project/testcontainers/#history) |
| ruff | `0.16.10` | `0.17.6` | 2026-10-01 | 상위 계열은 기간 또는 패치 조건 미달 · 2026-07-23 시작 | Python ≥3.7 · `target-version = "py314"`. 린트 도구. | [PyPI](https://pypi.org/project/ruff/0.16.10/) · [이력](https://pypi.org/project/ruff/#history) |
| httpx | `0.28.1` | `0.28.1` | 2024-12-06 | 2024-11-28 시작 · 패치 | Python ≥3.8 · ASGI 테스트는 `AsyncClient(transport=ASGITransport(app=app))`. Starlette 1.10.0의 `TestClient`에서 HTTPX fallback은 deprecated. | [PyPI](https://pypi.org/project/httpx/0.28.1/) · [이력](https://pypi.org/project/httpx/#history) |

### 2026-11-24 nats-py 업그레이드 보류

`flush()` 취소 또는 타임아웃 뒤 PONG 처리 오류는 CVE가 연결되지 않은 upstream 이슈 [nats.py #1030](https://github.com/nats-io/nats.py/issues/1030)으로 추적한다. 수정이 포함된 `2.17.0`은 2026-11-17 출시로 7일이 지났고 `2.17.x` 후속 패치가 없어 버전 정책에 따라 `2.13.1`과 기존 lock을 유지한다. JetStream 헬스 체크는 `is_connected`와 `stream_info`를 사용한다. [2.17.0 릴리스 내역](https://github.com/nats-io/nats.py/releases/tag/v2.17.0) · [PyPI 릴리스 이력](https://pypi.org/project/nats-py/#history)

## Go 라이브러리·린터

| component | chosen version (exact) | newest available | chosen release date | why (rule clause) | compatibility notes | source URL |
|---|---|---|---|---|---|---|
| github.com/jackc/pgx/v5 | `v5.9.2` | `v5.13.0` | 2026-04-19 | 상위 계열은 기간 또는 패치 조건 미달 · 2026-03-21 시작 | Go ≥1.25 · v5.9.2의 공식 CI 매트릭스에 PostgreSQL 17 포함. 현장 COPY·풀·SCRAM 검증은 별도. | [Go 모듈](https://pkg.go.dev/github.com/jackc/pgx/v5@v5.9.2) · [버전 목록](https://proxy.golang.org/github.com/jackc/pgx/v5/@v/list) |
| github.com/nats-io/nats.go | `v1.53.1` | `v1.56.0` | 2026-08-11 | 상위 계열은 기간 또는 패치 조건 미달 · 2026-08-11 시작 | Go ≥1.25 · `jetstream` 패키지 사용. 2.15.3과 사용 범위는 기본 stream/consumer API. | [Go 모듈](https://pkg.go.dev/github.com/nats-io/nats.go@v1.53.1) · [버전 목록](https://proxy.golang.org/github.com/nats-io/nats.go/@v/list) |
| github.com/parquet-go/parquet-go | `v0.32.1` | `v0.32.1` | 2026-11-17 | 2026-08-10 시작 · 패치 | Go ≥1.26 · compress ≥1.20.4와 protobuf v1.36.12 요구. | [Go 모듈](https://pkg.go.dev/github.com/parquet-go/parquet-go@v0.32.1) · [버전 목록](https://proxy.golang.org/github.com/parquet-go/parquet-go/@v/list) |
| github.com/aws/aws-sdk-go-v2 | `v1.47.8` | `v1.48.0` | 2026-11-06 | 상위 계열은 기간 또는 패치 조건 미달 · 2026-09-09 시작 | Go 1.27.4 사용. S3·DynamoDB는 ≥v1.47.1, SQS는 ≥v1.47.8을 요구한다. | [Go 모듈](https://pkg.go.dev/github.com/aws/aws-sdk-go-v2@v1.47.8) · [버전 목록](https://proxy.golang.org/github.com/aws/aws-sdk-go-v2/@v/list) |
| github.com/aws/aws-sdk-go-v2/service/s3 | `v1.113.4` | `v1.116.0` | 2026-09-24 | 상위 계열은 기간 또는 패치 조건 미달 · 2026-09-09 시작 | SeaweedFS에서 서명과 multipart를 확인. 로컬 endpoint는 `UsePathStyle` 지정. | [Go 모듈](https://pkg.go.dev/github.com/aws/aws-sdk-go-v2/service/s3@v1.113.4) · [버전 목록](https://proxy.golang.org/github.com/aws/aws-sdk-go-v2/service/s3/@v/list) |
| github.com/aws/aws-sdk-go-v2/service/dynamodb | `v1.69.1` | `v1.74.1` | 2026-09-24 | 상위 계열은 기간 또는 패치 조건 미달 · 2026-09-15 시작 | DynamoDB Local endpoint 사용. 조건부 쓰기와 lease 동작 확인 필요. | [Go 모듈](https://pkg.go.dev/github.com/aws/aws-sdk-go-v2/service/dynamodb@v1.69.1) · [버전 목록](https://proxy.golang.org/github.com/aws/aws-sdk-go-v2/service/dynamodb/@v/list) |
| github.com/aws/aws-sdk-go-v2/service/sqs | `v1.52.11` | `v1.53.0` | 2026-11-06 | 상위 계열은 기간 또는 패치 조건 미달 · 2026-09-09 시작 | ElasticMQ endpoint와 region 지정. visibility timeout 및 삭제 동작 확인 필요. | [Go 모듈](https://pkg.go.dev/github.com/aws/aws-sdk-go-v2/service/sqs@v1.52.11) · [버전 목록](https://proxy.golang.org/github.com/aws/aws-sdk-go-v2/service/sqs/@v/list) |
| github.com/aws/aws-sdk-go-v2/config | `v1.33.16` | 확인 안 함 | 확인 안 함 | 고정한 SDK v1.47.8과 같은 release commit (T2 codex 확인, Claude 승인 2026-11-23) | AWS 표준 자격 증명 체인. | [Go 모듈](https://pkg.go.dev/github.com/aws/aws-sdk-go-v2/config@v1.33.16) |
| github.com/prometheus/client_golang | `v1.24.1` | 확인 안 함 | 확인 안 함 | OTel Prometheus exporter v0.69.0의 요구 버전. 기존 선택 v1.24.1 유지 (Claude 승인 2026-11-23) | admin `/metrics` handler. | [Go 모듈](https://pkg.go.dev/github.com/prometheus/client_golang@v1.24.1) |
| github.com/klauspost/compress | `v1.20.5` | `v1.20.5` | 2026-11-19 | 2026-09-02 시작 · 패치 | Go ≥1.25 · nats.go와 parquet-go가 요구하는 하한을 충족. Zstd. | [Go 모듈](https://pkg.go.dev/github.com/klauspost/compress@v1.20.5) · [버전 목록](https://proxy.golang.org/github.com/klauspost/compress/@v/list) |
| google.golang.org/protobuf | `v1.36.12` | `v1.36.12` | 2026-08-10 | 2024-12-16 시작 · 패치 | Buf Go generator v1.36.12와 동일. OTel·parquet-go가 요구하는 버전. | [Go 모듈](https://pkg.go.dev/google.golang.org/protobuf@v1.36.12) · [버전 목록](https://proxy.golang.org/google.golang.org/protobuf/@v/list) |
| go.opentelemetry.io/otel | `v1.47.0` | `v1.47.0` | 2026-10-02 | 보안 패치 예외 · 2026-11-25 사용자 승인 | API. SDK 및 exporter의 go.mod와 일치. | [Go 모듈](https://pkg.go.dev/go.opentelemetry.io/otel@v1.47.0) · [버전 목록](https://proxy.golang.org/go.opentelemetry.io/otel/@v/list) |
| go.opentelemetry.io/otel/sdk | `v1.47.0` | `v1.47.0` | 2026-10-02 | 보안 패치 예외 · 2026-11-25 사용자 승인 | Trace SDK. | [Go 모듈](https://pkg.go.dev/go.opentelemetry.io/otel/sdk@v1.47.0) · [버전 목록](https://proxy.golang.org/go.opentelemetry.io/otel/sdk/@v/list) |
| go.opentelemetry.io/otel/sdk/metric | `v1.47.0` | `v1.47.0` | 2026-10-02 | 보안 패치 예외 · 2026-11-25 사용자 승인 | Metric SDK 및 API `otel/metric`을 같은 버전으로 맞춤. | [Go 모듈](https://pkg.go.dev/go.opentelemetry.io/otel/sdk/metric@v1.47.0) · [버전 목록](https://proxy.golang.org/go.opentelemetry.io/otel/sdk/metric/@v/list) |
| go.opentelemetry.io/otel/exporters/prometheus | `v0.69.0` | `v0.69.0` | 2026-10-02 | 보안 패치 예외 · 2026-11-25 사용자 승인 | Go OTel v1.47.0과 같은 릴리스. Alloy가 scrape. | [Go 모듈](https://pkg.go.dev/go.opentelemetry.io/otel/exporters/prometheus@v0.69.0) · [버전 목록](https://proxy.golang.org/go.opentelemetry.io/otel/exporters/prometheus/@v/list) |
| go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc | `v1.47.0` | `v1.47.0` | 2026-10-02 | 보안 패치 예외 · 2026-11-25 사용자 승인 | OTLP trace → Alloy. | [Go 모듈](https://pkg.go.dev/go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc@v1.47.0) · [버전 목록](https://proxy.golang.org/go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc/@v/list) |
| go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc | `v1.47.0` | `v1.47.0` | 2026-10-02 | 보안 패치 예외 · 2026-11-25 사용자 승인 | OTLP metric이 필요한 경우 동일 release 적용. 기본 경로는 Prometheus scrape. | [Go 모듈](https://pkg.go.dev/go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc@v1.47.0) · [버전 목록](https://proxy.golang.org/go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc/@v/list) |
| go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploggrpc | `v0.23.0` | `v0.23.0` | 2026-10-02 | 보안 패치 예외 · 2026-11-25 사용자 승인 | OTLP log → Alloy. Log API·SDK는 안정판 v1.47.0. | [Go 모듈](https://pkg.go.dev/go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploggrpc@v0.23.0) · [버전 목록](https://proxy.golang.org/go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploggrpc/@v/list) |
| github.com/testcontainers/testcontainers-go | `v0.44.0` | `v0.45.0` | 2026-08-07 | 예외 · 최근 계열에 패치 없음 · 0.45.0이 60일 미만 | Go ≥1.25 · Docker Desktop Mac 공식 지원. Ryuk·테스트 대상 이미지는 ARM64 필요. | [Go 모듈](https://pkg.go.dev/github.com/testcontainers/testcontainers-go@v0.44.0) · [버전 목록](https://proxy.golang.org/github.com/testcontainers/testcontainers-go/@v/list) |
| github.com/golangci/golangci-lint/v2 | `v2.14.0` | `v2.15.0` | 확인 안 함 | 예외(Claude 승인 2026-11-23): v2.13.2는 x/tools v0.49.0이라 Go 1.27.4의 export-data V5를 읽지 못해 lint가 실패(T2 codex 실측). v2.14.0은 x/tools v0.50.0 | Go 1.27.4와 함께 사용. | [Go 모듈](https://pkg.go.dev/github.com/golangci/golangci-lint/v2@v2.13.2) · [버전 목록](https://proxy.golang.org/github.com/golangci/golangci-lint/v2/@v/list) |

- **2026-11-24 보안 업데이트.** `google.golang.org/grpc`를 `v1.83.1`에서 `v1.83.2`로 올려 CVE-2026-84445를 수정했다. 1.83.0은 2026-07-30에 출시됐고 1.83.2는 2026-08-25에 나온 해당 계열의 최신 패치다. 상위 계열 1.84와 1.85에는 안정 패치가 없다. [Go 모듈 이력](https://proxy.golang.org/google.golang.org/grpc/@v/list) · [수정 릴리스](https://github.com/grpc/grpc-go/releases/tag/v1.83.2)
- **2026-11-24 보안 패치 예외 (2026-11-25 사용자 승인).** 애플리케이션에서 호출되는 코드에 HIGH 또는 CRITICAL CVE가 있으면, 출시 후 약 60일을 기다리지 않고 수정판을 적용할 수 있도록 예외를 적용한다(2026-11-25 사용자 승인).

| CVE | 모듈 | 최초 수정 버전 | 출시일 |
| --- | --- | --- | --- |
| [CVE-2026-128085](https://pkg.go.dev/vuln/GO-2026-6974) | Go OTel | [`v1.47.0`](https://github.com/open-telemetry/opentelemetry-go/releases/tag/v1.47.0) | 2026-10-02 |
| [CVE-2026-78659](https://pkg.go.dev/vuln/GO-2026-6603) | `golang.org/x/net` | [`v0.60.0`](https://proxy.golang.org/golang.org/x/net/@v/v0.60.0.info) | 2026-10-08 |
| [CVE-2026-78660](https://pkg.go.dev/vuln/GO-2026-6610) | `golang.org/x/net` | `v0.60.0` | 2026-10-08 |
| [CVE-2026-78669](https://pkg.go.dev/vuln/GO-2026-6611) | `golang.org/x/net` | `v0.60.0` | 2026-10-08 |
| [CVE-2026-78663](https://pkg.go.dev/vuln/GO-2026-6612) | `golang.org/x/net` | `v0.60.0` | 2026-10-08 |
| [CVE-2026-97032](https://pkg.go.dev/vuln/GO-2026-6617) | `golang.org/x/net` | `v0.60.0` | 2026-10-08 |
| [CVE-2026-78661](https://pkg.go.dev/vuln/GO-2026-6860) | `golang.org/x/net` | [`v0.62.0`](https://proxy.golang.org/golang.org/x/net/@v/v0.62.0.info) | 2026-10-27 |

`golang.org/x/net`은 위 수정판을 모두 포함하는 `v0.62.0`을 적용했다. Go OTel에서 함께 해결한 CVE-2026-128085의 upstream 심각도는 MEDIUM이다. Log API·SDK는 안정판 `v1.47.0`으로 전환했고, OTLP log exporter `v0.23.0`과 Prometheus exporter `v0.69.0`은 같은 릴리스의 동반 버전이다. [OTel 보안 공지](https://github.com/open-telemetry/opentelemetry-go/security/advisories/GHSA-q7gw-24fr-5p69) · [OTel 릴리스](https://github.com/open-telemetry/opentelemetry-go/releases/tag/v1.47.0)

## 빌드·검증·배포 도구

| component | chosen version (exact) | newest available | chosen release date | why (rule clause) | compatibility notes | source URL |
|---|---|---|---|---|---|---|
| github.com/bufbuild/buf | `v1.68.4` | `v1.75.0` | 2026-04-22 | 상위 계열은 기간 또는 패치 조건 미달 · 2026-04-14 시작 | `buf.gen.yaml` v2 사용. Go/Python remote plugin 버전을 생성 설정에 고정. | [Go 모듈](https://pkg.go.dev/github.com/bufbuild/buf@v1.68.4) · [버전 목록](https://proxy.golang.org/github.com/bufbuild/buf/@v/list) |
| Buf remote plugins | `protocolbuffers/go:v1.36.12 / protocolbuffers/python:v36.2 / protocolbuffers/pyi:v36.2` | `v1.36.12 / v36.2 / v36.2` | 2026-08-10 / 2026-09-17 / 2026-09-17 | generator를 채택 runtime과 같은 upstream 릴리스로 고정 | Python 7.36.2 ↔ protoc v36.2, Go v1.36.12 ↔ protoc-gen-go v1.36.12. 날짜는 generator upstream 기준. | [Go](https://buf.build/protocolbuffers/go) · [Python](https://buf.build/protocolbuffers/python) · [pyi](https://buf.build/protocolbuffers/pyi) · [runtime 보장](https://protobuf.dev/support/cross-version-runtime-guarantee/) |
| golang-migrate | `migrate/migrate:v4.20.1` | `v4.20.1` | 2026-09-09 | 4.20.0 2026-09-08 출시 · 패치 | 양쪽 이미지 · PostgreSQL driver와 별도 migration Job. | [릴리스](https://github.com/golang-migrate/migrate/releases/tag/v4.20.1) · [이미지](https://hub.docker.com/r/migrate/migrate/tags?name=v4.20.1) |
| k6 | `grafana/k6:1.8.3` | `v2.4.0` | 2026-11-13 | 2.x의 2.4·2.3·2.2·2.1·2.0은 .0만 확인 · 1.8 보안 패치 | 양쪽 이미지 · 컨테이너에서 HTTP 배치 부하 시험. | [릴리스](https://github.com/grafana/k6/releases/tag/v1.8.3) · [이미지](https://hub.docker.com/r/grafana/k6/tags?name=1.8.3) |
| Helm 4 | `v4.3.2` | `v4.3.2` | 2026-11-13 | 4.3.0 2026-09-09 출시 · 패치 · 1.37 지원 | 공식 skew 1.34–1.37. Helm 3 보안 지원은 2027-02-10까지. | [릴리스](https://github.com/helm/helm/releases/tag/v4.3.2) · [version skew](https://helm.sh/docs/topics/version_skew/) · [Helm 3 전환 공지](https://docs.helm.sh/blog/helm-v3-end-of-life/) |
| helm-unittest | `v1.1.2` | `v1.2.1` | 2026-07-24 | 상위 계열은 기간 또는 패치 조건 미달 · 2026-05-08 시작 | Helm 4 설치 경로와 1.1.x OCI 패키지 제공. 고정 버전의 서명된 HTTP archive 사용 가능. | [릴리스](https://github.com/helm-unittest/helm-unittest/releases/tag/v1.1.2) |
| github.com/yannh/kubeconform | `v0.6.7` | `v0.8.0` | 2024-07-30 | 상위 계열은 기간 또는 패치 조건 미달 · 2023-02-26 시작 | `-kubernetes-version 1.37.2 -strict`. 1.37.2 standalone-strict 스키마 확인. 아래 재현 경로 참조. | [Go 모듈](https://pkg.go.dev/github.com/yannh/kubeconform@v0.6.7) · [버전 목록](https://proxy.golang.org/github.com/yannh/kubeconform/@v/list) |
| k3d | `v5.8.3` | `v5.9.0` | 2025-02-15 | 상위 계열은 기간 또는 패치 조건 미달 · 2025-01-17 시작 | 기본 k3s 대신 `--image rancher/k3s:v1.37.2-k3s1`. 실제 조합 실행 확인 필요. | [릴리스](https://github.com/k3d-io/k3d/releases/tag/v5.8.3) |
| k3d k3s node image | `rancher/k3s:v1.37.2-k3s1` | `v1.37.2+k3s1` | 2026-11-03 | 1.37.0+k3s1 2026-09-14 출시 · 패치 · 현장과 동일 | 양쪽 이미지. release tag의 `+`가 Docker tag에서는 `-`. | [릴리스](https://github.com/k3s-io/k3s/releases/tag/v1.37.2%2Bk3s1) · [이미지](https://hub.docker.com/r/rancher/k3s/tags?name=v1.37.2-k3s1) |
| Trivy | `v0.76.2` | `v0.76.2` | 2026-11-05 | 예외 · 0.76.0은 40일 경과 · upstream 권고 보안 패치 | trivy-action의 `version`으로 고정. 선택적으로 `aquasec/trivy:0.76.2`도 양쪽 이미지. | [보안 릴리스](https://github.com/aquasecurity/trivy/releases/tag/v0.76.2) · [이미지](https://hub.docker.com/r/aquasec/trivy/tags?name=0.76.2) |
| github.com/anchore/syft | `v1.51.1` | `v1.57.2` | 2026-08-27 | 상위 계열은 기간 또는 패치 조건 미달 · 2026-08-10 시작 | sbom-action의 `syft-version` 입력에 동일 값 지정. SPDX JSON. | [Go 모듈](https://pkg.go.dev/github.com/anchore/syft@v1.51.1) · [버전 목록](https://proxy.golang.org/github.com/anchore/syft/@v/list) |
| github.com/sigstore/cosign/v3 | `v3.1.4` | `v3.1.4` | 2026-10-20 | 2026-06-05 시작 · 패치 | cosign-installer의 `cosign-release` 입력과 동일. keyless 서명에 GitHub OIDC 필요. | [Go 모듈](https://pkg.go.dev/github.com/sigstore/cosign/v3@v3.1.4) · [버전 목록](https://proxy.golang.org/github.com/sigstore/cosign/v3/@v/list) |
| github.com/rhysd/actionlint | `v1.7.12` | `v1.7.12` | 2026-03-30 | 2024-05-08 시작 · 패치 | Actions YAML 정적 검사. runner/서비스 실행 검증은 별도. | [Go 모듈](https://pkg.go.dev/github.com/rhysd/actionlint@v1.7.12) · [버전 목록](https://proxy.golang.org/github.com/rhysd/actionlint/@v/list) |
| @redocly/cli | `2.54.3` | `2.82.0` | 2026-09-25 | 2.54.0 2026-09-22 출시 · 상위 적격 계열에 패치 없음 | OpenAPI lint. npm ≥10, Node ≥22.12 또는 20.19 이상 21 미만. | [npm](https://www.npmjs.com/package/@redocly/cli/v/2.54.3) · [npm registry](https://registry.npmjs.org/@redocly%2fcli) |

- **2026-11-24 마이그레이션 이미지.** `migrate/migrate:v4.20.1` 대신 같은 `golang-migrate v4.20.1` 소스를 Go 1.27.4로 정적 빌드한다. 빌드 베이스는 `golang:1.27.4-trixie@sha256:c393c00864b56f4404b3fe4d029e47742fbb53ee11506521d2f3377009ad7a3a`, 실행 베이스는 위에서 승인한 Distroless static nonroot index다. PostgreSQL과 내장 file 드라이버를 포함하고 upstream의 테스트용 stub 등록을 제외한다. [upstream 소스](https://github.com/golang-migrate/migrate/tree/v4.20.1/cmd/migrate) · [Go 빌드 이미지](https://hub.docker.com/_/golang/tags?name=1.27.4-trixie)

- **2026-11-24 Helm 차트**. `jammy-platform`의 애플리케이션 버전 `appVersion`을 차트가 배포하는 JAMMY `0.1.0`으로 명시했다.

## 서비스 이미지

| component | chosen version (exact) | newest available | chosen release date | why (rule clause) | compatibility notes | source URL |
|---|---|---|---|---|---|---|
| PostgreSQL | `postgres:17.12-trixie` | `19.0 (17 계열 17.12)` | 2026-11-12 | 현장 major 17 유지 · 최신 보안 패치 · EOL 2029-11-08 | 양쪽 이미지. 현장 17.7과 같은 major, 로컬 primary·standby는 동일 패치. 19.0은 60일·패치 미달. | [릴리스](https://www.postgresql.org/docs/17/release-17-12.html) · [수명](https://www.postgresql.org/support/versioning/) · [이미지](https://hub.docker.com/_/postgres/tags?name=17.12-trixie) |
| NATS server | `nats:2.15.3` | `v2.15.3` | 2026-11-11 | 2026-09-17 시작 · 패치 | 양쪽 이미지. 2.15는 현재 지원 계열. 2.10 성능 수치와 새 버전 결과에는 서버 버전을 기록. | [릴리스](https://github.com/nats-io/nats-server/releases/tag/v2.15.3) · [이미지](https://hub.docker.com/_/nats/tags?name=2.15.3) · [지원 계열](https://docs.nats.io/release-notes/) |
| SeaweedFS | `chrislusf/seaweedfs:4.54` | `4.54` | 2026-11-16 | 예외 · 두 자리 릴리스 체계로 .0 후 patch 조건 적용 불가 | 양쪽 이미지. S3 endpoint·path-style·multipart를 검증할 로컬 대체 서비스. | [릴리스](https://github.com/seaweedfs/seaweedfs/releases/tag/4.54) · [이미지](https://hub.docker.com/r/chrislusf/seaweedfs/tags?name=4.54) |
| ElasticMQ native | `softwaremill/elasticmq-native:1.7.1` | `v1.7.1` | 2026-04-05 | 2026-04-04 시작 · 패치 | 양쪽 이미지 · Native SQS emulator. 실 AWS SQS의 권한과 동작 검증은 별도. | [릴리스](https://github.com/softwaremill/elasticmq/releases/tag/v1.7.1) · [이미지](https://hub.docker.com/r/softwaremill/elasticmq-native/tags?name=1.7.1) |
| DynamoDB Local | `amazon/dynamodb-local:3.3.2` | `3.3.2` | 2026-10-15 | 3.3.0 2026-01-19 출시 · 보안 패치 | 양쪽 이미지. 로컬 endpoint를 명시해 AWS SDK를 연결. | [AWS 릴리스](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DynamoDBLocalHistory.html) · [이미지](https://hub.docker.com/r/amazon/dynamodb-local/tags?name=3.3.2) |
| VictoriaMetrics single | `victoriametrics/victoria-metrics:v1.156.0` | `v1.156.0` | 2026-11-06 | 예외 · Community 최신 유지보수판 · LTS 패치는 Enterprise | 양쪽 이미지. Alloy에서 Prometheus Remote Write 1.0 → `/api/v1/write`, Grafana에서는 Prometheus datasource. | [릴리스](https://github.com/VictoriaMetrics/VictoriaMetrics/releases/tag/v1.156.0) · [변경 이력](https://docs.victoriametrics.com/changelog/) · [LTS](https://docs.victoriametrics.com/victoriametrics/lts-releases/) · [이미지](https://hub.docker.com/r/victoriametrics/victoria-metrics/tags?name=v1.156.0) |
| Grafana | `grafana/grafana:13.3.1` | `v13.3.1` | 2026-11-10 | 예외 · 13.3은 60일 미만 · 13.2 EOL 2027-05-18 | 양쪽 이미지 · 13.2.5에서 13.3.1로 변경 승인. Loki·Tempo datasource 및 기존 dashboard JSON 확인 필요. | [릴리스](https://github.com/grafana/grafana/releases/tag/v13.3.1) · [이미지](https://hub.docker.com/r/grafana/grafana/tags?name=13.3.1) · [지원 정책](https://grafana.com/docs/grafana/latest/upgrade-guide/when-to-upgrade/) |
| Loki | `grafana/loki:3.7.10` | `3.7.10` | 2026-11-13 | 3.7.0 2026-03-26 출시 · 패치 · 4.0은 RC | 양쪽 이미지 · 기존 3.7.7과 같은 계열. OTLP 로그는 TSDB v13 및 structured metadata 활성화 필요. | [릴리스](https://grafana.com/docs/loki/latest/release-notes/v3-7/) · [이미지](https://hub.docker.com/r/grafana/loki/tags?name=3.7.10) |
| Tempo | `grafana/tempo:3.1.1` | `v3.1.1` | 2026-10-21 | 예외 · 3.1은 60일 미만 · 3.0 패치 종료 2027-02-28 | 양쪽 이미지 · monolithic `-target=all`, OTLP 수신과 Grafana query endpoint 사용. | [릴리스](https://github.com/grafana/tempo/releases/tag/v3.1.1) · [이미지](https://hub.docker.com/r/grafana/tempo/tags?name=3.1.1) · [권장 버전](https://grafana.com/docs/tempo/latest/set-up-for-tracing/setup-tempo/recommended-versions/) |
| Grafana Alloy | `grafana/alloy:v1.20.1` | `v1.22.0` | 2026-09-28 | 1.20.0 2026-09-25 출시(59일) · 패치 · 상위는 기간 미달 | 양쪽 이미지 · OTLP 수신, Tempo로 OTLP, Loki로 OTLP HTTP, VictoriaMetrics로 Remote Write 1.0. | [릴리스](https://github.com/grafana/alloy/releases/tag/v1.20.1) · [이미지](https://hub.docker.com/r/grafana/alloy/tags?name=v1.20.1) · [공식 연동](https://docs.victoriametrics.com/data-ingestion/alloy/) |

## GitHub Actions

| component | chosen version (exact) | newest available | chosen release date | why (rule clause) | compatibility notes | source URL |
|---|---|---|---|---|---|---|
| actions/checkout | `v7.0.1` | `v7.0.1` | 2026-07-20 | 7.0.0 2026-06-18 출시 · 패치 · 기존 major | origin/main의 v7 유지. Node 24 runner 요구. | [릴리스](https://github.com/actions/checkout/releases/tag/v7.0.1) |
| actions/setup-go | `v7.1.0` | `v7.1.0` | 2026-11-17 | 예외 · 7.x는 성숙했으나 .0 뒤 minor만 발행 | go-version에 1.27.4 지정. origin/main에는 미사용. | [릴리스](https://github.com/actions/setup-go/releases/tag/v7.1.0) |
| actions/setup-python | `v7.1.0` | `v7.1.0` | 2026-10-22 | 예외 · 기존 v7 우선 · 7.x에 패치 없음 | origin/main의 v7 유지. 3.14.10 Ubuntu 24.04 x64/ARM64 공식 runner build 확인. | [릴리스](https://github.com/actions/setup-python/releases/tag/v7.1.0) · [빌드 매니페스트](https://github.com/actions/python-versions/blob/main/versions-manifest.json) |
| astral-sh/setup-uv | `v10.0.1` | `v10.4.0` | 2026-08-14 | 10.0.0 2026-08-12 출시 · 패치 · 10.3은 60일 미만 | uv 0.12.24를 `version`에 지정. | [릴리스](https://github.com/astral-sh/setup-uv/releases/tag/v10.0.1) |
| docker/setup-buildx-action | `v4.4.1` | `v4.4.1` | 2026-09-16 | 4.4.0 2026-09-15 출시 · 패치 | Buildx builder 준비. | [릴리스](https://github.com/docker/setup-buildx-action/releases/tag/v4.4.1) |
| docker/setup-qemu-action | `v4.4.0` | `v4.4.0` | 2026-09-15 | 예외 · 4.x·3.x·2.x에 패치 없음 · 2021년 1.0.x 대신 현행 4.x | amd64 runner의 ARM64 에뮬레이션. Buildx 이전에 실행. | [릴리스](https://github.com/docker/setup-qemu-action/releases/tag/v4.4.0) |
| docker/login-action | `v4.5.2` | `v4.8.0` | 2026-07-28 | 4.5.0 2026-07-23 출시 · 패치 · 상위는 .0만 | GHCR 로그인. `GITHUB_TOKEN`과 packages 권한 사용. | [릴리스](https://github.com/docker/login-action/releases/tag/v4.5.2) |
| docker/metadata-action | `v6.2.0` | `v6.2.0` | 2026-07-02 | 예외 · 6.x는 .0만 · 패치가 있는 5.6.1은 2024년 릴리스 | JAMMY tag와 OCI label 생성. | [릴리스](https://github.com/docker/metadata-action/releases/tag/v6.2.0) |
| docker/build-push-action | `v6.19.2` | `v7.4.0` | 2026-02-12 | 7.x는 .0만 · 6.19.0 2026-02-11 출시 · 패치 | `platforms: linux/amd64,linux/arm64`. Buildx 4.x action이 노출한 builder 소비. | [릴리스](https://github.com/docker/build-push-action/releases/tag/v6.19.2) |
| aquasecurity/trivy-action | `v0.36.0` | `v0.36.0` | 2026-04-22 | 예외 · 0.36 계열에 패치 없음 · 과거 tag 변조 이력 | `version` 입력으로 CLI 고정. 검증된 commit `ed142fd0673e97e23eac54620cfb913e5ce36c25` 사용. | [릴리스](https://github.com/aquasecurity/trivy-action/releases/tag/v0.36.0) |
| anchore/sbom-action | `v0.24.3` | `v0.24.3` | 2026-10-02 | 0.24.0 2026-03-20 출시 · 패치 | `syft-version` 및 SPDX 포맷 지정. | [릴리스](https://github.com/anchore/sbom-action/releases/tag/v0.24.3) |
| sigstore/cosign-installer | `v4.1.3` | `v4.1.3` | 2026-11-04 | 4.1.0 2026-03-09 출시 · 패치 | `cosign-release: v3.1.4` · keyless 서명용. | [릴리스](https://github.com/sigstore/cosign-installer/releases/tag/v4.1.3) |
| azure/setup-helm | `v5.0.1` | `v5.1.0` | 2026-06-23 | 5.0.0 2026-03-23 출시 · 패치 · 5.1은 기간·패치 미달 | `version: v4.3.2` 명시. Node 24 runner 요구. | [릴리스](https://github.com/azure/setup-helm/releases/tag/v5.0.1) |
| dorny/paths-filter | `v4.0.3` | `v4.0.3` | 2026-08-05 | 4.0.0 2026-03-12 출시 · 패치 | 변경한 앱 경로별 matrix 진입 조건. | [릴리스](https://github.com/dorny/paths-filter/releases/tag/v4.0.3) |
| actions/upload-artifact | `v7.0.2` | `v7.0.2` | 2026-10-07 | 7.0.0 2026-02-26 출시 · 최신 패치 | Node 24 · 스캔을 통과한 OCI 아카이브를 같은 실행의 게시 job에 전달. | [릴리스](https://github.com/actions/upload-artifact/releases/tag/v7.0.2) |
| actions/download-artifact | `v8.0.2` | `v8.0.2` | 2026-10-07 | 8.0.0 2026-02-26 출시 · 최신 패치 | Node 24 · 같은 실행에서 이름으로 아카이브를 내려받고 아티팩트 digest를 검증. | [릴리스](https://github.com/actions/download-artifact/releases/tag/v8.0.2) |

`origin/main`의 `c3aa2af`에서 `git show origin/main:.github/workflows/<file>`로 확인했다. `k8s-manifests.yml`은 checkout·setup-python v7, `secrets-scan.yml`, `shellcheck.yml`, `terraform-plan.yml`, `terraform.yml`은 checkout v7을 사용한다. 나머지 후보 Action은 당시 `origin/main` 워크플로에서 확인되지 않았다. 작업 트리의 CI 파일은 이 대조에 포함하지 않았다.

- **2026-11-24 CI 고정**. `backend.yml`의 checkout, paths-filter, setup-python, setup-uv, setup-go, setup-helm, setup-qemu, setup-buildx, build-push, trivy, sbom, login, cosign-installer는 표의 버전을 유지하고 GitHub API로 확인한 40자 커밋 SHA를 사용한다. 같은 실행에서 스캔한 이미지를 게시 job으로 넘기기 위해 upload-artifact `v7.0.2`와 download-artifact `v8.0.2`를 추가하고 같은 방식으로 고정했다.
- **2026-11-24 이미지 스캔**. 두 아키텍처에서 수정 버전이 있는 HIGH·CRITICAL 취약점을 Trivy로 검사한다. 수정 버전이 아직 없는 항목은 버전 갱신으로 해결할 수 없어 CI 실패 조건에서 제외한다. 버전 정책 때문에 적용할 수 없는 수정이 있다면 CVE ID와 사유를 해당 구성 요소 섹션에 기록한다.

## 충돌·위험

- **설계/계획과 런타임**. 기존 설계와 계획에 Python 3.13과 Helm 3이 남아 있다. 2026-11-23 사용자 결정은 Python 3.14와 Kubernetes 1.37 공식 skew를 만족하는 Helm 4.3.2다. Go 1.25는 2026-08-19에 지원이 종료됐고, parquet-go v0.32.1은 Go ≥1.26을 요구한다. [Go 지원 정책](https://go.dev/doc/devel/release) · [Helm 버전 지원](https://helm.sh/docs/topics/version_skew/)
- **Python protobuf와 OTLP**. 엄격히 60일을 적용하면 OTel 1.42.1/0.63b1로 내려가지만 `opentelemetry-proto==1.42.1`은 protobuf <7이라 protoc v36.2가 생성하는 코드에 필요한 Python 7.36.2와 충돌한다. 59일 된 1.45.x의 패치와 대응 beta를 적용해 제약을 해소했다. Python 라이브러리 표의 pin 22개 전체를 `uv pip compile --python-version 3.14 --python-platform <platform> --only-binary :all:`로 재검증했다. `x86_64-manylinux_2_28`과 `aarch64-manylinux_2_28` 모두 기존 pin으로 해석에 성공했고 `opentelemetry-proto==1.45.2`와 `protobuf==7.36.2`를 함께 선택했다. [1.42.1 메타데이터](https://pypi.org/pypi/opentelemetry-proto/1.42.1/json) · [1.45.2 메타데이터](https://pypi.org/pypi/opentelemetry-proto/1.45.2/json) · [생성 코드 규칙](https://protobuf.dev/support/cross-version-runtime-guarantee/)
- **HTTPX와 Starlette**. 양쪽 플랫폼의 해석 결과는 Starlette 1.10.0이다. 이 버전의 `TestClient`는 HTTPX 0.28.1을 deprecated fallback으로 지원한다. 표의 HTTPX를 사용하는 비동기 테스트는 `ASGITransport`로 ASGI 앱에 연결할 수 있다. [Starlette 소스](https://github.com/encode/starlette/blob/1.10.0/starlette/testclient.py) · [HTTPX ASGI transport](https://www.python-httpx.org/advanced/transports/#asgi-transport)
- **JetStream과 PostgreSQL**. NATS 2.15.3에서 nats-py 2.13.1과 nats.go 1.53.1의 JetStream publish, durable pull consumer, ACK를 쓰는 조합이다. 서버 2.15.3 자체 모듈은 nats.go 1.55.0을 쓰므로 표의 1.53.1과 실제 왕복은 따로 확인해야 한다. 2.10 기준 측정과 비교할 때 버전을 변수로 기록한다. PostgreSQL 17에서 psycopg로 COPY·풀, pgx로 COPY·풀을 사용하는 경로는 현장 17.7과 로컬 17.12에서 각각 검증이 필요하다. [NATS 프로토콜](https://docs.nats.io/reference/protocols/client) · [JetStream API](https://docs.nats.io/reference/reference-protocols/nats_api_reference) · [psycopg 지원](https://www.psycopg.org/psycopg3/docs/basic/install.html) · [pgx PostgreSQL 17 CI](https://github.com/jackc/pgx/blob/v5.9.2/.github/workflows/ci.yml) · [NATS 서버 go.mod](https://github.com/nats-io/nats-server/blob/v2.15.3/go.mod)
- **Kubernetes 1.37 검증**. `kubeconform -kubernetes-version 1.37.2 -strict`에 [스키마 저장소 커밋](https://github.com/yannh/kubernetes-json-schema/tree/0c4dd99d9873f7b4c2da24c008476f5841adbe2d/v1.37.2-standalone-strict)의 `v1.37.2-standalone-strict` 경로를 고정할 수 있다. CRD는 별도 스키마가 필요하다. k3d가 쓰는 `ghcr.io/k3d-io/k3d-proxy:5.8.3`과 `ghcr.io/k3d-io/k3d-tools:5.8.3`의 양쪽 manifest도 확인했다. Helm plugin 단위 테스트, kubeconform 스키마 검증, k3d API 서버 검증이 각각 필요하다. k3d 5.9.0에는 번들 Go 의존성 보안 보고가 있으며 5.8.3 조합의 실행 및 보안 결과도 미확인이다. [k3d 보안 이슈](https://github.com/k3d-io/k3d/issues/1710)
- **기존 Grafana 13.2.5 기준 관측 연동**. Grafana가 Loki·Tempo의 조회 API를 직접 사용한다. Alloy는 데이터 전송 경로에서 OTLP gRPC로 Tempo, OTLP HTTP로 Loki에 전송하고 VictoriaMetrics single에는 호환되는 Remote Write 1.0으로 쓴다. Loki는 TSDB v13/structured metadata를 사용한다. Tempo 3.x의 분산 모드는 Kafka 호환 브로커가 필요하므로 로컬 Compose에서는 monolithic을 사용한다. 13.2.5는 기능 호환 확인의 기준점이고 13.3.1은 지원 기간을 충족해 사용자가 승인한 버전이다. 12.4.x는 연장 지원이 2027-05-24까지지만, 13.x 환경을 major 단위로 내려야 하고 종료까지 6개월보다 하루 더 남아 후보에서 제외했다. Loki 3.7.7에는 이후 3.7.x 패치의 보안·WAL 수정이 없다. [Grafana Tempo datasource](https://grafana.com/docs/grafana/latest/datasources/tempo/) · [Grafana Loki datasource](https://grafana.com/docs/grafana/latest/datasources/loki/configure/) · [Alloy→Tempo](https://grafana.com/docs/tempo/latest/set-up-for-tracing/setup-tempo/instrument-send/set-up-collector/grafana-alloy/) · [Loki OTLP 스키마](https://grafana.com/docs/loki/latest/operations/storage/schema/) · [VM Remote Write](https://docs.victoriametrics.com/victoriametrics/data-ingestion/prometheus/) · [Tempo 모드](https://grafana.com/docs/tempo/latest/configuration/)
- **VictoriaMetrics LTS**. `victoriametrics/victoria-metrics:v1.148.9-enterprise`도 양쪽 아키텍처로 발행됐다. 1.148 LTS는 2027-07까지 지원하지만 Enterprise 조건과 `-eula`가 적용된다. Community를 전제로 지원되는 최신 이미지 v1.156.0을 선택했다. [LTS](https://docs.victoriametrics.com/victoriametrics/lts-releases/) · [Enterprise 조건](https://docs.victoriametrics.com/victoriametrics/enterprise/) · [이미지](https://hub.docker.com/r/victoriametrics/victoria-metrics/tags?name=v1.148.9-enterprise)
- **Docker Desktop macOS ARM64**. 선택한 Python Testcontainers의 기본 `testcontainers/ryuk:0.8.1`과 Go의 `testcontainers/ryuk:0.14.0`은 양쪽 이미지가 있다. 조사 환경의 Docker 서버는 ARM64 28.3.0, API 1.51이었으며 라이브러리 기동은 검증하지 않았다. [Python Ryuk 설정](https://github.com/testcontainers/testcontainers-python/blob/testcontainers-v4.14.2/core/testcontainers/core/config.py) · [Go Ryuk 설정](https://github.com/testcontainers/testcontainers-go/blob/v0.44.0/internal/config/config.go) · [Ryuk 태그](https://hub.docker.com/r/testcontainers/ryuk/tags)
- **패치가 없는 출시 체계**. SeaweedFS, distroless, zstandard, Go OTel, Go Testcontainers 및 표에 예외로 표시한 Actions에서 일반 패치 조건을 충족하지 못했다. Python OTel의 beta와 Go OTel의 0.x exporter는 upstream에서 지정한 동반 버전이다. Trivy Action은 과거 변조된 태그가 있는 계열로, 표의 검증 커밋을 기준으로 사용한다. [Trivy 공급망 공지](https://github.com/aquasecurity/trivy/security/advisories/GHSA-69fq-xp46-6x23) · [Go OTel 릴리스](https://github.com/open-telemetry/opentelemetry-go/releases/tag/v1.47.0)

## 확인 못 함

- Distroless OCI 이미지의 실제 게시일. OCI 설정의 생성 시간은 재현 빌드에 쓰인 1970-01-01이고, 표는 확인한 index digest를 제시한다.
- 수명이 명시된 프로젝트는 공개 날짜나 정책을 적용했다. 대부분의 애플리케이션 라이브러리·도구·Actions와 Loki·Alloy의 선택 계열에는 확인 가능한 확정 EOL 날짜를 찾지 못했다. NATS 2.15는 공식 지원 중이며 종료 날짜는 뒤의 minor 릴리스에 따라 정해진다. Tempo 3.1의 종료 날짜는 공식 표에서 확인하지 못했고 통상 9개월 정책을 적용할 때 2027-06 말이다.
- k3d 5.8.3 + k3s 1.37.2의 cluster 기동, Helm 4.3.2 + helm-unittest 1.1.2의 실제 실행, Testcontainers Python/Go의 Docker Desktop macOS ARM64 스모크, site PostgreSQL 17.7 및 NATS 2.15.3 왕복, Grafana 13.2.5/13.3.1에서 대시보드·로그·트레이스 조회는 실행하지 않았다. 버전, 레지스트리 플랫폼, 공식 인터페이스 및 Python 의존성 해석을 확인했다.
- Python/Go Testcontainers의 실제 Ryuk 기동과 Docker socket 설정. Go의 상위 도구 및 모듈을 한 프로젝트에 적용한 build와 전체 CI 매트릭스도 실행하지 않았다. [Testcontainers Go Docker 요구사항](https://golang.testcontainers.org/system_requirements/docker/) · [Testcontainers Python](https://testcontainers-python.readthedocs.io/en/latest/)
