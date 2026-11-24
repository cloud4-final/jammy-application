# JAMMY 백엔드 Kubernetes 인수인계

이 문서는 Kubernetes 운영자가 Arm-1(mvp, direct-db) 또는 Arm-2(split, JetStream)를 설치하고 수집 경로를 확인하는 절차다. 도구와 외부 의존성 버전은 [VERSIONS.md](VERSIONS.md)를 기준으로 준비한다.

## 1. 이미지와 태그

표의 이미지 이름 앞에 `ghcr.io/cloud4-final/jammy-application/`, 뒤에 `:<version>-<sha7>`을 붙인다.

| 서비스 | 이미지 | 컨테이너 포트 | 사용 arm |
| --- | --- | --- | --- |
| instruction-api | `instruction-api` | 8080, 9090 | Arm-1, Arm-2 |
| dispatcher | `dispatcher` | 9090 | Arm-1, Arm-2 |
| ingest | `ingest-go`, `ingest-py` | 8080, 9090 | Arm-1, Arm-2 |
| processor | `processor-go`, `processor-py` | 9090 | Arm-1, Arm-2 |
| archiver | `archiver-go`, `archiver-py` | 9090 | Arm-1, Arm-2 |
| portal-publisher | `portal-publisher` | 9090 | Arm-1, Arm-2 |
| migrate | `migrate` | — | Arm-1, Arm-2 |
| plantsim | `plantsim` | 8080, 9090 | Arm-1, Arm-2에서 선택 |

`version`은 빌드 커밋에서 도달 가능한 버전 태그에서 `v`를 뗀 값이며, 태그가 없다면 `0.1.0`이다. `sha7`은 해당 빌드 커밋 SHA의 앞 7자리다. 버전 태그가 가리키는 커밋은 버전만 붙인 이미지도 발행한다. 설치 전 main 빌드에서 활성화할 이미지가 모두 같은 `<version>-<sha7>`로 발행됐는지 확인하고 그 태그를 고정한다. 노드 아키텍처에 따라 `linux/amd64` 또는 `linux/arm64` 이미지가 선택된다. ingest, processor, archiver는 각각 `impl`에서 `go` 또는 `py`를 고른다. [이미지 워크플로](../../.github/workflows/backend.yml) · [태그 생성 스크립트](../../scripts/image-tag.sh)

k3d에서 로컬 Docker 이미지를 사용할 때는 차트가 렌더링할 전체 이미지 이름과 태그로 준비한다. 아래 예시처럼 GHCR 이름으로 태그했다면 `global.image.registry`는 기본값을 유지하고 `global.image.tag`를 `image_tag`와 맞춘다. 설치 테스트에서는 Docker Desktop에서 이미지를 한꺼번에 가져올 때 content digest 오류가 발생해 JAMMY 이미지를 하나씩 가져왔다. [k3d image import](https://k3d.io/v5.8.3/usage/commands/k3d_image_import/)

```sh
cluster=jammy-handover
k3d cluster create "$cluster" --servers 1 --agents 3 --image rancher/k3s:v1.37.2-k3s1 --wait

image_registry=ghcr.io/cloud4-final/jammy-application
image_tag=0.1.0-9e59316  # 로컬에 준비한 공통 태그로 변경
for image in instruction-api dispatcher ingest-go ingest-py processor-go processor-py \
  archiver-go archiver-py portal-publisher migrate plantsim; do
  k3d image import --cluster "$cluster" "$image_registry/$image:$image_tag" || break
done
```

차트는 `imagePullPolicy`를 생략한다. 위와 같이 버전 태그를 쓰면 Kubernetes가 `IfNotPresent`를 적용하므로 배치될 노드에 같은 이름과 태그의 이미지가 있으면 로컬 이미지를 사용하고, 없으면 레지스트리에서 가져온다. 로컬 이미지만 준비한 사이트는 `global.imagePullSecrets: []`로 기본 `ghcr-pull` 참조를 비울 수 있다. 설치 테스트에서는 이미지를 미리 가져온 뒤 기본 참조를 남겨도 Pod가 시작됐다. 이미지가 누락되어 비공개 GHCR에서 내려받아야 할 경우에는 2절의 pull Secret이 필요하다. [Kubernetes 이미지 pull 정책](https://kubernetes.io/docs/concepts/containers/images/#image-pull-policy)

## 2. 사전 준비

- **PostgreSQL**. 두 arm 모두 쓰기 가능한 primary를 준비한다. split은 동기 standby를 구성하고 복제가 정상인지 확인한다. 차트는 앱과 migrate Job에 같은 `DATABASE_URL`을 주입한다. primary의 writer endpoint를 `postgres://USER:PASSWORD@WRITER_HOST:5432/DBNAME` 형식으로 지정한다. TLS를 사용하지 않는 로컬 PostgreSQL에는 `?sslmode=disable`을 붙인다. Job의 역할은 최초 설치 시 대상 DB에서 `CREATE` 권한이 있어야 하며, migration이 만든 `jammy` 스키마와 테이블의 소유자로 후속 DDL과 파티션 생성을 수행할 수 있어야 한다. 설치 테스트에서는 대상 DB 소유자 역할을 사용했다. [DB 계약](../design/specs/2026-11-23-backend-platform-design.md#56-db-스키마-contractsdbmigrations-schema-jammy) · [migrate Job](../../deploy/helm/jammy-platform/templates/job-migrate.yaml)
- **NATS JetStream**. split에서 Pod가 연결할 JetStream 서버와 파일 저장 공간을 준비한다. ingest에는 stream 생성·조회·publish, processor와 archiver에는 durable consumer 생성·조회·pull·ACK에 필요한 권한을 부여한다. raw와 quarantine stream은 ingest가 만든다. [JetStream 계약](../design/specs/2026-11-23-backend-platform-design.md#41-arm-2-pipeline_modejetstream)
- **S3**. 두 arm 모두 Helm 설치 전에 archiver와 portal-publisher가 접근할 bucket을 만든다. SeaweedFS나 MinIO에 path-style로 접속할 때는 `global.s3.forcePathStyle: true`를 지정한다. SDK 자격 증명으로 bucket 확인 및 객체 업로드가 가능해야 하며, archiver는 이관 결과 검증을 위해 객체도 읽을 수 있어야 한다. [배치 인계 계약](../../contracts/BATCH_HANDOFF.md)
- **DynamoDB와 SQS**. split에서 `LEASE_MODE: dynamodb`를 쓰면 Helm 설치 전에 두 거점이 공유할 lease table을 만든다. 파티션 키는 문자열 `lease_name`이다. dispatcher는 최초 lease 획득 시 `UpdateItem`으로 `active-site`를 생성하므로 사전 입력은 생략한다. dispatcher에 `dynamodb:GetItem`, `dynamodb:UpdateItem` 권한을 부여한다. `expires_at`은 애플리케이션이 만료를 판단하는 속성이다. epoch를 이어가려면 DynamoDB TTL 자동 삭제를 이 항목에 적용하거나 항목을 직접 삭제하는 운영을 피한다. SQS 수집을 활성화한다면 Helm 설치 전에 원본 큐와 DLQ를 만들고 redrive 정책으로 연결한다. dispatcher에 원본 큐의 `sqs:GetQueueAttributes`, `sqs:ReceiveMessage`, `sqs:DeleteMessage` 권한을 부여한다. [SQS 계약](../design/specs/2026-11-23-backend-platform-design.md#54-명령--plantsim) · [lease 구현](../../apps/dispatcher/src/dispatcher/lease.py)

로컬 S3에서 bucket을 만드는 예시다. `s3_endpoint`는 운영자 터미널에서 접근 가능한 주소, `s3_bucket`은 `global.s3.bucket`에 넣을 이름이다. bucket 생성 권한이 있는 자격 증명을 사용한다. [MinIO Client endpoint 설정](https://docs.min.io/aistor/reference/cli/mc-alias/mc-alias-set/) · [bucket 생성](https://docs.min.io/aistor/reference/cli/mc-mb/)

```sh
s3_endpoint=http://127.0.0.1:8333
s3_bucket=jammy-local
mc alias set --path on jammy-local "$s3_endpoint" "$AWS_ACCESS_KEY_ID" "$AWS_SECRET_ACCESS_KEY"
mc mb "jammy-local/$s3_bucket"
```

아래 AWS CLI 예시의 endpoint와 리전은 설치 대상에 맞춘다. endpoint에는 운영자 터미널에서 접근 가능한 주소를 넣고, AWS 관리형 서비스를 사용하면 각 명령에서 `--endpoint-url` 옵션을 뺀다. AWS CLI에는 리소스 생성 권한이 있는 자격 증명을 설정한다. [DynamoDB 테이블 생성](https://docs.aws.amazon.com/cli/latest/reference/dynamodb/create-table.html) · [SQS 큐 생성과 redrive 정책](https://docs.aws.amazon.com/cli/latest/reference/sqs/create-queue.html)

```sh
aws_region=ap-northeast-2
dynamodb_endpoint=http://127.0.0.1:8000
lease_table=jammy-leases
aws dynamodb create-table --endpoint-url "$dynamodb_endpoint" --region "$aws_region" \
  --table-name "$lease_table" --billing-mode PAY_PER_REQUEST \
  --attribute-definitions AttributeName=lease_name,AttributeType=S \
  --key-schema AttributeName=lease_name,KeyType=HASH
aws dynamodb wait table-exists --endpoint-url "$dynamodb_endpoint" --region "$aws_region" \
  --table-name "$lease_table"
```

SQS를 사용할 때는 아래 예시로 DLQ와 원본 큐를 만든다. 마지막 명령이 반환하는 `QueueUrl`을 `dispatcher.env.SQS_QUEUE_URL`에 지정한다. 로컬 SQS는 큐 URL의 호스트를 Pod에서 접근 가능한 주소로 맞춘다.

```sh
sqs_endpoint=http://127.0.0.1:9324
dlq_url=$(aws sqs create-queue --endpoint-url "$sqs_endpoint" --region "$aws_region" \
  --queue-name jammy-instructions-dlq --query QueueUrl --output text)
dlq_arn=$(aws sqs get-queue-attributes --endpoint-url "$sqs_endpoint" --region "$aws_region" \
  --queue-url "$dlq_url" --attribute-names QueueArn --query Attributes.QueueArn --output text)
redrive=$(jq -nc --arg arn "$dlq_arn" \
  '{RedrivePolicy: ({deadLetterTargetArn: $arn, maxReceiveCount: "5"} | tojson)}')
aws sqs create-queue --endpoint-url "$sqs_endpoint" --region "$aws_region" \
  --queue-name jammy-instructions --attributes "$redrive" --output json
```

배포 namespace를 먼저 만들고 그 안에 `global.existingSecret`이 가리킬 Secret을 준비한다. 기본 활성 서비스가 참조하는 키는 아래와 같다.

| 구분 | Secret 키 이름 |
| --- | --- |
| 필수 | `DATABASE_URL`, `HMAC_KEYS` |
| AWS 자격 증명을 Secret으로 주입할 때 | `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` |

`HMAC_KEYS`에는 키 ID를 비밀값에 매핑한 JSON을 넣는다. 차트가 이 값을 키 파일로 마운트하므로 아래 예시의 비밀값을 설치 환경에 맞게 바꾼다. [환경 변수 계약](../../contracts/ENV.md#instruction-api)

```json
{"k1":"replace-with-your-hmac-secret"}
```

차트는 기본 ServiceAccount 토큰 자동 마운트를 끈다. 자격 증명을 검증하지 않는 로컬 S3와 DynamoDB Local에서도 SDK가 요청에 서명할 수 있도록 Secret의 `AWS_ACCESS_KEY_ID`와 `AWS_SECRET_ACCESS_KEY`에 비어 있지 않은 더미 값을 넣는다. 인증을 설정한 로컬 S3에는 해당 서버의 자격 증명을 사용한다. NATS 인증이 필요하면 `NATS_CREDS` 키를 담은 Secret을 별도로 만들고 `global.nats.existingSecret`에 지정한다. 비공개 GHCR 이미지를 사용하면 `global.imagePullSecrets`가 참조하는 `kubernetes.io/dockerconfigjson` Secret도 같은 namespace에 준비한다. [기본 values](../../deploy/helm/jammy-platform/values.yaml) · [차트 Secret 참조](../../deploy/helm/jammy-platform/templates/_helpers.tpl)

## 3. 사이트별 values

기본 설정은 [values.yaml](../../deploy/helm/jammy-platform/values.yaml)에 있다. [mvp](../../deploy/helm/jammy-platform/values-mvp.yaml) 또는 [split](../../deploy/helm/jammy-platform/values-split.yaml)을 선택한 뒤 저장소 밖에 사이트별 values 파일을 작성해 마지막 `-f`로 적용한다. [렌더링 테스트 fixture](../../deploy/helm/jammy-platform/tests/site-values.yaml)에서 키 구조를 확인할 수 있다. 아래 표에 따라 배포 대상의 입력을 채운다.

| Value | 설치 시 지정할 내용 |
| --- | --- |
| `global.image.tag` | 활성 이미지 전체가 발행된 공통 `<version>-<sha7>` |
| `global.existingSecret` | 앞에서 준비한 플랫폼 Secret 이름 |
| `global.siteId` | 거점별 고유 ID. 두 프로필에는 거점 A의 ID가 지정돼 있으므로 거점 B에서는 덮어쓴다. |
| `global.pipelineMode` | mvp는 `direct-db`, split은 `jetstream`. 선택한 프로필이 설정한다. |
| `dispatcher.env.LEASE_MODE` | 단일 거점 mvp는 `disabled`, 거점 간 인수를 구성하는 split은 `dynamodb` |
| `dispatcher.env.LEASE_TABLE`, `dispatcher.env.LEASE_AUTO_ACQUIRE` | DynamoDB 모드에서 공용 lease table을 지정한다. 자동 획득은 A에 `"true"`, B에 `"false"`를 설정한다. |
| `dispatcher.env.SQS_QUEUE_URL` | SQS 수집을 사용할 때 원본 큐 URL을 지정한다. 생략하거나 빈 문자열이면 SQS 수집을 끈다. |
| `dispatcher.env.DYNAMODB_ENDPOINT`, `dispatcher.env.SQS_ENDPOINT` | DynamoDB Local, ElasticMQ, LocalStack 등을 사용할 때 각 서비스의 Pod 접근 주소를 지정한다. 관리형 AWS 서비스는 기본 SDK endpoint를 사용한다. SDK 리전은 `dispatcher.env.AWS_DEFAULT_REGION`으로 지정한다. [dispatcher 환경 변수 계약](../../contracts/ENV.md#dispatcher) |
| `dispatcher.env.PLANT_GATEWAY_URL` | 두 arm의 명령 수신 gateway base URL. 시뮬레이터를 쓰려면 `plantsim.enabled: true`로 설정하고 같은 namespace에서 `http://jammy-platform-plantsim:8080`을 사용한다. |
| `global.s3.bucket`, `global.s3.endpoint` | 두 arm의 사전 생성 bucket과 Pod에서 접근 가능한 endpoint. `global.s3.region`을 서비스에 맞추고, SeaweedFS·MinIO를 path-style로 사용하면 기본값 `false`를 `global.s3.forcePathStyle: true`로 덮어쓴다. |
| `global.nats.url` | split의 NATS endpoint. 인증 시 `global.nats.existingSecret`도 지정한다. |

추가 환경 변수의 의미, 형식, 기본값은 [ENV.md](../../contracts/ENV.md)를 따른다. 사이트별 파일에는 Secret 이름을 설정한다.

## 4. 노드 배치

| 프로필 | 서비스 | `nodeSelector` | `tolerations` |
| --- | --- | --- | --- |
| mvp | instruction-api, dispatcher, ingest a1, processor, archiver, portal-publisher, plantsim, migrate | `role=main` | 기본 목록 비어 있음 |
| split | instruction-api, dispatcher, migrate | `role=control` | `role=control:NoSchedule` |
| split | ingest a1, a2 | `role=ingest` + 해당 `jammy.io/ingest-instance=a1\|a2` | 기본 목록 비어 있음 |
| split | processor, archiver, portal-publisher, plantsim | `role=ingest` | 기본 목록 비어 있음 |

아래 변수에 배치 대상으로 정한 노드 이름을 넣고 선택한 프로필의 명령을 실행한다. split의 dispatcher 두 replica가 각각 배치되려면 hostname이 서로 다른 control 노드 두 대가 필요하다. main·ingest 노드에 추가 taint가 있다면 해당 서비스의 `tolerations`에 일치하는 항목을 지정한다. [mvp 배치](../../deploy/helm/jammy-platform/values-mvp.yaml) · [split 배치](../../deploy/helm/jammy-platform/values-split.yaml)

k3d 서버 1대와 에이전트 3대로 split을 구성하면 `server-0`과 `agent-0`을 control에, `agent-1`과 `agent-2`를 각각 ingest a1과 a2에 배정한다. 두 control 노드에 `NoSchedule` taint를 적용한다면 시스템 Pod와 클러스터 내 PostgreSQL·NATS 등 인프라 Pod가 배치될 수 있도록 두 ingest 노드를 taint 없이 두고 여유 자원을 확보한다. arm 전환 시 `--overwrite`로 기존 `role` 라벨을 바꾼다. 이전 control 노드를 main 또는 ingest로 재사용하고 `role=control:NoSchedule` taint가 남아 있다면 `kubectl taint node "$node" role=control:NoSchedule-`로 제거한다.

```sh
# mvp
kubectl label node "$main_node" role=main --overwrite

# split
kubectl label node "$control_node_1" "$control_node_2" role=control --overwrite
kubectl label node "$ingest_node_a1" role=ingest jammy.io/ingest-instance=a1 --overwrite
kubectl label node "$ingest_node_a2" role=ingest jammy.io/ingest-instance=a2 --overwrite
# control 전용 노드에 다른 워크로드의 새 배치를 제한할 때 적용
kubectl taint node "$control_node_1" "$control_node_2" role=control:NoSchedule --overwrite

kubectl get nodes -L role,jammy.io/ingest-instance
```

mvp의 a1은 PostgreSQL로 보낸다. split의 a1은 `raw-a1`, a2는 `raw-a2`를 사용하고 각 stream에 대응하는 `quarantine-raw-a1`, `quarantine-raw-a2`도 만든다. 차트는 각 ingest 인스턴스에 Deployment와 Service를 만들며 processor·archiver에 두 raw stream 목록을 주입한다. 인스턴스별 ingest replica는 하나로 유지한다.

## 5. Helm 설치와 migration

Helm 대신 Kustomize로 배포한다면 migration Job과 앱 리소스를 따로 렌더링하는 절차(`make helm-render`, `kubectl kustomize deploy/helm/rendered/<arm>`)를 [chart README의 Kustomize 절](../../deploy/helm/jammy-platform/README.md)에서 따른다. Job이 끝난 뒤 앱 리소스를 적용한다.

`--wait=watcher`에는 Helm 4가 필요하다. `helm version --short`로 확인하고 [VERSIONS.md](VERSIONS.md)에 고정한 `v4.3.2`를 사용한다. 대상 클러스터의 context를 선택하고 아래 명령을 저장소 루트에서 실행한다. 예시는 `jammy` namespace와 `jammy-platform` release를 사용한다. namespace가 없으면 `kubectl create namespace jammy`로 만든 뒤 2절의 Secret을 준비한다. `site_values`는 해당 거점의 실제 values 파일 경로로 바꾼다.

```sh
chart=deploy/helm/jammy-platform
site_values=/path/to/site-values.yaml
```

같은 release에는 다음 두 설치 명령 중 선택한 프로필 하나를 실행한다.

```sh
# mvp
helm install jammy-platform "$chart" -n jammy \
  -f "$chart/values-mvp.yaml" -f "$site_values" --wait=watcher

# split
helm install jammy-platform "$chart" -n jammy \
  -f "$chart/values-split.yaml" -f "$site_values" --wait=watcher
```

Helm은 `jammy-platform-migrate`를 `pre-install` 및 `pre-upgrade` hook으로 실행한다. DB가 쓰기 가능하고 Secret과 이미지 pull이 준비돼 있어야 한다. Job은 DB 역할 이름이 `jammy`여도 `public.schema_migrations`에서 적용 버전을 읽고 기록한다. Job의 `up`이 성공하면 Helm이 앱 리소스를 적용하고 성공한 Job은 삭제한다. migration 실패 시 10절에서 로그와 재시도 절차를 확인한다. 동일 스키마를 별도로 관리하는 사이트는 앱에 필요한 migration 적용을 확인한 뒤 `migrate.enabled: false`를 사용한다. [migrate Job 템플릿](../../deploy/helm/jammy-platform/templates/job-migrate.yaml)

같은 namespace에서 arm을 바꿀 때는 `helm uninstall jammy-platform -n jammy --wait`를 실행해 기존 release의 리소스가 삭제될 때까지 기다린다. 4절에 따라 노드 라벨과 taint를 맞춘 뒤 새 프로필로 설치한다. 외부 PostgreSQL의 데이터와 migration 이력은 계속 사용하거나, 빈 상태로 시험해야 한다면 별도의 테스트 DB를 준비한다.

## 6. 포트, probe, 자원

- 장기 실행 서비스의 컨테이너는 admin TCP 9090에서 `/livez`, `/readyz`, `/metrics`를 제공한다. 차트는 `/livez`를 liveness probe, `/readyz`를 readiness probe로 사용한다. Alloy의 Pod scrape용 annotation도 설정한다. 성공하면 종료하는 migrate Job에는 포트와 HTTP probe가 없다.
- API TCP 8080은 instruction-api, ingest, plantsim에서 사용한다. 기본 Service는 ClusterIP다. 클러스터 외부에서 ingest를 호출해야 하는 사이트는 인스턴스의 `service.type`을 `NodePort`로 설정할 수 있다. 이 경우 9090 probe와 scrape는 Pod 포트를 사용한다. [포트·probe 템플릿](../../deploy/helm/jammy-platform/templates/_helpers.tpl) · [Service 템플릿](../../deploy/helm/jammy-platform/templates/service.yaml)
- CPU·메모리 요청과 한도는 [values.yaml](../../deploy/helm/jammy-platform/values.yaml)의 서비스별 `resources.requests`와 `resources.limits`를 확인한다. 사이트에서 덮어쓰는 경우 노드의 할당 가능 자원과 replica 수를 확인한다.

## 7. 설치 확인

```sh
kubectl -n jammy get pods -l app.kubernetes.io/instance=jammy-platform -o wide
kubectl -n jammy get deployments,services -l app.kubernetes.io/instance=jammy-platform
```

활성 앱의 Pod가 Ready인지 확인한다. mvp에서는 ingest a1, split에서는 ingest a1·a2가 각각 배치돼 있어야 한다. 한 터미널에서 다음 port-forward를 유지한다. split에서 a2도 확인하려면 같은 명령의 Deployment 이름을 `jammy-platform-ingest-a2`로 바꾼다.

```sh
kubectl -n jammy port-forward deployment/jammy-platform-ingest-a1 19090:9090 18080:8080
```

다른 터미널에서 readiness를 확인하고 테스트 데이터 전송이 허용된 환경에 합성 이벤트 하나를 넣는다. `payload`는 `SynthSmall(value=1.0)`의 protobuf 바이트를 base64로 인코딩한 값이다.

```sh
curl -fsS -o /dev/null -w '%{http_code}\n' http://127.0.0.1:19090/readyz

smoke_id="handover-$(uuidgen)"
event_time_ms="$(date -u +%s)000"
printf '{"batch_id":"%s","events":[{"event_id":"%s","routing_key":"%s","profile":"synth-small","schema_version":1,"event_time_unix_ms":"%s","payload":"CQAAAAAAAPA/"}]}\n' \
  "$smoke_id" "$smoke_id" "$smoke_id" "$event_time_ms" |
  curl -fsS -w '\n%{http_code}\n' -H 'Content-Type: application/json' \
    --data-binary @- http://127.0.0.1:18080/v1/batches
```

`/readyz`의 기대 응답은 HTTP 200이다. 새 배치는 HTTP 202와 `accepted: 1`, `rejected: 0`, `duplicate: false`를 기대한다. 202는 direct-db의 commit 또는 JetStream의 PubAck를 확인하는 지점이다. [Ingest API 계약](../../contracts/openapi/ingest.yaml) · [저장 및 후속 처리 계약](../design/specs/2026-11-23-backend-platform-design.md#43-정합성-규칙)

같은 터미널에서 `smoke_id`를 유지하고 `database_url`에 운영자 터미널에서 접근 가능한 PostgreSQL 접속 URI를 설정한다. 다음 조회에서 각 쿼리당 한 행을 확인한다. 아래 consumer 이름은 기본 설정 기준이다.

**mvp (`direct-db`)**에서는 ingest가 `raw_events`와 `processed_batches`를 기록한다. processor가 DB에서 읽어 `latest_state`를 갱신하므로 `primary_value=1`이 보일 때까지 조회한다.

```sh
psql -X -v ON_ERROR_STOP=1 -v smoke_id="$smoke_id" "$database_url" <<'SQL'
SELECT event_id, primary_value FROM jammy.raw_events WHERE event_id = :'smoke_id';
SELECT consumer, batch_id FROM jammy.processed_batches
  WHERE consumer = 'ingest' AND batch_id = :'smoke_id';
SELECT routing_key, primary_value FROM jammy.latest_state WHERE routing_key = :'smoke_id';
SQL
```

**split (`jetstream`)**에서는 processor가 JetStream에서 비동기로 읽어 아래 세 테이블에 기록한다. 수집 원본은 JetStream으로 보내므로 이 arm의 DB 확인은 `processed_batches`, `latest_state`, `summary_1m`에서 한다. 202 직후에는 행이 아직 없을 수 있다. `latest_state`는 별도로 기록되므로 `processed_batches`에 행이 보인 뒤에도 다음 `STATE_FLUSH_SECONDS` 주기(기본 1초)까지 기다려야 할 수 있다. `latest_state.primary_value=1`과 `summary_1m`의 `count=1`, `sum=1`이 보일 때까지 조회한다. [ENV.md](../../contracts/ENV.md)

```sh
psql -X -v ON_ERROR_STOP=1 -v smoke_id="$smoke_id" "$database_url" <<'SQL'
SELECT consumer, batch_id FROM jammy.processed_batches
  WHERE consumer = 'processor' AND batch_id = :'smoke_id';
SELECT routing_key, primary_value FROM jammy.latest_state WHERE routing_key = :'smoke_id';
SELECT routing_key, minute, count, sum FROM jammy.summary_1m
  WHERE profile = 'synth-small' AND routing_key = :'smoke_id';
SQL
```

split의 활성 거점에서는 DynamoDB lease도 조회한다. 2절의 `dynamodb_endpoint`, `aws_region`, `lease_table`을 사용한다. 관리형 DynamoDB를 사용하면 `--endpoint-url`을 뺀다. 반환된 `holder`가 활성 거점의 `global.siteId`와 일치하는지 확인한다. [DynamoDB 항목 조회](https://docs.aws.amazon.com/cli/latest/reference/dynamodb/get-item.html)

```sh
aws dynamodb get-item --endpoint-url "$dynamodb_endpoint" --region "$aws_region" \
  --table-name "$lease_table" --key '{"lease_name":{"S":"active-site"}}' \
  --consistent-read --query 'Item.{holder:holder.S,epoch:epoch.N,expires_at:expires_at.N}' --output json
```

## 8. 거점 B 수동 인수

1. B의 사이트 values에 고유한 `global.siteId`, `dispatcher.env.LEASE_MODE: dynamodb`, `dispatcher.env.LEASE_AUTO_ACQUIRE: "false"`를 지정한다. 두 거점이 같은 lease table을 보는지 확인한다. B의 dispatcher가 사용할 쓰기 가능한 DB와 SQS 큐를 준비하고 B의 Pod가 Ready인지 확인한다. DB를 승격하는 구성이라면 사이트의 DB 전환 절차로 기존 writer 차단과 승격을 먼저 완료한다.
2. A의 dispatcher가 명령 전송, SQS 수거, lease 갱신을 재개할 수 없도록 차단한다. 기존 lease가 만료된 뒤 B 클러스터 context에서 아래 명령을 실행한다. 유효한 lease가 남아 있으면 조건부 갱신이 실패하므로 A 차단 상태와 만료 여부를 재확인한다.

   ```sh
   kubectl -n jammy exec deployment/jammy-platform-dispatcher -c dispatcher -- \
     /workspace/.venv/bin/python -m dispatcher.cli lease takeover --confirm
   ```

3. 명령이 출력한 새 epoch를 기록한다. 7절의 lease 조회로 `holder`가 B의 `global.siteId`인지 확인한다. B의 dispatcher 중 DB leader에서 `jammy_dispatcher_is_leader`와 `jammy_dispatcher_lease_active`가 각각 1인지 확인한 뒤 명령 전송과 SQS 처리를 확인한다. [dispatcher metric 계약](../../contracts/METRICS.md)

## 9. 롤백

이전 정상 revision을 이력에서 확인한다. 해당 revision의 앱 이미지가 현재 DB 스키마와 호환되는지 먼저 확인한다. `helm rollback`은 release 리소스를 이전 상태로 복원하며 이 차트의 migrate hook은 install·upgrade에서 실행된다. DB 스키마나 외부 데이터 복구가 필요한 경우 별도의 DB 복구 절차를 적용한다.

```sh
helm history jammy-platform -n jammy
revision=1  # 위 이력에서 선택한 정상 revision으로 변경
helm rollback jammy-platform "$revision" -n jammy --wait=watcher
```

완료 후 7절의 Pod 상태, readiness, ingest 응답을 다시 확인한다.

## 10. 문제 해결

표의 `"$pod"`는 7절에서 확인한 문제가 있는 Pod 이름으로 지정한다.

| 증상 | 확인 명령 | 조치 |
| --- | --- | --- |
| Pod가 `Pending`이고 이벤트에 selector, taint 또는 anti-affinity 불일치가 표시됨 | `kubectl -n jammy describe pod "$pod"` · `kubectl get nodes -L role,jammy.io/ingest-instance` | 4절의 label·toleration을 맞춘다. split dispatcher라면 서로 다른 hostname의 control 노드 두 대의 가용 자원도 확인한다. |
| `ErrImagePull` 또는 `ImagePullBackOff` | `kubectl -n jammy describe pod "$pod"` | 이미지 경로와 태그가 활성 서비스 전체에 발행됐는지 확인한다. 인증 오류면 namespace의 pull Secret과 `global.imagePullSecrets`를 확인한다. |
| `CreateContainerConfigError` 또는 `FailedMount` | `kubectl -n jammy describe pod "$pod"` | 이벤트에 나온 Secret 이름과 키를 2절과 대조하고 올바른 namespace에 준비한다. |
| 설치가 migrate hook에서 중단됨 | `kubectl -n jammy describe job jammy-platform-migrate` · `kubectl -n jammy logs job/jammy-platform-migrate` · `helm status jammy-platform -n jammy` | 이미지와 Secret 오류를 먼저 해소한다. DB 오류면 writer 연결, 소유자 권한, split의 동기 standby 상태를 확인하고 실패 원인과 migration 상태를 정리한다. `pre-install` hook이 실패하면 release가 `failed`로 남는다. `helm uninstall jammy-platform -n jammy --wait` 후 `helm install`을 다시 실행하거나, 같은 프로필과 사이트 values로 `helm upgrade --install`을 실행한다. |
| split에서 ingest 또는 consumer가 Ready가 되지 않고 stream 오류가 남음 | `kubectl -n jammy logs deployment/jammy-platform-ingest-a1` · `kubectl -n jammy logs deployment/jammy-platform-ingest-a2` · `kubectl -n jammy logs deployment/jammy-platform-processor` | NATS 연결·인증, JetStream 활성화와 파일 저장 여유, stream과 consumer 권한을 확인한다. 두 ingest의 stream 이름을 4절과 일치시키고 두 Pod가 stream을 생성했는지 확인한다. |
