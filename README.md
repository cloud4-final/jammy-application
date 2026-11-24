# JAMMY 애플리케이션

JAMMY의 데이터 수집, 처리, 보관과 제어 서비스를 담은 저장소입니다.

| 경로 | 내용 |
| --- | --- |
| `apps/`, `libs/`, `tools/` | 애플리케이션, 공통 라이브러리, 개발 도구 |
| `contracts/` | 환경 변수, API, DB, 메트릭 및 데이터 계약 |
| `deploy/`, `observability/` | Helm·Compose 구성과 Alloy·대시보드 |
| `loadtest/`, `tests/` | 부하 및 E2E 테스트 |
| `docs/backend/`, `docs/design/`, `docs/measurements/` | 운영 가이드, 설계, 측정 결과 |

## 빌드와 테스트

가져온 애플리케이션 트리에서 Go 1.27.4, Python 3.14, uv 0.12.24, Helm 4와 helm-unittest를 준비합니다.

```sh
go build ./...
go test ./libs/go/...
uv run pytest libs/py -q
make helm-test
```

## 설치

`docs/backend/HANDOVER.md`의 절차에 따라 외부 서비스와 Secret을 준비한 뒤 사이트별 values 파일과 발행된 이미지 태그를 지정합니다.

```sh
helm upgrade --install jammy-platform deploy/helm/jammy-platform \
  --namespace jammy --create-namespace \
  -f deploy/helm/jammy-platform/values-mvp.yaml \
  -f ./site-values.yaml --set-string global.image.tag="$IMAGE_TAG"
```

팀 계약은 [jammy-docs](https://github.com/cloud4-final/jammy-docs), 인프라와 Kubernetes 구성은 [jammy-5th](https://github.com/cloud4-final/jammy-5th), 관측 스택은 [jammy-observability](https://github.com/cloud4-final/jammy-observability)에 있습니다.
