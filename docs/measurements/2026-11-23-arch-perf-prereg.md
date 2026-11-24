# 아키텍처 성능 시험 사전 등록 (v16, 로컬 예비)

작성 2026-11-23. 실행 전에 고정한다. 실행 후 바꾸는 항목은 "변경 기록"에 이유와 함께 남긴다.

## 목적
같은 이미지로 Arm-1(`PIPELINE_MODE=direct-db`, `values-mvp`)과 Arm-2(`PIPELINE_MODE=jetstream`, `values-split`)를 같은 조건에서 돌려, 수신 서버당 30MB/s(압축된 wire bytes) 부하에서의 처리 여부와 장애 시 동작 차이를 비교한다. Go와 Python 구현(④⑤⑥)도 같은 조건으로 비교한다.

## 범위와 한계
- 환경: Mac(arm64) 한 대, `deploy/compose`, 이미지 tag `0.1.0-<sha7>`. 모든 컨테이너가 CPU·디스크를 나눠 쓰므로 결과는 arm·구현 사이의 **상대 비교**로만 해석한다. 절대 용량 수치는 VM 위 k8s 시험(인계 후)에서 다시 잰다.
- Intel Linux 재현은 같은 절차를 VM에서 다시 돌리는 별도 실행으로 한다.
- 이 문서의 기준값은 모두 `[제안]`이다. 팀 SLO 확정 전이므로 판정은 "기준 대비 결과"로 보고하고 합격·불합격을 확정하지 않는다.

## 조합
IMPL `go|py` × ARM `mvp|split`, 4개. Arm-1은 ingest 1대(a1), Arm-2는 ingest 2대(a1, a2). 부하는 수신 서버마다 같은 MB/s를 건다.

## 부하
- 도구: `loadtest/k6/ingest.js`, `constant-arrival-rate`, `TARGET_WIRE_MBPS_PER_SERVER`.
- payload: `payloadgen`, 3개 profile 혼합, seed 고정, run마다 같은 파일 묶음.
- 단계: 10, 20, 30, 40 MB/s per server. 단계마다 warmup 60s 후 측정 180s, 단계 사이 부하 0으로 60s.
- 30MB/s 단계는 3회 반복한다(스택 재기동 후). 보고는 중앙값과 범위.

## 지표 (원천)
| 지표 | 원천 |
|---|---|
| 수락 wire MB/s, 상태 코드별 비율 | k6 `jammy_k6_wire_bytes`, 상태 코드 counter, `results/<RUN_ID>/summary.json` |
| ingest 응답 p50/p99 | k6 `http_req_duration` |
| 저장 지연 | `jammy_ingest_store_duration_seconds` |
| 처리 지연 | `jammy_processor_lag_seconds` (최대값, 부하 종료 후 0으로 돌아오는 시간) |
| JetStream 적체 (Arm-2) | `jammy_processor_consumer_pending`, `jammy_archiver_unacked_messages` |
| 자원 | 컨테이너별 CPU·메모리(`docker stats` 5s 간격 기록), MB당 CPU-초 |
| 무손실 | 부하 종료 후: k6가 202 받은 event 수 = DB `raw_events` 행 수 = Parquet 행 수(archiver flush 후) |

VictoriaMetrics(`make up-obs`)가 scrape한 값을 원본으로 저장한다.

## 장애 시험 (30MB/s 부하 중)
- F1 DB 정지: PostgreSQL primary를 `docker pause` 30s 후 재개.
- F2 processor 정지: processor를 60s 정지 후 재기동.
- 각각 기록: 정지 중 ingest 상태 코드 비율과 p99, 재개 후 lag가 0으로 돌아오는 시간, 무손실 여부.

## 가설 (사전 고정)
- H1: 30MB/s per server에서 두 arm 모두 202 비율 ≥ 99.9% `[제안]`, ingest p99 < 1s `[제안]`.
- H2: Arm-1은 처리 지연이 Arm-2보다 작다(JetStream 단계가 없음).
- H3: F1에서 Arm-1 ingest는 503 또는 지연 증가를 보이고, Arm-2 ingest는 202를 유지한다(JetStream이 완충). 재개 후 두 arm 모두 무손실.
- H4: F2에서 Arm-2는 적체 후 회복하고 무손실. Arm-1은 DB에 이미 저장되어 ingest에 영향이 없다.
- H5: 같은 arm에서 Go 구현의 MB당 CPU-초가 Python보다 작다.

## 판정 규칙
- 가설마다 "지지 / 반박 / 판단 불가"로 적고 근거 run과 수치를 붙인다.
- 반복 3회의 범위가 두 arm 차이보다 크면 "판단 불가"로 둔다.
- 측정하지 않은 수치는 쓰지 않는다. 실패한 run도 지우지 않고 원인과 함께 남긴다.

## 산출물
- 원본: `docs/measurements/raw/arch-perf-<날짜>/<IMPL>-<ARM>/<run>/` (k6 summary, docker stats, VictoriaMetrics export, 무손실 검사 결과).
- 보고서: `docs/measurements/<날짜>-arch-perf-local.md`.

## 변경 기록
- 2026-11-24, 본 측정 전(1MB/s 스모크 후): 무손실 검사를 arm별로 나눈다. Arm-2(jetstream)는 설계상 DB `raw_events`에 쓰지 않으므로(spec §4 표), `수락 event 수 = Parquet 행 수 = processor가 처리한 batch의 event 합`으로 검사한다. Arm-1은 원래 규칙 그대로다.
- 2026-11-24, 본 측정 전: 스모크에서 `*_duration_seconds` histogram이 OTel 기본 경계를 써 p99를 0–5s 구간으로만 알 수 있었다. 모든 duration histogram에 HTTP와 같은 경계(0.001–10s)를 쓰도록 계약(`contracts/METRICS.md`)과 구현을 고친 뒤 측정한다. p99는 VictoriaMetrics `histogram_quantile`로 읽는다.
- 2026-11-24, 본 측정 전(첫 시도 실패 후): ingest 부하 도구를 k6에서 Go open-model 부하기(`loadtest/perf`)로 바꾼다. k6 `k6/experimental/fs`의 `open()`이 payload 파일 전체를 메모리에 올려, 10MB/s 단계 묶음(5.89GB)에서 exit 137(6.3GiB, Docker VM 7.65GiB)이 났다. 네 단계 묶음 합계(약 53GB)가 디스크 여유(48GB)보다 커서 미리 생성 방식도 쓸 수 없다. 새 부하기는 `tools/payloadgen`과 같은 생성기로 고정 seed payload를 실행 중에 만들고, 일정 도착률(open model)로 보내며, 제때 못 보낸 요청 수를 기록한다. 부하기 컨테이너의 CPU는 따로 기록하고 SUT 자원에서 뺀다. 지표 이름과 판정 규칙은 그대로다.
- 2026-11-24, 본 측정 중(go-mvp sweep 직후): Arm-1 무손실 검사를 `202 수락 event 수 = DB raw_events 행 수`로 한다. Arm-1은 DB commit 후 202를 돌려주므로 DB가 내구 저장 지점이다. Parquet 이관은 시간 파티션이 닫힌 뒤(H+1h+SEAL_LATENESS)에만 일어나 run마다 최대 1시간을 기다리게 되어, 이관 정합성은 E2E `test_direct_db_partition_archived_and_dropped`가 확인한 것으로 갈음한다. 이미 끝난 go-mvp sweep(`20261123T190648-sweep-e19b13`)은 DB 기준 8,693,000 = 8,693,000이었고 Parquet 대기 중에 중단했다. Arm-2 규칙은 그대로다.
- 2026-11-24, 1차 측정 후: 조사(agy, 원본 `docs/measurements/raw/arch-perf-2026-11-24/`)에서 구현 버그 3개가 나왔다. archiver-py가 재전달로 생긴 sequence 구멍을 덮인 것으로 보고 쓰지 않은 채 ACK(py-split run마다 1만~42만 행 손실), processor-go 커서 쿼리가 generic plan으로 seq scan(go-mvp 처리 지연의 원인), archiver-go가 한 시간 파티션 전체를 tmpfs에 올림(4.1GiB). 고친 뒤(b3cf59d, 81083e5, fad85fe) 영향받는 go-mvp, py-split, 그리고 loss 검사가 backlog로 판단 불가였던 go-split을 같은 방법으로 다시 잰다. py-mvp는 영향이 없어 1차 결과를 쓴다. 1차 결과는 지우지 않고 "버그 수정 전"으로 남긴다.
- 같은 시점: Arm-2 loss 검사는 고정 300s 대신 archiver 적체가 0이 될 때까지(최대 1800s) 기다린 뒤 센다. ingest가 JetStream ack 5s 초과로 503을 준 뒤 실제로는 저장된 batch는 "오류 응답 후 저장(같은 batch_id 재전송 시 멱등)"으로 따로 센다.
- 알려진 비교 한계: Arm-2는 ingest 2대에 서버당 같은 MB/s를 걸어 같은 Mac 한 대의 총 부하가 Arm-1의 2배다. 결과 해석 때 총 부하 기준 비교를 함께 적는다.
- 2026-11-24, 측정 뒤(최종 평가 지적): step 무효 규칙 "정해진 시각에 못 보낸 요청(dropped) > 0 또는 부하기 생성 지연(generator_starved)"은 이 문서 본문이 아니라 하네스 규칙(`loadtest/perf/README.md`)으로 정해졌다. 본문의 판정 규칙에 이 무효 규칙을 더한다. 장애 시험의 정지 구간(F1 30s, F2 60s)은 창 전체가 무효여도 정지 구간의 202 비율과 무손실 결과를 H3·H4 판정에 쓴다. 두 규칙 모두 측정 뒤에 문서화한 결정이다.
