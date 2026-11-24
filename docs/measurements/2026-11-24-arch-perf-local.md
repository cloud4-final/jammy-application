# 아키텍처 성능 시험 결과 (v16, 로컬 예비)

작성 2026-11-24. 방법은 사전 등록 문서 [`2026-11-23-arch-perf-prereg.md`](2026-11-23-arch-perf-prereg.md)와 그 변경 기록을 따른다. 수치는 모두 원본에서 `loadtest/perf/summarize.py`로 계산한 값이다.

## 요약

- Mac(arm64) 한 대에서, 수신 서버당 30MB/s(압축 wire bytes)를 202 비율 100%·p99 1s 미만으로 받은 조합은 **go-mvp 하나**다(유효 run 1회. 나머지 2회는 202 99~100%였으나 요청 누락 1·13건으로 무효 처리). 반복 3회의 중앙값·범위는 판단할 수 없다.
- **DB 장애(F1) 흡수는 Arm-2만 된다.** DB를 30초 멈추면 Arm-1 수신은 202가 1% 미만으로 떨어지고, Arm-2는 JetStream이 받아 78~90%를 유지했다. 두 arm 모두 재개 후 무손실이었다.
- **처리 지연 비교(H2)는 판단 불가다.** 서버당 10MB/s에서 처리 지연 최대는 Go 0.27s·Python 0.94s(Arm-1) 대 Go 1.10s·Python 17.8s(Arm-2)로 Arm-1이 작았지만, Arm-2의 10MB/s step은 요청 누락(go)·부하기 생성 지연(py)으로 무효 run이다.
- **Go 구현이 MB당 CPU를 덜 쓴다.** 같은 arm·부하에서 ingest는 약 0.3~0.8배, processor는 약 0.3~0.6배.
- 시험 중 **구현 버그 3개를 찾아 고쳤다**(archiver-py 데이터 손실, processor-go 커서 쿼리 seq scan, archiver-go 메모리). 수정 후 모든 run의 무손실 검사가 일치했다.
- Arm-2는 같은 Mac에 수신 서버 2대 × 같은 MB/s를 걸어 **총 부하가 Arm-1의 2배**였다. 서버당 20MB/s 이상에서는 **부하기 자신이 payload를 제때 만들지 못해(generator_starved)** 대부분의 step이 무효다. Arm-2의 처리 한계는 이 환경에서 판단할 수 없다.

## 환경과 실행

| 항목 | 값 |
|---|---|
| 호스트 | Mac arm64 한 대, Docker Compose, 컨테이너가 CPU·디스크 공유 |
| 이미지 | 1차 `0.1.0-97dc510`, 2차(버그 수정 후) `0.1.0-7f923f7` |
| 부하 | Go open-model 부하기(`loadtest/perf/loadgen`), payloadgen 3개 profile, seed 42, batch당 1000 event |
| 단계 | sweep 10·20·30·40MB/s per server(준비 60s·측정 180s·휴식 60s), 30MB/s 3회 반복(매번 빈 볼륨), F1 PG primary `docker pause` 30s, F2 processor 정지 60s |
| 원본 | 원본 데이터는 jammy-5th 브랜치 feat/backend-platform |
| 사용 run | py-mvp는 1차(수정 대상 코드 아님), go-mvp·py-split·go-split은 2차 |

## 결과

### 30MB/s per server (반복 3회, 각 run의 측정 180s)

| 조합 | 수락 MB/s (총) | 202 비율 | p99 | 처리 지연 최대 | 유효 run |
|---|---|---|---|---|---|
| go-mvp | 30.0 / 30.0 / 29.7 | 100% / 100% / 99% | 0.86 / 0.63 / 2.25s | 47 / 74 / 72s | 1/3 |
| py-mvp | 23.1 / 23.4 / 24.8 | 77 / 79 / 82% | 3.8 / 3.5 / 3.0s | 165 / 172 / 180s | 0/3 |
| go-split (2대 합) | 35.9 / 36.4 / 39.4 | 86 / 86 / 87% | 8.2 / 5.4 / 6.4s | 84 / 103 / 73s | 0/3 |
| py-split (2대 합) | 34.9 / 30.8 / 45.9 | 72 / 70 / 86% | 6.2 / 4.7 / 5.0s | 162 / 117 / 155s | 0/3 |

무효 사유: go-mvp는 정해진 시각에 보내지 못한 요청 1·13건(dropped), split은 부하기의 payload 생성이 송신 일정을 따라가지 못함(generator_starved). split의 목표 총량은 60MB/s이고, 부하기와 SUT가 같은 Mac CPU를 나눠 썼다.

### sweep (서버당 MB/s → 202 비율)

| 조합 | 10 | 20 | 30 | 40 |
|---|---|---|---|---|
| go-mvp | 100% | 100% | 92% | 71% |
| py-mvp | 100% | 95% | 62% | 43% |
| go-split | 100% | 75% | 77% | 90% |
| py-split | 78% | 76% | 45% | 38% |

go-split sweep은 2차 재시도 회차(`20261124T065206-sweep-64b682`, 무손실 일치)의 값이다. 첫 회차(`…051251-sweep-45dcc1`)는 Parquet 계수 helper의 S3 GetObject 300s timeout으로 무손실이 "측정 안 됨"이었고, 그 회차의 202 비율은 96·89·91·91%였다. 두 회차 모두 10MB/s step은 요청 누락(dropped), 나머지 step은 generator_starved로 무효다. 같은 조건에서 두 회차 차이가 커서 Arm-2 sweep 수치는 경향으로만 본다.

### 장애 시험 (30MB/s per server 부하 중)

| 조합 | F1 정지 30s 동안 202 | F1 재개 후 | F2 정지 60s 동안 202 | 무손실 |
|---|---|---|---|---|
| go-mvp | 0.5% (나머지 503) | 지연 회복 197s | 100% | 일치 |
| py-mvp | 0.9% | — | 88% | 일치 |
| go-split | 90% | 지연 회복 208s | 96% | 일치 |
| py-split | 78% | — | 88% | 일치 |

### MB당 CPU-초 (같은 arm, 서버당 10MB/s sweep step)

| 컨테이너 | go-mvp | py-mvp |
|---|---|---|
| ingest | 0.020 | 0.026 |
| processor | 0.008 | 0.024 |
| PostgreSQL | 0.072 | 0.071 |

30MB/s 반복 1회 기준 ingest는 Go 0.015 대 Python 0.054, processor는 0.007 대 0.012다. Arm-2는 대부분 step에서 CPU 측정 구간 조건(90% 이상 수집)을 채우지 못해 "측정 안 됨"이다.

## 가설 판정

| 가설 | 판정 | 근거 |
|---|---|---|
| H1 30MB/s에서 두 arm 모두 202 ≥ 99.9%, p99 < 1s | go-mvp는 유효 1회에서 충족(범위 판단 불가), py-mvp 반박, Arm-2 판단 불가 | 30MB/s 표. Arm-2는 총 부하 2배와 부하기 생성 지연으로 무효 |
| H2 Arm-1 처리 지연 < Arm-2 | 판단 불가 | 10MB/s에서 Go 0.27s 대 1.10s, Python 0.94s 대 17.8s로 같은 방향이지만 Arm-2 쪽 step이 무효 run. 30MB/s도 Arm-2 run이 무효 |
| H3 F1에서 Arm-1은 503, Arm-2는 202 유지, 둘 다 무손실 | 지지 | 장애 표 |
| H4 F2에서 Arm-2는 회복·무손실, Arm-1 수신은 영향 없음 | 지지 | F2 정지 구간의 202: go-mvp 100%(30MB/s 기준선 99~100%), py-mvp 88%(기준선 77~82%), Arm-2 88~96%. 전부 무손실 일치 |
| H5 Go의 MB당 CPU가 Python보다 작다 | 지지(ingest·processor) | CPU 표 |

## 시험 중 발견하고 고친 결함

| 결함 | 증상(1차) | 수정 |
|---|---|---|
| archiver-py가 재전달로 생긴 sequence 구멍을 덮인 것으로 판정하고 ACK | py-split run마다 1만~42만 행이 Parquet에 없음 | b3cf59d: 실제로 쓴 sequence만 ACK |
| processor-go 커서 쿼리가 pgx prepared statement의 generic plan으로 seq scan | go-mvp 10MB/s에서 지연 218s, commit 146ms | 81083e5: index scan, commit 12.7ms |
| archiver-go가 한 시간 파티션 전체를 tmpfs에 올림 | 메모리 4.1GiB | fad85fe: 디스크 staging, row group 단위 |
| k6가 payload 파일 전체를 메모리에 올림 | k6 exit 137 | Go open-model 부하기로 교체 |
| 무손실 검사가 archiver 적체 중에 셈 | Arm-2 불일치 오판 | caf60e6: 적체 0까지 대기 |

ingest가 JetStream ack 5s 초과로 503을 준 뒤 실제로는 저장된 batch는 스펙대로의 동작이다(같은 batch_id 재전송 시 멱등). 무손실 검사에서 따로 센다.

## 한계

- 한 대의 Mac에 모든 서비스·부하기·관측 스택이 함께 있다. 절대 용량이 아니라 상대 비교다.
- Arm-2는 총 부하가 Arm-1의 2배였다. 같은 총 부하에서의 비교는 하지 않았다.
- 반복 3회 중 유효 run이 적어(go-mvp 1회, 나머지 0회) 범위 판정을 할 수 없다.
- Arm-2 무손실 검사는 archiver 적체가 0이 될 때까지 250~850s를 기다린 뒤 셌다. 30MB/s per server에서 archiver가 실시간을 따라가지 못한다는 뜻이며, 이 값도 같은 장비 경합의 영향을 받는다.
- 2차 Arm-2의 F1·F2 창 전체는 generator_starved로 무효지만, 정지 구간의 202 비율과 무손실 결과는 H3·H4 판단에 쓴다(정지 구간 자체에는 무효 규칙이 없다). 이 예외와 무효 규칙(dropped·generator_starved)은 사전 등록 본문이 아니라 하네스 규칙(`loadtest/perf/README.md`)이며, 측정 뒤 변경 기록에 남겼다.
- py 조합에서 응답 상태 0(연결 실패·client timeout)이 요청의 0.02~0.23% 있었다. 202가 아닌 응답으로 센다.
- Arm-1의 Parquet 이관은 이 시험에서 세지 않았다(변경 기록). E2E `test_direct_db_partition_archived_and_dropped`가 확인한다.

## 다음 측정

- VM/k8s(본체·A1·A2)에서 같은 절차를 반복한다. 부하기는 별도 장비(교육장 Wi-Fi의 노트북)에서 돌린다. 이번 Arm-2 무효의 주원인이 부하기의 CPU 경합이었다.
- Arm-2를 수신 서버당 15MB/s(총 30MB/s)로도 재서 같은 총 부하 비교를 추가한다(사전 등록 변경으로 먼저 기록).
- Intel Linux 재현.
