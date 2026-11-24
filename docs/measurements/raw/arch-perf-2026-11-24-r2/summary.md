# Local architecture performance

## go-mvp

| Run / target | MB/s per server offered | Accepted MB/s | Status ratios | Requests not sent (warmup + measure) | Generator starved | Ingest p50 ms | Ingest p99 ms | Store p99 s | Commit p99 s | Lag max s | Lag recovery s | Processor pending max | Archiver unacked max | Loss check |
|---|---:|---:|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| 20261124T022704-sweep-f5200b / 01-10MBps | 10 | 10.015 | 202 100.00% | 0 | 0 | 29 | 342 | 0.24831 | 0.019774 | 0.26984 | 0.268 | not measured | not measured | matched |
| 20261124T022704-sweep-f5200b / 02-20MBps | 20 | 19.984 | 202 100.00% | 0 | 0 | 53 | 902 | 0.83203 | 0.024206 | 3.147 | 4.007 | not measured | not measured | matched |
| 20261124T022704-sweep-f5200b / 03-30MBps (invalid: dropped iterations) | 30 | 26.943 | 202 92.12%, 503 7.88% | 1 | 0 | 132 | 3160 | 3.8061 | 0.082751 | 142.88 | not measured | not measured | not measured | matched |
| 20261124T022704-sweep-f5200b / 04-40MBps (invalid: dropped iterations) | 40 | 27.237 | 202 70.53%, 503 29.47% | 6 | 0 | 418 | 3905 | 4.6246 | 0.19161 | 224.7 | not measured | not measured | not measured | matched |
| 20261124T024824-repeat-30-1-c6f0fe / 01-30MBps | 30 | 29.999 | 202 100.00% | 0 | 0 | 41 | 858 | 0.8181 | 0.022764 | 46.843 | 32.668 | not measured | not measured | matched |
| 20261124T025428-repeat-30-2-f5c1d7 / 01-30MBps (invalid: dropped iterations) | 30 | 29.999 | 202 100.00% | 1 | 0 | 30 | 630 | 0.63333 | 0.017941 | 73.539 | 45.991 | not measured | not measured | matched |
| 20261124T030032-repeat-30-3-f85f6f / 01-30MBps (invalid: dropped iterations) | 30 | 29.718 | 202 99.29%, 503 0.71% | 13 | 0 | 71 | 2250 | 2.2664 | 0.0322 | 71.52 | 50.338 | not measured | not measured | matched |
| 20261124T030703-f1-889f19 / 01-30MBps | 30 | 24.976 | 202 83.25%, 503 16.75% | 1 requests not sent | 0 | 28 | 846 | 0.96783 | 0.020734 | 102.76 | 47.694 | not measured | not measured | matched |
| 20261124T030703-f1-889f19 / F1 interruption | 30 | 0.14078 | 202 0.47%, 503 99.53% | not measured | not measured | 0.67148 | 2026.1 | not measured | not measured | 56.667 | 197.48 | not measured | not measured | matched |
| 20261124T031306-f2-e68221 / 01-30MBps | 30 | 28.164 | 202 95.05%, 503 4.95% | 25 requests not sent | 0 | 43 | 2850 | 3.0305 | 0.042612 | 105.95 | 56.148 | not measured | not measured | matched |
| 20261124T031306-f2-e68221 / F2 interruption | 30 | 30.006 | 202 100.00% | not measured | not measured | 35.352 | 235.21 | not measured | not measured | not measured | 0.5748 | not measured | not measured | matched |

| Run / target | Ingest server | Achieved wire MB/s | Accepted MB/s | Status ratios | Ingest p50 ms | Ingest p95 ms | Ingest p99 ms |
|---|---|---:|---:|---|---:|---:|---:|
| 20261124T022704-sweep-f5200b / 01-10MBps | ingest-a1 | 10.015 | 10.015 | 202 100.00% | 29 | 208 | 342 |
| 20261124T022704-sweep-f5200b / 02-20MBps | ingest-a1 | 19.984 | 19.984 | 202 100.00% | 53 | 370 | 902 |
| 20261124T022704-sweep-f5200b / 03-30MBps | ingest-a1 | 29.999 | 26.943 | 202 92.12%, 503 7.88% | 132 | 2079 | 3160 |
| 20261124T022704-sweep-f5200b / 04-40MBps | ingest-a1 | 39.944 | 27.237 | 202 70.53%, 503 29.47% | 418 | 2641 | 3905 |
| 20261124T024824-repeat-30-1-c6f0fe / 01-30MBps | ingest-a1 | 29.999 | 29.999 | 202 100.00% | 41 | 313 | 858 |
| 20261124T025428-repeat-30-2-f5c1d7 / 01-30MBps | ingest-a1 | 29.999 | 29.999 | 202 100.00% | 30 | 199 | 630 |
| 20261124T030032-repeat-30-3-f85f6f / 01-30MBps | ingest-a1 | 29.953 | 29.718 | 202 99.29%, 503 0.71% | 71 | 1144 | 2250 |
| 20261124T030703-f1-889f19 / 01-30MBps | ingest-a1 | 29.995 | 24.976 | 202 83.25%, 503 16.75% | 28 | 272 | 846 |
| 20261124T031306-f2-e68221 / 01-30MBps | ingest-a1 | 29.807 | 28.164 | 202 95.05%, 503 4.95% | 43 | 1417 | 2850 |

| Run / target | Container | CPU-s / accepted MB | Max memory MiB |
|---|---|---:|---:|
| 20261124T022704-sweep-f5200b / 01-10MBps | alloy-1 | 0.0019025 | 46.55 |
| 20261124T022704-sweep-f5200b / 01-10MBps | archiver-1 | 0.00018298 | 12.75 |
| 20261124T022704-sweep-f5200b / 01-10MBps | dispatcher-1-1 | 0.0018672 | 80.5 |
| 20261124T022704-sweep-f5200b / 01-10MBps | dynamodb-local-1 | 0.0014444 | 155.6 |
| 20261124T022704-sweep-f5200b / 01-10MBps | elasticmq-1 | 0.0007879 | 55.49 |
| 20261124T022704-sweep-f5200b / 01-10MBps | grafana-1 | 0.0022717 | 643.9 |
| 20261124T022704-sweep-f5200b / 01-10MBps | ingest-a1-1 | 0.019728 | 39.11 |
| 20261124T022704-sweep-f5200b / 01-10MBps | instruction-api-1 | 0.00082953 | 61.05 |
| 20261124T022704-sweep-f5200b / 01-10MBps | loki-1 | 0.00097237 | 64.48 |
| 20261124T022704-sweep-f5200b / 01-10MBps | pg-primary-1 | 0.071847 | 404.1 |
| 20261124T022704-sweep-f5200b / 01-10MBps | plantsim-1 | 0 | 12.23 |
| 20261124T022704-sweep-f5200b / 01-10MBps | portal-publisher-1 | 0.00023275 | 83.16 |
| 20261124T022704-sweep-f5200b / 01-10MBps | processor-1 | 0.0083077 | 9.68 |
| 20261124T022704-sweep-f5200b / 01-10MBps | seaweedfs-1 | 0.00054988 | 80.16 |
| 20261124T022704-sweep-f5200b / 01-10MBps | tempo-1 | 0.00043179 | 36.42 |
| 20261124T022704-sweep-f5200b / 01-10MBps | victoriametrics-1 | 0.00052079 | 71.43 |
| 20261124T022704-sweep-f5200b / 02-20MBps | alloy-1 | 0.0013003 | 47.98 |
| 20261124T022704-sweep-f5200b / 02-20MBps | archiver-1 | 0.00025371 | 15.15 |
| 20261124T022704-sweep-f5200b / 02-20MBps | dispatcher-1-1 | 0.001478 | 80.61 |
| 20261124T022704-sweep-f5200b / 02-20MBps | dynamodb-local-1 | 0.0004838 | 158.5 |
| 20261124T022704-sweep-f5200b / 02-20MBps | elasticmq-1 | 0.0012022 | 65.51 |
| 20261124T022704-sweep-f5200b / 02-20MBps | grafana-1 | 0.0028337 | 436 |
| 20261124T022704-sweep-f5200b / 02-20MBps | ingest-a1-1 | 0.020784 | 107.9 |
| 20261124T022704-sweep-f5200b / 02-20MBps | instruction-api-1 | 0.00097367 | 61.05 |
| 20261124T022704-sweep-f5200b / 02-20MBps | loki-1 | 0.0011148 | 69.74 |
| 20261124T022704-sweep-f5200b / 02-20MBps | pg-primary-1 | 0.084204 | 565.4 |
| 20261124T022704-sweep-f5200b / 02-20MBps | plantsim-1 | 0 | 13.92 |
| 20261124T022704-sweep-f5200b / 02-20MBps | portal-publisher-1 | 0.00024032 | 83.16 |
| 20261124T022704-sweep-f5200b / 02-20MBps | processor-1 | 0.0095862 | 10.22 |
| 20261124T022704-sweep-f5200b / 02-20MBps | seaweedfs-1 | 0.00054116 | 81.61 |
| 20261124T022704-sweep-f5200b / 02-20MBps | tempo-1 | 0.00032077 | 38 |
| 20261124T022704-sweep-f5200b / 02-20MBps | victoriametrics-1 | 0.0012452 | 73.01 |
| 20261124T022704-sweep-f5200b / 03-30MBps | alloy-1 | 0.0014031 | 49.54 |
| 20261124T022704-sweep-f5200b / 03-30MBps | archiver-1 | 0.00013724 | 15.59 |
| 20261124T022704-sweep-f5200b / 03-30MBps | dispatcher-1-1 | 0.0013929 | 80.65 |
| 20261124T022704-sweep-f5200b / 03-30MBps | dynamodb-local-1 | 0.00052603 | 158.6 |
| 20261124T022704-sweep-f5200b / 03-30MBps | elasticmq-1 | 0.00087366 | 67.12 |
| 20261124T022704-sweep-f5200b / 03-30MBps | grafana-1 | 0.0024364 | 440.8 |
| 20261124T022704-sweep-f5200b / 03-30MBps | ingest-a1-1 | 0.028139 | 451.4 |
| 20261124T022704-sweep-f5200b / 03-30MBps | instruction-api-1 | 0.00035373 | 61.05 |
| 20261124T022704-sweep-f5200b / 03-30MBps | loki-1 | 0.0013263 | 71.79 |
| 20261124T022704-sweep-f5200b / 03-30MBps | pg-primary-1 | 0.086917 | 634.9 |
| 20261124T022704-sweep-f5200b / 03-30MBps | plantsim-1 | 0.00038405 | 14.36 |
| 20261124T022704-sweep-f5200b / 03-30MBps | portal-publisher-1 | 0.00024057 | 83.21 |
| 20261124T022704-sweep-f5200b / 03-30MBps | processor-1 | 0.0054306 | 10.25 |
| 20261124T022704-sweep-f5200b / 03-30MBps | seaweedfs-1 | 0.00079735 | 82.29 |
| 20261124T022704-sweep-f5200b / 03-30MBps | tempo-1 | 0.00051616 | 38.78 |
| 20261124T022704-sweep-f5200b / 03-30MBps | victoriametrics-1 | 0.001231 | 74.41 |
| 20261124T022704-sweep-f5200b / 04-40MBps | alloy-1 | 0.001245 | 49.94 |
| 20261124T022704-sweep-f5200b / 04-40MBps | archiver-1 | 0.00014689 | 14.14 |
| 20261124T022704-sweep-f5200b / 04-40MBps | dispatcher-1-1 | 0.0016388 | 80.69 |
| 20261124T022704-sweep-f5200b / 04-40MBps | dynamodb-local-1 | 0.00075134 | 158.9 |
| 20261124T022704-sweep-f5200b / 04-40MBps | elasticmq-1 | 0.0010364 | 67.05 |
| 20261124T022704-sweep-f5200b / 04-40MBps | grafana-1 | 0.003287 | 442.1 |
| 20261124T022704-sweep-f5200b / 04-40MBps | ingest-a1-1 | 0.032334 | 479.1 |
| 20261124T022704-sweep-f5200b / 04-40MBps | instruction-api-1 | 0.00036354 | 61.05 |
| 20261124T022704-sweep-f5200b / 04-40MBps | loki-1 | 0.0012877 | 72.39 |
| 20261124T022704-sweep-f5200b / 04-40MBps | pg-primary-1 | 0.10935 | 659.6 |
| 20261124T022704-sweep-f5200b / 04-40MBps | plantsim-1 | 0.00049702 | 13.64 |
| 20261124T022704-sweep-f5200b / 04-40MBps | portal-publisher-1 | 0.00051015 | 83.21 |
| 20261124T022704-sweep-f5200b / 04-40MBps | processor-1 | 0.0036978 | 10.18 |
| 20261124T022704-sweep-f5200b / 04-40MBps | seaweedfs-1 | 0.0010619 | 82.78 |
| 20261124T022704-sweep-f5200b / 04-40MBps | tempo-1 | 0.00071702 | 39.51 |
| 20261124T022704-sweep-f5200b / 04-40MBps | victoriametrics-1 | 0.0008674 | 75.07 |
| 20261124T024824-repeat-30-1-c6f0fe / 01-30MBps | alloy-1 | 0.0003886 | 55.16 |
| 20261124T024824-repeat-30-1-c6f0fe / 01-30MBps | archiver-1 | 0 | 13.37 |
| 20261124T024824-repeat-30-1-c6f0fe / 01-30MBps | dispatcher-1-1 | 0.00054433 | 80.38 |
| 20261124T024824-repeat-30-1-c6f0fe / 01-30MBps | dynamodb-local-1 | 0.0004792 | 148.1 |
| 20261124T024824-repeat-30-1-c6f0fe / 01-30MBps | elasticmq-1 | 0.00042894 | 48.06 |
| 20261124T024824-repeat-30-1-c6f0fe / 01-30MBps | grafana-1 | 0.00060676 | 358.6 |
| 20261124T024824-repeat-30-1-c6f0fe / 01-30MBps | ingest-a1-1 | 0.015363 | 295.5 |
| 20261124T024824-repeat-30-1-c6f0fe / 01-30MBps | instruction-api-1 | 0.00012386 | 60.6 |
| 20261124T024824-repeat-30-1-c6f0fe / 01-30MBps | loki-1 | 0.00049172 | 53.58 |
| 20261124T024824-repeat-30-1-c6f0fe / 01-30MBps | pg-primary-1 | 0.056098 | 540.3 |
| 20261124T024824-repeat-30-1-c6f0fe / 01-30MBps | plantsim-1 | 4.7831e-05 | 8.414 |
| 20261124T024824-repeat-30-1-c6f0fe / 01-30MBps | portal-publisher-1 | 9.1452e-05 | 81.55 |
| 20261124T024824-repeat-30-1-c6f0fe / 01-30MBps | processor-1 | 0.0073263 | 10.07 |
| 20261124T024824-repeat-30-1-c6f0fe / 01-30MBps | seaweedfs-1 | 0.00091355 | 91.45 |
| 20261124T024824-repeat-30-1-c6f0fe / 01-30MBps | tempo-1 | 0.00014703 | 28.14 |
| 20261124T024824-repeat-30-1-c6f0fe / 01-30MBps | victoriametrics-1 | 0.00021962 | 94.1 |
| 20261124T025428-repeat-30-2-f5c1d7 / 01-30MBps | alloy-1 | 0.00026638 | 52.48 |
| 20261124T025428-repeat-30-2-f5c1d7 / 01-30MBps | archiver-1 | 0 | 10.36 |
| 20261124T025428-repeat-30-2-f5c1d7 / 01-30MBps | dispatcher-1-1 | 0.00062957 | 83.3 |
| 20261124T025428-repeat-30-2-f5c1d7 / 01-30MBps | dynamodb-local-1 | 0.00020818 | 159.7 |
| 20261124T025428-repeat-30-2-f5c1d7 / 01-30MBps | elasticmq-1 | 0.00026775 | 46.6 |
| 20261124T025428-repeat-30-2-f5c1d7 / 01-30MBps | grafana-1 | 0.00054369 | 346 |
| 20261124T025428-repeat-30-2-f5c1d7 / 01-30MBps | ingest-a1-1 | 0.011261 | 85.68 |
| 20261124T025428-repeat-30-2-f5c1d7 / 01-30MBps | instruction-api-1 | 9.2101e-05 | 63.91 |
| 20261124T025428-repeat-30-2-f5c1d7 / 01-30MBps | loki-1 | 0.00044166 | 63.5 |
| 20261124T025428-repeat-30-2-f5c1d7 / 01-30MBps | pg-primary-1 | 0.049806 | 1104.9 |
| 20261124T025428-repeat-30-2-f5c1d7 / 01-30MBps | plantsim-1 | 0 | 9.656 |
| 20261124T025428-repeat-30-2-f5c1d7 / 01-30MBps | portal-publisher-1 | 0.00012527 | 84.88 |
| 20261124T025428-repeat-30-2-f5c1d7 / 01-30MBps | processor-1 | 0.0070133 | 11.5 |
| 20261124T025428-repeat-30-2-f5c1d7 / 01-30MBps | seaweedfs-1 | 0.0005356 | 85.9 |
| 20261124T025428-repeat-30-2-f5c1d7 / 01-30MBps | tempo-1 | 0.00011857 | 26.82 |
| 20261124T025428-repeat-30-2-f5c1d7 / 01-30MBps | victoriametrics-1 | 0.00039223 | 73.51 |
| 20261124T030032-repeat-30-3-f85f6f / 01-30MBps | alloy-1 | 0.00056655 | 60.75 |
| 20261124T030032-repeat-30-3-f85f6f / 01-30MBps | archiver-1 | 0.00019222 | 12.43 |
| 20261124T030032-repeat-30-3-f85f6f / 01-30MBps | dispatcher-1-1 | 0.00093722 | 80.54 |
| 20261124T030032-repeat-30-3-f85f6f / 01-30MBps | dynamodb-local-1 | 0.00040591 | 152.9 |
| 20261124T030032-repeat-30-3-f85f6f / 01-30MBps | elasticmq-1 | 0.00055944 | 49.24 |
| 20261124T030032-repeat-30-3-f85f6f / 01-30MBps | grafana-1 | 0.0021834 | 367.3 |
| 20261124T030032-repeat-30-3-f85f6f / 01-30MBps | ingest-a1-1 | 0.01707 | 360.7 |
| 20261124T030032-repeat-30-3-f85f6f / 01-30MBps | instruction-api-1 | 0.00015175 | 60.85 |
| 20261124T030032-repeat-30-3-f85f6f / 01-30MBps | loki-1 | 0.00057975 | 56.91 |
| 20261124T030032-repeat-30-3-f85f6f / 01-30MBps | pg-primary-1 | 0.061219 | 510.8 |
| 20261124T030032-repeat-30-3-f85f6f / 01-30MBps | plantsim-1 | 0.00016003 | 10.8 |
| 20261124T030032-repeat-30-3-f85f6f / 01-30MBps | portal-publisher-1 | 0.00019249 | 81.88 |
| 20261124T030032-repeat-30-3-f85f6f / 01-30MBps | processor-1 | 0.0070894 | 10.54 |
| 20261124T030032-repeat-30-3-f85f6f / 01-30MBps | seaweedfs-1 | 0.0010177 | 82.24 |
| 20261124T030032-repeat-30-3-f85f6f / 01-30MBps | tempo-1 | 0.00028333 | 30.86 |
| 20261124T030032-repeat-30-3-f85f6f / 01-30MBps | victoriametrics-1 | 0.0010732 | 88.73 |
| 20261124T030703-f1-889f19 / 01-30MBps | alloy-1 | 0.00078281 | 53 |
| 20261124T030703-f1-889f19 / 01-30MBps | archiver-1 | 8.237e-05 | 14.02 |
| 20261124T030703-f1-889f19 / 01-30MBps | dispatcher-1-1 | 0.0007865 | 83.19 |
| 20261124T030703-f1-889f19 / 01-30MBps | dynamodb-local-1 | 0.00059426 | 156.6 |
| 20261124T030703-f1-889f19 / 01-30MBps | elasticmq-1 | 0.00042505 | 44.84 |
| 20261124T030703-f1-889f19 / 01-30MBps | grafana-1 | 0.0013126 | 340.4 |
| 20261124T030703-f1-889f19 / 01-30MBps | ingest-a1-1 | 0.013847 | 172.2 |
| 20261124T030703-f1-889f19 / 01-30MBps | instruction-api-1 | 0.00018853 | 63.96 |
| 20261124T030703-f1-889f19 / 01-30MBps | loki-1 | 0.00068498 | 52.63 |
| 20261124T030703-f1-889f19 / 01-30MBps | pg-primary-1 | 0.05764 | 1060.9 |
| 20261124T030703-f1-889f19 / 01-30MBps | plantsim-1 | 0 | 7.75 |
| 20261124T030703-f1-889f19 / 01-30MBps | portal-publisher-1 | 0.00012052 | 84.88 |
| 20261124T030703-f1-889f19 / 01-30MBps | processor-1 | 0.0074535 | 11.88 |
| 20261124T030703-f1-889f19 / 01-30MBps | seaweedfs-1 | 0.00042487 | 76.45 |
| 20261124T030703-f1-889f19 / 01-30MBps | tempo-1 | 0.00017162 | 28.12 |
| 20261124T030703-f1-889f19 / 01-30MBps | victoriametrics-1 | 0.0003326 | 74.25 |
| 20261124T031306-f2-e68221 / 01-30MBps | alloy-1 | 0.0007598 | 52.65 |
| 20261124T031306-f2-e68221 / 01-30MBps | archiver-1 | 0.00010042 | 10.66 |
| 20261124T031306-f2-e68221 / 01-30MBps | dispatcher-1-1 | 0.0011603 | 79.89 |
| 20261124T031306-f2-e68221 / 01-30MBps | dynamodb-local-1 | 0.00070657 | 143.3 |
| 20261124T031306-f2-e68221 / 01-30MBps | elasticmq-1 | 0.0010694 | 47.31 |
| 20261124T031306-f2-e68221 / 01-30MBps | grafana-1 | 0.0023367 | 360.6 |
| 20261124T031306-f2-e68221 / 01-30MBps | ingest-a1-1 | 0.020785 | 396.3 |
| 20261124T031306-f2-e68221 / 01-30MBps | instruction-api-1 | 0.00058844 | 60.52 |
| 20261124T031306-f2-e68221 / 01-30MBps | loki-1 | 0.001106 | 55.35 |
| 20261124T031306-f2-e68221 / 01-30MBps | pg-primary-1 | 0.053781 | 601 |
| 20261124T031306-f2-e68221 / 01-30MBps | plantsim-1 | 0 | 10.31 |
| 20261124T031306-f2-e68221 / 01-30MBps | portal-publisher-1 | 0.00022456 | 81.75 |
| 20261124T031306-f2-e68221 / 01-30MBps | processor-1 | not measured | 9.344 |
| 20261124T031306-f2-e68221 / 01-30MBps | seaweedfs-1 | 0.0012712 | 78.63 |
| 20261124T031306-f2-e68221 / 01-30MBps | tempo-1 | 0.00029385 | 27.34 |
| 20261124T031306-f2-e68221 / 01-30MBps | victoriametrics-1 | 0.00047071 | 104.7 |

| Run / target | Load generator CPU-s | Max memory MiB |
|---|---:|---:|
| 20261124T022704-sweep-f5200b / 01-10MBps | 29.948 | 646.1 |
| 20261124T022704-sweep-f5200b / 02-20MBps | 74.507 | 672.6 |
| 20261124T022704-sweep-f5200b / 03-30MBps | 137.54 | 737.6 |
| 20261124T022704-sweep-f5200b / 04-40MBps | 169.67 | 773.5 |
| 20261124T024824-repeat-30-1-c6f0fe / 01-30MBps | 58.95 | 697.2 |
| 20261124T025428-repeat-30-2-f5c1d7 / 01-30MBps | 44.271 | 679.9 |
| 20261124T030032-repeat-30-3-f85f6f / 01-30MBps | 71.243 | 718.5 |
| 20261124T030703-f1-889f19 / 01-30MBps | 51.843 | 676.9 |
| 20261124T031306-f2-e68221 / 01-30MBps | 76.586 | 719.5 |

The DB rule compares 202 receipt events with live DB rows plus rows in dropped exports. The Parquet rule also requires the Parquet row count to match.

| Run | 202 receipt events | Live DB rows on first check | Live DB rows on final check | DB rows in dropped exports | DB rows accounted | Parquet rows | Arm-1 loss rule | Result |
|---|---:|---:|---:|---:|---:|---:|---|---|
| 20261124T022704-sweep-f5200b | 14933000 | 14933000 | 14933000 | 0 | 14933000 | not measured (Arm-1 export runs after hour close; see prereg change log) | DB | matched |
| 20261124T024824-repeat-30-1-c6f0fe | 5074000 | 5074000 | 5074000 | 0 | 5074000 | not measured (Arm-1 export runs after hour close; see prereg change log) | DB | matched |
| 20261124T025428-repeat-30-2-f5c1d7 | 5061000 | 5061000 | 5061000 | 0 | 5061000 | not measured (Arm-1 export runs after hour close; see prereg change log) | DB | matched |
| 20261124T030032-repeat-30-3-f85f6f | 4728000 | 4728000 | 4728000 | 0 | 4728000 | not measured (Arm-1 export runs after hour close; see prereg change log) | DB | matched |
| 20261124T030703-f1-889f19 | 4452000 | 4452000 | 4452000 | 0 | 4452000 | not measured (Arm-1 export runs after hour close; see prereg change log) | DB | matched |
| 20261124T031306-f2-e68221 | 4855000 | 4855000 | 4855000 | 0 | 4855000 | not measured (Arm-1 export runs after hour close; see prereg change log) | DB | matched |

### Fresh 30 MB/s repetitions

| Metric | Observations | Median | Min | Max |
|---|---:|---:|---:|---:|
| accepted_MB_s | 1 | 29.999 | 29.999 | 29.999 |
| p50_ms | 1 | 41 | 41 | 41 |
| p99_ms | 1 | 858 | 858 | 858 |
| store_p99_seconds | 1 | 0.8181 | 0.8181 | 0.8181 |
| commit_p99_seconds | 1 | 0.022764 | 0.022764 | 0.022764 |
| lag_max_seconds | 1 | 46.843 | 46.843 | 46.843 |
| lag_recovery_seconds | 1 | 32.668 | 32.668 | 32.668 |
| processor_pending_max | not measured | not measured | not measured | not measured |
| archiver_unacked_max | not measured | not measured | not measured | not measured |
| status_202_ratio | 1 | 1 | 1 | 1 |
| alloy-1 memory_max_MiB | 1 | 55.16 | 55.16 | 55.16 |
| alloy-1 cpu_seconds_per_accepted_MB | 1 | 0.0003886 | 0.0003886 | 0.0003886 |
| archiver-1 memory_max_MiB | 1 | 13.37 | 13.37 | 13.37 |
| archiver-1 cpu_seconds_per_accepted_MB | 1 | 0 | 0 | 0 |
| dispatcher-1-1 memory_max_MiB | 1 | 80.38 | 80.38 | 80.38 |
| dispatcher-1-1 cpu_seconds_per_accepted_MB | 1 | 0.00054433 | 0.00054433 | 0.00054433 |
| dynamodb-local-1 memory_max_MiB | 1 | 148.1 | 148.1 | 148.1 |
| dynamodb-local-1 cpu_seconds_per_accepted_MB | 1 | 0.0004792 | 0.0004792 | 0.0004792 |
| elasticmq-1 memory_max_MiB | 1 | 48.06 | 48.06 | 48.06 |
| elasticmq-1 cpu_seconds_per_accepted_MB | 1 | 0.00042894 | 0.00042894 | 0.00042894 |
| grafana-1 memory_max_MiB | 1 | 358.6 | 358.6 | 358.6 |
| grafana-1 cpu_seconds_per_accepted_MB | 1 | 0.00060676 | 0.00060676 | 0.00060676 |
| ingest-a1-1 memory_max_MiB | 1 | 295.5 | 295.5 | 295.5 |
| ingest-a1-1 cpu_seconds_per_accepted_MB | 1 | 0.015363 | 0.015363 | 0.015363 |
| instruction-api-1 memory_max_MiB | 1 | 60.6 | 60.6 | 60.6 |
| instruction-api-1 cpu_seconds_per_accepted_MB | 1 | 0.00012386 | 0.00012386 | 0.00012386 |
| loki-1 memory_max_MiB | 1 | 53.58 | 53.58 | 53.58 |
| loki-1 cpu_seconds_per_accepted_MB | 1 | 0.00049172 | 0.00049172 | 0.00049172 |
| pg-primary-1 memory_max_MiB | 1 | 540.3 | 540.3 | 540.3 |
| pg-primary-1 cpu_seconds_per_accepted_MB | 1 | 0.056098 | 0.056098 | 0.056098 |
| plantsim-1 memory_max_MiB | 1 | 8.414 | 8.414 | 8.414 |
| plantsim-1 cpu_seconds_per_accepted_MB | 1 | 4.7831e-05 | 4.7831e-05 | 4.7831e-05 |
| portal-publisher-1 memory_max_MiB | 1 | 81.55 | 81.55 | 81.55 |
| portal-publisher-1 cpu_seconds_per_accepted_MB | 1 | 9.1452e-05 | 9.1452e-05 | 9.1452e-05 |
| processor-1 memory_max_MiB | 1 | 10.07 | 10.07 | 10.07 |
| processor-1 cpu_seconds_per_accepted_MB | 1 | 0.0073263 | 0.0073263 | 0.0073263 |
| seaweedfs-1 memory_max_MiB | 1 | 91.45 | 91.45 | 91.45 |
| seaweedfs-1 cpu_seconds_per_accepted_MB | 1 | 0.00091355 | 0.00091355 | 0.00091355 |
| tempo-1 memory_max_MiB | 1 | 28.14 | 28.14 | 28.14 |
| tempo-1 cpu_seconds_per_accepted_MB | 1 | 0.00014703 | 0.00014703 | 0.00014703 |
| victoriametrics-1 memory_max_MiB | 1 | 94.1 | 94.1 | 94.1 |
| victoriametrics-1 cpu_seconds_per_accepted_MB | 1 | 0.00021962 | 0.00021962 | 0.00021962 |

## go-split

| Run / target | MB/s per server offered | Accepted MB/s | Status ratios | Requests not sent (warmup + measure) | Generator starved | Ingest p50 ms | Ingest p99 ms | Store p99 s | Commit p99 s | Lag max s | Lag recovery s | Processor pending max | Archiver unacked max | Loss check |
|---|---:|---:|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| 20261124T051251-sweep-45dcc1 / 01-10MBps (invalid: dropped iterations) | 10 | 18.974 | 202 95.87%, 503 4.13% | 6 | 0 | 71 | 2654 | 2.2635 | 3 | 11.533 | 2.749 | 80 | 109 | not measured |
| 20261124T051251-sweep-45dcc1 / 02-20MBps (invalid: generator starved) | 20 | 30.855 | 202 88.67%, 503 11.33% | 494 | 468 | 448 | 5383 | 3.528 | 8.83 | 37.443 | 21.906 | 854 | 140 | not measured |
| 20261124T051251-sweep-45dcc1 / 03-30MBps (invalid: generator starved) | 30 | 29.542 | 202 90.93%, 503 9.07% | 3871 | 3847 | 454 | 6573 | 4.6714 | 8.5125 | 81.744 | 44.597 | 1698 | 150 | not measured |
| 20261124T051251-sweep-45dcc1 / 04-40MBps (invalid: generator starved) | 40 | 36.815 | 202 91.03%, 503 8.97% | 6547 | 6365 | 447 | 6882 | 4.7432 | 7.175 | 83.79 | not measured | 2501 | 136 | not measured |
| 20261124T055207-repeat-30-1-1fb9de / 01-30MBps (invalid: generator starved) | 30 | 35.943 | 202 86.03%, 503 13.97% | 2668 | 2549 | 527 | 8178 | 4.696 | 8.0167 | 84.074 | 45.737 | 2041 | 153 | matched |
| 20261124T060437-repeat-30-2-db8a79 / 01-30MBps (invalid: generator starved) | 30 | 36.395 | 202 85.94%, 503 14.06% | 2297 | 2228 | 560 | 5413 | 4.256 | 8.5625 | 103.03 | 52.36 | 2583 | 139 | matched |
| 20261124T061521-repeat-30-3-adeb87 / 01-30MBps (invalid: generator starved) | 30 | 39.416 | 202 86.87%, 503 13.13% | 1625 | 1429 | 503 | 6360 | 4.6362 | 9.3062 | 72.835 | 46.437 | 1978 | 165 | matched |
| 20261124T062630-f1-61d0eb / 01-30MBps (invalid: generator starved) | 30 | 42.22 | 202 85.72%, 503 14.28% | 1426 requests not sent | 1264 | 407 | 7226 | 4.5215 | 10 | 106.95 | 58.411 | 2973 | 112 | matched |
| 20261124T062630-f1-61d0eb / F1 interruption | 30 | 52.022 | 202 89.98%, 503 10.02% | not measured | not measured | 110.44 | 3279.8 | not measured | not measured | 9.913 | 207.59 | not measured | not measured | matched |
| 20261124T063829-f2-ede95b / 01-30MBps (invalid: generator starved) | 30 | 42.532 | 202 89.73%, 503 10.27% | 1682 requests not sent | 1575 | 275 | 5075 | 4.4453 | 9.25 | 147.8 | not measured | 4390 | 178 | matched |
| 20261124T063829-f2-ede95b / F2 interruption | 30 | 55.517 | 202 96.49%, 503 3.51% | not measured | not measured | 69.72 | 2435.3 | not measured | not measured | not measured | 3.7717 | not measured | not measured | matched |
| 20261124T065206-sweep-64b682 / 01-10MBps (invalid: dropped iterations) | 10 | 19.981 | 202 100.00% | 2 | 0 | 25 | 645 | 0.33861 | 0.9375 | 1.0983 | 4.212 | 30 | 98 | matched |
| 20261124T065206-sweep-64b682 / 02-20MBps (invalid: generator starved) | 20 | 22.911 | 202 74.97%, 503 25.03% | 872 | 837 | 560 | 7013 | 6.4304 | 10 | 63.651 | 43.518 | 1192 | 136 | matched |
| 20261124T065206-sweep-64b682 / 03-30MBps (invalid: generator starved) | 30 | 20.125 | 202 76.78%, 503 23.22% | 5317 | 5268 | 656 | 4793 | 4.7167 | 4.9643 | 113.96 | 45.649 | 1571 | 139 | matched |
| 20261124T065206-sweep-64b682 / 04-40MBps (invalid: generator starved) | 40 | 24.583 | 202 89.92%, 503 10.08% | 8640 | 8584 | 623 | 4759 | 3.8775 | 9.3375 | 101.63 | not measured | 1714 | 126 | matched |

| Run / target | Ingest server | Achieved wire MB/s | Accepted MB/s | Status ratios | Ingest p50 ms | Ingest p95 ms | Ingest p99 ms |
|---|---|---:|---:|---|---:|---:|---:|
| 20261124T051251-sweep-45dcc1 / 01-10MBps | ingest-a1 | 9.9888 | 9.4038 | 202 95.03%, 503 4.97% | 66 | 1313 | 2667 |
| 20261124T051251-sweep-45dcc1 / 01-10MBps | ingest-a2 | 9.9908 | 9.5705 | 202 96.70%, 503 3.30% | 78 | 1312 | 2654 |
| 20261124T051251-sweep-45dcc1 / 02-20MBps | ingest-a1 | 18.543 | 14.921 | 202 86.74%, 503 13.26% | 472 | 3308 | 5713 |
| 20261124T051251-sweep-45dcc1 / 02-20MBps | ingest-a2 | 18.331 | 15.934 | 202 90.62%, 503 9.38% | 433 | 2632 | 4311 |
| 20261124T051251-sweep-45dcc1 / 03-30MBps | ingest-a1 | 16.991 | 14.364 | 202 89.27%, 503 10.73% | 441 | 3356 | 8011 |
| 20261124T051251-sweep-45dcc1 / 03-30MBps | ingest-a2 | 17.06 | 15.179 | 202 92.59%, 503 7.41% | 466 | 4125 | 5990 |
| 20261124T051251-sweep-45dcc1 / 04-40MBps | ingest-a1 | 20.992 | 18.293 | 202 90.67%, 503 9.33% | 430 | 3394 | 6675 |
| 20261124T051251-sweep-45dcc1 / 04-40MBps | ingest-a2 | 20.898 | 18.522 | 202 91.38%, 503 8.62% | 464 | 4264 | 7147 |
| 20261124T055207-repeat-30-1-1fb9de / 01-30MBps | ingest-a1 | 22.501 | 17.931 | 202 84.52%, 503 15.48% | 550 | 5022 | 9693 |
| 20261124T055207-repeat-30-1-1fb9de / 01-30MBps | ingest-a2 | 22.008 | 18.012 | 202 87.58%, 503 12.42% | 506 | 4268 | 6240 |
| 20261124T060437-repeat-30-2-db8a79 / 01-30MBps | ingest-a1 | 22.786 | 18.745 | 202 87.96%, 503 12.04% | 553 | 3172 | 5111 |
| 20261124T060437-repeat-30-2-db8a79 / 01-30MBps | ingest-a2 | 23.017 | 17.649 | 202 83.94%, 503 16.06% | 573 | 3408 | 5974 |
| 20261124T061521-repeat-30-3-adeb87 / 01-30MBps | ingest-a1 | 24.291 | 19.958 | 202 86.85%, 503 13.15% | 471 | 3851 | 6083 |
| 20261124T061521-repeat-30-3-adeb87 / 01-30MBps | ingest-a2 | 24.104 | 19.458 | 202 86.89%, 503 13.11% | 542 | 4705 | 6536 |
| 20261124T062630-f1-61d0eb / 01-30MBps | ingest-a1 | 25.562 | 21.57 | 202 87.68%, 503 12.32% | 434 | 3496 | 6784 |
| 20261124T062630-f1-61d0eb / 01-30MBps | ingest-a2 | 25.609 | 20.65 | 202 83.76%, 503 16.24% | 380 | 4138 | 8191 |
| 20261124T063829-f2-ede95b / 01-30MBps | ingest-a1 | 24.903 | 21.283 | 202 90.46%, 503 9.54% | 280 | 2858 | 4490 |
| 20261124T063829-f2-ede95b / 01-30MBps | ingest-a2 | 24.998 | 21.249 | 202 88.99%, 503 11.01% | 274 | 3591 | 5625 |
| 20261124T065206-sweep-64b682 / 01-10MBps | ingest-a1 | 10.013 | 10.013 | 202 100.00% | 28 | 283 | 651 |
| 20261124T065206-sweep-64b682 / 01-10MBps | ingest-a2 | 9.9678 | 9.9678 | 202 100.00% | 24 | 277 | 645 |
| 20261124T065206-sweep-64b682 / 02-20MBps | ingest-a1 | 16.917 | 11.384 | 202 73.99%, 503 26.01% | 579 | 4233 | 7475 |
| 20261124T065206-sweep-64b682 / 02-20MBps | ingest-a2 | 16.73 | 11.527 | 202 75.95%, 503 24.05% | 551 | 4157 | 5619 |
| 20261124T065206-sweep-64b682 / 03-30MBps | ingest-a1 | 14.188 | 9.6303 | 202 73.69%, 503 26.31% | 600 | 3546 | 5387 |
| 20261124T065206-sweep-64b682 / 03-30MBps | ingest-a2 | 14.134 | 10.495 | 202 79.91%, 503 20.09% | 704 | 3517 | 4534 |
| 20261124T065206-sweep-64b682 / 04-40MBps | ingest-a1 | 13.829 | 12.358 | 202 91.46%, 503 8.54% | 653 | 2769 | 4296 |
| 20261124T065206-sweep-64b682 / 04-40MBps | ingest-a2 | 14.303 | 12.225 | 202 88.44%, 503 11.56% | 591 | 2836 | 5710 |

| Run / target | Container | CPU-s / accepted MB | Max memory MiB |
|---|---|---:|---:|
| 20261124T051251-sweep-45dcc1 / 01-10MBps | alloy-1 | not measured | 82.81 |
| 20261124T051251-sweep-45dcc1 / 01-10MBps | archiver-1 | not measured | 726.6 |
| 20261124T051251-sweep-45dcc1 / 01-10MBps | dispatcher-1-1 | not measured | 78.09 |
| 20261124T051251-sweep-45dcc1 / 01-10MBps | dispatcher-2-1 | not measured | 76.69 |
| 20261124T051251-sweep-45dcc1 / 01-10MBps | dynamodb-local-1 | not measured | 166.1 |
| 20261124T051251-sweep-45dcc1 / 01-10MBps | elasticmq-1 | not measured | 51.69 |
| 20261124T051251-sweep-45dcc1 / 01-10MBps | grafana-1 | not measured | 404.7 |
| 20261124T051251-sweep-45dcc1 / 01-10MBps | ingest-a1-1 | not measured | 98.37 |
| 20261124T051251-sweep-45dcc1 / 01-10MBps | ingest-a2-1 | not measured | 100.7 |
| 20261124T051251-sweep-45dcc1 / 01-10MBps | instruction-api-1 | not measured | 58.57 |
| 20261124T051251-sweep-45dcc1 / 01-10MBps | loki-1 | not measured | 71.43 |
| 20261124T051251-sweep-45dcc1 / 01-10MBps | nats-1 | not measured | 1396.7 |
| 20261124T051251-sweep-45dcc1 / 01-10MBps | pg-primary-1 | not measured | 58.45 |
| 20261124T051251-sweep-45dcc1 / 01-10MBps | pg-standby-1 | not measured | 29.66 |
| 20261124T051251-sweep-45dcc1 / 01-10MBps | plantsim-1 | not measured | 11.8 |
| 20261124T051251-sweep-45dcc1 / 01-10MBps | portal-publisher-1 | not measured | 80.36 |
| 20261124T051251-sweep-45dcc1 / 01-10MBps | processor-1 | not measured | 184.6 |
| 20261124T051251-sweep-45dcc1 / 01-10MBps | seaweedfs-1 | not measured | 732.9 |
| 20261124T051251-sweep-45dcc1 / 01-10MBps | tempo-1 | not measured | 49.11 |
| 20261124T051251-sweep-45dcc1 / 01-10MBps | victoriametrics-1 | not measured | 86.8 |
| 20261124T051251-sweep-45dcc1 / 02-20MBps | alloy-1 | not measured | 51.86 |
| 20261124T051251-sweep-45dcc1 / 02-20MBps | archiver-1 | not measured | 900.3 |
| 20261124T051251-sweep-45dcc1 / 02-20MBps | dispatcher-1-1 | not measured | 42.49 |
| 20261124T051251-sweep-45dcc1 / 02-20MBps | dispatcher-2-1 | not measured | 40.66 |
| 20261124T051251-sweep-45dcc1 / 02-20MBps | dynamodb-local-1 | not measured | 113.8 |
| 20261124T051251-sweep-45dcc1 / 02-20MBps | elasticmq-1 | not measured | 64.66 |
| 20261124T051251-sweep-45dcc1 / 02-20MBps | grafana-1 | not measured | 298.7 |
| 20261124T051251-sweep-45dcc1 / 02-20MBps | ingest-a1-1 | not measured | 501.5 |
| 20261124T051251-sweep-45dcc1 / 02-20MBps | ingest-a2-1 | not measured | 401.8 |
| 20261124T051251-sweep-45dcc1 / 02-20MBps | instruction-api-1 | not measured | 27.29 |
| 20261124T051251-sweep-45dcc1 / 02-20MBps | loki-1 | not measured | 61.81 |
| 20261124T051251-sweep-45dcc1 / 02-20MBps | nats-1 | not measured | 797.9 |
| 20261124T051251-sweep-45dcc1 / 02-20MBps | pg-primary-1 | not measured | 45.6 |
| 20261124T051251-sweep-45dcc1 / 02-20MBps | pg-standby-1 | not measured | 19.99 |
| 20261124T051251-sweep-45dcc1 / 02-20MBps | plantsim-1 | not measured | 15.58 |
| 20261124T051251-sweep-45dcc1 / 02-20MBps | portal-publisher-1 | not measured | 43.34 |
| 20261124T051251-sweep-45dcc1 / 02-20MBps | processor-1 | not measured | 444.5 |
| 20261124T051251-sweep-45dcc1 / 02-20MBps | seaweedfs-1 | not measured | 754.9 |
| 20261124T051251-sweep-45dcc1 / 02-20MBps | tempo-1 | not measured | 30.46 |
| 20261124T051251-sweep-45dcc1 / 02-20MBps | victoriametrics-1 | not measured | 82.94 |
| 20261124T051251-sweep-45dcc1 / 03-30MBps | alloy-1 | 0.0017014 | 46.93 |
| 20261124T051251-sweep-45dcc1 / 03-30MBps | archiver-1 | 0.021559 | 859.4 |
| 20261124T051251-sweep-45dcc1 / 03-30MBps | dispatcher-1-1 | 0.0037651 | 22.9 |
| 20261124T051251-sweep-45dcc1 / 03-30MBps | dispatcher-2-1 | 0.0013786 | 14.87 |
| 20261124T051251-sweep-45dcc1 / 03-30MBps | dynamodb-local-1 | 0.0033163 | 111.7 |
| 20261124T051251-sweep-45dcc1 / 03-30MBps | elasticmq-1 | 0.0016207 | 59.15 |
| 20261124T051251-sweep-45dcc1 / 03-30MBps | grafana-1 | 0.0022013 | 268 |
| 20261124T051251-sweep-45dcc1 / 03-30MBps | ingest-a1-1 | 0.027928 | 719.5 |
| 20261124T051251-sweep-45dcc1 / 03-30MBps | ingest-a2-1 | 0.024325 | 508.9 |
| 20261124T051251-sweep-45dcc1 / 03-30MBps | instruction-api-1 | 0.00046403 | 14.77 |
| 20261124T051251-sweep-45dcc1 / 03-30MBps | loki-1 | 0.0020257 | 60.45 |
| 20261124T051251-sweep-45dcc1 / 03-30MBps | nats-1 | 0.038316 | 874.7 |
| 20261124T051251-sweep-45dcc1 / 03-30MBps | pg-primary-1 | 0.0048271 | 43.9 |
| 20261124T051251-sweep-45dcc1 / 03-30MBps | pg-standby-1 | 0.0036668 | 21.54 |
| 20261124T051251-sweep-45dcc1 / 03-30MBps | plantsim-1 | 0 | 11.96 |
| 20261124T051251-sweep-45dcc1 / 03-30MBps | portal-publisher-1 | 0.00053073 | 16.66 |
| 20261124T051251-sweep-45dcc1 / 03-30MBps | processor-1 | 0.021505 | 390.9 |
| 20261124T051251-sweep-45dcc1 / 03-30MBps | seaweedfs-1 | 0.015269 | 765.7 |
| 20261124T051251-sweep-45dcc1 / 03-30MBps | tempo-1 | 0.00086168 | 33.71 |
| 20261124T051251-sweep-45dcc1 / 03-30MBps | victoriametrics-1 | 0.0022115 | 70.3 |
| 20261124T051251-sweep-45dcc1 / 04-40MBps | alloy-1 | not measured | 46.26 |
| 20261124T051251-sweep-45dcc1 / 04-40MBps | archiver-1 | not measured | 931.5 |
| 20261124T051251-sweep-45dcc1 / 04-40MBps | dispatcher-1-1 | not measured | 23.02 |
| 20261124T051251-sweep-45dcc1 / 04-40MBps | dispatcher-2-1 | not measured | 14.74 |
| 20261124T051251-sweep-45dcc1 / 04-40MBps | dynamodb-local-1 | not measured | 108.7 |
| 20261124T051251-sweep-45dcc1 / 04-40MBps | elasticmq-1 | not measured | 57.64 |
| 20261124T051251-sweep-45dcc1 / 04-40MBps | grafana-1 | not measured | 259 |
| 20261124T051251-sweep-45dcc1 / 04-40MBps | ingest-a1-1 | not measured | 585.6 |
| 20261124T051251-sweep-45dcc1 / 04-40MBps | ingest-a2-1 | not measured | 703.3 |
| 20261124T051251-sweep-45dcc1 / 04-40MBps | instruction-api-1 | not measured | 11.57 |
| 20261124T051251-sweep-45dcc1 / 04-40MBps | loki-1 | not measured | 58.74 |
| 20261124T051251-sweep-45dcc1 / 04-40MBps | nats-1 | not measured | 734.4 |
| 20261124T051251-sweep-45dcc1 / 04-40MBps | pg-primary-1 | not measured | 44.51 |
| 20261124T051251-sweep-45dcc1 / 04-40MBps | pg-standby-1 | not measured | 23.71 |
| 20261124T051251-sweep-45dcc1 / 04-40MBps | plantsim-1 | not measured | 13.92 |
| 20261124T051251-sweep-45dcc1 / 04-40MBps | portal-publisher-1 | not measured | 16.48 |
| 20261124T051251-sweep-45dcc1 / 04-40MBps | processor-1 | not measured | 430.4 |
| 20261124T051251-sweep-45dcc1 / 04-40MBps | seaweedfs-1 | not measured | 816.3 |
| 20261124T051251-sweep-45dcc1 / 04-40MBps | tempo-1 | not measured | 28.72 |
| 20261124T051251-sweep-45dcc1 / 04-40MBps | victoriametrics-1 | not measured | 69.82 |
| 20261124T055207-repeat-30-1-1fb9de / 01-30MBps | alloy-1 | not measured | 50.11 |
| 20261124T055207-repeat-30-1-1fb9de / 01-30MBps | archiver-1 | not measured | 819 |
| 20261124T055207-repeat-30-1-1fb9de / 01-30MBps | dispatcher-1-1 | not measured | 42.56 |
| 20261124T055207-repeat-30-1-1fb9de / 01-30MBps | dispatcher-2-1 | not measured | 41.76 |
| 20261124T055207-repeat-30-1-1fb9de / 01-30MBps | dynamodb-local-1 | not measured | 109.6 |
| 20261124T055207-repeat-30-1-1fb9de / 01-30MBps | elasticmq-1 | not measured | 28.08 |
| 20261124T055207-repeat-30-1-1fb9de / 01-30MBps | grafana-1 | not measured | 408.9 |
| 20261124T055207-repeat-30-1-1fb9de / 01-30MBps | ingest-a1-1 | not measured | 862 |
| 20261124T055207-repeat-30-1-1fb9de / 01-30MBps | ingest-a2-1 | not measured | 613.8 |
| 20261124T055207-repeat-30-1-1fb9de / 01-30MBps | instruction-api-1 | not measured | 30.03 |
| 20261124T055207-repeat-30-1-1fb9de / 01-30MBps | loki-1 | not measured | 58.11 |
| 20261124T055207-repeat-30-1-1fb9de / 01-30MBps | nats-1 | not measured | 676.1 |
| 20261124T055207-repeat-30-1-1fb9de / 01-30MBps | pg-primary-1 | not measured | 42.18 |
| 20261124T055207-repeat-30-1-1fb9de / 01-30MBps | pg-standby-1 | not measured | 17.66 |
| 20261124T055207-repeat-30-1-1fb9de / 01-30MBps | plantsim-1 | not measured | 10.22 |
| 20261124T055207-repeat-30-1-1fb9de / 01-30MBps | portal-publisher-1 | not measured | 41.89 |
| 20261124T055207-repeat-30-1-1fb9de / 01-30MBps | processor-1 | not measured | 448.7 |
| 20261124T055207-repeat-30-1-1fb9de / 01-30MBps | seaweedfs-1 | not measured | 712.7 |
| 20261124T055207-repeat-30-1-1fb9de / 01-30MBps | tempo-1 | not measured | 39.98 |
| 20261124T055207-repeat-30-1-1fb9de / 01-30MBps | victoriametrics-1 | not measured | 86.84 |
| 20261124T060437-repeat-30-2-db8a79 / 01-30MBps | alloy-1 | not measured | 54.76 |
| 20261124T060437-repeat-30-2-db8a79 / 01-30MBps | archiver-1 | not measured | 746.9 |
| 20261124T060437-repeat-30-2-db8a79 / 01-30MBps | dispatcher-1-1 | not measured | 65.71 |
| 20261124T060437-repeat-30-2-db8a79 / 01-30MBps | dispatcher-2-1 | not measured | 68.9 |
| 20261124T060437-repeat-30-2-db8a79 / 01-30MBps | dynamodb-local-1 | not measured | 171.8 |
| 20261124T060437-repeat-30-2-db8a79 / 01-30MBps | elasticmq-1 | not measured | 50.42 |
| 20261124T060437-repeat-30-2-db8a79 / 01-30MBps | grafana-1 | not measured | 493 |
| 20261124T060437-repeat-30-2-db8a79 / 01-30MBps | ingest-a1-1 | not measured | 538.8 |
| 20261124T060437-repeat-30-2-db8a79 / 01-30MBps | ingest-a2-1 | not measured | 559.9 |
| 20261124T060437-repeat-30-2-db8a79 / 01-30MBps | instruction-api-1 | not measured | 47.91 |
| 20261124T060437-repeat-30-2-db8a79 / 01-30MBps | loki-1 | not measured | 79.07 |
| 20261124T060437-repeat-30-2-db8a79 / 01-30MBps | nats-1 | not measured | 665.8 |
| 20261124T060437-repeat-30-2-db8a79 / 01-30MBps | pg-primary-1 | not measured | 41.39 |
| 20261124T060437-repeat-30-2-db8a79 / 01-30MBps | pg-standby-1 | not measured | 25.34 |
| 20261124T060437-repeat-30-2-db8a79 / 01-30MBps | plantsim-1 | not measured | 12.93 |
| 20261124T060437-repeat-30-2-db8a79 / 01-30MBps | portal-publisher-1 | not measured | 67.13 |
| 20261124T060437-repeat-30-2-db8a79 / 01-30MBps | processor-1 | not measured | 509.6 |
| 20261124T060437-repeat-30-2-db8a79 / 01-30MBps | seaweedfs-1 | not measured | 731.3 |
| 20261124T060437-repeat-30-2-db8a79 / 01-30MBps | tempo-1 | not measured | 44.3 |
| 20261124T060437-repeat-30-2-db8a79 / 01-30MBps | victoriametrics-1 | not measured | 91.18 |
| 20261124T061521-repeat-30-3-adeb87 / 01-30MBps | alloy-1 | not measured | 71.96 |
| 20261124T061521-repeat-30-3-adeb87 / 01-30MBps | archiver-1 | not measured | 940 |
| 20261124T061521-repeat-30-3-adeb87 / 01-30MBps | dispatcher-1-1 | not measured | 65.1 |
| 20261124T061521-repeat-30-3-adeb87 / 01-30MBps | dispatcher-2-1 | not measured | 60.61 |
| 20261124T061521-repeat-30-3-adeb87 / 01-30MBps | dynamodb-local-1 | not measured | 161.4 |
| 20261124T061521-repeat-30-3-adeb87 / 01-30MBps | elasticmq-1 | not measured | 49.59 |
| 20261124T061521-repeat-30-3-adeb87 / 01-30MBps | grafana-1 | not measured | 471.8 |
| 20261124T061521-repeat-30-3-adeb87 / 01-30MBps | ingest-a1-1 | not measured | 583.6 |
| 20261124T061521-repeat-30-3-adeb87 / 01-30MBps | ingest-a2-1 | not measured | 724 |
| 20261124T061521-repeat-30-3-adeb87 / 01-30MBps | instruction-api-1 | not measured | 49.03 |
| 20261124T061521-repeat-30-3-adeb87 / 01-30MBps | loki-1 | not measured | 77.89 |
| 20261124T061521-repeat-30-3-adeb87 / 01-30MBps | nats-1 | not measured | 672.9 |
| 20261124T061521-repeat-30-3-adeb87 / 01-30MBps | pg-primary-1 | not measured | 48.78 |
| 20261124T061521-repeat-30-3-adeb87 / 01-30MBps | pg-standby-1 | not measured | 27 |
| 20261124T061521-repeat-30-3-adeb87 / 01-30MBps | plantsim-1 | not measured | 15.71 |
| 20261124T061521-repeat-30-3-adeb87 / 01-30MBps | portal-publisher-1 | not measured | 68.36 |
| 20261124T061521-repeat-30-3-adeb87 / 01-30MBps | processor-1 | not measured | 453.8 |
| 20261124T061521-repeat-30-3-adeb87 / 01-30MBps | seaweedfs-1 | not measured | 731.6 |
| 20261124T061521-repeat-30-3-adeb87 / 01-30MBps | tempo-1 | not measured | 58.04 |
| 20261124T061521-repeat-30-3-adeb87 / 01-30MBps | victoriametrics-1 | not measured | 81.83 |
| 20261124T062630-f1-61d0eb / 01-30MBps | alloy-1 | 0.00061851 | 52.19 |
| 20261124T062630-f1-61d0eb / 01-30MBps | archiver-1 | 0.012874 | 868.5 |
| 20261124T062630-f1-61d0eb / 01-30MBps | dispatcher-1-1 | 0.0016648 | 50.73 |
| 20261124T062630-f1-61d0eb / 01-30MBps | dispatcher-2-1 | 0.00058068 | 44.41 |
| 20261124T062630-f1-61d0eb / 01-30MBps | dynamodb-local-1 | 0.0027139 | 123.6 |
| 20261124T062630-f1-61d0eb / 01-30MBps | elasticmq-1 | 0.00088799 | 24.04 |
| 20261124T062630-f1-61d0eb / 01-30MBps | grafana-1 | 0.0022837 | 433.2 |
| 20261124T062630-f1-61d0eb / 01-30MBps | ingest-a1-1 | 0.019213 | 773.9 |
| 20261124T062630-f1-61d0eb / 01-30MBps | ingest-a2-1 | 0.01752 | 739.8 |
| 20261124T062630-f1-61d0eb / 01-30MBps | instruction-api-1 | 0.0002682 | 31.2 |
| 20261124T062630-f1-61d0eb / 01-30MBps | loki-1 | 0.00084985 | 61.39 |
| 20261124T062630-f1-61d0eb / 01-30MBps | nats-1 | 0.021648 | 675.1 |
| 20261124T062630-f1-61d0eb / 01-30MBps | pg-primary-1 | 0.0016776 | 48.61 |
| 20261124T062630-f1-61d0eb / 01-30MBps | pg-standby-1 | 0.001917 | 16.29 |
| 20261124T062630-f1-61d0eb / 01-30MBps | plantsim-1 | 9.0273e-05 | 10.24 |
| 20261124T062630-f1-61d0eb / 01-30MBps | portal-publisher-1 | 0.00026033 | 50.23 |
| 20261124T062630-f1-61d0eb / 01-30MBps | processor-1 | 0.012619 | 437.2 |
| 20261124T062630-f1-61d0eb / 01-30MBps | seaweedfs-1 | 0.011198 | 756.3 |
| 20261124T062630-f1-61d0eb / 01-30MBps | tempo-1 | 0.00051652 | 32.87 |
| 20261124T062630-f1-61d0eb / 01-30MBps | victoriametrics-1 | 0.0014825 | 72 |
| 20261124T063829-f2-ede95b / 01-30MBps | alloy-1 | not measured | 56.79 |
| 20261124T063829-f2-ede95b / 01-30MBps | archiver-1 | not measured | 932.6 |
| 20261124T063829-f2-ede95b / 01-30MBps | dispatcher-1-1 | not measured | 68.15 |
| 20261124T063829-f2-ede95b / 01-30MBps | dispatcher-2-1 | not measured | 63.23 |
| 20261124T063829-f2-ede95b / 01-30MBps | dynamodb-local-1 | not measured | 172.3 |
| 20261124T063829-f2-ede95b / 01-30MBps | elasticmq-1 | not measured | 58.23 |
| 20261124T063829-f2-ede95b / 01-30MBps | grafana-1 | not measured | 475 |
| 20261124T063829-f2-ede95b / 01-30MBps | ingest-a1-1 | not measured | 712.9 |
| 20261124T063829-f2-ede95b / 01-30MBps | ingest-a2-1 | not measured | 630.2 |
| 20261124T063829-f2-ede95b / 01-30MBps | instruction-api-1 | not measured | 47.93 |
| 20261124T063829-f2-ede95b / 01-30MBps | loki-1 | not measured | 76.76 |
| 20261124T063829-f2-ede95b / 01-30MBps | nats-1 | not measured | 691.6 |
| 20261124T063829-f2-ede95b / 01-30MBps | pg-primary-1 | not measured | 43.57 |
| 20261124T063829-f2-ede95b / 01-30MBps | pg-standby-1 | not measured | 25.99 |
| 20261124T063829-f2-ede95b / 01-30MBps | plantsim-1 | not measured | 13.99 |
| 20261124T063829-f2-ede95b / 01-30MBps | portal-publisher-1 | not measured | 67.01 |
| 20261124T063829-f2-ede95b / 01-30MBps | processor-1 | not measured | 503.3 |
| 20261124T063829-f2-ede95b / 01-30MBps | seaweedfs-1 | not measured | 788.5 |
| 20261124T063829-f2-ede95b / 01-30MBps | tempo-1 | not measured | 43.73 |
| 20261124T063829-f2-ede95b / 01-30MBps | victoriametrics-1 | not measured | 76.91 |
| 20261124T065206-sweep-64b682 / 01-10MBps | alloy-1 | 0.0012292 | 172.9 |
| 20261124T065206-sweep-64b682 / 01-10MBps | archiver-1 | 0.024791 | 582.6 |
| 20261124T065206-sweep-64b682 / 01-10MBps | dispatcher-1-1 | 0.00080015 | 85.85 |
| 20261124T065206-sweep-64b682 / 01-10MBps | dispatcher-2-1 | 0.0022714 | 87.66 |
| 20261124T065206-sweep-64b682 / 01-10MBps | dynamodb-local-1 | 0.0028676 | 190.5 |
| 20261124T065206-sweep-64b682 / 01-10MBps | elasticmq-1 | 0.00092687 | 58.45 |
| 20261124T065206-sweep-64b682 / 01-10MBps | grafana-1 | 0.0031342 | 508.8 |
| 20261124T065206-sweep-64b682 / 01-10MBps | ingest-a1-1 | 0.0085197 | 59.16 |
| 20261124T065206-sweep-64b682 / 01-10MBps | ingest-a2-1 | 0.008816 | 58.11 |
| 20261124T065206-sweep-64b682 / 01-10MBps | instruction-api-1 | 0.00030372 | 67.08 |
| 20261124T065206-sweep-64b682 / 01-10MBps | loki-1 | 0.0011173 | 116 |
| 20261124T065206-sweep-64b682 / 01-10MBps | nats-1 | 0.01613 | 1197.1 |
| 20261124T065206-sweep-64b682 / 01-10MBps | pg-primary-1 | 0.0035723 | 49.81 |
| 20261124T065206-sweep-64b682 / 01-10MBps | pg-standby-1 | 0.002219 | 27.09 |
| 20261124T065206-sweep-64b682 / 01-10MBps | plantsim-1 | 3.8783e-05 | 16.01 |
| 20261124T065206-sweep-64b682 / 01-10MBps | portal-publisher-1 | 0.00047374 | 89.96 |
| 20261124T065206-sweep-64b682 / 01-10MBps | processor-1 | 0.010173 | 124.4 |
| 20261124T065206-sweep-64b682 / 01-10MBps | seaweedfs-1 | 0.030361 | 835.3 |
| 20261124T065206-sweep-64b682 / 01-10MBps | tempo-1 | 0.00034554 | 79.9 |
| 20261124T065206-sweep-64b682 / 01-10MBps | victoriametrics-1 | 0.00082164 | 81.21 |
| 20261124T065206-sweep-64b682 / 02-20MBps | alloy-1 | not measured | 52.07 |
| 20261124T065206-sweep-64b682 / 02-20MBps | archiver-1 | not measured | 739.2 |
| 20261124T065206-sweep-64b682 / 02-20MBps | dispatcher-1-1 | not measured | 63.17 |
| 20261124T065206-sweep-64b682 / 02-20MBps | dispatcher-2-1 | not measured | 70.41 |
| 20261124T065206-sweep-64b682 / 02-20MBps | dynamodb-local-1 | not measured | 170.2 |
| 20261124T065206-sweep-64b682 / 02-20MBps | elasticmq-1 | not measured | 63.53 |
| 20261124T065206-sweep-64b682 / 02-20MBps | grafana-1 | not measured | 300.3 |
| 20261124T065206-sweep-64b682 / 02-20MBps | ingest-a1-1 | not measured | 386 |
| 20261124T065206-sweep-64b682 / 02-20MBps | ingest-a2-1 | not measured | 369.5 |
| 20261124T065206-sweep-64b682 / 02-20MBps | instruction-api-1 | not measured | 48.54 |
| 20261124T065206-sweep-64b682 / 02-20MBps | loki-1 | not measured | 71.84 |
| 20261124T065206-sweep-64b682 / 02-20MBps | nats-1 | not measured | 1003 |
| 20261124T065206-sweep-64b682 / 02-20MBps | pg-primary-1 | not measured | 49.12 |
| 20261124T065206-sweep-64b682 / 02-20MBps | pg-standby-1 | not measured | 22.98 |
| 20261124T065206-sweep-64b682 / 02-20MBps | plantsim-1 | not measured | 14.33 |
| 20261124T065206-sweep-64b682 / 02-20MBps | portal-publisher-1 | not measured | 70.66 |
| 20261124T065206-sweep-64b682 / 02-20MBps | processor-1 | not measured | 311.2 |
| 20261124T065206-sweep-64b682 / 02-20MBps | seaweedfs-1 | not measured | 769.8 |
| 20261124T065206-sweep-64b682 / 02-20MBps | tempo-1 | not measured | 39.92 |
| 20261124T065206-sweep-64b682 / 02-20MBps | victoriametrics-1 | not measured | 72.71 |
| 20261124T065206-sweep-64b682 / 03-30MBps | alloy-1 | not measured | 44.66 |
| 20261124T065206-sweep-64b682 / 03-30MBps | archiver-1 | not measured | 689.4 |
| 20261124T065206-sweep-64b682 / 03-30MBps | dispatcher-1-1 | not measured | 15.39 |
| 20261124T065206-sweep-64b682 / 03-30MBps | dispatcher-2-1 | not measured | 23.7 |
| 20261124T065206-sweep-64b682 / 03-30MBps | dynamodb-local-1 | not measured | 119.7 |
| 20261124T065206-sweep-64b682 / 03-30MBps | elasticmq-1 | not measured | 53.63 |
| 20261124T065206-sweep-64b682 / 03-30MBps | grafana-1 | not measured | 241.4 |
| 20261124T065206-sweep-64b682 / 03-30MBps | ingest-a1-1 | not measured | 383 |
| 20261124T065206-sweep-64b682 / 03-30MBps | ingest-a2-1 | not measured | 315.1 |
| 20261124T065206-sweep-64b682 / 03-30MBps | instruction-api-1 | not measured | 9.359 |
| 20261124T065206-sweep-64b682 / 03-30MBps | loki-1 | not measured | 54.62 |
| 20261124T065206-sweep-64b682 / 03-30MBps | nats-1 | not measured | 788.3 |
| 20261124T065206-sweep-64b682 / 03-30MBps | pg-primary-1 | not measured | 39.57 |
| 20261124T065206-sweep-64b682 / 03-30MBps | pg-standby-1 | not measured | 18.07 |
| 20261124T065206-sweep-64b682 / 03-30MBps | plantsim-1 | not measured | 12.91 |
| 20261124T065206-sweep-64b682 / 03-30MBps | portal-publisher-1 | not measured | 16.48 |
| 20261124T065206-sweep-64b682 / 03-30MBps | processor-1 | not measured | 174.7 |
| 20261124T065206-sweep-64b682 / 03-30MBps | seaweedfs-1 | not measured | 795.3 |
| 20261124T065206-sweep-64b682 / 03-30MBps | tempo-1 | not measured | 24.02 |
| 20261124T065206-sweep-64b682 / 03-30MBps | victoriametrics-1 | not measured | 56.29 |
| 20261124T065206-sweep-64b682 / 04-40MBps | alloy-1 | not measured | 47.78 |
| 20261124T065206-sweep-64b682 / 04-40MBps | archiver-1 | not measured | 719.5 |
| 20261124T065206-sweep-64b682 / 04-40MBps | dispatcher-1-1 | not measured | 15.08 |
| 20261124T065206-sweep-64b682 / 04-40MBps | dispatcher-2-1 | not measured | 23.58 |
| 20261124T065206-sweep-64b682 / 04-40MBps | dynamodb-local-1 | not measured | 117.5 |
| 20261124T065206-sweep-64b682 / 04-40MBps | elasticmq-1 | not measured | 57.77 |
| 20261124T065206-sweep-64b682 / 04-40MBps | grafana-1 | not measured | 251.8 |
| 20261124T065206-sweep-64b682 / 04-40MBps | ingest-a1-1 | not measured | 283.1 |
| 20261124T065206-sweep-64b682 / 04-40MBps | ingest-a2-1 | not measured | 316.4 |
| 20261124T065206-sweep-64b682 / 04-40MBps | instruction-api-1 | not measured | 9.246 |
| 20261124T065206-sweep-64b682 / 04-40MBps | loki-1 | not measured | 57.73 |
| 20261124T065206-sweep-64b682 / 04-40MBps | nats-1 | not measured | 688.3 |
| 20261124T065206-sweep-64b682 / 04-40MBps | pg-primary-1 | not measured | 37.96 |
| 20261124T065206-sweep-64b682 / 04-40MBps | pg-standby-1 | not measured | 19.98 |
| 20261124T065206-sweep-64b682 / 04-40MBps | plantsim-1 | not measured | 14.93 |
| 20261124T065206-sweep-64b682 / 04-40MBps | portal-publisher-1 | not measured | 16.41 |
| 20261124T065206-sweep-64b682 / 04-40MBps | processor-1 | not measured | 339.6 |
| 20261124T065206-sweep-64b682 / 04-40MBps | seaweedfs-1 | not measured | 774.1 |
| 20261124T065206-sweep-64b682 / 04-40MBps | tempo-1 | not measured | 31.68 |
| 20261124T065206-sweep-64b682 / 04-40MBps | victoriametrics-1 | not measured | 58.92 |

| Run / target | Load generator CPU-s | Max memory MiB |
|---|---:|---:|
| 20261124T051251-sweep-45dcc1 / 01-10MBps | not measured | 771.2 |
| 20261124T051251-sweep-45dcc1 / 02-20MBps | not measured | 783.8 |
| 20261124T051251-sweep-45dcc1 / 03-30MBps | 215.56 | 557.9 |
| 20261124T051251-sweep-45dcc1 / 04-40MBps | not measured | 880.7 |
| 20261124T055207-repeat-30-1-1fb9de / 01-30MBps | not measured | 701.1 |
| 20261124T060437-repeat-30-2-db8a79 / 01-30MBps | not measured | 582.5 |
| 20261124T061521-repeat-30-3-adeb87 / 01-30MBps | not measured | 769.3 |
| 20261124T062630-f1-61d0eb / 01-30MBps | 197.69 | 761.5 |
| 20261124T063829-f2-ede95b / 01-30MBps | not measured | 744.7 |
| 20261124T065206-sweep-64b682 / 01-10MBps | 66.388 | 732.6 |
| 20261124T065206-sweep-64b682 / 02-20MBps | not measured | 739.7 |
| 20261124T065206-sweep-64b682 / 03-30MBps | not measured | 389.2 |
| 20261124T065206-sweep-64b682 / 04-40MBps | not measured | 314.4 |

| Run | 202 receipt events | DB processor events (`sum(jammy.summary_1m.count)`) | Parquet rows | Result |
|---|---:|---:|---:|---|
| 20261124T051251-sweep-45dcc1 | 21180000 | 21247000 | not measured | not measured |
| 20261124T055207-repeat-30-1-1fb9de | 6324000 | 6368000 | 6368000 | matched |
| 20261124T060437-repeat-30-2-db8a79 | 6853000 | 6853000 | 6853000 | matched |
| 20261124T061521-repeat-30-3-adeb87 | 7285000 | 7341000 | 7341000 | matched |
| 20261124T062630-f1-61d0eb | 7487000 | 7511000 | 7511000 | matched |
| 20261124T063829-f2-ede95b | 7651000 | 7660000 | 7660000 | matched |
| 20261124T065206-sweep-64b682 | 16108000 | 16154000 | 16154000 | matched |

20261124T051251-sweep-45dcc1 — Parquet count failed (Command '['$HOME/.cache/jammy-tools/perf-tools-16hkpre4/perf-helper', 'parquet-rows', '$HOME/orca/workspaces/Final-Project/bp-e2e/docs/measurements/raw/arch-perf-2026-11-24-r2/go-split/20261124T051251-sweep-45dcc1/parquet-objects.json']' returned non-zero exit status 1.)


20261124T051251-sweep-45dcc1 — Archive drain wait 830.03 s.


20261124T051251-sweep-45dcc1 — Processor batch IDs matched. 202 receipts with accepted events covered 21180 distinct batches and DB recorded 21247 processor batches.


20261124T051251-sweep-45dcc1 — 67 batches persisted after error response (client would retry; idempotent by batch_id). Missing accepted batches 0. Unexplained processor batches 0.


20261124T055207-repeat-30-1-1fb9de — Archive drain wait 340.02 s.


20261124T055207-repeat-30-1-1fb9de — Processor batch IDs matched. 202 receipts with accepted events covered 6324 distinct batches and DB recorded 6368 processor batches.


20261124T055207-repeat-30-1-1fb9de — 44 batches persisted after error response (client would retry; idempotent by batch_id). Missing accepted batches 0. Unexplained processor batches 0.


20261124T060437-repeat-30-2-db8a79 — Archive drain wait 250.05 s.


20261124T060437-repeat-30-2-db8a79 — Processor batch IDs matched. 202 receipts with accepted events covered 6853 distinct batches and DB recorded 6853 processor batches.


20261124T060437-repeat-30-2-db8a79 — 0 batches persisted after error response (client would retry; idempotent by batch_id). Missing accepted batches 0. Unexplained processor batches 0.


20261124T061521-repeat-30-3-adeb87 — Archive drain wait 270.02 s.


20261124T061521-repeat-30-3-adeb87 — Processor batch IDs matched. 202 receipts with accepted events covered 7285 distinct batches and DB recorded 7341 processor batches.


20261124T061521-repeat-30-3-adeb87 — 56 batches persisted after error response (client would retry; idempotent by batch_id). Missing accepted batches 0. Unexplained processor batches 0.


20261124T062630-f1-61d0eb — Archive drain wait 320.02 s.


20261124T062630-f1-61d0eb — Processor batch IDs matched. 202 receipts with accepted events covered 7487 distinct batches and DB recorded 7511 processor batches.


20261124T062630-f1-61d0eb — 24 batches persisted after error response (client would retry; idempotent by batch_id). Missing accepted batches 0. Unexplained processor batches 0.


20261124T063829-f2-ede95b — Archive drain wait 370.03 s.


20261124T063829-f2-ede95b — Processor batch IDs matched. 202 receipts with accepted events covered 7651 distinct batches and DB recorded 7660 processor batches.


20261124T063829-f2-ede95b — 9 batches persisted after error response (client would retry; idempotent by batch_id). Missing accepted batches 0. Unexplained processor batches 0.


20261124T065206-sweep-64b682 — Archive drain wait 840.39 s.


20261124T065206-sweep-64b682 — Processor batch IDs matched. 202 receipts with accepted events covered 16108 distinct batches and DB recorded 16154 processor batches.


20261124T065206-sweep-64b682 — 46 batches persisted after error response (client would retry; idempotent by batch_id). Missing accepted batches 0. Unexplained processor batches 0.


## py-split

| Run / target | MB/s per server offered | Accepted MB/s | Status ratios | Requests not sent (warmup + measure) | Generator starved | Ingest p50 ms | Ingest p99 ms | Store p99 s | Commit p99 s | Lag max s | Lag recovery s | Processor pending max | Archiver unacked max | Loss check |
|---|---:|---:|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| 20261124T031930-sweep-3d2630 / 01-10MBps (invalid: generator starved) | 10 | 14.71 | 202 78.11%, 503 21.89% | 133 | 86 | 127 | 6139 | 6.4333 | 1.2745 | 17.846 | 3.025 | 109 | 101 | matched |
| 20261124T031930-sweep-3d2630 / 02-20MBps (invalid: generator starved) | 20 | 23.798 | 0 0.02%, 202 76.09%, 503 23.88% | 1011 | 965 | 234 | 6678 | 4.9712 | 9.4214 | 113.33 | not measured | 1559 | 100 | matched |
| 20261124T031930-sweep-3d2630 / 03-30MBps (invalid: generator starved) | 30 | 12.989 | 0 0.06%, 202 44.70%, 503 55.24% | 4789 | 4587 | 110 | 4691 | 3.6768 | 10 | 195.86 | not measured | 1512 | 82 | matched |
| 20261124T031930-sweep-3d2630 / 04-40MBps (invalid: generator starved) | 40 | 8.2599 | 0 0.04%, 202 37.76%, 503 62.20% | 10222 | 10041 | 91 | 4673 | 6.3154 | 10 | 316.07 | not measured | 1483 | 75 | matched |
| 20261124T035438-repeat-30-1-215051 / 01-30MBps (invalid: generator starved) | 30 | 34.904 | 0 0.08%, 202 71.96%, 503 27.96% | 2458 | 2313 | 236 | 6197 | 4.6729 | 8.7667 | 162.25 | not measured | 3827 | 146 | matched |
| 20261124T041202-repeat-30-2-642d5a / 01-30MBps (invalid: generator starved) | 30 | 30.767 | 0 0.07%, 202 70.45%, 503 29.48% | 3375 | 3251 | 279 | 4655 | 3.6778 | 10 | 116.71 | not measured | 2575 | 99 | matched |
| 20261124T042449-repeat-30-3-b62269 / 01-30MBps (invalid: generator starved) | 30 | 45.888 | 0 0.15%, 202 86.36%, 503 13.49% | 1201 | 1091 | 344 | 5007 | 3.5795 | 8.8125 | 154.95 | not measured | 4571 | 114 | matched |
| 20261124T044040-f1-18eb44 / 01-30MBps (invalid: generator starved) | 30 | 38.258 | 0 0.07%, 202 69.93%, 503 30.00% | 882 requests not sent | 788 | 152 | 3151 | 2.2998 | 10 | 153.06 | not measured | 3679 | 103 | matched |
| 20261124T044040-f1-18eb44 / F1 interruption | 30 | 46.275 | 0 0.23%, 202 77.73%, 503 22.04% | not measured | not measured | 70.436 | 1367.1 | not measured | not measured | 15.95 | not measured | not measured | not measured | matched |
| 20261124T045552-f2-818ec4 / 01-30MBps (invalid: generator starved) | 30 | 45.745 | 0 0.07%, 202 83.18%, 503 16.75% | 1045 requests not sent | 947 | 155 | 3688 | 2.3261 | 8.8 | 177.92 | not measured | 5714 | 119 | matched |
| 20261124T045552-f2-818ec4 / F2 interruption | 30 | 52.248 | 0 0.04%, 202 88.20%, 503 11.76% | not measured | not measured | 77.404 | 3068.3 | not measured | not measured | not measured | not measured | not measured | not measured | matched |

| Run / target | Ingest server | Achieved wire MB/s | Accepted MB/s | Status ratios | Ingest p50 ms | Ingest p95 ms | Ingest p99 ms |
|---|---|---:|---:|---|---:|---:|---:|
| 20261124T031930-sweep-3d2630 / 01-10MBps | ingest-a1 | 9.5187 | 7.119 | 202 75.54%, 503 24.46% | 118 | 1715 | 6873 |
| 20261124T031930-sweep-3d2630 / 01-10MBps | ingest-a2 | 9.4951 | 7.5908 | 202 80.70%, 503 19.30% | 134 | 1976 | 5837 |
| 20261124T031930-sweep-3d2630 / 02-20MBps | ingest-a1 | 15.975 | 10.806 | 202 69.90%, 503 30.10% | 195 | 2351 | 6740 |
| 20261124T031930-sweep-3d2630 / 02-20MBps | ingest-a2 | 16.094 | 12.993 | 0 0.05%, 202 82.23%, 503 17.72% | 276 | 2569 | 6532 |
| 20261124T031930-sweep-3d2630 / 03-30MBps | ingest-a1 | 14.255 | 7.166 | 0 0.11%, 202 50.72%, 503 49.17% | 144 | 2323 | 5049 |
| 20261124T031930-sweep-3d2630 / 03-30MBps | ingest-a2 | 14.11 | 5.8232 | 202 38.60%, 503 61.40% | 94 | 1846 | 4238 |
| 20261124T031930-sweep-3d2630 / 04-40MBps | ingest-a1 | 9.8002 | 4.7822 | 0 0.08%, 202 43.25%, 503 56.67% | 104 | 2343 | 4471 |
| 20261124T031930-sweep-3d2630 / 04-40MBps | ingest-a2 | 10.037 | 3.4777 | 202 32.42%, 503 67.58% | 77 | 2030 | 4838 |
| 20261124T035438-repeat-30-1-215051 / 01-30MBps | ingest-a1 | 24.267 | 17.176 | 0 0.03%, 202 70.75%, 503 29.22% | 210 | 2769 | 5402 |
| 20261124T035438-repeat-30-1-215051 / 01-30MBps | ingest-a2 | 24.666 | 17.728 | 0 0.13%, 202 73.15%, 503 26.72% | 258 | 3029 | 6950 |
| 20261124T041202-repeat-30-2-642d5a / 01-30MBps | ingest-a1 | 22.36 | 15.249 | 0 0.14%, 202 68.58%, 503 31.28% | 260 | 2448 | 4511 |
| 20261124T041202-repeat-30-2-642d5a / 01-30MBps | ingest-a2 | 21.794 | 15.518 | 202 72.38%, 503 27.62% | 299 | 2729 | 4856 |
| 20261124T042449-repeat-30-3-b62269 / 01-30MBps | ingest-a1 | 26.649 | 24.937 | 0 0.18%, 202 94.32%, 503 5.51% | 481 | 3313 | 6087 |
| 20261124T042449-repeat-30-3-b62269 / 01-30MBps | ingest-a2 | 26.862 | 20.951 | 0 0.12%, 202 78.47%, 503 21.41% | 239 | 2389 | 4554 |
| 20261124T044040-f1-18eb44 / 01-30MBps | ingest-a1 | 27.098 | 17.769 | 0 0.09%, 202 65.57%, 503 34.35% | 131 | 1403 | 2471 |
| 20261124T044040-f1-18eb44 / 01-30MBps | ingest-a2 | 27.472 | 20.489 | 0 0.06%, 202 74.24%, 503 25.70% | 174 | 1794 | 3603 |
| 20261124T045552-f2-818ec4 / 01-30MBps | ingest-a1 | 27.798 | 23.803 | 0 0.14%, 202 86.46%, 503 13.39% | 175 | 2360 | 3924 |
| 20261124T045552-f2-818ec4 / 01-30MBps | ingest-a2 | 27.752 | 21.943 | 202 79.89%, 503 20.11% | 143 | 1772 | 3478 |

| Run / target | Container | CPU-s / accepted MB | Max memory MiB |
|---|---|---:|---:|
| 20261124T031930-sweep-3d2630 / 01-10MBps | alloy-1 | not measured | 63.81 |
| 20261124T031930-sweep-3d2630 / 01-10MBps | archiver-1 | not measured | 757 |
| 20261124T031930-sweep-3d2630 / 01-10MBps | dispatcher-1-1 | not measured | 56.89 |
| 20261124T031930-sweep-3d2630 / 01-10MBps | dispatcher-2-1 | not measured | 63.52 |
| 20261124T031930-sweep-3d2630 / 01-10MBps | dynamodb-local-1 | not measured | 148 |
| 20261124T031930-sweep-3d2630 / 01-10MBps | elasticmq-1 | not measured | 52.75 |
| 20261124T031930-sweep-3d2630 / 01-10MBps | grafana-1 | not measured | 336.5 |
| 20261124T031930-sweep-3d2630 / 01-10MBps | ingest-a1-1 | not measured | 641.3 |
| 20261124T031930-sweep-3d2630 / 01-10MBps | ingest-a2-1 | not measured | 548.3 |
| 20261124T031930-sweep-3d2630 / 01-10MBps | instruction-api-1 | not measured | 41.13 |
| 20261124T031930-sweep-3d2630 / 01-10MBps | loki-1 | not measured | 68.3 |
| 20261124T031930-sweep-3d2630 / 01-10MBps | nats-1 | not measured | 940.9 |
| 20261124T031930-sweep-3d2630 / 01-10MBps | pg-primary-1 | not measured | 45.66 |
| 20261124T031930-sweep-3d2630 / 01-10MBps | pg-standby-1 | not measured | 24.98 |
| 20261124T031930-sweep-3d2630 / 01-10MBps | plantsim-1 | not measured | 14.23 |
| 20261124T031930-sweep-3d2630 / 01-10MBps | portal-publisher-1 | not measured | 55.54 |
| 20261124T031930-sweep-3d2630 / 01-10MBps | processor-1 | not measured | 116.2 |
| 20261124T031930-sweep-3d2630 / 01-10MBps | seaweedfs-1 | not measured | 1082.4 |
| 20261124T031930-sweep-3d2630 / 01-10MBps | tempo-1 | not measured | 49.26 |
| 20261124T031930-sweep-3d2630 / 01-10MBps | victoriametrics-1 | not measured | 85.36 |
| 20261124T031930-sweep-3d2630 / 02-20MBps | alloy-1 | not measured | 66.64 |
| 20261124T031930-sweep-3d2630 / 02-20MBps | archiver-1 | not measured | 687.8 |
| 20261124T031930-sweep-3d2630 / 02-20MBps | dispatcher-1-1 | not measured | 52.23 |
| 20261124T031930-sweep-3d2630 / 02-20MBps | dispatcher-2-1 | not measured | 58.99 |
| 20261124T031930-sweep-3d2630 / 02-20MBps | dynamodb-local-1 | not measured | 151.1 |
| 20261124T031930-sweep-3d2630 / 02-20MBps | elasticmq-1 | not measured | 58.7 |
| 20261124T031930-sweep-3d2630 / 02-20MBps | grafana-1 | not measured | 330.7 |
| 20261124T031930-sweep-3d2630 / 02-20MBps | ingest-a1-1 | not measured | 662.9 |
| 20261124T031930-sweep-3d2630 / 02-20MBps | ingest-a2-1 | not measured | 612.7 |
| 20261124T031930-sweep-3d2630 / 02-20MBps | instruction-api-1 | not measured | 35.48 |
| 20261124T031930-sweep-3d2630 / 02-20MBps | loki-1 | not measured | 76.84 |
| 20261124T031930-sweep-3d2630 / 02-20MBps | nats-1 | not measured | 1259.5 |
| 20261124T031930-sweep-3d2630 / 02-20MBps | pg-primary-1 | not measured | 45.43 |
| 20261124T031930-sweep-3d2630 / 02-20MBps | pg-standby-1 | not measured | 22.39 |
| 20261124T031930-sweep-3d2630 / 02-20MBps | plantsim-1 | not measured | 14.84 |
| 20261124T031930-sweep-3d2630 / 02-20MBps | portal-publisher-1 | not measured | 48.86 |
| 20261124T031930-sweep-3d2630 / 02-20MBps | processor-1 | not measured | 318.8 |
| 20261124T031930-sweep-3d2630 / 02-20MBps | seaweedfs-1 | not measured | 1104.9 |
| 20261124T031930-sweep-3d2630 / 02-20MBps | tempo-1 | not measured | 47.24 |
| 20261124T031930-sweep-3d2630 / 02-20MBps | victoriametrics-1 | not measured | 84.79 |
| 20261124T031930-sweep-3d2630 / 03-30MBps | alloy-1 | not measured | 50.85 |
| 20261124T031930-sweep-3d2630 / 03-30MBps | archiver-1 | not measured | 608.4 |
| 20261124T031930-sweep-3d2630 / 03-30MBps | dispatcher-1-1 | not measured | 51.77 |
| 20261124T031930-sweep-3d2630 / 03-30MBps | dispatcher-2-1 | not measured | 56.28 |
| 20261124T031930-sweep-3d2630 / 03-30MBps | dynamodb-local-1 | not measured | 151.5 |
| 20261124T031930-sweep-3d2630 / 03-30MBps | elasticmq-1 | not measured | 67.46 |
| 20261124T031930-sweep-3d2630 / 03-30MBps | grafana-1 | not measured | 304.5 |
| 20261124T031930-sweep-3d2630 / 03-30MBps | ingest-a1-1 | not measured | 694.8 |
| 20261124T031930-sweep-3d2630 / 03-30MBps | ingest-a2-1 | not measured | 635.6 |
| 20261124T031930-sweep-3d2630 / 03-30MBps | instruction-api-1 | not measured | 34.18 |
| 20261124T031930-sweep-3d2630 / 03-30MBps | loki-1 | not measured | 67.79 |
| 20261124T031930-sweep-3d2630 / 03-30MBps | nats-1 | not measured | 580.9 |
| 20261124T031930-sweep-3d2630 / 03-30MBps | pg-primary-1 | not measured | 37.27 |
| 20261124T031930-sweep-3d2630 / 03-30MBps | pg-standby-1 | not measured | 16.84 |
| 20261124T031930-sweep-3d2630 / 03-30MBps | plantsim-1 | not measured | 15.19 |
| 20261124T031930-sweep-3d2630 / 03-30MBps | portal-publisher-1 | not measured | 46.82 |
| 20261124T031930-sweep-3d2630 / 03-30MBps | processor-1 | not measured | 280.6 |
| 20261124T031930-sweep-3d2630 / 03-30MBps | seaweedfs-1 | not measured | 1097.7 |
| 20261124T031930-sweep-3d2630 / 03-30MBps | tempo-1 | not measured | 31.66 |
| 20261124T031930-sweep-3d2630 / 03-30MBps | victoriametrics-1 | not measured | 82.17 |
| 20261124T031930-sweep-3d2630 / 04-40MBps | alloy-1 | not measured | 49.74 |
| 20261124T031930-sweep-3d2630 / 04-40MBps | archiver-1 | not measured | 489.9 |
| 20261124T031930-sweep-3d2630 / 04-40MBps | dispatcher-1-1 | not measured | 50.65 |
| 20261124T031930-sweep-3d2630 / 04-40MBps | dispatcher-2-1 | not measured | 56.16 |
| 20261124T031930-sweep-3d2630 / 04-40MBps | dynamodb-local-1 | not measured | 149.6 |
| 20261124T031930-sweep-3d2630 / 04-40MBps | elasticmq-1 | not measured | 59.4 |
| 20261124T031930-sweep-3d2630 / 04-40MBps | grafana-1 | not measured | 268.1 |
| 20261124T031930-sweep-3d2630 / 04-40MBps | ingest-a1-1 | not measured | 617.4 |
| 20261124T031930-sweep-3d2630 / 04-40MBps | ingest-a2-1 | not measured | 578.6 |
| 20261124T031930-sweep-3d2630 / 04-40MBps | instruction-api-1 | not measured | 32.95 |
| 20261124T031930-sweep-3d2630 / 04-40MBps | loki-1 | not measured | 60.18 |
| 20261124T031930-sweep-3d2630 / 04-40MBps | nats-1 | not measured | 777.1 |
| 20261124T031930-sweep-3d2630 / 04-40MBps | pg-primary-1 | not measured | 38.52 |
| 20261124T031930-sweep-3d2630 / 04-40MBps | pg-standby-1 | not measured | 15.45 |
| 20261124T031930-sweep-3d2630 / 04-40MBps | plantsim-1 | not measured | 14.61 |
| 20261124T031930-sweep-3d2630 / 04-40MBps | portal-publisher-1 | not measured | 44.92 |
| 20261124T031930-sweep-3d2630 / 04-40MBps | processor-1 | not measured | 248.6 |
| 20261124T031930-sweep-3d2630 / 04-40MBps | seaweedfs-1 | not measured | 1119.2 |
| 20261124T031930-sweep-3d2630 / 04-40MBps | tempo-1 | not measured | 28.95 |
| 20261124T031930-sweep-3d2630 / 04-40MBps | victoriametrics-1 | not measured | 81.29 |
| 20261124T035438-repeat-30-1-215051 / 01-30MBps | alloy-1 | 0.00096004 | 69.08 |
| 20261124T035438-repeat-30-1-215051 / 01-30MBps | archiver-1 | 0.018714 | 747.6 |
| 20261124T035438-repeat-30-1-215051 / 01-30MBps | dispatcher-1-1 | 0.0023455 | 60.73 |
| 20261124T035438-repeat-30-1-215051 / 01-30MBps | dispatcher-2-1 | 0.00065142 | 55.15 |
| 20261124T035438-repeat-30-1-215051 / 01-30MBps | dynamodb-local-1 | 0.0034979 | 151 |
| 20261124T035438-repeat-30-1-215051 / 01-30MBps | elasticmq-1 | 0.0010666 | 53.24 |
| 20261124T035438-repeat-30-1-215051 / 01-30MBps | grafana-1 | 0.0020528 | 390.4 |
| 20261124T035438-repeat-30-1-215051 / 01-30MBps | ingest-a1-1 | 0.030911 | 723.8 |
| 20261124T035438-repeat-30-1-215051 / 01-30MBps | ingest-a2-1 | 0.031996 | 746.3 |
| 20261124T035438-repeat-30-1-215051 / 01-30MBps | instruction-api-1 | 0.00025522 | 41.59 |
| 20261124T035438-repeat-30-1-215051 / 01-30MBps | loki-1 | 0.0014601 | 60.35 |
| 20261124T035438-repeat-30-1-215051 / 01-30MBps | nats-1 | 0.024326 | 697 |
| 20261124T035438-repeat-30-1-215051 / 01-30MBps | pg-primary-1 | 0.0026086 | 40.1 |
| 20261124T035438-repeat-30-1-215051 / 01-30MBps | pg-standby-1 | 0.0018635 | 19.56 |
| 20261124T035438-repeat-30-1-215051 / 01-30MBps | plantsim-1 | 0.0004643 | 12.96 |
| 20261124T035438-repeat-30-1-215051 / 01-30MBps | portal-publisher-1 | 0.00074766 | 61.03 |
| 20261124T035438-repeat-30-1-215051 / 01-30MBps | processor-1 | 0.017088 | 320.6 |
| 20261124T035438-repeat-30-1-215051 / 01-30MBps | seaweedfs-1 | 0.0067895 | 1041.4 |
| 20261124T035438-repeat-30-1-215051 / 01-30MBps | tempo-1 | 0.00039368 | 39.39 |
| 20261124T035438-repeat-30-1-215051 / 01-30MBps | victoriametrics-1 | 0.0012331 | 81.71 |
| 20261124T041202-repeat-30-2-642d5a / 01-30MBps | alloy-1 | not measured | 55.39 |
| 20261124T041202-repeat-30-2-642d5a / 01-30MBps | archiver-1 | not measured | 739.4 |
| 20261124T041202-repeat-30-2-642d5a / 01-30MBps | dispatcher-1-1 | not measured | 64.02 |
| 20261124T041202-repeat-30-2-642d5a / 01-30MBps | dispatcher-2-1 | not measured | 69.35 |
| 20261124T041202-repeat-30-2-642d5a / 01-30MBps | dynamodb-local-1 | not measured | 145 |
| 20261124T041202-repeat-30-2-642d5a / 01-30MBps | elasticmq-1 | not measured | 52.34 |
| 20261124T041202-repeat-30-2-642d5a / 01-30MBps | grafana-1 | not measured | 385.3 |
| 20261124T041202-repeat-30-2-642d5a / 01-30MBps | ingest-a1-1 | not measured | 718.4 |
| 20261124T041202-repeat-30-2-642d5a / 01-30MBps | ingest-a2-1 | not measured | 704.7 |
| 20261124T041202-repeat-30-2-642d5a / 01-30MBps | instruction-api-1 | not measured | 48.25 |
| 20261124T041202-repeat-30-2-642d5a / 01-30MBps | loki-1 | not measured | 66.98 |
| 20261124T041202-repeat-30-2-642d5a / 01-30MBps | nats-1 | not measured | 699.1 |
| 20261124T041202-repeat-30-2-642d5a / 01-30MBps | pg-primary-1 | not measured | 44.63 |
| 20261124T041202-repeat-30-2-642d5a / 01-30MBps | pg-standby-1 | not measured | 22.11 |
| 20261124T041202-repeat-30-2-642d5a / 01-30MBps | plantsim-1 | not measured | 12.14 |
| 20261124T041202-repeat-30-2-642d5a / 01-30MBps | portal-publisher-1 | not measured | 64.66 |
| 20261124T041202-repeat-30-2-642d5a / 01-30MBps | processor-1 | not measured | 303.4 |
| 20261124T041202-repeat-30-2-642d5a / 01-30MBps | seaweedfs-1 | not measured | 1032.2 |
| 20261124T041202-repeat-30-2-642d5a / 01-30MBps | tempo-1 | not measured | 35.53 |
| 20261124T041202-repeat-30-2-642d5a / 01-30MBps | victoriametrics-1 | not measured | 85.43 |
| 20261124T042449-repeat-30-3-b62269 / 01-30MBps | alloy-1 | not measured | 61.55 |
| 20261124T042449-repeat-30-3-b62269 / 01-30MBps | archiver-1 | not measured | 749.2 |
| 20261124T042449-repeat-30-3-b62269 / 01-30MBps | dispatcher-1-1 | not measured | 62.63 |
| 20261124T042449-repeat-30-3-b62269 / 01-30MBps | dispatcher-2-1 | not measured | 61.39 |
| 20261124T042449-repeat-30-3-b62269 / 01-30MBps | dynamodb-local-1 | not measured | 146.4 |
| 20261124T042449-repeat-30-3-b62269 / 01-30MBps | elasticmq-1 | not measured | 53.69 |
| 20261124T042449-repeat-30-3-b62269 / 01-30MBps | grafana-1 | not measured | 391.7 |
| 20261124T042449-repeat-30-3-b62269 / 01-30MBps | ingest-a1-1 | not measured | 830.8 |
| 20261124T042449-repeat-30-3-b62269 / 01-30MBps | ingest-a2-1 | not measured | 715.5 |
| 20261124T042449-repeat-30-3-b62269 / 01-30MBps | instruction-api-1 | not measured | 44.19 |
| 20261124T042449-repeat-30-3-b62269 / 01-30MBps | loki-1 | not measured | 75.17 |
| 20261124T042449-repeat-30-3-b62269 / 01-30MBps | nats-1 | not measured | 724.2 |
| 20261124T042449-repeat-30-3-b62269 / 01-30MBps | pg-primary-1 | not measured | 38.12 |
| 20261124T042449-repeat-30-3-b62269 / 01-30MBps | pg-standby-1 | not measured | 27.36 |
| 20261124T042449-repeat-30-3-b62269 / 01-30MBps | plantsim-1 | not measured | 12.07 |
| 20261124T042449-repeat-30-3-b62269 / 01-30MBps | portal-publisher-1 | not measured | 61.09 |
| 20261124T042449-repeat-30-3-b62269 / 01-30MBps | processor-1 | not measured | 340.4 |
| 20261124T042449-repeat-30-3-b62269 / 01-30MBps | seaweedfs-1 | not measured | 1042.4 |
| 20261124T042449-repeat-30-3-b62269 / 01-30MBps | tempo-1 | not measured | 37.23 |
| 20261124T042449-repeat-30-3-b62269 / 01-30MBps | victoriametrics-1 | not measured | 87.32 |
| 20261124T044040-f1-18eb44 / 01-30MBps | alloy-1 | not measured | 67.43 |
| 20261124T044040-f1-18eb44 / 01-30MBps | archiver-1 | not measured | 755.4 |
| 20261124T044040-f1-18eb44 / 01-30MBps | dispatcher-1-1 | not measured | 65.56 |
| 20261124T044040-f1-18eb44 / 01-30MBps | dispatcher-2-1 | not measured | 60.22 |
| 20261124T044040-f1-18eb44 / 01-30MBps | dynamodb-local-1 | not measured | 141.3 |
| 20261124T044040-f1-18eb44 / 01-30MBps | elasticmq-1 | not measured | 52.84 |
| 20261124T044040-f1-18eb44 / 01-30MBps | grafana-1 | not measured | 369.3 |
| 20261124T044040-f1-18eb44 / 01-30MBps | ingest-a1-1 | not measured | 720.5 |
| 20261124T044040-f1-18eb44 / 01-30MBps | ingest-a2-1 | not measured | 676.5 |
| 20261124T044040-f1-18eb44 / 01-30MBps | instruction-api-1 | not measured | 42.9 |
| 20261124T044040-f1-18eb44 / 01-30MBps | loki-1 | not measured | 65.91 |
| 20261124T044040-f1-18eb44 / 01-30MBps | nats-1 | not measured | 521.9 |
| 20261124T044040-f1-18eb44 / 01-30MBps | pg-primary-1 | not measured | 46.84 |
| 20261124T044040-f1-18eb44 / 01-30MBps | pg-standby-1 | not measured | 17.68 |
| 20261124T044040-f1-18eb44 / 01-30MBps | plantsim-1 | not measured | 11.92 |
| 20261124T044040-f1-18eb44 / 01-30MBps | portal-publisher-1 | not measured | 62.68 |
| 20261124T044040-f1-18eb44 / 01-30MBps | processor-1 | not measured | 311.1 |
| 20261124T044040-f1-18eb44 / 01-30MBps | seaweedfs-1 | not measured | 1073.2 |
| 20261124T044040-f1-18eb44 / 01-30MBps | tempo-1 | not measured | 45.8 |
| 20261124T044040-f1-18eb44 / 01-30MBps | victoriametrics-1 | not measured | 105.7 |
| 20261124T045552-f2-818ec4 / 01-30MBps | alloy-1 | not measured | 50.88 |
| 20261124T045552-f2-818ec4 / 01-30MBps | archiver-1 | not measured | 747.8 |
| 20261124T045552-f2-818ec4 / 01-30MBps | dispatcher-1-1 | not measured | 60.45 |
| 20261124T045552-f2-818ec4 / 01-30MBps | dispatcher-2-1 | not measured | 67.27 |
| 20261124T045552-f2-818ec4 / 01-30MBps | dynamodb-local-1 | not measured | 163.6 |
| 20261124T045552-f2-818ec4 / 01-30MBps | elasticmq-1 | not measured | 54.31 |
| 20261124T045552-f2-818ec4 / 01-30MBps | grafana-1 | not measured | 370.1 |
| 20261124T045552-f2-818ec4 / 01-30MBps | ingest-a1-1 | not measured | 763.3 |
| 20261124T045552-f2-818ec4 / 01-30MBps | ingest-a2-1 | not measured | 746.2 |
| 20261124T045552-f2-818ec4 / 01-30MBps | instruction-api-1 | not measured | 45.19 |
| 20261124T045552-f2-818ec4 / 01-30MBps | loki-1 | not measured | 60.58 |
| 20261124T045552-f2-818ec4 / 01-30MBps | nats-1 | not measured | 595.3 |
| 20261124T045552-f2-818ec4 / 01-30MBps | pg-primary-1 | not measured | 45.09 |
| 20261124T045552-f2-818ec4 / 01-30MBps | pg-standby-1 | not measured | 26.04 |
| 20261124T045552-f2-818ec4 / 01-30MBps | plantsim-1 | not measured | 11.84 |
| 20261124T045552-f2-818ec4 / 01-30MBps | portal-publisher-1 | not measured | 66.7 |
| 20261124T045552-f2-818ec4 / 01-30MBps | processor-1 | not measured | 315.8 |
| 20261124T045552-f2-818ec4 / 01-30MBps | seaweedfs-1 | not measured | 1159.2 |
| 20261124T045552-f2-818ec4 / 01-30MBps | tempo-1 | not measured | 41.36 |
| 20261124T045552-f2-818ec4 / 01-30MBps | victoriametrics-1 | not measured | 99.94 |

| Run / target | Load generator CPU-s | Max memory MiB |
|---|---:|---:|
| 20261124T031930-sweep-3d2630 / 01-10MBps | not measured | 749 |
| 20261124T031930-sweep-3d2630 / 02-20MBps | not measured | 741.9 |
| 20261124T031930-sweep-3d2630 / 03-30MBps | not measured | 654 |
| 20261124T031930-sweep-3d2630 / 04-40MBps | not measured | 266.5 |
| 20261124T035438-repeat-30-1-215051 / 01-30MBps | 198.4 | 744.5 |
| 20261124T041202-repeat-30-2-642d5a / 01-30MBps | not measured | 670.8 |
| 20261124T042449-repeat-30-3-b62269 / 01-30MBps | not measured | 747.1 |
| 20261124T044040-f1-18eb44 / 01-30MBps | not measured | 745.6 |
| 20261124T045552-f2-818ec4 / 01-30MBps | not measured | 756.8 |

| Run | 202 receipt events | DB processor events (`sum(jammy.summary_1m.count)`) | Parquet rows | Result |
|---|---:|---:|---:|---|
| 20261124T031930-sweep-3d2630 | 10580000 | 10710000 | 10710000 | matched |
| 20261124T035438-repeat-30-1-215051 | 5869000 | 5988000 | 5988000 | matched |
| 20261124T041202-repeat-30-2-642d5a | 4550000 | 4742000 | 4742000 | matched |
| 20261124T042449-repeat-30-3-b62269 | 7784000 | 7807000 | 7807000 | matched |
| 20261124T044040-f1-18eb44 | 6346000 | 6385000 | 6385000 | matched |
| 20261124T045552-f2-818ec4 | 7606000 | 7641000 | 7641000 | matched |

20261124T031930-sweep-3d2630 — Archive drain wait 850.02 s.


20261124T031930-sweep-3d2630 — Processor batch IDs matched. 202 receipts with accepted events covered 10580 distinct batches and DB recorded 10710 processor batches.


20261124T031930-sweep-3d2630 — 130 batches persisted after error response (client would retry; idempotent by batch_id). Missing accepted batches 0. Unexplained processor batches 0.


20261124T035438-repeat-30-1-215051 — Archive drain wait 700.03 s.


20261124T035438-repeat-30-1-215051 — Processor batch IDs matched. 202 receipts with accepted events covered 5869 distinct batches and DB recorded 5988 processor batches.


20261124T035438-repeat-30-1-215051 — 119 batches persisted after error response (client would retry; idempotent by batch_id). Missing accepted batches 0. Unexplained processor batches 0.


20261124T041202-repeat-30-2-642d5a — Archive drain wait 430.02 s.


20261124T041202-repeat-30-2-642d5a — Processor batch IDs matched. 202 receipts with accepted events covered 4550 distinct batches and DB recorded 4742 processor batches.


20261124T041202-repeat-30-2-642d5a — 192 batches persisted after error response (client would retry; idempotent by batch_id). Missing accepted batches 0. Unexplained processor batches 0.


20261124T042449-repeat-30-3-b62269 — Archive drain wait 610.04 s.


20261124T042449-repeat-30-3-b62269 — Processor batch IDs matched. 202 receipts with accepted events covered 7784 distinct batches and DB recorded 7807 processor batches.


20261124T042449-repeat-30-3-b62269 — 23 batches persisted after error response (client would retry; idempotent by batch_id). Missing accepted batches 0. Unexplained processor batches 0.


20261124T044040-f1-18eb44 — Archive drain wait 570.03 s.


20261124T044040-f1-18eb44 — Processor batch IDs matched. 202 receipts with accepted events covered 6346 distinct batches and DB recorded 6385 processor batches.


20261124T044040-f1-18eb44 — 39 batches persisted after error response (client would retry; idempotent by batch_id). Missing accepted batches 0. Unexplained processor batches 0.


20261124T045552-f2-818ec4 — Archive drain wait 670.02 s.


20261124T045552-f2-818ec4 — Processor batch IDs matched. 202 receipts with accepted events covered 7606 distinct batches and DB recorded 7641 processor batches.


20261124T045552-f2-818ec4 — 35 batches persisted after error response (client would retry; idempotent by batch_id). Missing accepted batches 0. Unexplained processor batches 0.


## Excluded runs

No runs excluded.
