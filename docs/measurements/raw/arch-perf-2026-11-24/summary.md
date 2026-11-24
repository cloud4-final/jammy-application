# Local architecture performance

## go-mvp

| Run / target | MB/s per server offered | Accepted MB/s | Status ratios | Requests not sent (warmup + measure) | Generator starved | Ingest p50 ms | Ingest p99 ms | Store p99 s | Commit p99 s | Lag max s | Lag recovery s | Processor pending max | Archiver unacked max | Loss check |
|---|---:|---:|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| 20261123T190648-sweep-e19b13 / 01-10MBps | 10 | 10.014 | 202 100.00% | 0 | 0 | 69 | 846 | 0.76167 | 0.99159 | 218.26 | not measured | not measured | not measured | matched |
| 20261123T190648-sweep-e19b13 / 02-20MBps (invalid: dropped iterations) | 20 | 13.265 | 202 69.00%, 503 31.00% | 3 | 0 | 583 | 5621 | 8.0755 | 9.47 | 513.6 | not measured | not measured | not measured | matched |
| 20261123T190648-sweep-e19b13 / 03-30MBps (invalid: generator starved) | 30 | 9.4123 | 202 38.08%, 503 61.92% | 46 | 40 | 346 | 7114 | 9.2827 | 10 | 815.91 | not measured | not measured | not measured | matched |
| 20261123T190648-sweep-e19b13 / 04-40MBps (invalid: generator starved) | 40 | 11.385 | 202 36.57%, 503 63.43% | 180 | 132 | 324 | 6520 | 8.559 | 10 | 226.46 | not measured | not measured | not measured | matched |
| 20261123T195051-sweep-b2474f / 01-10MBps | 10 | 10.014 | 202 100.00% | 0 | 0 | 124 | 2645 | 2.2711 | 1.6695 | 223.93 | not measured | not measured | not measured | not measured |
| 20261123T195051-sweep-b2474f / 02-20MBps (invalid: dropped iterations) | 20 | 10.488 | 202 55.42%, 503 44.58% | 1 | 0 | 733 | 6262 | 9.19 | 9.7556 | 521.98 | not measured | not measured | not measured | not measured |
| 20261123T195051-sweep-b2474f / 03-30MBps (invalid: generator starved) | 30 | 10.445 | 202 41.56%, 503 58.44% | 131 | 128 | 470 | 7020 | 9.4167 | 10 | 818.31 | not measured | not measured | not measured | not measured |
| 20261123T195051-sweep-b2474f / 04-40MBps (load generator incomplete) | 40 | not measured | not measured | not measured | not measured | not measured | not measured | 10 | 10 | 1132.1 | not measured | not measured | not measured | not measured |
| 20261123T201215-repeat-30-1-6eecee / 01-30MBps (invalid: dropped iterations) | 30 | 17.914 | 202 63.90%, 503 36.10% | 8 | 0 | 378 | 5075 | 4.9789 | 8.9125 | 235.32 | not measured | not measured | not measured | matched |
| 20261123T201819-repeat-30-2-a5183e / 01-30MBps (invalid: dropped iterations) | 30 | 16.854 | 202 60.30%, 503 39.70% | 1 | 0 | 351 | 5585 | 7.7445 | 8.3 | 232.83 | not measured | not measured | not measured | matched |
| 20261123T202425-repeat-30-3-fb8302 / 01-30MBps (invalid: dropped iterations) | 30 | 15.895 | 202 56.01%, 503 43.99% | 3 | 0 | 224 | 5921 | 7.5931 | 8.64 | 228.96 | not measured | not measured | not measured | matched |
| 20261123T203112-f1-fdfb8d / 01-30MBps | 30 | 13.336 | 202 48.19%, 503 51.81% | 4 requests not sent | 0 | 106 | 5479 | 8.491 | 10 | 232.44 | not measured | not measured | not measured | matched |
| 20261123T203112-f1-fdfb8d / F1 interruption | 30 | 0 | 503 100.00% | not measured | not measured | 0.82398 | 2026 | not measured | not measured | 55.733 | not measured | not measured | not measured | matched |
| 20261123T203719-f2-cc93af / 01-30MBps | 30 | 19.684 | 202 69.82%, 503 30.18% | 0 requests not sent | 0 | 115 | 5238 | 4.9652 | 9.5 | 236.2 | not measured | not measured | not measured | matched |
| 20261123T203719-f2-cc93af / F2 interruption | 30 | 29.996 | 202 100.00% | not measured | not measured | 65.872 | 1523.9 | not measured | not measured | not measured | 3.3591 | not measured | not measured | matched |

| Run / target | Ingest server | Achieved wire MB/s | Accepted MB/s | Status ratios | Ingest p50 ms | Ingest p95 ms | Ingest p99 ms |
|---|---|---:|---:|---|---:|---:|---:|
| 20261123T190648-sweep-e19b13 / 01-10MBps | ingest-a1 | 10.014 | 10.014 | 202 100.00% | 69 | 389 | 846 |
| 20261123T190648-sweep-e19b13 / 02-20MBps | ingest-a1 | 20.004 | 13.265 | 202 69.00%, 503 31.00% | 583 | 4462 | 5621 |
| 20261123T190648-sweep-e19b13 / 03-30MBps | ingest-a1 | 29.642 | 9.4123 | 202 38.08%, 503 61.92% | 346 | 4610 | 7114 |
| 20261123T190648-sweep-e19b13 / 04-40MBps | ingest-a1 | 38.925 | 11.385 | 202 36.57%, 503 63.43% | 324 | 3898 | 6520 |
| 20261123T195051-sweep-b2474f / 01-10MBps | ingest-a1 | 10.014 | 10.014 | 202 100.00% | 124 | 989 | 2645 |
| 20261123T195051-sweep-b2474f / 02-20MBps | ingest-a1 | 19.981 | 10.488 | 202 55.42%, 503 44.58% | 733 | 5127 | 6262 |
| 20261123T195051-sweep-b2474f / 03-30MBps | ingest-a1 | 29.382 | 10.445 | 202 41.56%, 503 58.44% | 470 | 4435 | 7020 |
| 20261123T195051-sweep-b2474f / 04-40MBps | ingest-a1 | not measured | not measured | not measured | not measured | not measured | not measured |
| 20261123T201215-repeat-30-1-6eecee / 01-30MBps | ingest-a1 | 29.925 | 17.914 | 202 63.90%, 503 36.10% | 378 | 3576 | 5075 |
| 20261123T201819-repeat-30-2-a5183e / 01-30MBps | ingest-a1 | 29.972 | 16.854 | 202 60.30%, 503 39.70% | 351 | 3802 | 5585 |
| 20261123T202425-repeat-30-3-fb8302 / 01-30MBps | ingest-a1 | 29.949 | 15.895 | 202 56.01%, 503 43.99% | 224 | 3867 | 5921 |
| 20261123T203112-f1-fdfb8d / 01-30MBps | ingest-a1 | 29.994 | 13.336 | 202 48.19%, 503 51.81% | 106 | 3641 | 5479 |
| 20261123T203719-f2-cc93af / 01-30MBps | ingest-a1 | 29.995 | 19.684 | 202 69.82%, 503 30.18% | 115 | 3347 | 5238 |

| Run / target | Container | CPU-s / accepted MB | Max memory MiB |
|---|---|---:|---:|
| 20261123T190648-sweep-e19b13 / 01-10MBps | alloy-1 | 0.0025631 | 44.18 |
| 20261123T190648-sweep-e19b13 / 01-10MBps | archiver-1 | 0.00028604 | 12.88 |
| 20261123T190648-sweep-e19b13 / 01-10MBps | dispatcher-1-1 | 0.0026782 | 79.58 |
| 20261123T190648-sweep-e19b13 / 01-10MBps | dynamodb-local-1 | 0.0026976 | 150.7 |
| 20261123T190648-sweep-e19b13 / 01-10MBps | elasticmq-1 | 0.0020297 | 46.12 |
| 20261123T190648-sweep-e19b13 / 01-10MBps | grafana-1 | 0.0044378 | 577.1 |
| 20261123T190648-sweep-e19b13 / 01-10MBps | ingest-a1-1 | 0.027442 | 49.43 |
| 20261123T190648-sweep-e19b13 / 01-10MBps | instruction-api-1 | 0.0004565 | 60.15 |
| 20261123T190648-sweep-e19b13 / 01-10MBps | loki-1 | 0.0020523 | 61.94 |
| 20261123T190648-sweep-e19b13 / 01-10MBps | pg-primary-1 | 0.32649 | 656.8 |
| 20261123T190648-sweep-e19b13 / 01-10MBps | plantsim-1 | 0 | 10.14 |
| 20261123T190648-sweep-e19b13 / 01-10MBps | portal-publisher-1 | 0.0022717 | 81.1 |
| 20261123T190648-sweep-e19b13 / 01-10MBps | processor-1 | 0.0022602 | 11.49 |
| 20261123T190648-sweep-e19b13 / 01-10MBps | seaweedfs-1 | 0.0019411 | 73.72 |
| 20261123T190648-sweep-e19b13 / 01-10MBps | tempo-1 | 0.0011847 | 26.39 |
| 20261123T190648-sweep-e19b13 / 01-10MBps | victoriametrics-1 | 0.00081121 | 95.11 |
| 20261123T190648-sweep-e19b13 / 02-20MBps | alloy-1 | 0.0030349 | 111.3 |
| 20261123T190648-sweep-e19b13 / 02-20MBps | archiver-1 | 0 | 28.84 |
| 20261123T190648-sweep-e19b13 / 02-20MBps | dispatcher-1-1 | 0.0041153 | 79.59 |
| 20261123T190648-sweep-e19b13 / 02-20MBps | dynamodb-local-1 | 0.00313 | 156.2 |
| 20261123T190648-sweep-e19b13 / 02-20MBps | elasticmq-1 | 0.0027 | 59.28 |
| 20261123T190648-sweep-e19b13 / 02-20MBps | grafana-1 | 0.0070866 | 322.9 |
| 20261123T190648-sweep-e19b13 / 02-20MBps | ingest-a1-1 | 0.056078 | 337.3 |
| 20261123T190648-sweep-e19b13 / 02-20MBps | instruction-api-1 | 0.00064674 | 60.57 |
| 20261123T190648-sweep-e19b13 / 02-20MBps | loki-1 | 0.0030675 | 103.4 |
| 20261123T190648-sweep-e19b13 / 02-20MBps | pg-primary-1 | 0.30751 | 1315.8 |
| 20261123T190648-sweep-e19b13 / 02-20MBps | plantsim-1 | 0.00067948 | 12.57 |
| 20261123T190648-sweep-e19b13 / 02-20MBps | portal-publisher-1 | 0.0013707 | 81.1 |
| 20261123T190648-sweep-e19b13 / 02-20MBps | processor-1 | 0.0017109 | 24.05 |
| 20261123T190648-sweep-e19b13 / 02-20MBps | seaweedfs-1 | 0.0033068 | 130.2 |
| 20261123T190648-sweep-e19b13 / 02-20MBps | tempo-1 | 0.0010025 | 51.22 |
| 20261123T190648-sweep-e19b13 / 02-20MBps | victoriametrics-1 | 0.0038628 | 97.79 |
| 20261123T190648-sweep-e19b13 / 03-30MBps | alloy-1 | not measured | 131.6 |
| 20261123T190648-sweep-e19b13 / 03-30MBps | archiver-1 | not measured | 37.66 |
| 20261123T190648-sweep-e19b13 / 03-30MBps | dispatcher-1-1 | not measured | 79.64 |
| 20261123T190648-sweep-e19b13 / 03-30MBps | dynamodb-local-1 | not measured | 156.9 |
| 20261123T190648-sweep-e19b13 / 03-30MBps | elasticmq-1 | not measured | 69.45 |
| 20261123T190648-sweep-e19b13 / 03-30MBps | grafana-1 | not measured | 444.8 |
| 20261123T190648-sweep-e19b13 / 03-30MBps | ingest-a1-1 | not measured | 449.4 |
| 20261123T190648-sweep-e19b13 / 03-30MBps | instruction-api-1 | not measured | 60.57 |
| 20261123T190648-sweep-e19b13 / 03-30MBps | loki-1 | not measured | 113.4 |
| 20261123T190648-sweep-e19b13 / 03-30MBps | pg-primary-1 | not measured | 1735.7 |
| 20261123T190648-sweep-e19b13 / 03-30MBps | plantsim-1 | not measured | 13.43 |
| 20261123T190648-sweep-e19b13 / 03-30MBps | portal-publisher-1 | not measured | 81.46 |
| 20261123T190648-sweep-e19b13 / 03-30MBps | processor-1 | not measured | 30.6 |
| 20261123T190648-sweep-e19b13 / 03-30MBps | seaweedfs-1 | not measured | 131.5 |
| 20261123T190648-sweep-e19b13 / 03-30MBps | tempo-1 | not measured | 58.28 |
| 20261123T190648-sweep-e19b13 / 03-30MBps | victoriametrics-1 | not measured | 98.25 |
| 20261123T190648-sweep-e19b13 / 04-40MBps | alloy-1 | not measured | 49.59 |
| 20261123T190648-sweep-e19b13 / 04-40MBps | archiver-1 | not measured | 16.68 |
| 20261123T190648-sweep-e19b13 / 04-40MBps | dispatcher-1-1 | not measured | 61.66 |
| 20261123T190648-sweep-e19b13 / 04-40MBps | dynamodb-local-1 | not measured | 131.4 |
| 20261123T190648-sweep-e19b13 / 04-40MBps | elasticmq-1 | not measured | 64.68 |
| 20261123T190648-sweep-e19b13 / 04-40MBps | grafana-1 | not measured | 273.2 |
| 20261123T190648-sweep-e19b13 / 04-40MBps | ingest-a1-1 | not measured | 658.3 |
| 20261123T190648-sweep-e19b13 / 04-40MBps | instruction-api-1 | not measured | 42.75 |
| 20261123T190648-sweep-e19b13 / 04-40MBps | loki-1 | not measured | 74.95 |
| 20261123T190648-sweep-e19b13 / 04-40MBps | pg-primary-1 | not measured | 2370.6 |
| 20261123T190648-sweep-e19b13 / 04-40MBps | plantsim-1 | not measured | 15.21 |
| 20261123T190648-sweep-e19b13 / 04-40MBps | portal-publisher-1 | not measured | 57.73 |
| 20261123T190648-sweep-e19b13 / 04-40MBps | processor-1 | not measured | 18.51 |
| 20261123T190648-sweep-e19b13 / 04-40MBps | seaweedfs-1 | not measured | 76.89 |
| 20261123T190648-sweep-e19b13 / 04-40MBps | tempo-1 | not measured | 32 |
| 20261123T190648-sweep-e19b13 / 04-40MBps | victoriametrics-1 | not measured | 96.48 |
| 20261123T195051-sweep-b2474f / 01-10MBps | alloy-1 | 0.0009165 | 128.7 |
| 20261123T195051-sweep-b2474f / 01-10MBps | archiver-1 | 3.7538e-05 | 21.44 |
| 20261123T195051-sweep-b2474f / 01-10MBps | dispatcher-1-1 | 0.0033669 | 88 |
| 20261123T195051-sweep-b2474f / 01-10MBps | dynamodb-local-1 | 0.0026629 | 165.6 |
| 20261123T195051-sweep-b2474f / 01-10MBps | elasticmq-1 | 0.0023912 | 68.07 |
| 20261123T195051-sweep-b2474f / 01-10MBps | grafana-1 | 0.0076669 | 507.2 |
| 20261123T195051-sweep-b2474f / 01-10MBps | ingest-a1-1 | 0.030067 | 165.8 |
| 20261123T195051-sweep-b2474f / 01-10MBps | instruction-api-1 | 0.00062057 | 67.35 |
| 20261123T195051-sweep-b2474f / 01-10MBps | loki-1 | 0.0021772 | 108.8 |
| 20261123T195051-sweep-b2474f / 01-10MBps | pg-primary-1 | 0.34301 | 634.5 |
| 20261123T195051-sweep-b2474f / 01-10MBps | plantsim-1 | 0 | 13.33 |
| 20261123T195051-sweep-b2474f / 01-10MBps | portal-publisher-1 | 0.0013037 | 90.74 |
| 20261123T195051-sweep-b2474f / 01-10MBps | processor-1 | 0.0013851 | 18.64 |
| 20261123T195051-sweep-b2474f / 01-10MBps | seaweedfs-1 | 0.0027151 | 129.2 |
| 20261123T195051-sweep-b2474f / 01-10MBps | tempo-1 | 0.00088891 | 49.34 |
| 20261123T195051-sweep-b2474f / 01-10MBps | victoriametrics-1 | 0.0034205 | 82.12 |
| 20261123T195051-sweep-b2474f / 02-20MBps | alloy-1 | 0.0025703 | 166.8 |
| 20261123T195051-sweep-b2474f / 02-20MBps | archiver-1 | 0.0014985 | 24.91 |
| 20261123T195051-sweep-b2474f / 02-20MBps | dispatcher-1-1 | 0.0054626 | 88.9 |
| 20261123T195051-sweep-b2474f / 02-20MBps | dynamodb-local-1 | 0.0028659 | 166.2 |
| 20261123T195051-sweep-b2474f / 02-20MBps | elasticmq-1 | 0.0038662 | 82.14 |
| 20261123T195051-sweep-b2474f / 02-20MBps | grafana-1 | 0.010864 | 514.9 |
| 20261123T195051-sweep-b2474f / 02-20MBps | ingest-a1-1 | 0.066922 | 366.4 |
| 20261123T195051-sweep-b2474f / 02-20MBps | instruction-api-1 | 0.00080777 | 67.35 |
| 20261123T195051-sweep-b2474f / 02-20MBps | loki-1 | 0.0060907 | 127.3 |
| 20261123T195051-sweep-b2474f / 02-20MBps | pg-primary-1 | 0.39626 | 1289.2 |
| 20261123T195051-sweep-b2474f / 02-20MBps | plantsim-1 | 0 | 14.34 |
| 20261123T195051-sweep-b2474f / 02-20MBps | portal-publisher-1 | 0.00070829 | 89.72 |
| 20261123T195051-sweep-b2474f / 02-20MBps | processor-1 | 0.0013023 | 24.24 |
| 20261123T195051-sweep-b2474f / 02-20MBps | seaweedfs-1 | 0.0022827 | 151.9 |
| 20261123T195051-sweep-b2474f / 02-20MBps | tempo-1 | 0.0024688 | 76.08 |
| 20261123T195051-sweep-b2474f / 02-20MBps | victoriametrics-1 | 0.0048525 | 85.41 |
| 20261123T195051-sweep-b2474f / 03-30MBps | alloy-1 | not measured | 48.5 |
| 20261123T195051-sweep-b2474f / 03-30MBps | archiver-1 | not measured | 2781.2 |
| 20261123T195051-sweep-b2474f / 03-30MBps | dispatcher-1-1 | not measured | 32.82 |
| 20261123T195051-sweep-b2474f / 03-30MBps | dynamodb-local-1 | not measured | 69.42 |
| 20261123T195051-sweep-b2474f / 03-30MBps | elasticmq-1 | not measured | 56.74 |
| 20261123T195051-sweep-b2474f / 03-30MBps | grafana-1 | not measured | 317.7 |
| 20261123T195051-sweep-b2474f / 03-30MBps | ingest-a1-1 | not measured | 466 |
| 20261123T195051-sweep-b2474f / 03-30MBps | instruction-api-1 | not measured | 13.3 |
| 20261123T195051-sweep-b2474f / 03-30MBps | loki-1 | not measured | 63.54 |
| 20261123T195051-sweep-b2474f / 03-30MBps | pg-primary-1 | not measured | 748.2 |
| 20261123T195051-sweep-b2474f / 03-30MBps | plantsim-1 | not measured | 12.37 |
| 20261123T195051-sweep-b2474f / 03-30MBps | portal-publisher-1 | not measured | 18.13 |
| 20261123T195051-sweep-b2474f / 03-30MBps | processor-1 | not measured | 18.84 |
| 20261123T195051-sweep-b2474f / 03-30MBps | seaweedfs-1 | not measured | 326.5 |
| 20261123T195051-sweep-b2474f / 03-30MBps | tempo-1 | not measured | 35.07 |
| 20261123T195051-sweep-b2474f / 03-30MBps | victoriametrics-1 | not measured | 82.73 |
| 20261123T195051-sweep-b2474f / 04-40MBps | alloy-1 | not measured | 72.5 |
| 20261123T195051-sweep-b2474f / 04-40MBps | archiver-1 | not measured | 4130.8 |
| 20261123T195051-sweep-b2474f / 04-40MBps | dispatcher-1-1 | not measured | 31.78 |
| 20261123T195051-sweep-b2474f / 04-40MBps | dynamodb-local-1 | not measured | 83.29 |
| 20261123T195051-sweep-b2474f / 04-40MBps | elasticmq-1 | not measured | 70.22 |
| 20261123T195051-sweep-b2474f / 04-40MBps | grafana-1 | not measured | 379.4 |
| 20261123T195051-sweep-b2474f / 04-40MBps | ingest-a1-1 | not measured | 318.1 |
| 20261123T195051-sweep-b2474f / 04-40MBps | instruction-api-1 | not measured | 13.07 |
| 20261123T195051-sweep-b2474f / 04-40MBps | loki-1 | not measured | 68.66 |
| 20261123T195051-sweep-b2474f / 04-40MBps | pg-primary-1 | not measured | 1656.8 |
| 20261123T195051-sweep-b2474f / 04-40MBps | plantsim-1 | not measured | 14.84 |
| 20261123T195051-sweep-b2474f / 04-40MBps | portal-publisher-1 | not measured | 18.68 |
| 20261123T195051-sweep-b2474f / 04-40MBps | processor-1 | not measured | 22.3 |
| 20261123T195051-sweep-b2474f / 04-40MBps | seaweedfs-1 | not measured | 381.4 |
| 20261123T195051-sweep-b2474f / 04-40MBps | tempo-1 | not measured | 55.05 |
| 20261123T195051-sweep-b2474f / 04-40MBps | victoriametrics-1 | not measured | 114.4 |
| 20261123T201215-repeat-30-1-6eecee / 01-30MBps | alloy-1 | 0.0021282 | 188.9 |
| 20261123T201215-repeat-30-1-6eecee / 01-30MBps | archiver-1 | 0.00017763 | 17.53 |
| 20261123T201215-repeat-30-1-6eecee / 01-30MBps | dispatcher-1-1 | 0.0039149 | 97.62 |
| 20261123T201215-repeat-30-1-6eecee / 01-30MBps | dynamodb-local-1 | 0.0033706 | 170.1 |
| 20261123T201215-repeat-30-1-6eecee / 01-30MBps | elasticmq-1 | 0.0021158 | 50.15 |
| 20261123T201215-repeat-30-1-6eecee / 01-30MBps | grafana-1 | 0.0054948 | 617.4 |
| 20261123T201215-repeat-30-1-6eecee / 01-30MBps | ingest-a1-1 | 0.049229 | 438.2 |
| 20261123T201215-repeat-30-1-6eecee / 01-30MBps | instruction-api-1 | 0.00058816 | 75.27 |
| 20261123T201215-repeat-30-1-6eecee / 01-30MBps | loki-1 | 0.0022838 | 98.46 |
| 20261123T201215-repeat-30-1-6eecee / 01-30MBps | pg-primary-1 | 0.21293 | 1501.2 |
| 20261123T201215-repeat-30-1-6eecee / 01-30MBps | plantsim-1 | 0.00084121 | 11.71 |
| 20261123T201215-repeat-30-1-6eecee / 01-30MBps | portal-publisher-1 | 0.0025754 | 98.98 |
| 20261123T201215-repeat-30-1-6eecee / 01-30MBps | processor-1 | 0.00072732 | 20 |
| 20261123T201215-repeat-30-1-6eecee / 01-30MBps | seaweedfs-1 | 0.0012909 | 134.7 |
| 20261123T201215-repeat-30-1-6eecee / 01-30MBps | tempo-1 | 0.00082218 | 62.48 |
| 20261123T201215-repeat-30-1-6eecee / 01-30MBps | victoriametrics-1 | 0.0013872 | 81.67 |
| 20261123T201819-repeat-30-2-a5183e / 01-30MBps | alloy-1 | 0.0017933 | 57.25 |
| 20261123T201819-repeat-30-2-a5183e / 01-30MBps | archiver-1 | 1.5838e-05 | 22.37 |
| 20261123T201819-repeat-30-2-a5183e / 01-30MBps | dispatcher-1-1 | 0.0032089 | 83.62 |
| 20261123T201819-repeat-30-2-a5183e / 01-30MBps | dynamodb-local-1 | 0.0028499 | 164.6 |
| 20261123T201819-repeat-30-2-a5183e / 01-30MBps | elasticmq-1 | 0.0023277 | 52.84 |
| 20261123T201819-repeat-30-2-a5183e / 01-30MBps | grafana-1 | 0.0045666 | 437.1 |
| 20261123T201819-repeat-30-2-a5183e / 01-30MBps | ingest-a1-1 | 0.034639 | 517.8 |
| 20261123T201819-repeat-30-2-a5183e / 01-30MBps | instruction-api-1 | 0.00062508 | 64.41 |
| 20261123T201819-repeat-30-2-a5183e / 01-30MBps | loki-1 | 0.0028189 | 54.26 |
| 20261123T201819-repeat-30-2-a5183e / 01-30MBps | pg-primary-1 | 0.23045 | 1354.8 |
| 20261123T201819-repeat-30-2-a5183e / 01-30MBps | plantsim-1 | 0.00056859 | 9.191 |
| 20261123T201819-repeat-30-2-a5183e / 01-30MBps | portal-publisher-1 | 0.0006331 | 85.49 |
| 20261123T201819-repeat-30-2-a5183e / 01-30MBps | processor-1 | 0.00084231 | 16.52 |
| 20261123T201819-repeat-30-2-a5183e / 01-30MBps | seaweedfs-1 | 0.001511 | 84.37 |
| 20261123T201819-repeat-30-2-a5183e / 01-30MBps | tempo-1 | 0.0006312 | 35.28 |
| 20261123T201819-repeat-30-2-a5183e / 01-30MBps | victoriametrics-1 | 0.0018846 | 80.54 |
| 20261123T202425-repeat-30-3-fb8302 / 01-30MBps | alloy-1 | 0.0022647 | 128.9 |
| 20261123T202425-repeat-30-3-fb8302 / 01-30MBps | archiver-1 | 0 | 16.9 |
| 20261123T202425-repeat-30-3-fb8302 / 01-30MBps | dispatcher-1-1 | 0.0044938 | 81.85 |
| 20261123T202425-repeat-30-3-fb8302 / 01-30MBps | dynamodb-local-1 | 0.0029228 | 154.7 |
| 20261123T202425-repeat-30-3-fb8302 / 01-30MBps | elasticmq-1 | 0.0026093 | 56.24 |
| 20261123T202425-repeat-30-3-fb8302 / 01-30MBps | grafana-1 | 0.0034153 | 479.7 |
| 20261123T202425-repeat-30-3-fb8302 / 01-30MBps | ingest-a1-1 | 0.039381 | 415 |
| 20261123T202425-repeat-30-3-fb8302 / 01-30MBps | instruction-api-1 | 0.0019065 | 62.36 |
| 20261123T202425-repeat-30-3-fb8302 / 01-30MBps | loki-1 | 0.0028699 | 85.1 |
| 20261123T202425-repeat-30-3-fb8302 / 01-30MBps | pg-primary-1 | 0.24339 | 1104.9 |
| 20261123T202425-repeat-30-3-fb8302 / 01-30MBps | plantsim-1 | 0.00057464 | 11.2 |
| 20261123T202425-repeat-30-3-fb8302 / 01-30MBps | portal-publisher-1 | 0.0021746 | 83.93 |
| 20261123T202425-repeat-30-3-fb8302 / 01-30MBps | processor-1 | 0.0011356 | 20.68 |
| 20261123T202425-repeat-30-3-fb8302 / 01-30MBps | seaweedfs-1 | 0.0041368 | 90.74 |
| 20261123T202425-repeat-30-3-fb8302 / 01-30MBps | tempo-1 | 0.00074658 | 48.18 |
| 20261123T202425-repeat-30-3-fb8302 / 01-30MBps | victoriametrics-1 | 0.0016896 | 96.32 |
| 20261123T203112-f1-fdfb8d / 01-30MBps | alloy-1 | 0.002805 | 58.34 |
| 20261123T203112-f1-fdfb8d / 01-30MBps | archiver-1 | 0.00020588 | 19.8 |
| 20261123T203112-f1-fdfb8d / 01-30MBps | dispatcher-1-1 | 0.0034968 | 83.19 |
| 20261123T203112-f1-fdfb8d / 01-30MBps | dynamodb-local-1 | 0.0027784 | 166.6 |
| 20261123T203112-f1-fdfb8d / 01-30MBps | elasticmq-1 | 0.0026462 | 42.77 |
| 20261123T203112-f1-fdfb8d / 01-30MBps | grafana-1 | 0.0033185 | 401.1 |
| 20261123T203112-f1-fdfb8d / 01-30MBps | ingest-a1-1 | 0.060703 | 462.3 |
| 20261123T203112-f1-fdfb8d / 01-30MBps | instruction-api-1 | 0.0020534 | 63.81 |
| 20261123T203112-f1-fdfb8d / 01-30MBps | loki-1 | 0.0032846 | 56.61 |
| 20261123T203112-f1-fdfb8d / 01-30MBps | pg-primary-1 | 0.22637 | 1029.1 |
| 20261123T203112-f1-fdfb8d / 01-30MBps | plantsim-1 | 0 | 11.98 |
| 20261123T203112-f1-fdfb8d / 01-30MBps | portal-publisher-1 | 0.00052969 | 85.27 |
| 20261123T203112-f1-fdfb8d / 01-30MBps | processor-1 | 0.0021282 | 15.09 |
| 20261123T203112-f1-fdfb8d / 01-30MBps | seaweedfs-1 | 0.0025003 | 95.72 |
| 20261123T203112-f1-fdfb8d / 01-30MBps | tempo-1 | 0.0016017 | 36.89 |
| 20261123T203112-f1-fdfb8d / 01-30MBps | victoriametrics-1 | 0.0018673 | 103.6 |
| 20261123T203719-f2-cc93af / 01-30MBps | alloy-1 | 0.0017661 | 130.6 |
| 20261123T203719-f2-cc93af / 01-30MBps | archiver-1 | 0.0013026 | 18.31 |
| 20261123T203719-f2-cc93af / 01-30MBps | dispatcher-1-1 | 0.0031016 | 82.04 |
| 20261123T203719-f2-cc93af / 01-30MBps | dynamodb-local-1 | 0.0024817 | 154.5 |
| 20261123T203719-f2-cc93af / 01-30MBps | elasticmq-1 | 0.0019761 | 60.72 |
| 20261123T203719-f2-cc93af / 01-30MBps | grafana-1 | 0.0043527 | 470.4 |
| 20261123T203719-f2-cc93af / 01-30MBps | ingest-a1-1 | 0.036517 | 482.4 |
| 20261123T203719-f2-cc93af / 01-30MBps | instruction-api-1 | 0.00050754 | 62.3 |
| 20261123T203719-f2-cc93af / 01-30MBps | loki-1 | 0.0019361 | 86.85 |
| 20261123T203719-f2-cc93af / 01-30MBps | pg-primary-1 | 0.14496 | 1228.8 |
| 20261123T203719-f2-cc93af / 01-30MBps | plantsim-1 | 0.00060093 | 8.262 |
| 20261123T203719-f2-cc93af / 01-30MBps | portal-publisher-1 | 0.00041439 | 83.89 |
| 20261123T203719-f2-cc93af / 01-30MBps | processor-1 | not measured | 16.87 |
| 20261123T203719-f2-cc93af / 01-30MBps | seaweedfs-1 | 0.0019634 | 86.8 |
| 20261123T203719-f2-cc93af / 01-30MBps | tempo-1 | 0.0011 | 49.1 |
| 20261123T203719-f2-cc93af / 01-30MBps | victoriametrics-1 | 0.0031768 | 89.91 |

| Run / target | Load generator CPU-s | Max memory MiB |
|---|---:|---:|
| 20261123T190648-sweep-e19b13 / 01-10MBps | 45.84 | 653.4 |
| 20261123T190648-sweep-e19b13 / 02-20MBps | 146.01 | 746.4 |
| 20261123T190648-sweep-e19b13 / 03-30MBps | not measured | 726.8 |
| 20261123T190648-sweep-e19b13 / 04-40MBps | not measured | 759.3 |
| 20261123T195051-sweep-b2474f / 01-10MBps | 62.673 | 673.3 |
| 20261123T195051-sweep-b2474f / 02-20MBps | 161.08 | 759.4 |
| 20261123T195051-sweep-b2474f / 03-30MBps | not measured | 696.6 |
| 20261123T201215-repeat-30-1-6eecee / 01-30MBps | 153.74 | 777 |
| 20261123T201819-repeat-30-2-a5183e / 01-30MBps | 146.66 | 795.2 |
| 20261123T202425-repeat-30-3-fb8302 / 01-30MBps | 147.68 | 785.2 |
| 20261123T203112-f1-fdfb8d / 01-30MBps | 132.21 | 769.6 |
| 20261123T203719-f2-cc93af / 01-30MBps | 131.22 | 768.1 |

The DB rule compares 202 receipt events with live DB rows plus rows in dropped exports. The Parquet rule also requires the Parquet row count to match.

| Run | 202 receipt events | Live DB rows on first check | Live DB rows on final check | DB rows in dropped exports | DB rows accounted | Parquet rows | Arm-1 loss rule | Result |
|---|---:|---:|---:|---:|---:|---:|---|---|
| 20261123T190648-sweep-e19b13 | 8693000 | 8693000 | 8693000 | 0 | 8693000 | not measured (Arm-1 export runs after hour close; see prereg change log) | DB | matched |
| 20261123T195051-sweep-b2474f | not measured | 6157000 | 6157000 | 0 | 6157000 | not measured (Arm-1 export runs after hour close; see prereg change log) | DB | not measured |
| 20261123T201215-repeat-30-1-6eecee | 3659000 | 3659000 | 3659000 | 0 | 3659000 | not measured (Arm-1 export runs after hour close; see prereg change log) | DB | matched |
| 20261123T201819-repeat-30-2-a5183e | 3576000 | 3576000 | 3576000 | 0 | 3576000 | not measured (Arm-1 export runs after hour close; see prereg change log) | DB | matched |
| 20261123T202425-repeat-30-3-fb8302 | 3319000 | 3319000 | 3319000 | 0 | 3319000 | not measured (Arm-1 export runs after hour close; see prereg change log) | DB | matched |
| 20261123T203112-f1-fdfb8d | 3012000 | 3012000 | 3012000 | 0 | 3012000 | not measured (Arm-1 export runs after hour close; see prereg change log) | DB | matched |
| 20261123T203719-f2-cc93af | 3809000 | 3809000 | 3809000 | 0 | 3809000 | not measured (Arm-1 export runs after hour close; see prereg change log) | DB | matched |

20261123T195051-sweep-b2474f — Incomplete load generator request records or invalid 202 receipts


20261123T195051-sweep-b2474f — RuntimeError loadgen failed with status 137; see $HOME/orca/workspaces/Final-Project/bp-e2e/docs/measurements/raw/arch-perf-2026-11-24/go-mvp/20261123T195051-sweep-b2474f/04-40MBps/loadgen.log


## go-split

| Run / target | MB/s per server offered | Accepted MB/s | Status ratios | Requests not sent (warmup + measure) | Generator starved | Ingest p50 ms | Ingest p99 ms | Store p99 s | Commit p99 s | Lag max s | Lag recovery s | Processor pending max | Archiver unacked max | Loss check |
|---|---:|---:|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| 20261123T213634-sweep-79544a / 01-10MBps (invalid: dropped iterations) | 10 | 17.992 | 202 91.66%, 503 8.34% | 5 | 0 | 261 | 3075 | 2.3496 | 2.7875 | 7.6625 | 4.066 | 94 | 107 | mismatch |
| 20261123T213634-sweep-79544a / 02-20MBps (invalid: generator starved) | 20 | 21.356 | 202 87.38%, 503 12.62% | 2368 | 2358 | 533 | 6747 | 4.1506 | 8.9 | 96.013 | 42.347 | 1319 | 98 | mismatch |
| 20261123T213634-sweep-79544a / 03-30MBps (invalid: generator starved) | 30 | 17.77 | 202 77.90%, 503 22.10% | 5726 | 5690 | 731 | 4650 | 4.8411 | 4.9625 | 132.45 | 54.864 | 1734 | 122 | mismatch |
| 20261123T213634-sweep-79544a / 04-40MBps (invalid: generator starved) | 40 | 19.2 | 202 79.42%, 503 20.58% | 9013 | 8982 | 648 | 6322 | 4.324 | 4.7125 | 117.07 | not measured | 1586 | 141 | mismatch |
| 20261123T220307-repeat-30-1-9c8e08 / 01-30MBps (invalid: generator starved) | 30 | 26.198 | 202 78.32%, 503 21.68% | 3711 | 3650 | 711 | 6471 | 4.7971 | 7 | 134.62 | 53.064 | 2560 | 136 | mismatch |
| 20261123T221422-repeat-30-2-83c046 / 01-30MBps (invalid: generator starved) | 30 | 23.552 | 202 73.20%, 503 26.80% | 3816 | 3770 | 808 | 5864 | 4.7985 | 4.8265 | 122.14 | 59.921 | 2122 | 147 | mismatch |
| 20261123T222534-repeat-30-3-9bacbd / 01-30MBps (invalid: generator starved) | 30 | 24.597 | 202 78.72%, 503 21.28% | 4095 | 4007 | 744 | 7645 | 4.5436 | 7.125 | 98.841 | 46.97 | 1914 | 165 | mismatch |
| 20261123T223656-f1-95e168 / 01-30MBps (invalid: generator starved) | 30 | 27.708 | 202 80.60%, 503 19.40% | 3764 requests not sent | 3655 | 560 | 7490 | 4.8007 | 10 | 144.07 | not measured | 2817 | 170 | mismatch |
| 20261123T223656-f1-95e168 / F1 interruption | 30 | 35.349 | 202 81.09%, 503 18.91% | not measured | not measured | 239.95 | 9398.8 | not measured | not measured | 29.166 | not measured | not measured | not measured | mismatch |
| 20261123T224829-f2-f767c3 / 01-30MBps (invalid: generator starved) | 30 | 17.123 | 202 68.05%, 503 31.95% | 5067 requests not sent | 4989 | 699 | 16243 | 8.2039 | 6.6 | 144.96 | not measured | 2124 | 134 | mismatch |
| 20261123T224829-f2-f767c3 / F2 interruption | 30 | 21.398 | 202 75.15%, 503 24.85% | not measured | not measured | 649.42 | 22913 | not measured | not measured | not measured | not measured | not measured | not measured | mismatch |

| Run / target | Ingest server | Achieved wire MB/s | Accepted MB/s | Status ratios | Ingest p50 ms | Ingest p95 ms | Ingest p99 ms |
|---|---|---:|---:|---|---:|---:|---:|
| 20261123T213634-sweep-79544a / 01-10MBps | ingest-a1 | 9.9896 | 8.9834 | 202 91.42%, 503 8.58% | 270 | 1739 | 3037 |
| 20261123T213634-sweep-79544a / 01-10MBps | ingest-a2 | 10.013 | 9.0085 | 202 91.90%, 503 8.10% | 250 | 2024 | 3153 |
| 20261123T213634-sweep-79544a / 02-20MBps | ingest-a1 | 12.887 | 11.023 | 202 89.07%, 503 10.93% | 482 | 3095 | 5205 |
| 20261123T213634-sweep-79544a / 02-20MBps | ingest-a2 | 13.029 | 10.333 | 202 85.71%, 503 14.29% | 578 | 4381 | 7928 |
| 20261123T213634-sweep-79544a / 03-30MBps | ingest-a1 | 12.656 | 8.9684 | 202 77.28%, 503 22.72% | 779 | 3504 | 4425 |
| 20261123T213634-sweep-79544a / 03-30MBps | ingest-a2 | 12.254 | 8.8019 | 202 78.53%, 503 21.47% | 698 | 2889 | 5059 |
| 20261123T213634-sweep-79544a / 04-40MBps | ingest-a1 | 13.101 | 10.212 | 202 83.69%, 503 16.31% | 710 | 3104 | 4537 |
| 20261123T213634-sweep-79544a / 04-40MBps | ingest-a2 | 13.006 | 8.9886 | 202 75.11%, 503 24.89% | 582 | 3039 | 7628 |
| 20261123T220307-repeat-30-1-9c8e08 / 01-30MBps | ingest-a1 | 18.353 | 12.666 | 202 76.61%, 503 23.39% | 675 | 3954 | 5712 |
| 20261123T220307-repeat-30-1-9c8e08 / 01-30MBps | ingest-a2 | 18.404 | 13.532 | 202 80.03%, 503 19.97% | 739 | 3785 | 7425 |
| 20261123T221422-repeat-30-2-83c046 / 01-30MBps | ingest-a1 | 18.522 | 11.501 | 202 73.56%, 503 26.44% | 815 | 3876 | 5927 |
| 20261123T221422-repeat-30-2-83c046 / 01-30MBps | ingest-a2 | 18.307 | 12.051 | 202 72.83%, 503 27.17% | 797 | 3710 | 5745 |
| 20261123T222534-repeat-30-3-9bacbd / 01-30MBps | ingest-a1 | 17.554 | 12.318 | 202 78.52%, 503 21.48% | 789 | 4439 | 6946 |
| 20261123T222534-repeat-30-3-9bacbd / 01-30MBps | ingest-a2 | 17.648 | 12.279 | 202 78.91%, 503 21.09% | 702 | 3437 | 8312 |
| 20261123T223656-f1-95e168 / 01-30MBps | ingest-a1 | 18.45 | 13.164 | 202 78.78%, 503 21.22% | 565 | 4546 | 8065 |
| 20261123T223656-f1-95e168 / 01-30MBps | ingest-a2 | 19.108 | 14.544 | 202 82.36%, 503 17.64% | 558 | 3670 | 6440 |
| 20261123T224829-f2-f767c3 / 01-30MBps | ingest-a1 | 15.105 | 8.2791 | 202 66.39%, 503 33.61% | 693 | 7957 | 22398 |
| 20261123T224829-f2-f767c3 / 01-30MBps | ingest-a2 | 15.267 | 8.8438 | 202 69.70%, 503 30.30% | 708 | 4280 | 9291 |

| Run / target | Container | CPU-s / accepted MB | Max memory MiB |
|---|---|---:|---:|
| 20261123T213634-sweep-79544a / 01-10MBps | alloy-1 | not measured | 50.72 |
| 20261123T213634-sweep-79544a / 01-10MBps | archiver-1 | not measured | 850.2 |
| 20261123T213634-sweep-79544a / 01-10MBps | dispatcher-1-1 | not measured | 82.67 |
| 20261123T213634-sweep-79544a / 01-10MBps | dispatcher-2-1 | not measured | 82.9 |
| 20261123T213634-sweep-79544a / 01-10MBps | dynamodb-local-1 | not measured | 167.8 |
| 20261123T213634-sweep-79544a / 01-10MBps | elasticmq-1 | not measured | 43.4 |
| 20261123T213634-sweep-79544a / 01-10MBps | grafana-1 | not measured | 425.7 |
| 20261123T213634-sweep-79544a / 01-10MBps | ingest-a1-1 | not measured | 116.4 |
| 20261123T213634-sweep-79544a / 01-10MBps | ingest-a2-1 | not measured | 118 |
| 20261123T213634-sweep-79544a / 01-10MBps | instruction-api-1 | not measured | 60.16 |
| 20261123T213634-sweep-79544a / 01-10MBps | loki-1 | not measured | 73.58 |
| 20261123T213634-sweep-79544a / 01-10MBps | nats-1 | not measured | 1143.8 |
| 20261123T213634-sweep-79544a / 01-10MBps | pg-primary-1 | not measured | 56.46 |
| 20261123T213634-sweep-79544a / 01-10MBps | pg-standby-1 | not measured | 28.33 |
| 20261123T213634-sweep-79544a / 01-10MBps | plantsim-1 | not measured | 12.02 |
| 20261123T213634-sweep-79544a / 01-10MBps | portal-publisher-1 | not measured | 82.29 |
| 20261123T213634-sweep-79544a / 01-10MBps | processor-1 | not measured | 279.3 |
| 20261123T213634-sweep-79544a / 01-10MBps | seaweedfs-1 | not measured | 717.1 |
| 20261123T213634-sweep-79544a / 01-10MBps | tempo-1 | not measured | 40.36 |
| 20261123T213634-sweep-79544a / 01-10MBps | victoriametrics-1 | not measured | 85.52 |
| 20261123T213634-sweep-79544a / 02-20MBps | alloy-1 | not measured | 46.68 |
| 20261123T213634-sweep-79544a / 02-20MBps | archiver-1 | not measured | 756.6 |
| 20261123T213634-sweep-79544a / 02-20MBps | dispatcher-1-1 | not measured | 31.11 |
| 20261123T213634-sweep-79544a / 02-20MBps | dispatcher-2-1 | not measured | 33.67 |
| 20261123T213634-sweep-79544a / 02-20MBps | dynamodb-local-1 | not measured | 116.7 |
| 20261123T213634-sweep-79544a / 02-20MBps | elasticmq-1 | not measured | 61.32 |
| 20261123T213634-sweep-79544a / 02-20MBps | grafana-1 | not measured | 235.8 |
| 20261123T213634-sweep-79544a / 02-20MBps | ingest-a1-1 | not measured | 347.2 |
| 20261123T213634-sweep-79544a / 02-20MBps | ingest-a2-1 | not measured | 620.7 |
| 20261123T213634-sweep-79544a / 02-20MBps | instruction-api-1 | not measured | 20.36 |
| 20261123T213634-sweep-79544a / 02-20MBps | loki-1 | not measured | 70.62 |
| 20261123T213634-sweep-79544a / 02-20MBps | nats-1 | not measured | 720.5 |
| 20261123T213634-sweep-79544a / 02-20MBps | pg-primary-1 | not measured | 42.74 |
| 20261123T213634-sweep-79544a / 02-20MBps | pg-standby-1 | not measured | 24.52 |
| 20261123T213634-sweep-79544a / 02-20MBps | plantsim-1 | not measured | 14.16 |
| 20261123T213634-sweep-79544a / 02-20MBps | portal-publisher-1 | not measured | 31.73 |
| 20261123T213634-sweep-79544a / 02-20MBps | processor-1 | not measured | 356.6 |
| 20261123T213634-sweep-79544a / 02-20MBps | seaweedfs-1 | not measured | 739.8 |
| 20261123T213634-sweep-79544a / 02-20MBps | tempo-1 | not measured | 34.04 |
| 20261123T213634-sweep-79544a / 02-20MBps | victoriametrics-1 | not measured | 60.32 |
| 20261123T213634-sweep-79544a / 03-30MBps | alloy-1 | not measured | 48.31 |
| 20261123T213634-sweep-79544a / 03-30MBps | archiver-1 | not measured | 688.2 |
| 20261123T213634-sweep-79544a / 03-30MBps | dispatcher-1-1 | not measured | 18.88 |
| 20261123T213634-sweep-79544a / 03-30MBps | dispatcher-2-1 | not measured | 24.56 |
| 20261123T213634-sweep-79544a / 03-30MBps | dynamodb-local-1 | not measured | 125.7 |
| 20261123T213634-sweep-79544a / 03-30MBps | elasticmq-1 | not measured | 55.64 |
| 20261123T213634-sweep-79544a / 03-30MBps | grafana-1 | not measured | 276 |
| 20261123T213634-sweep-79544a / 03-30MBps | ingest-a1-1 | not measured | 397.2 |
| 20261123T213634-sweep-79544a / 03-30MBps | ingest-a2-1 | not measured | 150.8 |
| 20261123T213634-sweep-79544a / 03-30MBps | instruction-api-1 | not measured | 13.5 |
| 20261123T213634-sweep-79544a / 03-30MBps | loki-1 | not measured | 66.9 |
| 20261123T213634-sweep-79544a / 03-30MBps | nats-1 | not measured | 555.4 |
| 20261123T213634-sweep-79544a / 03-30MBps | pg-primary-1 | not measured | 35.75 |
| 20261123T213634-sweep-79544a / 03-30MBps | pg-standby-1 | not measured | 18.24 |
| 20261123T213634-sweep-79544a / 03-30MBps | plantsim-1 | not measured | 14.68 |
| 20261123T213634-sweep-79544a / 03-30MBps | portal-publisher-1 | not measured | 17.61 |
| 20261123T213634-sweep-79544a / 03-30MBps | processor-1 | not measured | 138.2 |
| 20261123T213634-sweep-79544a / 03-30MBps | seaweedfs-1 | not measured | 722.4 |
| 20261123T213634-sweep-79544a / 03-30MBps | tempo-1 | not measured | 29.34 |
| 20261123T213634-sweep-79544a / 03-30MBps | victoriametrics-1 | not measured | 59.68 |
| 20261123T213634-sweep-79544a / 04-40MBps | alloy-1 | not measured | 46.34 |
| 20261123T213634-sweep-79544a / 04-40MBps | archiver-1 | not measured | 766.4 |
| 20261123T213634-sweep-79544a / 04-40MBps | dispatcher-1-1 | not measured | 15.75 |
| 20261123T213634-sweep-79544a / 04-40MBps | dispatcher-2-1 | not measured | 24.51 |
| 20261123T213634-sweep-79544a / 04-40MBps | dynamodb-local-1 | not measured | 136.9 |
| 20261123T213634-sweep-79544a / 04-40MBps | elasticmq-1 | not measured | 60.2 |
| 20261123T213634-sweep-79544a / 04-40MBps | grafana-1 | not measured | 245.9 |
| 20261123T213634-sweep-79544a / 04-40MBps | ingest-a1-1 | not measured | 229.2 |
| 20261123T213634-sweep-79544a / 04-40MBps | ingest-a2-1 | not measured | 549.1 |
| 20261123T213634-sweep-79544a / 04-40MBps | instruction-api-1 | not measured | 12.86 |
| 20261123T213634-sweep-79544a / 04-40MBps | loki-1 | not measured | 69.81 |
| 20261123T213634-sweep-79544a / 04-40MBps | nats-1 | not measured | 649.5 |
| 20261123T213634-sweep-79544a / 04-40MBps | pg-primary-1 | not measured | 37.87 |
| 20261123T213634-sweep-79544a / 04-40MBps | pg-standby-1 | not measured | 21.03 |
| 20261123T213634-sweep-79544a / 04-40MBps | plantsim-1 | not measured | 12.4 |
| 20261123T213634-sweep-79544a / 04-40MBps | portal-publisher-1 | not measured | 17.03 |
| 20261123T213634-sweep-79544a / 04-40MBps | processor-1 | not measured | 188.6 |
| 20261123T213634-sweep-79544a / 04-40MBps | seaweedfs-1 | not measured | 742.5 |
| 20261123T213634-sweep-79544a / 04-40MBps | tempo-1 | not measured | 31.65 |
| 20261123T213634-sweep-79544a / 04-40MBps | victoriametrics-1 | not measured | 58.31 |
| 20261123T220307-repeat-30-1-9c8e08 / 01-30MBps | alloy-1 | not measured | 85.83 |
| 20261123T220307-repeat-30-1-9c8e08 / 01-30MBps | archiver-1 | not measured | 691.6 |
| 20261123T220307-repeat-30-1-9c8e08 / 01-30MBps | dispatcher-1-1 | not measured | 82.73 |
| 20261123T220307-repeat-30-1-9c8e08 / 01-30MBps | dispatcher-2-1 | not measured | 82.84 |
| 20261123T220307-repeat-30-1-9c8e08 / 01-30MBps | dynamodb-local-1 | not measured | 172.3 |
| 20261123T220307-repeat-30-1-9c8e08 / 01-30MBps | elasticmq-1 | not measured | 39.09 |
| 20261123T220307-repeat-30-1-9c8e08 / 01-30MBps | grafana-1 | not measured | 444.6 |
| 20261123T220307-repeat-30-1-9c8e08 / 01-30MBps | ingest-a1-1 | not measured | 387.7 |
| 20261123T220307-repeat-30-1-9c8e08 / 01-30MBps | ingest-a2-1 | not measured | 506.2 |
| 20261123T220307-repeat-30-1-9c8e08 / 01-30MBps | instruction-api-1 | not measured | 60.45 |
| 20261123T220307-repeat-30-1-9c8e08 / 01-30MBps | loki-1 | not measured | 73.86 |
| 20261123T220307-repeat-30-1-9c8e08 / 01-30MBps | nats-1 | not measured | 733.1 |
| 20261123T220307-repeat-30-1-9c8e08 / 01-30MBps | pg-primary-1 | not measured | 54.58 |
| 20261123T220307-repeat-30-1-9c8e08 / 01-30MBps | pg-standby-1 | not measured | 31.59 |
| 20261123T220307-repeat-30-1-9c8e08 / 01-30MBps | plantsim-1 | not measured | 10.78 |
| 20261123T220307-repeat-30-1-9c8e08 / 01-30MBps | portal-publisher-1 | not measured | 82.73 |
| 20261123T220307-repeat-30-1-9c8e08 / 01-30MBps | processor-1 | not measured | 212.4 |
| 20261123T220307-repeat-30-1-9c8e08 / 01-30MBps | seaweedfs-1 | not measured | 695.7 |
| 20261123T220307-repeat-30-1-9c8e08 / 01-30MBps | tempo-1 | not measured | 47.21 |
| 20261123T220307-repeat-30-1-9c8e08 / 01-30MBps | victoriametrics-1 | not measured | 91.91 |
| 20261123T221422-repeat-30-2-83c046 / 01-30MBps | alloy-1 | not measured | 82.52 |
| 20261123T221422-repeat-30-2-83c046 / 01-30MBps | archiver-1 | not measured | 683 |
| 20261123T221422-repeat-30-2-83c046 / 01-30MBps | dispatcher-1-1 | not measured | 76.09 |
| 20261123T221422-repeat-30-2-83c046 / 01-30MBps | dispatcher-2-1 | not measured | 77.23 |
| 20261123T221422-repeat-30-2-83c046 / 01-30MBps | dynamodb-local-1 | not measured | 159.4 |
| 20261123T221422-repeat-30-2-83c046 / 01-30MBps | elasticmq-1 | not measured | 38.36 |
| 20261123T221422-repeat-30-2-83c046 / 01-30MBps | grafana-1 | not measured | 499.9 |
| 20261123T221422-repeat-30-2-83c046 / 01-30MBps | ingest-a1-1 | not measured | 402.6 |
| 20261123T221422-repeat-30-2-83c046 / 01-30MBps | ingest-a2-1 | not measured | 415.1 |
| 20261123T221422-repeat-30-2-83c046 / 01-30MBps | instruction-api-1 | not measured | 56.98 |
| 20261123T221422-repeat-30-2-83c046 / 01-30MBps | loki-1 | not measured | 70.21 |
| 20261123T221422-repeat-30-2-83c046 / 01-30MBps | nats-1 | not measured | 676 |
| 20261123T221422-repeat-30-2-83c046 / 01-30MBps | pg-primary-1 | not measured | 57.09 |
| 20261123T221422-repeat-30-2-83c046 / 01-30MBps | pg-standby-1 | not measured | 29.53 |
| 20261123T221422-repeat-30-2-83c046 / 01-30MBps | plantsim-1 | not measured | 10.91 |
| 20261123T221422-repeat-30-2-83c046 / 01-30MBps | portal-publisher-1 | not measured | 77.72 |
| 20261123T221422-repeat-30-2-83c046 / 01-30MBps | processor-1 | not measured | 196.8 |
| 20261123T221422-repeat-30-2-83c046 / 01-30MBps | seaweedfs-1 | not measured | 664.1 |
| 20261123T221422-repeat-30-2-83c046 / 01-30MBps | tempo-1 | not measured | 39.45 |
| 20261123T221422-repeat-30-2-83c046 / 01-30MBps | victoriametrics-1 | not measured | 87.25 |
| 20261123T222534-repeat-30-3-9bacbd / 01-30MBps | alloy-1 | not measured | 78.86 |
| 20261123T222534-repeat-30-3-9bacbd / 01-30MBps | archiver-1 | not measured | 761.5 |
| 20261123T222534-repeat-30-3-9bacbd / 01-30MBps | dispatcher-1-1 | not measured | 74.45 |
| 20261123T222534-repeat-30-3-9bacbd / 01-30MBps | dispatcher-2-1 | not measured | 72.81 |
| 20261123T222534-repeat-30-3-9bacbd / 01-30MBps | dynamodb-local-1 | not measured | 162.9 |
| 20261123T222534-repeat-30-3-9bacbd / 01-30MBps | elasticmq-1 | not measured | 39.14 |
| 20261123T222534-repeat-30-3-9bacbd / 01-30MBps | grafana-1 | not measured | 460.5 |
| 20261123T222534-repeat-30-3-9bacbd / 01-30MBps | ingest-a1-1 | not measured | 397.7 |
| 20261123T222534-repeat-30-3-9bacbd / 01-30MBps | ingest-a2-1 | not measured | 600.2 |
| 20261123T222534-repeat-30-3-9bacbd / 01-30MBps | instruction-api-1 | not measured | 51.28 |
| 20261123T222534-repeat-30-3-9bacbd / 01-30MBps | loki-1 | not measured | 67.62 |
| 20261123T222534-repeat-30-3-9bacbd / 01-30MBps | nats-1 | not measured | 660.6 |
| 20261123T222534-repeat-30-3-9bacbd / 01-30MBps | pg-primary-1 | not measured | 58.88 |
| 20261123T222534-repeat-30-3-9bacbd / 01-30MBps | pg-standby-1 | not measured | 30.44 |
| 20261123T222534-repeat-30-3-9bacbd / 01-30MBps | plantsim-1 | not measured | 11.1 |
| 20261123T222534-repeat-30-3-9bacbd / 01-30MBps | portal-publisher-1 | not measured | 73.65 |
| 20261123T222534-repeat-30-3-9bacbd / 01-30MBps | processor-1 | not measured | 264.7 |
| 20261123T222534-repeat-30-3-9bacbd / 01-30MBps | seaweedfs-1 | not measured | 646.5 |
| 20261123T222534-repeat-30-3-9bacbd / 01-30MBps | tempo-1 | not measured | 36.21 |
| 20261123T222534-repeat-30-3-9bacbd / 01-30MBps | victoriametrics-1 | not measured | 85.16 |
| 20261123T223656-f1-95e168 / 01-30MBps | alloy-1 | not measured | 80.09 |
| 20261123T223656-f1-95e168 / 01-30MBps | archiver-1 | not measured | 714.2 |
| 20261123T223656-f1-95e168 / 01-30MBps | dispatcher-1-1 | not measured | 82.74 |
| 20261123T223656-f1-95e168 / 01-30MBps | dispatcher-2-1 | not measured | 80.52 |
| 20261123T223656-f1-95e168 / 01-30MBps | dynamodb-local-1 | not measured | 168.5 |
| 20261123T223656-f1-95e168 / 01-30MBps | elasticmq-1 | not measured | 37.68 |
| 20261123T223656-f1-95e168 / 01-30MBps | grafana-1 | not measured | 499 |
| 20261123T223656-f1-95e168 / 01-30MBps | ingest-a1-1 | not measured | 372 |
| 20261123T223656-f1-95e168 / 01-30MBps | ingest-a2-1 | not measured | 450.4 |
| 20261123T223656-f1-95e168 / 01-30MBps | instruction-api-1 | not measured | 60.15 |
| 20261123T223656-f1-95e168 / 01-30MBps | loki-1 | not measured | 72.87 |
| 20261123T223656-f1-95e168 / 01-30MBps | nats-1 | not measured | 625.6 |
| 20261123T223656-f1-95e168 / 01-30MBps | pg-primary-1 | not measured | 68.74 |
| 20261123T223656-f1-95e168 / 01-30MBps | pg-standby-1 | not measured | 31.57 |
| 20261123T223656-f1-95e168 / 01-30MBps | plantsim-1 | not measured | 13.5 |
| 20261123T223656-f1-95e168 / 01-30MBps | portal-publisher-1 | not measured | 80.12 |
| 20261123T223656-f1-95e168 / 01-30MBps | processor-1 | not measured | 273.2 |
| 20261123T223656-f1-95e168 / 01-30MBps | seaweedfs-1 | not measured | 712.5 |
| 20261123T223656-f1-95e168 / 01-30MBps | tempo-1 | not measured | 42.59 |
| 20261123T223656-f1-95e168 / 01-30MBps | victoriametrics-1 | not measured | 80.67 |
| 20261123T224829-f2-f767c3 / 01-30MBps | alloy-1 | not measured | 85.02 |
| 20261123T224829-f2-f767c3 / 01-30MBps | archiver-1 | not measured | 665.8 |
| 20261123T224829-f2-f767c3 / 01-30MBps | dispatcher-1-1 | not measured | 85.07 |
| 20261123T224829-f2-f767c3 / 01-30MBps | dispatcher-2-1 | not measured | 86.11 |
| 20261123T224829-f2-f767c3 / 01-30MBps | dynamodb-local-1 | not measured | 179.4 |
| 20261123T224829-f2-f767c3 / 01-30MBps | elasticmq-1 | not measured | 39.23 |
| 20261123T224829-f2-f767c3 / 01-30MBps | grafana-1 | not measured | 529.6 |
| 20261123T224829-f2-f767c3 / 01-30MBps | ingest-a1-1 | not measured | 892.6 |
| 20261123T224829-f2-f767c3 / 01-30MBps | ingest-a2-1 | not measured | 399.8 |
| 20261123T224829-f2-f767c3 / 01-30MBps | instruction-api-1 | not measured | 62.24 |
| 20261123T224829-f2-f767c3 / 01-30MBps | loki-1 | not measured | 81.81 |
| 20261123T224829-f2-f767c3 / 01-30MBps | nats-1 | not measured | 527.5 |
| 20261123T224829-f2-f767c3 / 01-30MBps | pg-primary-1 | not measured | 56.18 |
| 20261123T224829-f2-f767c3 / 01-30MBps | pg-standby-1 | not measured | 26.38 |
| 20261123T224829-f2-f767c3 / 01-30MBps | plantsim-1 | not measured | 11.66 |
| 20261123T224829-f2-f767c3 / 01-30MBps | portal-publisher-1 | not measured | 86.37 |
| 20261123T224829-f2-f767c3 / 01-30MBps | processor-1 | not measured | 143.4 |
| 20261123T224829-f2-f767c3 / 01-30MBps | seaweedfs-1 | not measured | 654.6 |
| 20261123T224829-f2-f767c3 / 01-30MBps | tempo-1 | not measured | 46.13 |
| 20261123T224829-f2-f767c3 / 01-30MBps | victoriametrics-1 | not measured | 76.25 |

| Run / target | Load generator CPU-s | Max memory MiB |
|---|---:|---:|
| 20261123T213634-sweep-79544a / 01-10MBps | not measured | 763.9 |
| 20261123T213634-sweep-79544a / 02-20MBps | not measured | 462.1 |
| 20261123T213634-sweep-79544a / 03-30MBps | not measured | 361.6 |
| 20261123T213634-sweep-79544a / 04-40MBps | not measured | 361 |
| 20261123T220307-repeat-30-1-9c8e08 / 01-30MBps | not measured | 410.8 |
| 20261123T221422-repeat-30-2-83c046 / 01-30MBps | not measured | 417.2 |
| 20261123T222534-repeat-30-3-9bacbd / 01-30MBps | not measured | 558 |
| 20261123T223656-f1-95e168 / 01-30MBps | not measured | 591.1 |
| 20261123T224829-f2-f767c3 / 01-30MBps | not measured | 501.8 |

| Run | 202 receipt events | DB processor events (`sum(jammy.summary_1m.count)`) | Parquet rows | Result |
|---|---:|---:|---:|---|
| 20261123T213634-sweep-79544a | 14040000 | 14054000 | 8289000 | mismatch |
| 20261123T220307-repeat-30-1-9c8e08 | 5169000 | 5178000 | 5178000 | mismatch |
| 20261123T221422-repeat-30-2-83c046 | 4841000 | 4856000 | 4856000 | mismatch |
| 20261123T222534-repeat-30-3-9bacbd | 4688000 | 4697000 | 4697000 | mismatch |
| 20261123T223656-f1-95e168 | 5151000 | 5197000 | 5197000 | mismatch |
| 20261123T224829-f2-f767c3 | 3415000 | 3496000 | 3496000 | mismatch |

20261123T213634-sweep-79544a — Processor batch IDs mismatch. 202 receipts with accepted events covered 14040 distinct batches and DB recorded 14054 processor batches.


20261123T220307-repeat-30-1-9c8e08 — Processor batch IDs mismatch. 202 receipts with accepted events covered 5169 distinct batches and DB recorded 5178 processor batches.


20261123T221422-repeat-30-2-83c046 — Processor batch IDs mismatch. 202 receipts with accepted events covered 4841 distinct batches and DB recorded 4856 processor batches.


20261123T222534-repeat-30-3-9bacbd — Processor batch IDs mismatch. 202 receipts with accepted events covered 4688 distinct batches and DB recorded 4697 processor batches.


20261123T223656-f1-95e168 — Processor batch IDs mismatch. 202 receipts with accepted events covered 5151 distinct batches and DB recorded 5197 processor batches.


20261123T224829-f2-f767c3 — Processor batch IDs mismatch. 202 receipts with accepted events covered 3415 distinct batches and DB recorded 3496 processor batches.


## py-mvp

| Run / target | MB/s per server offered | Accepted MB/s | Status ratios | Requests not sent (warmup + measure) | Generator starved | Ingest p50 ms | Ingest p99 ms | Store p99 s | Commit p99 s | Lag max s | Lag recovery s | Processor pending max | Archiver unacked max | Loss check |
|---|---:|---:|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| 20261123T204337-sweep-4189f5 / 01-10MBps | 10 | 10.014 | 202 100.00% | 0 | 0 | 67 | 424 | 0.37889 | 0.027745 | 0.93833 | 0.338 | not measured | not measured | matched |
| 20261123T204337-sweep-4189f5 / 02-20MBps | 20 | 19.086 | 202 95.40%, 503 4.60% | 0 | 0 | 109 | 2195 | 2.2025 | 0.070485 | 76.698 | not measured | not measured | not measured | matched |
| 20261123T204337-sweep-4189f5 / 03-30MBps (invalid: dropped iterations) | 30 | 18.31 | 202 61.67%, 503 38.33% | 3 | 0 | 226 | 3817 | 4.5162 | 0.2434 | 200.44 | not measured | not measured | not measured | matched |
| 20261123T204337-sweep-4189f5 / 04-40MBps (invalid: generator starved) | 40 | 16.775 | 0 0.06%, 202 42.99%, 503 56.95% | 68 | 46 | 112 | 4068 | 4.7582 | 0.29908 | 406.12 | not measured | not measured | not measured | matched |
| 20261123T210455-repeat-30-1-17ca6b / 01-30MBps (invalid: dropped iterations) | 30 | 23.064 | 0 0.03%, 202 76.93%, 503 23.05% | 6 | 0 | 136 | 3812 | 3.8567 | 0.15284 | 164.65 | not measured | not measured | not measured | matched |
| 20261123T211110-repeat-30-2-9bd6ea / 01-30MBps (invalid: dropped iterations) | 30 | 23.447 | 202 78.58%, 503 21.42% | 1 | 0 | 155 | 3508 | 3.4819 | 0.17509 | 171.92 | not measured | not measured | not measured | matched |
| 20261123T211723-repeat-30-3-196625 / 01-30MBps (invalid: dropped iterations) | 30 | 24.784 | 0 0.05%, 202 82.43%, 503 17.52% | 1 | 0 | 178 | 3006 | 2.7715 | 0.18081 | 180.1 | not measured | not measured | not measured | matched |
| 20261123T212358-f1-0fa0d5 / 01-30MBps | 30 | 18.832 | 202 63.21%, 503 36.79% | 1 requests not sent | 0 | 135 | 3344 | 4.283 | 0.20786 | 185.97 | not measured | not measured | not measured | matched |
| 20261123T212358-f1-0fa0d5 / F1 interruption | 30 | 0.14668 | 202 0.93%, 503 99.07% | not measured | not measured | 41.77 | 2067.2 | not measured | not measured | 32.807 | not measured | not measured | not measured | matched |
| 20261123T213008-f2-e07b57 / 01-30MBps | 30 | 25.025 | 202 83.73%, 503 16.27% | 2 requests not sent | 0 | 156 | 2750 | 2.4793 | 0.15914 | 183.23 | not measured | not measured | not measured | matched |
| 20261123T213008-f2-e07b57 / F2 interruption | 30 | 26.526 | 202 87.66%, 503 12.34% | not measured | not measured | 142.94 | 2588.1 | not measured | not measured | not measured | not measured | not measured | not measured | matched |

| Run / target | Ingest server | Achieved wire MB/s | Accepted MB/s | Status ratios | Ingest p50 ms | Ingest p95 ms | Ingest p99 ms |
|---|---|---:|---:|---|---:|---:|---:|
| 20261123T204337-sweep-4189f5 / 01-10MBps | ingest-a1 | 10.014 | 10.014 | 202 100.00% | 67 | 236 | 424 |
| 20261123T204337-sweep-4189f5 / 02-20MBps | ingest-a1 | 19.981 | 19.086 | 202 95.40%, 503 4.60% | 109 | 961 | 2195 |
| 20261123T204337-sweep-4189f5 / 03-30MBps | ingest-a1 | 29.994 | 18.31 | 202 61.67%, 503 38.33% | 226 | 2516 | 3817 |
| 20261123T204337-sweep-4189f5 / 04-40MBps | ingest-a1 | 39.355 | 16.775 | 0 0.06%, 202 42.99%, 503 56.95% | 112 | 2812 | 4068 |
| 20261123T210455-repeat-30-1-17ca6b / 01-30MBps | ingest-a1 | 29.948 | 23.064 | 0 0.03%, 202 76.93%, 503 23.05% | 136 | 2172 | 3812 |
| 20261123T211110-repeat-30-2-9bd6ea / 01-30MBps | ingest-a1 | 29.995 | 23.447 | 202 78.58%, 503 21.42% | 155 | 2032 | 3508 |
| 20261123T211723-repeat-30-3-196625 / 01-30MBps | ingest-a1 | 29.995 | 24.784 | 0 0.05%, 202 82.43%, 503 17.52% | 178 | 2078 | 3006 |
| 20261123T212358-f1-0fa0d5 / 01-30MBps | ingest-a1 | 29.994 | 18.832 | 202 63.21%, 503 36.79% | 135 | 2071 | 3344 |
| 20261123T213008-f2-e07b57 / 01-30MBps | ingest-a1 | 29.972 | 25.025 | 202 83.73%, 503 16.27% | 156 | 1915 | 2750 |

| Run / target | Container | CPU-s / accepted MB | Max memory MiB |
|---|---|---:|---:|
| 20261123T204337-sweep-4189f5 / 01-10MBps | alloy-1 | 0.0012486 | 57.4 |
| 20261123T204337-sweep-4189f5 / 01-10MBps | archiver-1 | 0.00081013 | 146.7 |
| 20261123T204337-sweep-4189f5 / 01-10MBps | dispatcher-1-1 | 0.0017302 | 83.32 |
| 20261123T204337-sweep-4189f5 / 01-10MBps | dynamodb-local-1 | 0.0013435 | 162.5 |
| 20261123T204337-sweep-4189f5 / 01-10MBps | elasticmq-1 | 0.0017261 | 49.09 |
| 20261123T204337-sweep-4189f5 / 01-10MBps | grafana-1 | 0.0036137 | 368.8 |
| 20261123T204337-sweep-4189f5 / 01-10MBps | ingest-a1-1 | 0.02617 | 623.9 |
| 20261123T204337-sweep-4189f5 / 01-10MBps | instruction-api-1 | 0.0003509 | 63.85 |
| 20261123T204337-sweep-4189f5 / 01-10MBps | loki-1 | 0.0015851 | 68.76 |
| 20261123T204337-sweep-4189f5 / 01-10MBps | pg-primary-1 | 0.07055 | 624.6 |
| 20261123T204337-sweep-4189f5 / 01-10MBps | plantsim-1 | 0 | 12.76 |
| 20261123T204337-sweep-4189f5 / 01-10MBps | portal-publisher-1 | 0.000265 | 85.21 |
| 20261123T204337-sweep-4189f5 / 01-10MBps | processor-1 | 0.023922 | 84.11 |
| 20261123T204337-sweep-4189f5 / 01-10MBps | seaweedfs-1 | 0.00069537 | 90.58 |
| 20261123T204337-sweep-4189f5 / 01-10MBps | tempo-1 | 0.00050707 | 38.72 |
| 20261123T204337-sweep-4189f5 / 01-10MBps | victoriametrics-1 | 0.00092117 | 88.76 |
| 20261123T204337-sweep-4189f5 / 02-20MBps | alloy-1 | 0.00060522 | 58.68 |
| 20261123T204337-sweep-4189f5 / 02-20MBps | archiver-1 | 0.00068579 | 146.7 |
| 20261123T204337-sweep-4189f5 / 02-20MBps | dispatcher-1-1 | 0.0016838 | 83.82 |
| 20261123T204337-sweep-4189f5 / 02-20MBps | dynamodb-local-1 | 0.00078564 | 164.1 |
| 20261123T204337-sweep-4189f5 / 02-20MBps | elasticmq-1 | 0.0013659 | 57.46 |
| 20261123T204337-sweep-4189f5 / 02-20MBps | grafana-1 | 0.0027781 | 324.6 |
| 20261123T204337-sweep-4189f5 / 02-20MBps | ingest-a1-1 | 0.034153 | 713.2 |
| 20261123T204337-sweep-4189f5 / 02-20MBps | instruction-api-1 | 0.00022204 | 63.85 |
| 20261123T204337-sweep-4189f5 / 02-20MBps | loki-1 | 0.0016028 | 73.09 |
| 20261123T204337-sweep-4189f5 / 02-20MBps | pg-primary-1 | 0.072781 | 644.6 |
| 20261123T204337-sweep-4189f5 / 02-20MBps | plantsim-1 | 0 | 12.86 |
| 20261123T204337-sweep-4189f5 / 02-20MBps | portal-publisher-1 | 0.00019671 | 85.21 |
| 20261123T204337-sweep-4189f5 / 02-20MBps | processor-1 | 0.019759 | 83.83 |
| 20261123T204337-sweep-4189f5 / 02-20MBps | seaweedfs-1 | 0.0016696 | 91.29 |
| 20261123T204337-sweep-4189f5 / 02-20MBps | tempo-1 | 0.00043272 | 36.92 |
| 20261123T204337-sweep-4189f5 / 02-20MBps | victoriametrics-1 | 0.00074634 | 87.21 |
| 20261123T204337-sweep-4189f5 / 03-30MBps | alloy-1 | 0.0023346 | 46.72 |
| 20261123T204337-sweep-4189f5 / 03-30MBps | archiver-1 | 0.0011421 | 99.59 |
| 20261123T204337-sweep-4189f5 / 03-30MBps | dispatcher-1-1 | 0.0029962 | 79.73 |
| 20261123T204337-sweep-4189f5 / 03-30MBps | dynamodb-local-1 | 0.0014261 | 150.7 |
| 20261123T204337-sweep-4189f5 / 03-30MBps | elasticmq-1 | 0.0020084 | 56.48 |
| 20261123T204337-sweep-4189f5 / 03-30MBps | grafana-1 | 0.004506 | 384.7 |
| 20261123T204337-sweep-4189f5 / 03-30MBps | ingest-a1-1 | 0.073761 | 825.6 |
| 20261123T204337-sweep-4189f5 / 03-30MBps | instruction-api-1 | 0.00048606 | 60.27 |
| 20261123T204337-sweep-4189f5 / 03-30MBps | loki-1 | 0.0026827 | 65.91 |
| 20261123T204337-sweep-4189f5 / 03-30MBps | pg-primary-1 | 0.12687 | 562.4 |
| 20261123T204337-sweep-4189f5 / 03-30MBps | plantsim-1 | 0.00069407 | 11.6 |
| 20261123T204337-sweep-4189f5 / 03-30MBps | portal-publisher-1 | 0.00096552 | 81.36 |
| 20261123T204337-sweep-4189f5 / 03-30MBps | processor-1 | 0.010193 | 63.96 |
| 20261123T204337-sweep-4189f5 / 03-30MBps | seaweedfs-1 | 0.0027813 | 80.14 |
| 20261123T204337-sweep-4189f5 / 03-30MBps | tempo-1 | 0.0010895 | 27.48 |
| 20261123T204337-sweep-4189f5 / 03-30MBps | victoriametrics-1 | 0.0024033 | 86.85 |
| 20261123T204337-sweep-4189f5 / 04-40MBps | alloy-1 | 0.0020182 | 47.25 |
| 20261123T204337-sweep-4189f5 / 04-40MBps | archiver-1 | 0.0015428 | 99.6 |
| 20261123T204337-sweep-4189f5 / 04-40MBps | dispatcher-1-1 | 0.0028209 | 80.01 |
| 20261123T204337-sweep-4189f5 / 04-40MBps | dynamodb-local-1 | 0.0016058 | 151 |
| 20261123T204337-sweep-4189f5 / 04-40MBps | elasticmq-1 | 0.0023754 | 56.75 |
| 20261123T204337-sweep-4189f5 / 04-40MBps | grafana-1 | 0.0047734 | 386.2 |
| 20261123T204337-sweep-4189f5 / 04-40MBps | ingest-a1-1 | 0.095142 | 909.3 |
| 20261123T204337-sweep-4189f5 / 04-40MBps | instruction-api-1 | 0.0006617 | 60.27 |
| 20261123T204337-sweep-4189f5 / 04-40MBps | loki-1 | 0.0031611 | 66.41 |
| 20261123T204337-sweep-4189f5 / 04-40MBps | pg-primary-1 | 0.13537 | 823.9 |
| 20261123T204337-sweep-4189f5 / 04-40MBps | plantsim-1 | 0 | 12.1 |
| 20261123T204337-sweep-4189f5 / 04-40MBps | portal-publisher-1 | 0.00041148 | 81.36 |
| 20261123T204337-sweep-4189f5 / 04-40MBps | processor-1 | 0.012779 | 63.77 |
| 20261123T204337-sweep-4189f5 / 04-40MBps | seaweedfs-1 | 0.0026418 | 80.72 |
| 20261123T204337-sweep-4189f5 / 04-40MBps | tempo-1 | 0.00069519 | 27.98 |
| 20261123T204337-sweep-4189f5 / 04-40MBps | victoriametrics-1 | 0.00088016 | 86.72 |
| 20261123T210455-repeat-30-1-17ca6b / 01-30MBps | alloy-1 | 0.0011959 | 133.5 |
| 20261123T210455-repeat-30-1-17ca6b / 01-30MBps | archiver-1 | 0.00089567 | 89.47 |
| 20261123T210455-repeat-30-1-17ca6b / 01-30MBps | dispatcher-1-1 | 0.0020324 | 81.79 |
| 20261123T210455-repeat-30-1-17ca6b / 01-30MBps | dynamodb-local-1 | 0.0014661 | 158.5 |
| 20261123T210455-repeat-30-1-17ca6b / 01-30MBps | elasticmq-1 | 0.0015213 | 58.68 |
| 20261123T210455-repeat-30-1-17ca6b / 01-30MBps | grafana-1 | 0.0047943 | 490.3 |
| 20261123T210455-repeat-30-1-17ca6b / 01-30MBps | ingest-a1-1 | 0.053973 | 817.9 |
| 20261123T210455-repeat-30-1-17ca6b / 01-30MBps | instruction-api-1 | 0.00051495 | 62.02 |
| 20261123T210455-repeat-30-1-17ca6b / 01-30MBps | loki-1 | 0.0015276 | 85.91 |
| 20261123T210455-repeat-30-1-17ca6b / 01-30MBps | pg-primary-1 | 0.091504 | 525.4 |
| 20261123T210455-repeat-30-1-17ca6b / 01-30MBps | plantsim-1 | 0 | 7.414 |
| 20261123T210455-repeat-30-1-17ca6b / 01-30MBps | portal-publisher-1 | 0.00031734 | 83.79 |
| 20261123T210455-repeat-30-1-17ca6b / 01-30MBps | processor-1 | 0.011897 | 63.02 |
| 20261123T210455-repeat-30-1-17ca6b / 01-30MBps | seaweedfs-1 | 0.0010579 | 97.21 |
| 20261123T210455-repeat-30-1-17ca6b / 01-30MBps | tempo-1 | 0.00071627 | 50.32 |
| 20261123T210455-repeat-30-1-17ca6b / 01-30MBps | victoriametrics-1 | 0.00078176 | 83.26 |
| 20261123T211110-repeat-30-2-9bd6ea / 01-30MBps | alloy-1 | 0.0018646 | 59.02 |
| 20261123T211110-repeat-30-2-9bd6ea / 01-30MBps | archiver-1 | 0.00066193 | 123.4 |
| 20261123T211110-repeat-30-2-9bd6ea / 01-30MBps | dispatcher-1-1 | 0.0021031 | 83.29 |
| 20261123T211110-repeat-30-2-9bd6ea / 01-30MBps | dynamodb-local-1 | 0.0021016 | 172.1 |
| 20261123T211110-repeat-30-2-9bd6ea / 01-30MBps | elasticmq-1 | 0.0015996 | 46.27 |
| 20261123T211110-repeat-30-2-9bd6ea / 01-30MBps | grafana-1 | 0.0028008 | 384.6 |
| 20261123T211110-repeat-30-2-9bd6ea / 01-30MBps | ingest-a1-1 | 0.045374 | 818.9 |
| 20261123T211110-repeat-30-2-9bd6ea / 01-30MBps | instruction-api-1 | 0.00032369 | 63.91 |
| 20261123T211110-repeat-30-2-9bd6ea / 01-30MBps | loki-1 | 0.0018659 | 68 |
| 20261123T211110-repeat-30-2-9bd6ea / 01-30MBps | pg-primary-1 | 0.08762 | 499 |
| 20261123T211110-repeat-30-2-9bd6ea / 01-30MBps | plantsim-1 | 0 | 11.18 |
| 20261123T211110-repeat-30-2-9bd6ea / 01-30MBps | portal-publisher-1 | 0.00034232 | 85.13 |
| 20261123T211110-repeat-30-2-9bd6ea / 01-30MBps | processor-1 | 0.012238 | 74.43 |
| 20261123T211110-repeat-30-2-9bd6ea / 01-30MBps | seaweedfs-1 | 0.0024126 | 91.01 |
| 20261123T211110-repeat-30-2-9bd6ea / 01-30MBps | tempo-1 | 0.00043591 | 35.66 |
| 20261123T211110-repeat-30-2-9bd6ea / 01-30MBps | victoriametrics-1 | 0.0008779 | 72.05 |
| 20261123T211723-repeat-30-3-196625 / 01-30MBps | alloy-1 | 0.0010935 | 52.34 |
| 20261123T211723-repeat-30-3-196625 / 01-30MBps | archiver-1 | 0.00078706 | 89.3 |
| 20261123T211723-repeat-30-3-196625 / 01-30MBps | dispatcher-1-1 | 0.0017089 | 80.12 |
| 20261123T211723-repeat-30-3-196625 / 01-30MBps | dynamodb-local-1 | 0.0016678 | 145.6 |
| 20261123T211723-repeat-30-3-196625 / 01-30MBps | elasticmq-1 | 0.0013512 | 44.52 |
| 20261123T211723-repeat-30-3-196625 / 01-30MBps | grafana-1 | 0.002918 | 321.8 |
| 20261123T211723-repeat-30-3-196625 / 01-30MBps | ingest-a1-1 | 0.048076 | 784.8 |
| 20261123T211723-repeat-30-3-196625 / 01-30MBps | instruction-api-1 | 0.00029302 | 60.72 |
| 20261123T211723-repeat-30-3-196625 / 01-30MBps | loki-1 | 0.001559 | 62.33 |
| 20261123T211723-repeat-30-3-196625 / 01-30MBps | pg-primary-1 | 0.090734 | 600.1 |
| 20261123T211723-repeat-30-3-196625 / 01-30MBps | plantsim-1 | 5.9774e-06 | 9.996 |
| 20261123T211723-repeat-30-3-196625 / 01-30MBps | portal-publisher-1 | 0.00036244 | 81.51 |
| 20261123T211723-repeat-30-3-196625 / 01-30MBps | processor-1 | 0.01133 | 60.75 |
| 20261123T211723-repeat-30-3-196625 / 01-30MBps | seaweedfs-1 | 0.0011128 | 86.27 |
| 20261123T211723-repeat-30-3-196625 / 01-30MBps | tempo-1 | 0.00051994 | 27.58 |
| 20261123T211723-repeat-30-3-196625 / 01-30MBps | victoriametrics-1 | 0.00070705 | 110.2 |
| 20261123T212358-f1-0fa0d5 / 01-30MBps | alloy-1 | not measured | 44.44 |
| 20261123T212358-f1-0fa0d5 / 01-30MBps | archiver-1 | not measured | 89.23 |
| 20261123T212358-f1-0fa0d5 / 01-30MBps | dispatcher-1-1 | not measured | 79.58 |
| 20261123T212358-f1-0fa0d5 / 01-30MBps | dynamodb-local-1 | not measured | 145.3 |
| 20261123T212358-f1-0fa0d5 / 01-30MBps | elasticmq-1 | not measured | 44.48 |
| 20261123T212358-f1-0fa0d5 / 01-30MBps | grafana-1 | not measured | 385.2 |
| 20261123T212358-f1-0fa0d5 / 01-30MBps | ingest-a1-1 | not measured | 788.7 |
| 20261123T212358-f1-0fa0d5 / 01-30MBps | instruction-api-1 | not measured | 60.13 |
| 20261123T212358-f1-0fa0d5 / 01-30MBps | loki-1 | not measured | 50.27 |
| 20261123T212358-f1-0fa0d5 / 01-30MBps | pg-primary-1 | not measured | 496.8 |
| 20261123T212358-f1-0fa0d5 / 01-30MBps | plantsim-1 | not measured | 8.395 |
| 20261123T212358-f1-0fa0d5 / 01-30MBps | portal-publisher-1 | not measured | 81.11 |
| 20261123T212358-f1-0fa0d5 / 01-30MBps | processor-1 | not measured | 60.42 |
| 20261123T212358-f1-0fa0d5 / 01-30MBps | seaweedfs-1 | not measured | 80.99 |
| 20261123T212358-f1-0fa0d5 / 01-30MBps | tempo-1 | not measured | 25.54 |
| 20261123T212358-f1-0fa0d5 / 01-30MBps | victoriametrics-1 | not measured | 76.43 |
| 20261123T213008-f2-e07b57 / 01-30MBps | alloy-1 | 0.0015313 | 52.02 |
| 20261123T213008-f2-e07b57 / 01-30MBps | archiver-1 | 0.00091399 | 89.93 |
| 20261123T213008-f2-e07b57 / 01-30MBps | dispatcher-1-1 | 0.0015635 | 80.32 |
| 20261123T213008-f2-e07b57 / 01-30MBps | dynamodb-local-1 | 0.0018203 | 154.7 |
| 20261123T213008-f2-e07b57 / 01-30MBps | elasticmq-1 | 0.0012299 | 47.25 |
| 20261123T213008-f2-e07b57 / 01-30MBps | grafana-1 | 0.0018815 | 336.9 |
| 20261123T213008-f2-e07b57 / 01-30MBps | ingest-a1-1 | 0.043019 | 812.2 |
| 20261123T213008-f2-e07b57 / 01-30MBps | instruction-api-1 | 0.00030267 | 60.58 |
| 20261123T213008-f2-e07b57 / 01-30MBps | loki-1 | 0.0012073 | 53.48 |
| 20261123T213008-f2-e07b57 / 01-30MBps | pg-primary-1 | 0.074592 | 566.4 |
| 20261123T213008-f2-e07b57 / 01-30MBps | plantsim-1 | 7.3817e-06 | 8.613 |
| 20261123T213008-f2-e07b57 / 01-30MBps | portal-publisher-1 | 0.00021595 | 81.68 |
| 20261123T213008-f2-e07b57 / 01-30MBps | processor-1 | not measured | 64.78 |
| 20261123T213008-f2-e07b57 / 01-30MBps | seaweedfs-1 | 0.00063985 | 84.52 |
| 20261123T213008-f2-e07b57 / 01-30MBps | tempo-1 | 0.00031182 | 28.09 |
| 20261123T213008-f2-e07b57 / 01-30MBps | victoriametrics-1 | 0.0011804 | 79.59 |

| Run / target | Load generator CPU-s | Max memory MiB |
|---|---:|---:|
| 20261123T204337-sweep-4189f5 / 01-10MBps | 25.125 | 654.2 |
| 20261123T204337-sweep-4189f5 / 02-20MBps | 72.067 | 692.7 |
| 20261123T204337-sweep-4189f5 / 03-30MBps | 183.13 | 739.1 |
| 20261123T204337-sweep-4189f5 / 04-40MBps | 214.68 | 736.5 |
| 20261123T210455-repeat-30-1-17ca6b / 01-30MBps | 132.17 | 738.2 |
| 20261123T211110-repeat-30-2-9bd6ea / 01-30MBps | 123.83 | 716.8 |
| 20261123T211723-repeat-30-3-196625 / 01-30MBps | 126.69 | 740.3 |
| 20261123T212358-f1-0fa0d5 / 01-30MBps | not measured | 713.9 |
| 20261123T213008-f2-e07b57 / 01-30MBps | 117.76 | 742.4 |

The DB rule compares 202 receipt events with live DB rows plus rows in dropped exports. The Parquet rule also requires the Parquet row count to match.

| Run | 202 receipt events | Live DB rows on first check | Live DB rows on final check | DB rows in dropped exports | DB rows accounted | Parquet rows | Arm-1 loss rule | Result |
|---|---:|---:|---:|---:|---:|---:|---|---|
| 20261123T204337-sweep-4189f5 | 10999000 | 10999000 | 10999000 | 0 | 10999000 | not measured (Arm-1 export runs after hour close; see prereg change log) | DB | matched |
| 20261123T210455-repeat-30-1-17ca6b | 4069000 | 4069000 | 4069000 | 0 | 4069000 | not measured (Arm-1 export runs after hour close; see prereg change log) | DB | matched |
| 20261123T211110-repeat-30-2-9bd6ea | 4107000 | 4107000 | 4107000 | 0 | 4107000 | not measured (Arm-1 export runs after hour close; see prereg change log) | DB | matched |
| 20261123T211723-repeat-30-3-196625 | 4262000 | 4262000 | 4262000 | 0 | 4262000 | not measured (Arm-1 export runs after hour close; see prereg change log) | DB | matched |
| 20261123T212358-f1-0fa0d5 | 3474000 | 3474000 | 3474000 | 0 | 3474000 | not measured (Arm-1 export runs after hour close; see prereg change log) | DB | matched |
| 20261123T213008-f2-e07b57 | 4375000 | 4375000 | 4375000 | 0 | 4375000 | not measured (Arm-1 export runs after hour close; see prereg change log) | DB | matched |

## py-split

| Run / target | MB/s per server offered | Accepted MB/s | Status ratios | Requests not sent (warmup + measure) | Generator starved | Ingest p50 ms | Ingest p99 ms | Store p99 s | Commit p99 s | Lag max s | Lag recovery s | Processor pending max | Archiver unacked max | Loss check |
|---|---:|---:|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| 20261124T000734-repeat-30-1-d4f8c9 / 01-30MBps (invalid: generator starved) | 30 | 0.78336 | 0 0.04%, 202 2.31%, 503 97.65% | 4070 | 3892 | 47 | 1236 | 8.76 | 2.14 | 3.8344 | 4.037 | 7 | 106 | mismatch |
| 20261124T002013-repeat-30-2-07c1d5 (no load window) | not measured | not measured | not measured | not measured | not measured | not measured | not measured | not measured | not measured | not measured | not measured | not measured | not measured | not measured |
| 20261124T002444-repeat-30-3-84e86a (no load window) | not measured | not measured | not measured | not measured | not measured | not measured | not measured | not measured | not measured | not measured | not measured | not measured | not measured | not measured |
| 20261124T002802-f1-bdd2b8 / 01-30MBps (invalid: generator starved) | 30 | 12.453 | 0 0.05%, 202 25.87%, 503 74.09% | 2835 requests not sent | 2638 | 74 | 2636 | 6.6771 | 10 | 107.65 | 39.312 | 999 | 78 | mismatch |
| 20261124T002802-f1-bdd2b8 / F1 interruption | 30 | 12.716 | 202 26.97%, 503 73.03% | not measured | not measured | 65.637 | 2856.9 | not measured | not measured | 24.02 | 187.25 | not measured | not measured | mismatch |
| 20261124T003919-f2-ab6f66 / 01-30MBps (invalid: generator starved) | 30 | 20.38 | 0 0.05%, 202 43.71%, 503 56.24% | 2613 requests not sent | 2397 | 78 | 3646 | 4.7156 | 9.575 | 173.33 | not measured | 2405 | 111 | mismatch |
| 20261124T003919-f2-ab6f66 / F2 interruption | 30 | 29.94 | 202 55.39%, 503 44.61% | not measured | not measured | 72.188 | 2238.1 | not measured | not measured | not measured | not measured | not measured | not measured | mismatch |
| 20261124T005050-sweep-d27b03 / 01-10MBps (invalid: generator starved) | 10 | 7.8527 | 0 0.04%, 202 41.07%, 503 58.89% | 91 | 42 | 61 | 5668 | 7.4548 | 0.983 | 7.1217 | 2.484 | 50 | 97 | mismatch |
| 20261124T005050-sweep-d27b03 / 02-20MBps (invalid: generator starved) | 20 | 19.184 | 202 61.04%, 503 38.96% | 1185 | 1121 | 219 | 6549 | 6.2941 | 10 | 99.76 | not measured | 1173 | 94 | mismatch |
| 20261124T005050-sweep-d27b03 / 03-30MBps (invalid: generator starved) | 30 | 19.252 | 0 0.11%, 202 54.52%, 503 45.36% | 3714 | 3619 | 152 | 3104 | 2.4355 | 10 | 181.09 | not measured | 1998 | 103 | mismatch |
| 20261124T005050-sweep-d27b03 / 04-40MBps (invalid: generator starved) | 40 | 0.42839 | 0 0.14%, 202 1.90%, 503 97.96% | 8557 | 8422 | 46 | 1753 | 10 | 10 | 368 | not measured | 1512 | 80 | mismatch |
| 20261124T011732-repeat-30-1-71ec8f / 01-30MBps (invalid: generator starved) | 30 | 16.719 | 0 0.02%, 202 39.91%, 503 60.07% | 3677 | 3516 | 107 | 5178 | 6.5172 | 10 | 131.17 | not measured | 1383 | 115 | mismatch |
| 20261124T012956-repeat-30-2-1c12f8 / 01-30MBps (invalid: generator starved) | 30 | 14.475 | 202 37.95%, 503 62.05% | 4311 | 4205 | 108 | 4276 | 7.2662 | 8.9375 | 119.77 | not measured | 1171 | 119 | mismatch |
| 20261124T014135-repeat-30-3-241fae / 01-30MBps (invalid: generator starved) | 30 | 10.041 | 0 0.04%, 202 24.36%, 503 75.61% | 3748 | 3500 | 74 | 3647 | 7.2104 | 10 | 69.987 | 7.691 | 313 | 95 | mismatch |

| Run / target | Ingest server | Achieved wire MB/s | Accepted MB/s | Status ratios | Ingest p50 ms | Ingest p95 ms | Ingest p99 ms |
|---|---|---:|---:|---|---:|---:|---:|
| 20261124T000734-repeat-30-1-d4f8c9 / 01-30MBps | ingest-a1 | 19.226 | 0.78336 | 0 0.08%, 202 4.62%, 503 95.30% | 47 | 551 | 1605 |
| 20261124T000734-repeat-30-1-d4f8c9 / 01-30MBps | ingest-a2 | 19.246 | 0 | 503 100.00% | 46 | 291 | 751 |
| 20261124T002802-f1-bdd2b8 / 01-30MBps | ingest-a1 | 23.465 | 7.7399 | 0 0.03%, 202 32.19%, 503 67.78% | 86 | 1392 | 2694 |
| 20261124T002802-f1-bdd2b8 / 01-30MBps | ingest-a2 | 24.079 | 4.7133 | 0 0.07%, 202 19.72%, 503 80.22% | 65 | 1225 | 2512 |
| 20261124T003919-f2-ab6f66 / 01-30MBps | ingest-a1 | 23.441 | 8.0355 | 0 0.07%, 202 34.96%, 503 64.97% | 68 | 907 | 2017 |
| 20261124T003919-f2-ab6f66 / 01-30MBps | ingest-a2 | 23.56 | 12.344 | 0 0.03%, 202 52.40%, 503 47.56% | 92 | 1525 | 4789 |
| 20261124T005050-sweep-d27b03 / 01-10MBps | ingest-a1 | 9.7054 | 1.4417 | 202 15.69%, 503 84.31% | 46 | 767 | 4792 |
| 20261124T005050-sweep-d27b03 / 01-10MBps | ingest-a2 | 9.6368 | 6.411 | 0 0.08%, 202 66.50%, 503 33.41% | 109 | 1761 | 5827 |
| 20261124T005050-sweep-d27b03 / 02-20MBps | ingest-a1 | 15.857 | 9.7424 | 202 63.30%, 503 36.70% | 235 | 3074 | 8546 |
| 20261124T005050-sweep-d27b03 / 02-20MBps | ingest-a2 | 15.905 | 9.4414 | 202 58.79%, 503 41.21% | 203 | 2286 | 5830 |
| 20261124T005050-sweep-d27b03 / 03-30MBps | ingest-a1 | 16.826 | 10.509 | 0 0.09%, 202 62.12%, 503 37.79% | 182 | 1707 | 3139 |
| 20261124T005050-sweep-d27b03 / 03-30MBps | ingest-a2 | 17.438 | 8.7438 | 0 0.13%, 202 47.19%, 503 52.67% | 127 | 1558 | 3044 |
| 20261124T005050-sweep-d27b03 / 04-40MBps | ingest-a1 | 13.712 | 0.26215 | 0 0.17%, 202 2.35%, 503 97.47% | 46 | 313 | 1538 |
| 20261124T005050-sweep-d27b03 / 04-40MBps | ingest-a2 | 14.042 | 0.16623 | 0 0.11%, 202 1.46%, 503 98.43% | 46 | 381 | 2056 |
| 20261124T011732-repeat-30-1-71ec8f / 01-30MBps | ingest-a1 | 21.392 | 7.0346 | 202 34.41%, 503 65.59% | 92 | 2194 | 5202 |
| 20261124T011732-repeat-30-1-71ec8f / 01-30MBps | ingest-a2 | 21.257 | 9.6843 | 0 0.04%, 202 45.40%, 503 54.56% | 129 | 2117 | 5161 |
| 20261124T012956-repeat-30-2-1c12f8 / 01-30MBps | ingest-a1 | 18.896 | 5.2478 | 202 27.39%, 503 72.61% | 79 | 1504 | 3727 |
| 20261124T012956-repeat-30-2-1c12f8 / 01-30MBps | ingest-a2 | 18.591 | 9.227 | 202 48.67%, 503 51.33% | 157 | 2092 | 4779 |
| 20261124T014135-repeat-30-3-241fae / 01-30MBps | ingest-a1 | 20.88 | 4.5747 | 0 0.07%, 202 22.48%, 503 77.44% | 71 | 1313 | 3125 |
| 20261124T014135-repeat-30-3-241fae / 01-30MBps | ingest-a2 | 20.636 | 5.4659 | 202 26.27%, 503 73.73% | 78 | 1552 | 4737 |

| Run / target | Container | CPU-s / accepted MB | Max memory MiB |
|---|---|---:|---:|
| 20261124T000734-repeat-30-1-d4f8c9 / 01-30MBps | alloy-1 | not measured | 118.7 |
| 20261124T000734-repeat-30-1-d4f8c9 / 01-30MBps | archiver-1 | not measured | 580.8 |
| 20261124T000734-repeat-30-1-d4f8c9 / 01-30MBps | dispatcher-1-1 | not measured | 88.8 |
| 20261124T000734-repeat-30-1-d4f8c9 / 01-30MBps | dispatcher-2-1 | not measured | 87.67 |
| 20261124T000734-repeat-30-1-d4f8c9 / 01-30MBps | dynamodb-local-1 | not measured | 177.1 |
| 20261124T000734-repeat-30-1-d4f8c9 / 01-30MBps | elasticmq-1 | not measured | 52.34 |
| 20261124T000734-repeat-30-1-d4f8c9 / 01-30MBps | grafana-1 | not measured | 642.3 |
| 20261124T000734-repeat-30-1-d4f8c9 / 01-30MBps | ingest-a1-1 | not measured | 655.1 |
| 20261124T000734-repeat-30-1-d4f8c9 / 01-30MBps | ingest-a2-1 | not measured | 626.6 |
| 20261124T000734-repeat-30-1-d4f8c9 / 01-30MBps | instruction-api-1 | not measured | 71.66 |
| 20261124T000734-repeat-30-1-d4f8c9 / 01-30MBps | loki-1 | not measured | 102.8 |
| 20261124T000734-repeat-30-1-d4f8c9 / 01-30MBps | nats-1 | not measured | 596.4 |
| 20261124T000734-repeat-30-1-d4f8c9 / 01-30MBps | pg-primary-1 | not measured | 62.31 |
| 20261124T000734-repeat-30-1-d4f8c9 / 01-30MBps | pg-standby-1 | not measured | 28.59 |
| 20261124T000734-repeat-30-1-d4f8c9 / 01-30MBps | plantsim-1 | not measured | 15.3 |
| 20261124T000734-repeat-30-1-d4f8c9 / 01-30MBps | portal-publisher-1 | not measured | 92.86 |
| 20261124T000734-repeat-30-1-d4f8c9 / 01-30MBps | processor-1 | not measured | 112.2 |
| 20261124T000734-repeat-30-1-d4f8c9 / 01-30MBps | seaweedfs-1 | not measured | 860.9 |
| 20261124T000734-repeat-30-1-d4f8c9 / 01-30MBps | tempo-1 | not measured | 69.86 |
| 20261124T000734-repeat-30-1-d4f8c9 / 01-30MBps | victoriametrics-1 | not measured | 73.61 |
| 20261124T002802-f1-bdd2b8 / 01-30MBps | alloy-1 | not measured | 58.89 |
| 20261124T002802-f1-bdd2b8 / 01-30MBps | archiver-1 | not measured | 717.6 |
| 20261124T002802-f1-bdd2b8 / 01-30MBps | dispatcher-1-1 | not measured | 82.67 |
| 20261124T002802-f1-bdd2b8 / 01-30MBps | dispatcher-2-1 | not measured | 82.61 |
| 20261124T002802-f1-bdd2b8 / 01-30MBps | dynamodb-local-1 | not measured | 182.8 |
| 20261124T002802-f1-bdd2b8 / 01-30MBps | elasticmq-1 | not measured | 37.4 |
| 20261124T002802-f1-bdd2b8 / 01-30MBps | grafana-1 | not measured | 598.9 |
| 20261124T002802-f1-bdd2b8 / 01-30MBps | ingest-a1-1 | not measured | 631.6 |
| 20261124T002802-f1-bdd2b8 / 01-30MBps | ingest-a2-1 | not measured | 642.9 |
| 20261124T002802-f1-bdd2b8 / 01-30MBps | instruction-api-1 | not measured | 59.98 |
| 20261124T002802-f1-bdd2b8 / 01-30MBps | loki-1 | not measured | 76.4 |
| 20261124T002802-f1-bdd2b8 / 01-30MBps | nats-1 | not measured | 606.5 |
| 20261124T002802-f1-bdd2b8 / 01-30MBps | pg-primary-1 | not measured | 55.34 |
| 20261124T002802-f1-bdd2b8 / 01-30MBps | pg-standby-1 | not measured | 31.44 |
| 20261124T002802-f1-bdd2b8 / 01-30MBps | plantsim-1 | not measured | 13.41 |
| 20261124T002802-f1-bdd2b8 / 01-30MBps | portal-publisher-1 | not measured | 82.5 |
| 20261124T002802-f1-bdd2b8 / 01-30MBps | processor-1 | not measured | 269 |
| 20261124T002802-f1-bdd2b8 / 01-30MBps | seaweedfs-1 | not measured | 1007 |
| 20261124T002802-f1-bdd2b8 / 01-30MBps | tempo-1 | not measured | 47.79 |
| 20261124T002802-f1-bdd2b8 / 01-30MBps | victoriametrics-1 | not measured | 110.7 |
| 20261124T003919-f2-ab6f66 / 01-30MBps | alloy-1 | not measured | 85.46 |
| 20261124T003919-f2-ab6f66 / 01-30MBps | archiver-1 | not measured | 765.7 |
| 20261124T003919-f2-ab6f66 / 01-30MBps | dispatcher-1-1 | not measured | 68.48 |
| 20261124T003919-f2-ab6f66 / 01-30MBps | dispatcher-2-1 | not measured | 73.55 |
| 20261124T003919-f2-ab6f66 / 01-30MBps | dynamodb-local-1 | not measured | 174.4 |
| 20261124T003919-f2-ab6f66 / 01-30MBps | elasticmq-1 | not measured | 37.23 |
| 20261124T003919-f2-ab6f66 / 01-30MBps | grafana-1 | not measured | 474.5 |
| 20261124T003919-f2-ab6f66 / 01-30MBps | ingest-a1-1 | not measured | 549.4 |
| 20261124T003919-f2-ab6f66 / 01-30MBps | ingest-a2-1 | not measured | 529.6 |
| 20261124T003919-f2-ab6f66 / 01-30MBps | instruction-api-1 | not measured | 54.13 |
| 20261124T003919-f2-ab6f66 / 01-30MBps | loki-1 | not measured | 79.76 |
| 20261124T003919-f2-ab6f66 / 01-30MBps | nats-1 | not measured | 656 |
| 20261124T003919-f2-ab6f66 / 01-30MBps | pg-primary-1 | not measured | 51.73 |
| 20261124T003919-f2-ab6f66 / 01-30MBps | pg-standby-1 | not measured | 27.98 |
| 20261124T003919-f2-ab6f66 / 01-30MBps | plantsim-1 | not measured | 13.05 |
| 20261124T003919-f2-ab6f66 / 01-30MBps | portal-publisher-1 | not measured | 71.68 |
| 20261124T003919-f2-ab6f66 / 01-30MBps | processor-1 | not measured | 272 |
| 20261124T003919-f2-ab6f66 / 01-30MBps | seaweedfs-1 | not measured | 1051.6 |
| 20261124T003919-f2-ab6f66 / 01-30MBps | tempo-1 | not measured | 47.89 |
| 20261124T003919-f2-ab6f66 / 01-30MBps | victoriametrics-1 | not measured | 80.5 |
| 20261124T005050-sweep-d27b03 / 01-10MBps | alloy-1 | not measured | 66.92 |
| 20261124T005050-sweep-d27b03 / 01-10MBps | archiver-1 | not measured | 773.3 |
| 20261124T005050-sweep-d27b03 / 01-10MBps | dispatcher-1-1 | not measured | 57.23 |
| 20261124T005050-sweep-d27b03 / 01-10MBps | dispatcher-2-1 | not measured | 62.84 |
| 20261124T005050-sweep-d27b03 / 01-10MBps | dynamodb-local-1 | not measured | 157.8 |
| 20261124T005050-sweep-d27b03 / 01-10MBps | elasticmq-1 | not measured | 55.35 |
| 20261124T005050-sweep-d27b03 / 01-10MBps | grafana-1 | not measured | 407.6 |
| 20261124T005050-sweep-d27b03 / 01-10MBps | ingest-a1-1 | not measured | 543.3 |
| 20261124T005050-sweep-d27b03 / 01-10MBps | ingest-a2-1 | not measured | 557.8 |
| 20261124T005050-sweep-d27b03 / 01-10MBps | instruction-api-1 | not measured | 44.76 |
| 20261124T005050-sweep-d27b03 / 01-10MBps | loki-1 | not measured | 76.55 |
| 20261124T005050-sweep-d27b03 / 01-10MBps | nats-1 | not measured | 1397.8 |
| 20261124T005050-sweep-d27b03 / 01-10MBps | pg-primary-1 | not measured | 49.82 |
| 20261124T005050-sweep-d27b03 / 01-10MBps | pg-standby-1 | not measured | 24.61 |
| 20261124T005050-sweep-d27b03 / 01-10MBps | plantsim-1 | not measured | 13.96 |
| 20261124T005050-sweep-d27b03 / 01-10MBps | portal-publisher-1 | not measured | 60.21 |
| 20261124T005050-sweep-d27b03 / 01-10MBps | processor-1 | not measured | 125.8 |
| 20261124T005050-sweep-d27b03 / 01-10MBps | seaweedfs-1 | not measured | 1091.6 |
| 20261124T005050-sweep-d27b03 / 01-10MBps | tempo-1 | not measured | 52.22 |
| 20261124T005050-sweep-d27b03 / 01-10MBps | victoriametrics-1 | not measured | 92.59 |
| 20261124T005050-sweep-d27b03 / 02-20MBps | alloy-1 | not measured | 56.93 |
| 20261124T005050-sweep-d27b03 / 02-20MBps | archiver-1 | not measured | 670.8 |
| 20261124T005050-sweep-d27b03 / 02-20MBps | dispatcher-1-1 | not measured | 61.16 |
| 20261124T005050-sweep-d27b03 / 02-20MBps | dispatcher-2-1 | not measured | 63.67 |
| 20261124T005050-sweep-d27b03 / 02-20MBps | dynamodb-local-1 | not measured | 163.2 |
| 20261124T005050-sweep-d27b03 / 02-20MBps | elasticmq-1 | not measured | 58.41 |
| 20261124T005050-sweep-d27b03 / 02-20MBps | grafana-1 | not measured | 295.9 |
| 20261124T005050-sweep-d27b03 / 02-20MBps | ingest-a1-1 | not measured | 617.3 |
| 20261124T005050-sweep-d27b03 / 02-20MBps | ingest-a2-1 | not measured | 602.1 |
| 20261124T005050-sweep-d27b03 / 02-20MBps | instruction-api-1 | not measured | 41.26 |
| 20261124T005050-sweep-d27b03 / 02-20MBps | loki-1 | not measured | 70.27 |
| 20261124T005050-sweep-d27b03 / 02-20MBps | nats-1 | not measured | 1171.5 |
| 20261124T005050-sweep-d27b03 / 02-20MBps | pg-primary-1 | not measured | 47.45 |
| 20261124T005050-sweep-d27b03 / 02-20MBps | pg-standby-1 | not measured | 21.12 |
| 20261124T005050-sweep-d27b03 / 02-20MBps | plantsim-1 | not measured | 14.44 |
| 20261124T005050-sweep-d27b03 / 02-20MBps | portal-publisher-1 | not measured | 55.13 |
| 20261124T005050-sweep-d27b03 / 02-20MBps | processor-1 | not measured | 308.7 |
| 20261124T005050-sweep-d27b03 / 02-20MBps | seaweedfs-1 | not measured | 1108 |
| 20261124T005050-sweep-d27b03 / 02-20MBps | tempo-1 | not measured | 38.74 |
| 20261124T005050-sweep-d27b03 / 02-20MBps | victoriametrics-1 | not measured | 93.2 |
| 20261124T005050-sweep-d27b03 / 03-30MBps | alloy-1 | not measured | 55.92 |
| 20261124T005050-sweep-d27b03 / 03-30MBps | archiver-1 | not measured | 709.5 |
| 20261124T005050-sweep-d27b03 / 03-30MBps | dispatcher-1-1 | not measured | 58.53 |
| 20261124T005050-sweep-d27b03 / 03-30MBps | dispatcher-2-1 | not measured | 59.3 |
| 20261124T005050-sweep-d27b03 / 03-30MBps | dynamodb-local-1 | not measured | 163.6 |
| 20261124T005050-sweep-d27b03 / 03-30MBps | elasticmq-1 | not measured | 68.08 |
| 20261124T005050-sweep-d27b03 / 03-30MBps | grafana-1 | not measured | 301.1 |
| 20261124T005050-sweep-d27b03 / 03-30MBps | ingest-a1-1 | not measured | 581.1 |
| 20261124T005050-sweep-d27b03 / 03-30MBps | ingest-a2-1 | not measured | 606 |
| 20261124T005050-sweep-d27b03 / 03-30MBps | instruction-api-1 | not measured | 36.85 |
| 20261124T005050-sweep-d27b03 / 03-30MBps | loki-1 | not measured | 68.94 |
| 20261124T005050-sweep-d27b03 / 03-30MBps | nats-1 | not measured | 789.9 |
| 20261124T005050-sweep-d27b03 / 03-30MBps | pg-primary-1 | not measured | 41.87 |
| 20261124T005050-sweep-d27b03 / 03-30MBps | pg-standby-1 | not measured | 22.2 |
| 20261124T005050-sweep-d27b03 / 03-30MBps | plantsim-1 | not measured | 14.63 |
| 20261124T005050-sweep-d27b03 / 03-30MBps | portal-publisher-1 | not measured | 50.41 |
| 20261124T005050-sweep-d27b03 / 03-30MBps | processor-1 | not measured | 312.1 |
| 20261124T005050-sweep-d27b03 / 03-30MBps | seaweedfs-1 | not measured | 1121.3 |
| 20261124T005050-sweep-d27b03 / 03-30MBps | tempo-1 | not measured | 35.52 |
| 20261124T005050-sweep-d27b03 / 03-30MBps | victoriametrics-1 | not measured | 90.99 |
| 20261124T005050-sweep-d27b03 / 04-40MBps | alloy-1 | not measured | 58.04 |
| 20261124T005050-sweep-d27b03 / 04-40MBps | archiver-1 | not measured | 493.5 |
| 20261124T005050-sweep-d27b03 / 04-40MBps | dispatcher-1-1 | not measured | 60.85 |
| 20261124T005050-sweep-d27b03 / 04-40MBps | dispatcher-2-1 | not measured | 59.38 |
| 20261124T005050-sweep-d27b03 / 04-40MBps | dynamodb-local-1 | not measured | 163.7 |
| 20261124T005050-sweep-d27b03 / 04-40MBps | elasticmq-1 | not measured | 73.7 |
| 20261124T005050-sweep-d27b03 / 04-40MBps | grafana-1 | not measured | 262.6 |
| 20261124T005050-sweep-d27b03 / 04-40MBps | ingest-a1-1 | not measured | 539.8 |
| 20261124T005050-sweep-d27b03 / 04-40MBps | ingest-a2-1 | not measured | 578.3 |
| 20261124T005050-sweep-d27b03 / 04-40MBps | instruction-api-1 | not measured | 36.92 |
| 20261124T005050-sweep-d27b03 / 04-40MBps | loki-1 | not measured | 67.51 |
| 20261124T005050-sweep-d27b03 / 04-40MBps | nats-1 | not measured | 436.9 |
| 20261124T005050-sweep-d27b03 / 04-40MBps | pg-primary-1 | not measured | 38.63 |
| 20261124T005050-sweep-d27b03 / 04-40MBps | pg-standby-1 | not measured | 18.26 |
| 20261124T005050-sweep-d27b03 / 04-40MBps | plantsim-1 | not measured | 14.81 |
| 20261124T005050-sweep-d27b03 / 04-40MBps | portal-publisher-1 | not measured | 48.98 |
| 20261124T005050-sweep-d27b03 / 04-40MBps | processor-1 | not measured | 230.9 |
| 20261124T005050-sweep-d27b03 / 04-40MBps | seaweedfs-1 | not measured | 1016 |
| 20261124T005050-sweep-d27b03 / 04-40MBps | tempo-1 | not measured | 34 |
| 20261124T005050-sweep-d27b03 / 04-40MBps | victoriametrics-1 | not measured | 87.12 |
| 20261124T011732-repeat-30-1-71ec8f / 01-30MBps | alloy-1 | not measured | 100.2 |
| 20261124T011732-repeat-30-1-71ec8f / 01-30MBps | archiver-1 | not measured | 659.1 |
| 20261124T011732-repeat-30-1-71ec8f / 01-30MBps | dispatcher-1-1 | not measured | 84.76 |
| 20261124T011732-repeat-30-1-71ec8f / 01-30MBps | dispatcher-2-1 | not measured | 82.79 |
| 20261124T011732-repeat-30-1-71ec8f / 01-30MBps | dynamodb-local-1 | not measured | 166.7 |
| 20261124T011732-repeat-30-1-71ec8f / 01-30MBps | elasticmq-1 | not measured | 61.76 |
| 20261124T011732-repeat-30-1-71ec8f / 01-30MBps | grafana-1 | not measured | 458.1 |
| 20261124T011732-repeat-30-1-71ec8f / 01-30MBps | ingest-a1-1 | not measured | 647.5 |
| 20261124T011732-repeat-30-1-71ec8f / 01-30MBps | ingest-a2-1 | not measured | 724.6 |
| 20261124T011732-repeat-30-1-71ec8f / 01-30MBps | instruction-api-1 | not measured | 61.36 |
| 20261124T011732-repeat-30-1-71ec8f / 01-30MBps | loki-1 | not measured | 64.39 |
| 20261124T011732-repeat-30-1-71ec8f / 01-30MBps | nats-1 | not measured | 659.5 |
| 20261124T011732-repeat-30-1-71ec8f / 01-30MBps | pg-primary-1 | not measured | 53.45 |
| 20261124T011732-repeat-30-1-71ec8f / 01-30MBps | pg-standby-1 | not measured | 27.74 |
| 20261124T011732-repeat-30-1-71ec8f / 01-30MBps | plantsim-1 | not measured | 13.49 |
| 20261124T011732-repeat-30-1-71ec8f / 01-30MBps | portal-publisher-1 | not measured | 86.25 |
| 20261124T011732-repeat-30-1-71ec8f / 01-30MBps | processor-1 | not measured | 314.9 |
| 20261124T011732-repeat-30-1-71ec8f / 01-30MBps | seaweedfs-1 | not measured | 902 |
| 20261124T011732-repeat-30-1-71ec8f / 01-30MBps | tempo-1 | not measured | 46.54 |
| 20261124T011732-repeat-30-1-71ec8f / 01-30MBps | victoriametrics-1 | not measured | 79.16 |
| 20261124T012956-repeat-30-2-1c12f8 / 01-30MBps | alloy-1 | not measured | 65.54 |
| 20261124T012956-repeat-30-2-1c12f8 / 01-30MBps | archiver-1 | not measured | 546.7 |
| 20261124T012956-repeat-30-2-1c12f8 / 01-30MBps | dispatcher-1-1 | not measured | 69.23 |
| 20261124T012956-repeat-30-2-1c12f8 / 01-30MBps | dispatcher-2-1 | not measured | 60.8 |
| 20261124T012956-repeat-30-2-1c12f8 / 01-30MBps | dynamodb-local-1 | not measured | 163 |
| 20261124T012956-repeat-30-2-1c12f8 / 01-30MBps | elasticmq-1 | not measured | 54.7 |
| 20261124T012956-repeat-30-2-1c12f8 / 01-30MBps | grafana-1 | not measured | 483.5 |
| 20261124T012956-repeat-30-2-1c12f8 / 01-30MBps | ingest-a1-1 | not measured | 661.1 |
| 20261124T012956-repeat-30-2-1c12f8 / 01-30MBps | ingest-a2-1 | not measured | 702.4 |
| 20261124T012956-repeat-30-2-1c12f8 / 01-30MBps | instruction-api-1 | not measured | 48.1 |
| 20261124T012956-repeat-30-2-1c12f8 / 01-30MBps | loki-1 | not measured | 71.42 |
| 20261124T012956-repeat-30-2-1c12f8 / 01-30MBps | nats-1 | not measured | 626 |
| 20261124T012956-repeat-30-2-1c12f8 / 01-30MBps | pg-primary-1 | not measured | 49.71 |
| 20261124T012956-repeat-30-2-1c12f8 / 01-30MBps | pg-standby-1 | not measured | 22.89 |
| 20261124T012956-repeat-30-2-1c12f8 / 01-30MBps | plantsim-1 | not measured | 16.25 |
| 20261124T012956-repeat-30-2-1c12f8 / 01-30MBps | portal-publisher-1 | not measured | 67.43 |
| 20261124T012956-repeat-30-2-1c12f8 / 01-30MBps | processor-1 | not measured | 224.1 |
| 20261124T012956-repeat-30-2-1c12f8 / 01-30MBps | seaweedfs-1 | not measured | 910.9 |
| 20261124T012956-repeat-30-2-1c12f8 / 01-30MBps | tempo-1 | not measured | 38.14 |
| 20261124T012956-repeat-30-2-1c12f8 / 01-30MBps | victoriametrics-1 | not measured | 103.7 |
| 20261124T014135-repeat-30-3-241fae / 01-30MBps | alloy-1 | not measured | 156.1 |
| 20261124T014135-repeat-30-3-241fae / 01-30MBps | archiver-1 | not measured | 692 |
| 20261124T014135-repeat-30-3-241fae / 01-30MBps | dispatcher-1-1 | not measured | 86.1 |
| 20261124T014135-repeat-30-3-241fae / 01-30MBps | dispatcher-2-1 | not measured | 86.58 |
| 20261124T014135-repeat-30-3-241fae / 01-30MBps | dynamodb-local-1 | not measured | 193.9 |
| 20261124T014135-repeat-30-3-241fae / 01-30MBps | elasticmq-1 | not measured | 47.29 |
| 20261124T014135-repeat-30-3-241fae / 01-30MBps | grafana-1 | not measured | 552.9 |
| 20261124T014135-repeat-30-3-241fae / 01-30MBps | ingest-a1-1 | not measured | 605.7 |
| 20261124T014135-repeat-30-3-241fae / 01-30MBps | ingest-a2-1 | not measured | 727.1 |
| 20261124T014135-repeat-30-3-241fae / 01-30MBps | instruction-api-1 | not measured | 67.37 |
| 20261124T014135-repeat-30-3-241fae / 01-30MBps | loki-1 | not measured | 94.34 |
| 20261124T014135-repeat-30-3-241fae / 01-30MBps | nats-1 | not measured | 615.3 |
| 20261124T014135-repeat-30-3-241fae / 01-30MBps | pg-primary-1 | not measured | 59.66 |
| 20261124T014135-repeat-30-3-241fae / 01-30MBps | pg-standby-1 | not measured | 28.05 |
| 20261124T014135-repeat-30-3-241fae / 01-30MBps | plantsim-1 | not measured | 13.5 |
| 20261124T014135-repeat-30-3-241fae / 01-30MBps | portal-publisher-1 | not measured | 88.65 |
| 20261124T014135-repeat-30-3-241fae / 01-30MBps | processor-1 | not measured | 252.2 |
| 20261124T014135-repeat-30-3-241fae / 01-30MBps | seaweedfs-1 | not measured | 908.4 |
| 20261124T014135-repeat-30-3-241fae / 01-30MBps | tempo-1 | not measured | 73.14 |
| 20261124T014135-repeat-30-3-241fae / 01-30MBps | victoriametrics-1 | not measured | 100.9 |

| Run / target | Load generator CPU-s | Max memory MiB |
|---|---:|---:|
| 20261124T000734-repeat-30-1-d4f8c9 / 01-30MBps | not measured | 712.9 |
| 20261124T002802-f1-bdd2b8 / 01-30MBps | not measured | 717.3 |
| 20261124T003919-f2-ab6f66 / 01-30MBps | not measured | 736.7 |
| 20261124T005050-sweep-d27b03 / 01-10MBps | not measured | 755.2 |
| 20261124T005050-sweep-d27b03 / 02-20MBps | not measured | 624.4 |
| 20261124T005050-sweep-d27b03 / 03-30MBps | not measured | 695.9 |
| 20261124T005050-sweep-d27b03 / 04-40MBps | not measured | 398.3 |
| 20261124T011732-repeat-30-1-71ec8f / 01-30MBps | not measured | 654 |
| 20261124T012956-repeat-30-2-1c12f8 / 01-30MBps | not measured | 598.7 |
| 20261124T014135-repeat-30-3-241fae / 01-30MBps | not measured | 711.1 |

| Run | 202 receipt events | DB processor events (`sum(jammy.summary_1m.count)`) | Parquet rows | Result |
|---|---:|---:|---:|---|
| 20261124T000734-repeat-30-1-d4f8c9 | 349000 | 399000 | 389000 | mismatch |
| 20261124T002013-repeat-30-2-07c1d5 | not measured | not measured | not measured | not measured |
| 20261124T002444-repeat-30-3-84e86a | not measured | not measured | not measured | not measured |
| 20261124T002802-f1-bdd2b8 | 2125000 | 2249000 | 2115000 | mismatch |
| 20261124T003919-f2-ab6f66 | 3530000 | 3570000 | 3317000 | mismatch |
| 20261124T005050-sweep-d27b03 | 9010000 | 9118000 | 4921000 | mismatch |
| 20261124T011732-repeat-30-1-71ec8f | 2806000 | 2865000 | 1799000 | mismatch |
| 20261124T012956-repeat-30-2-1c12f8 | 2615000 | 2665000 | 2339000 | mismatch |
| 20261124T014135-repeat-30-3-241fae | 1675000 | 1750000 | 1617000 | mismatch |

20261124T000734-repeat-30-1-d4f8c9 — Processor batch IDs mismatch. 202 receipts with accepted events covered 349 distinct batches and DB recorded 399 processor batches.


20261124T002013-repeat-30-2-07c1d5 — CalledProcessError Command '['/Library/Developer/CommandLineTools/usr/bin/python3', 'tests/e2e/wait_ready.py', 'split']' returned non-zero exit status 1.


20261124T002444-repeat-30-3-84e86a — CalledProcessError Command '['/Library/Developer/CommandLineTools/usr/bin/python3', 'tests/e2e/wait_ready.py', 'split']' returned non-zero exit status 1.


20261124T002802-f1-bdd2b8 — Processor batch IDs mismatch. 202 receipts with accepted events covered 2125 distinct batches and DB recorded 2249 processor batches.


20261124T003919-f2-ab6f66 — Processor batch IDs mismatch. 202 receipts with accepted events covered 3530 distinct batches and DB recorded 3570 processor batches.


20261124T005050-sweep-d27b03 — Processor batch IDs mismatch. 202 receipts with accepted events covered 9010 distinct batches and DB recorded 9118 processor batches.


20261124T011732-repeat-30-1-71ec8f — Processor batch IDs mismatch. 202 receipts with accepted events covered 2806 distinct batches and DB recorded 2865 processor batches.


20261124T012956-repeat-30-2-1c12f8 — Processor batch IDs mismatch. 202 receipts with accepted events covered 2615 distinct batches and DB recorded 2665 processor batches.


20261124T014135-repeat-30-3-241fae — Processor batch IDs mismatch. 202 receipts with accepted events covered 1675 distinct batches and DB recorded 1750 processor batches.


## Excluded runs

- `failed-k6-oom/20261123T182459-sweep-8d66db` — Failed first attempt with k6 load generator: k6 experimental/fs open() loaded the entire 10MB/s payload bundle (5.89GB) into memory and OOMed with exit status 137 (6.3GiB / 7.65GiB Docker VM).
- `failed-k6-oom/20261123T182753-repeat-30-1-b0b6f9` — Failed first attempt with k6 load generator: k6 experimental/fs open() loaded the entire 10MB/s payload bundle (5.89GB) into memory and OOMed with exit status 137 (6.3GiB / 7.65GiB Docker VM).
- `failed-k6-oom/20261123T182929-repeat-30-2-35048d` — Failed first attempt with k6 load generator: k6 experimental/fs open() loaded the entire 10MB/s payload bundle (5.89GB) into memory and OOMed with exit status 137 (6.3GiB / 7.65GiB Docker VM).
- `failed-k6-oom/20261123T183110-repeat-30-3-6b8fbf` — Failed first attempt with k6 load generator: k6 experimental/fs open() loaded the entire 10MB/s payload bundle (5.89GB) into memory and OOMed with exit status 137 (6.3GiB / 7.65GiB Docker VM).
- `py-split/20261123T230008-sweep-4dfae9` — host suspended during run, 2026-11-24 ~08:20-09:00 KST
