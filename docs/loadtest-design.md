# 6회차: 부하 테스트 및 장애 대응 전략

## 1. 부하 시나리오 설계

### 테스트 도구

`kafka/stress_test.py` — multiprocessing 기반 Kafka 부하 생성기

- CLI 인자: `<workers> <duration_sec> <rate_per_worker>`
- producer.py와 동일한 메시지 포맷 사용
- 워커별 독립 Producer 인스턴스 + delivery callback 기반 성공/실패 추적

### 테스트 단계


| 단계          | 명령어                       | 목표 처리량      |
| ----------- | ------------------------- | ----------- |
| 1x baseline | `stress_test.py 1 30 1`   | 1 msg/s     |
| 10x         | `stress_test.py 1 60 10`  | 10 msg/s    |
| 100x        | `stress_test.py 2 60 50`  | 100 msg/s   |
| 1000x       | `stress_test.py 4 60 250` | 1,000 msg/s |


### 측정 항목

- stress_test.py 출력: 총 전송, 실패, 처리량(msg/s), 실패율
- `docker stats --no-stream`: 컨테이너별 CPU/MEM 사용량
- PostgreSQL: `SELECT count(*) FROM raw_events;` / `SELECT count(*) FROM anomalies;`

## 2. 부하 테스트 결과

### 처리량 및 리소스


| 단계    | 전송     | 실패  | 처리량     | Spark CPU | Spark MEM    |
| ----- | ------ | --- | ------- | --------- | ------------ |
| 1x    | 30     | 0   | 1.0/s   | 79%       | 1.34GB (67%) |
| 10x   | 600    | 0   | 9.9/s   | -         | -            |
| 100x  | 6,000  | 0   | 98.7/s  | 267%      | 1.56GB (78%) |
| 1000x | 60,000 | 0   | 987.2/s | 203%      | 1.96GB (98%) |


### 분석

- **Kafka**: 1000x에서도 Producer 전송 실패 0건. Kafka 자체는 병목이 아님.
- **Spark**: 1000x에서 메모리 1.96GB/2GB(98%) — OOM 직전. 이 이상의 부하에서는 Spark가 병목.
- **PostgreSQL**: 1000x에서도 JDBC write 정상. 현재 규모에서는 DB가 병목이 아님.
- **시스템 한계**: 현재 환경(Docker Desktop 7.6GB RAM, Spark 2GB limit)에서 약 1,000 msg/s가 안정 운영 상한선.

### 개선 방안 (스케일업 시)

- Spark mem_limit 증가 (4GB~8GB)
- Spark executor 분리 (Kubernetes 기반 클러스터 모드)
- Kafka 파티션 증가 (3 → 6+) + Spark consumer 병렬도 매칭

## 3. 장애 시나리오 및 대응 전략

### 시나리오 A: Kafka Broker 장애

**테스트**: Spark Streaming 실행 중 `docker stop kafka`


| 항목       | 결과                                                                   |
| -------- | -------------------------------------------------------------------- |
| Spark 동작 | Stream 유지 (죽지 않음)                                                    |
| 에러 로그    | `UnknownHostException: kafka`, `Broker may not be available` WARN 반복 |
| 복구       | `docker start kafka` → Spark consumer 자동 재연결 → 처리 재개                 |
| 데이터 유실   | Kafka down 중 producer 전송분만 유실 (broker가 없으므로)                         |


**대응 전략**:

- Spark의 Kafka consumer는 자동 retry를 내장하고 있어 별도 처리 불필요
- Producer 측에서는 retries=3 + acks=all로 일시 장애 대응
- 장기 장애 시 Producer에서 BufferError 발생 → flush 후 재시도

### 시나리오 B: PostgreSQL 장애

**테스트**: Spark Streaming 실행 중 `docker stop postgres`


| 항목       | v2 결과 (DLQ 없음)                                                          | v3 결과 (DLQ 적용)                    |
| -------- | ----------------------------------------------------------------------- | --------------------------------- |
| Spark 동작 | **Stream 종료 (죽음)**                                                      | Stream 유지                         |
| 에러       | `PSQLException: database system is shutting down` → foreachBatch 밖으로 전파 | 같은 에러지만 try-catch로 포착             |
| 데이터      | 실패 배치 유실                                                                | `/data/fallback/` JSON 파일로 DLQ 저장 |
| 복구       | spark-submit 재실행 필요                                                     | `docker start postgres` → 자동 복구   |


**DLQ (Dead Letter Queue) 전략**:

- JDBC write 실패 시 `process_batch` 내 try-catch로 예외 포착
- 실패한 배치 데이터를 `/data/fallback/{table}_batch{id}_{timestamp}.json`으로 저장
- Stream은 중단 없이 다음 배치 처리 계속
- DB 복구 후 DLQ 파일을 수동 또는 자동으로 DB에 재적재 가능

## 4. 모니터링 전략

### 현재 구현된 모니터링


| 대상         | 방법                        | 확인 내용                                 |
| ---------- | ------------------------- | ------------------------------------- |
| Kafka      | Kafka UI (localhost:8080) | 토픽 상태, consumer lag, 파티션 분배           |
| Spark      | foreachBatch 내 print()    | 배치별 처리 건수, 이상치 건수                     |
| PostgreSQL | psql 쿼리                   | raw_events/anomalies 건수, daily_agg 집계 |
| 컨테이너       | `docker stats`            | CPU/MEM 사용량                           |


### 향후 개선 예정 사항

- Prometheus + Grafana: Spark/Kafka/PostgreSQL 메트릭 수집 및 시각화
- Spark UI (port 4040): 배치 처리 시간, 셔플 크기, GC 시간
- PostgreSQL pg_stat: 커넥션 수, 쿼리 실행 시간, 락 대기

## 5. Fallback 및 Alert 전략

### Fallback 계층

```
1차: PostgreSQL JDBC write (정상 경로)
  ↓ 실패 시
2차: 로컬 JSON 파일 DLQ 저장 (/data/fallback/)
  ↓ 파일 저장도 실패 시
3차: 에러 로그 출력 (print) — 데이터 유실 발생
```

### DLQ 파일 형식

```
/data/fallback/
├── raw_events_batch28_20260428T121639.json
├── raw_events_batch29_20260428T121640.json
└── ...
```

### Alert 기준 (향후 구현)


| 조건                           | 심각도      | 알림             |
| ---------------------------- | -------- | -------------- |
| DLQ 파일 1건 이상 생성              | WARNING  | Slack 알림       |
| 연속 5배치 이상 DB 실패              | CRITICAL | Slack + 운영자 호출 |
| Spark 메모리 > 90%              | WARNING  | Grafana Alert  |
| Kafka consumer lag > 10,000  | WARNING  | Kafka UI Alert |
| daily_agg anomaly_rate > 50% | CRITICAL | 공정 관리자 알림      |


## 6. 실행 방법

### 부하 테스트

```bash
# 환경 시작
docker compose up -d --build

# 토픽 생성
docker exec kafka kafka-topics --bootstrap-server localhost:9092 \
  --create --topic secom-sensors --partitions 3 --replication-factor 1

# Spark Streaming 시작
docker exec spark /opt/bitnami/spark/bin/spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.1 \
  /app/streaming.py

# 부하 테스트 (별도 터미널)
python kafka/stress_test.py 1 30 1     # 1x baseline
python kafka/stress_test.py 1 60 10    # 10x
python kafka/stress_test.py 2 60 50    # 100x
python kafka/stress_test.py 4 60 250   # 1000x
```

### 장애 시뮬레이션

```bash
# Kafka 장애
docker stop kafka
# 30초 후
docker start kafka

# PostgreSQL 장애
docker stop postgres
# 30초 후
docker start postgres
# DLQ 파일 확인
ls data/fallback/
```

