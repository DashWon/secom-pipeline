# Spark 전처리 및 저장 설계

## 1. 처리 방식 선택

### Streaming vs Batch

- 선택: Spark Structured Streaming (micro-batch 방식)
- 이유: Kafka 토픽의 센서 데이터를 실시간으로 처리해야 하므로 Streaming 선택
- Batch는 daily_agg 생성에만 사용 (5차시 Airflow에서 구현 예정)

### 처리 주기

- 실시간 스트리밍: Kafka 메시지 도착 즉시 micro-batch 단위로 처리
- 일배치: 매일 자정 raw_events에서 daily_agg 생성 (5차시)

## 2. 데이터 처리 흐름

### Kafka에서 데이터 읽는 방식

- readStream + format("kafka") 사용
- Kafka 내부 리스너(kafka:29092)로 연결 (Docker 네트워크 내부)
- startingOffsets: latest (스트림 시작 시점부터 처리)
- secom-sensors 토픽 구독

### 전처리/변환 로직

1. Kafka value (bytes) -> String 변환
2. from_json으로 JSON 파싱 (event_time, sensors, pass_fail 추출)
3. event_time을 timestamp 타입으로 캐스팅
4. sensors는 MapType(String, Float)으로 파싱
5. foreachBatch 내에서 2개 sink로 분기 처리

### foreachBatch 처리 패턴

1. batch_df.persist() - 메모리 캐싱 (2번 읽기 방지)
2. Sink 1: raw_events - to_json()으로 Map을 JSON 변환 후 PostgreSQL INSERT
3. Sink 2: anomalies - UDF + explode로 분산 처리 후 INSERT (collect 미사용)
4. batch_df.unpersist() - 메모리 해제

### Error Handling

- batch_df.isEmpty() 체크로 빈 배치 스킵
- persist/unpersist로 Lazy Evaluation 중복 실행 방지
- checkpoint 설정으로 장애 시 마지막 처리 지점부터 재시작

## 3. 처리 전/후 데이터 예시

### 입력 (Kafka 메시지)

```
{  
  "event_time": "2026-04-22T14:30:00.123456",  
  "sensors": {  
    "0": 3030.93,  
    "1": 2564.00,
      .
      .
      .
    "589": null  
  },  
  "pass_fail": -1,  
  "metadata": {  
    "source": "secom_replay",  
    "replay_speed": 1,  
    "version": "v1"  
  }  
}
```

### 출력 1 - raw_events (원본 저장)

| event_time | sensors (JSONB) | pass_fail | inserted_at |
| 2026-04-22 14:30:00 | {"0": 3030.93, "1": 2564.00, ...} | -1 | 2026-04-22 14:30:01 |

### 출력 2 - anomalies (이상치)

| event_time | sensor_id | sensor_value | z_score | anomaly_type | threshold_upper | threshold_lower |
| 2026-04-22 14:30:00 | 10 | -0.011 | 4.6 | 3SIGMA_VIOLATION | 0.027 | -0.003 |

## 4. 이상치 탐지 전략

### 1단계 (현재): 3시그마 룰

- EDA에서 주요 센서 (현재 임의 선정) 5개의 평균/표준편차 사전 계산
- 센서 값이 평균 +- 3시그마 벗어나면 이상치 판정
- z_score = abs(sensor_value - mean) / std
- Broadcast Variable로 통계값을 모든 워커에 공유

### NaN 처리

- NaN인 센서는 이상치 판정에서 제외 (잘못된 판정 방지)
- NaN이 100개 이상이면 SENSOR_FAULT로 별도 분류

### Pass/Fail 라벨 활용

- 탐지 룰 자체로 사용 X (실무에서 라벨은 공정 끝나야 나옴)
- 탐지 결과의 정확도 검증용 정답지로만 사용

## 5. 저장소 설계

### 저장소 선택

- PostgreSQL 15
- 선택 이유: JSONB 지원 (590개 센서 압축 저장), SQL 집계 강력, 윈도우 함수 지원

### 테이블 스키마

raw_events (Bronze):

- id: SERIAL PRIMARY KEY
- event_time: TIMESTAMPTZ NOT NULL (UTC 표준 저장)
- sensors: JSONB NOT NULL (590개 센서값)
- pass_fail: INTEGER NOT NULL
- inserted_at: TIMESTAMPTZ DEFAULT NOW()

anomalies (Silver):

- id: SERIAL PRIMARY KEY
- event_time: TIMESTAMPTZ NOT NULL (UTC 표준 저장)
- sensor_id: VARCHAR(10) NOT NULL
- sensor_value: FLOAT NOT NULL
- z_score: FLOAT NOT NULL
- anomaly_type: VARCHAR(50) NOT NULL
- threshold_upper: FLOAT
- threshold_lower: FLOAT
- inserted_at: TIMESTAMPTZ DEFAULT NOW()

daily_agg (Gold, 5차시 구현 예정):

- date: DATE PRIMARY KEY
- total_events: INTEGER
- pass_count: INTEGER
- anomaly_count: INTEGER
- anomaly_rate: FLOAT
- created_at: TIMESTAMPTZ DEFAULT NOW()

### 인덱스

- raw_events(event_time) - 날짜별 조회용
- anomalies(event_time) - 이상치 시간대 조회용

### 파티셔닝

- 현재 미적용
- 로드 테스트에서 데이터 증가 시 event_time 기준 날짜별 파티셔닝 검토

## 6. Spark Configuration


| 설정                           | 값                               | 이유                   |
| ---------------------------- | ------------------------------- | -------------------- |
| spark.sql.shuffle.partitions | 3                               | Kafka 파티션 3개에 매칭     |
| spark.jars.packages          | spark-sql-kafka, postgresql     | Kafka 연결 + JDBC 드라이버 |
| checkpointLocation           | /app/checkpoint/secom-streaming | 컨테이너 재시작 후에도 복구 지점 유지 |
| 메모리 제한                       | 2GB (docker-compose)            | 노트북 리소스 고려           |


### 로컬 환경 고려

- Docker Desktop RAM 7.4GB 중 Spark에 2GB 할당
- Kafka 500MB + PostgreSQL 500MB + Kafka UI 200MB + 여유
- 단일 executor, local mode로 운영

## 7. 실행 방법

1. 환경 시작

docker compose up -d

1. 토픽 생성

docker exec kafka kafka-topics --bootstrap-server localhost:9092 --create --topic secom-sensors --partitions 3 --replication-factor 1

1. Spark Streaming 시작

docker exec spark /opt/bitnami/spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.1 /app/streaming.py

1. Producer 시작 (별도 터미널)

python kafka/producer.py

1. 결과 확인

docker exec postgres psql -U secom -d secom -c "SELECT count(*) FROM raw_events;"
docker exec postgres psql -U secom -d secom -c "SELECT * FROM anomalies LIMIT 5;"