# SECOM 반도체 공정 실시간 이상 탐지 파이프라인

SECOM 반도체 공정 데이터를 기반으로, 데이터 수집부터 실시간 처리/저장/시각화/일배치 집계까지
엔드투엔드로 구현한 부트캠프 프로젝트입니다.

## 1) 프로젝트 개요 및 목적

- 정적 CSV 데이터(SECOM)를 실시간 스트림처럼 재생하여 파이프라인을 구성
- 센서 이벤트를 실시간으로 수집/저장하고 이상 탐지 결과를 제공
- 운영 관점(장애 대응, 재실행 안정성, 모니터링 포인트)을 함께 학습
- 발표 시연에서 데이터 흐름 전체(수집 -> 처리 -> 저장 -> 조회 -> 집계)를 재현

## 2) 전체 아키텍처

`Producer -> Kafka -> Spark Structured Streaming -> PostgreSQL -> FastAPI -> Streamlit`  
`Airflow -> PostgreSQL(daily_agg)`

- `raw_events`: 원본 이벤트 저장 (Bronze)
- `event_anomalies`: 모델 스코어링 결과 저장 (is_anomaly, anomaly_score) (Silver)
- `daily_agg`: 일 단위 집계 결과 (Gold)

## 3) 데이터

- 출처: SECOM (UCI ML Repository / Kaggle)
- 특성: 센서 590개 + `Pass/Fail` 라벨
- 샘플 수: 1567
- 수집 방식: CSV replay 무한 발행 (`kafka/producer.py`)

## 4) 기술 스택

- 데이터 수집: Kafka (KRaft)
- 실시간 처리: Spark Structured Streaming
- 배치 스케줄링: Airflow
- 저장소: PostgreSQL
- API: FastAPI
- 대시보드: Streamlit + Plotly
- 환경 구성: Docker Compose

## 5) 컴포넌트 설명

- `kafka/producer.py`: CSV를 실시간 이벤트로 변환해 Kafka 토픽 발행
- `spark/streaming.py`: Kafka 이벤트 파싱, 모델 추론, DB 적재
- `api/main.py`: 이상치/집계 조회 API + 단건 예측 API
- `streamlit/app.py`: 실시간 모니터링/집계 대시보드
- `airflow/dags/daily_agg_dag.py`: 일 단위 집계(`daily_agg`) UPSERT

## 6) 설치 및 실행 방법

### 사전 준비

- Docker Desktop 실행
- 프로젝트 루트에 데이터 파일 준비: `data/uci-secom.csv`

### 1. 인프라/서비스 기동

```bash
docker compose up -d --build
```

### 2. Kafka 토픽 생성 (최초 1회)

```bash
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --create --topic secom-sensors --partitions 3 --replication-factor 1
```

### 3. 모델 학습 (API 컨테이너)

```bash
docker exec secom-api python train_model.py
```

모델 파일은 `/data/model.joblib`에 저장되며, API/Spark가 같은 파일을 공유해 사용합니다.

### 4. Spark Streaming 시작

```bash
docker exec spark /opt/bitnami/spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.1 /app/streaming.py
```

### 5. 데이터 발행 (별도 터미널)

```bash
python kafka/stress_test.py 1 60 10
```

위 명령은 1개 워커(1코어) 기준으로 60초 동안 초당 10건의 이벤트를 발행합니다.

### 6. 화면 접속

- Streamlit: `http://localhost:8501`
- FastAPI docs: `http://localhost:8000/docs`
- Airflow: `http://localhost:8090`
- Kafka UI: `http://localhost:8080`

## 7) 검증용 쿼리

```bash
docker exec postgres psql -U secom -d secom -c "SELECT COUNT(*) FROM raw_events;"
docker exec postgres psql -U secom -d secom -c "SELECT event_time, is_anomaly, anomaly_score, anomaly_type, model_version FROM event_anomalies ORDER BY id DESC LIMIT 20;"
docker exec postgres psql -U secom -d secom -c "SELECT * FROM daily_agg ORDER BY date DESC LIMIT 5;"
```

## 8) 기술적 의사결정 및 트레이드오프

- 이상 탐지 방식
  - 기존: 3시그마 룰 기반
  - 현재: Isolation Forest 모델 기반 이벤트 스코어링
  - 이유: 고정 임계값 방식의 오탐을 줄이고 다변량 패턴 반영
- event 결과 저장 정책
  - `event_anomalies`에 `is_anomaly`와 `anomaly_score` 저장
  - 장점: threshold 튜닝/모델 품질 분석 용이
- 데이터 발행 방식 확장
  - 초기에는 `kafka/producer.py` 단일 프로세스 발행 구조 사용
  - 고속 발행/부하 검증 한계를 보완하기 위해 `kafka/stress_test.py`(멀티프로세스) 추가
  - 이유: 단일 코어 발행 한계를 넘어서 처리량 테스트와 병목 구간 검증 필요

## 9) 설계 문서

- 최종 통합 문서: `docs/final-design.md`
- 회차별 원본 문서:
  - `docs/kafka-design.md`
  - `docs/spark-design.md`
  - `docs/airflow-design.md`
  - `docs/loadtest-design.md`

## 10) 회고

- 잘된 점
  - 실시간/배치/API/대시보드까지 엔드투엔드 구현 완료
  - 장애 시나리오 테스트 및 DLQ fallback 구현/검증 완료
- 어려웠던 점
  - 스트리밍 체크포인트 충돌 및 재시작 안정화 이슈
  - 초기 아키텍처 설계 단계에서의 불확실성과 의사결정 부담
- 다시 한다면
  - `event_scores`/`detected_anomalies` 저장 구조 분리
  - 자동 threshold 튜닝 및 모델 버전 실험 자동화
  - 센서군/라인 기준 토픽 분리 및 파티션 전략 고도화

## 11) 향후 개선 아이디어

- 이상치 알람 기능 추가 (Slack/메일 등)
- Prometheus/Grafana 기반 운영 메트릭 도입
- Kubernetes 기반 분산 실행 확장
- DAG 기반 DLQ 재적재(재처리) 자동화
