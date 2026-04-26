# Airflow DAG 설계서

## 1. DAG 목적 / 실행 단위

### 목적
raw_events와 anomalies 테이블에서 전일(ds) 데이터를 집계하여 daily_agg 테이블에 적재.
일별 트렌드 분석용 Gold 레이어 생성.

### 입력
- raw_events 테이블 (event_time 기준 날짜 필터)
- anomalies 테이블 (event_time 기준 날짜 필터)
- 날짜 파라미터: Airflow logical date (ds)

### 출력
- daily_agg 테이블에 1행 UPSERT
  - date, total_events, pass_count, anomaly_count, anomaly_rate

## 2. DAG 구조 (태스크 & 의존성)

check_data → aggregate_and_load → validate

### check_data (PythonOperator)
- raw_events에서 해당 날짜 데이터 건수 확인
- 0건이면 AirflowSkipException → 하위 태스크 skip

### aggregate_and_load (PythonOperator)
- CTE로 raw_events + anomalies 동시 집계
- daily_agg에 UPSERT (INSERT ON CONFLICT DO UPDATE)
- 단일 SQL문 = PostgreSQL 트랜잭션 보장 (Atomicity)

### validate (PythonOperator)
- daily_agg에서 적재된 행 확인
- anomaly_rate > 0.5이면 raise ValueError (태스크 Failed 처리)

### 데이터 전달 방식
- XCom 미사용 (각 태스크가 PostgreSQL에 직접 접근)
- PostgresHook으로 커넥션 라이프사이클 관리

## 3. 스케줄

schedule: @daily (매일 자정 UTC)
start_date: 2026-04-22
catchup: False
timezone: UTC (UI에서 KST 표시 가능)

### 왜 @daily인가
- 일 단위 집계가 자연스러운 주기
- hourly는 과도 (데이터 규모 작음)

## 4. Retry / Failure Handling

retries: 2
retry_delay: 3분
retry_exponential_backoff: True
max_retry_delay: 10분

### 재시도 의미 있는 실패
- PostgreSQL 연결 일시 끊김
- DB lock 대기 타임아웃

### 재시도 의미 없는 실패
- SQL 문법 오류 → 코드 수정 필요
- 데이터 0건 → check_data에서 skip 처리
- anomaly_rate 임계치 초과 → validate에서 Failed 처리

## 5. Idempotency (재실행 안전성)

INSERT ... ON CONFLICT (date) DO UPDATE SET ...

- daily_agg PK가 date → 같은 날짜 재실행 시 기존 행 UPDATE
- 중복 INSERT 없음
- backfill로 과거 날짜 재실행해도 결과 동일

## 6. 기술 선택 이유

### PythonOperator + PostgresHook vs SparkSubmitOperator
- daily_agg 집계는 PostgreSQL SQL 한 줄로 완료
- 데이터가 DB 안에 있으므로 DB 내부 처리가 효율적
- Spark 띄우는 오버헤드 (JVM + 패키지 다운로드) 불필요

### Airflow 2.10.5 선택 이유
- Airflow 3.x는 인증 시스템 변경 (SimpleAuthManager)으로 설정 복잡도 높음
- 2.10.5는 검증된 안정 버전, 부트캠프 실습 환경과 동일한 구조
- DAG 코드 자체는 3.x 마이그레이션 용이 (import 2줄 변경)

### LocalExecutor vs CeleryExecutor
- DAG 1개, 태스크 3개 → 병렬 실행 불필요
- CeleryExecutor는 Redis + Worker 추가 → 리소스 과도

### Timezone 처리
- 저장: event_time은 UTC 없이 로컬 시간
- 집계: AT TIME ZONE 'Asia/Seoul'로 명시적 변환
- DAG 실행: UTC 기준, UI 표시는 KST 변환 가능

## 7. Docker Compose 구성

airflow-init: DB 마이그레이션 + admin 유저 생성 + 커넥션 등록 → 종료
airflow-webserver: 웹 UI (localhost:8090)
airflow-scheduler: DAG 스케줄링

### 포트 매핑
- Kafka UI: localhost:8080
- Airflow UI: localhost:8090
- PostgreSQL: localhost:5432

## 8. 실행 방법

1. 환경 시작
docker compose up -d --build

2. Airflow UI 접속
localhost:8090 (admin / admin)

3. DAG 수동 실행
UI에서 secom_daily_aggregation → Trigger DAG

4. 결과 확인
docker exec postgres psql -U secom -d secom -c "SELECT * FROM daily_agg;"

## 9. 검증 결과

date=2026-04-26, total_events=75, pass_count=71, anomaly_count=20, anomaly_rate=0.2667
