# SECOM 파이프라인 최종 설계 문서

이 문서는 회차별 설계 문서를 최종 구조 기준으로 통합 정리한 문서입니다.

## 1. 최종 파이프라인 구성도

`Producer -> Kafka -> Spark Streaming -> PostgreSQL -> FastAPI -> Streamlit`  
`Airflow -> PostgreSQL(daily_agg)`

저장 계층:
- Bronze: `raw_events` (원본 JSON 이벤트)
- Silver: `event_anomalies` (모델 스코어링 결과)
- Gold: `daily_agg` (일 단위 집계)

## 2. 데이터 수집 (Kafka)

- 정적 CSV(SECOM)를 replay 방식으로 실시간 발행
- 토픽: `secom-sensors`, 파티션 3개
- producer 안정성 설정:
  - `acks=all`
  - `retries=3`
  - `retry.backoff.ms=500`
- 이벤트 스키마:
  - `event_time`(UTC)
  - `sensors`(590개)
  - `pass_fail`
  - `metadata`

## 3. 실시간 처리 (Spark Structured Streaming)

- Kafka readStream으로 수신 후 JSON 파싱
- `raw_events`에 원본 저장
- 모델(`model.joblib`)로 이벤트 단위 점수 계산
- `event_anomalies`에 저장:
  - `event_time`
  - `is_anomaly`
  - `anomaly_score`
  - `anomaly_type`
  - `model_version`
- 장애 대응:
  - JDBC write 실패 시 `/data/fallback`에 DLQ(JSON) 저장
  - 스트림 중단 없이 다음 배치 처리

## 4. 이상 탐지 전략 변경 이력

- 초기: 3시그마 룰 기반(센서 일부 기준)
- 최종: Isolation Forest 기반 이벤트 스코어링

변경 이유:
- 3시그마는 고정 임계값 기반이라 오탐이 잦았음
- 모델 기반은 다변량 패턴 반영 가능
- `anomaly_score`를 저장해 threshold 튜닝 가능

## 5. 저장소 설계 (PostgreSQL)

주요 테이블:
- `raw_events`
- `event_anomalies`
- `daily_agg`

시간 정책:
- 저장은 UTC (`TIMESTAMPTZ`)
- 조회/집계는 KST 경계 기준

조회 편의:
- `raw_events_kst`, `anomalies_kst`, `event_anomalies_kst` 뷰 제공

## 6. API/대시보드 설계

FastAPI:
- `/anomalies/latest`: 최신 이상 이벤트 조회
- `/anomalies/stats`: 스코어링 요약/유형별 통계
- `/daily-agg`: 일별 집계 조회
- `/predict`: 입력 센서값 단건 예측

Streamlit:
- 실시간 지표: 총 이벤트/총 이상치/총 스코어링
- 이상치 모니터링: `Anomaly Score` 분포/상세 테이블
- 일별 집계 시각화

## 7. 배치 집계 (Airflow)

DAG: `secom_daily_aggregation`
- Task: `check_data -> aggregate_and_load -> validate`
- `daily_agg`에 UPSERT로 재실행 안전성(idempotency) 보장
- manual run 보완:
  - 최신 데이터 날짜(KST) 자동 선택
  - scheduled run은 기존 `ds` 기준 유지

## 8. 부하 테스트 및 장애 대응

부하 테스트:
- 도구: `kafka/stress_test.py`
- 최대 약 1000 msg/s 구간까지 테스트
- 병목: Spark 메모리(2GB 제한 근접)

장애 시나리오:
- Kafka 다운: 재기동 후 소비 복구
- PostgreSQL 다운: DLQ fallback으로 유실 최소화

## 9. 주요 트레이드오프

- 단순성 vs 정확성:
  - 3시그마는 단순하지만 오탐 증가
  - 모델 기반은 정확도 향상 여지, 운영 복잡도 증가
- 저장량 vs 분석 유연성:
  - 이상 이벤트만 저장하면 용량 절약
  - 전체 스코어링 저장 시 튜닝/검증 유리

## 10. 회차별 원본 설계 문서

- Kafka: `docs/kafka-design.md`
- Spark: `docs/spark-design.md`
- Airflow: `docs/airflow-design.md`
- Load Test: `docs/loadtest-design.md`
