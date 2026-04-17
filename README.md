# SECOM 반도체 공정 실시간 이상 탐지 파이프라인

스마트팩토리 도메인 데이터 엔지니어 전환 준비 프로젝트.
현직자 스터디 (2026년 4-5월).

## 아키텍처
Replay(Producer) -> Kafka -> Spark Streaming -> PostgreSQL
- raw_events (원본)
- anomalies (이상치)
Airflow -> Spark Batch -> daily_agg (집계)

## 데이터
- SECOM (UCI ML Repository / Kaggle)
- 반도체 공정 센서 590개 + Pass/Fail 라벨
- 1567 샘플을 replay로 무한 순환 발행

## 기술 스택
- Kafka, Spark Structured Streaming, Airflow, PostgreSQL
- Docker Compose
- Python
