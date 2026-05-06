import os
import json
from datetime import datetime, timezone

import joblib
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_json, udf, lit
from pyspark.sql.types import (
    StringType, IntegerType, FloatType, BooleanType,
    StructType, StructField, MapType
)

# ============================================================
# SECOM Spark Streaming (v3 — UDF 분산 처리 + DLQ Fallback)
# Kafka -> raw_events + anomalies (PostgreSQL)
# DB 장애 시 로컬 JSON 파일로 DLQ 저장 → Stream 유지
# ============================================================

# 1. SparkSession
spark = SparkSession.builder \
    .appName("SECOM-Streaming") \
    .config("spark.sql.shuffle.partitions", "3") \
    .config("spark.sql.session.timeZone", "UTC") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
print("SparkSession created")

# 2. PostgreSQL 접속 정보
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "secom")
POSTGRES_USER = os.getenv("POSTGRES_USER", "secom")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "secom123")
SPARK_CHECKPOINT_LOCATION = os.getenv("SPARK_CHECKPOINT_LOCATION", "/app/checkpoint/secom-streaming")
FALLBACK_DIR = os.getenv("FALLBACK_DIR", "/data/fallback")
MODEL_PATH = os.getenv("MODEL_PATH", "/data/model.joblib")

JDBC_URL = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
JDBC_PROPERTIES = {
    "user": POSTGRES_USER,
    "password": POSTGRES_PASSWORD,
    "driver": "org.postgresql.Driver",
}

# Fallback 디렉토리 생성
os.makedirs(FALLBACK_DIR, exist_ok=True)

# 3. 모델 로드 (드라이버에서 1회)
if not os.path.exists(MODEL_PATH):
    raise FileNotFoundError(f"Model file not found: {MODEL_PATH}")

model_data = joblib.load(MODEL_PATH)
model = model_data["model"]
feature_names = model_data["feature_names"]
feature_means = model_data["feature_means"]
anomaly_threshold = float(model_data.get("anomaly_threshold", 0.0))
model_version = str(model_data.get("model_version", "iforest-v1"))

print(f"Model loaded from {MODEL_PATH}")
print(f"Feature count: {len(feature_names)} | threshold: {anomaly_threshold} | model_version: {model_version}")

# 4. 모델 기반 이상치 판정 UDF
model_result_schema = StructType([
    StructField("is_anomaly", BooleanType(), False),
    StructField("anomaly_score", FloatType(), False),
    StructField("anomaly_type", StringType(), False),
])

_model = model
_feature_names = feature_names
_feature_means = feature_means
_threshold = anomaly_threshold

@udf(returnType=model_result_schema)
def score_event_with_model(sensors):
    if sensors is None:
        return (False, 0.0, "NO_DATA")

    values = []
    for feature_name in _feature_names:
        value = sensors.get(feature_name)
        if value is None:
            value = _feature_means.get(feature_name, 0.0)
        values.append(float(value))

    # 학습 시 DataFrame(컬럼명 포함)으로 fit 했으므로 추론도 동일 포맷 사용
    X = pd.DataFrame([values], columns=_feature_names)
    score = float(_model.decision_function(X)[0])  # 낮을수록 이상
    is_anomaly = score < _threshold

    return (is_anomaly, score, "IFOREST")

# 5. DLQ Fallback 함수
def save_to_fallback(batch_df, batch_id, table_name):
    """DB 장애 시 로컬 JSON 파일로 저장 (Dead Letter Queue)"""
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    path = os.path.join(FALLBACK_DIR, f"{table_name}_batch{batch_id}_{timestamp}.json")

    try:
        # Spark DataFrame → JSON 파일 (Driver 수집)
        rows = batch_df.toJSON().collect()
        with open(path, "w") as f:
            json.dump(rows, f)
        print(f"  [DLQ] Saved {len(rows)} rows -> {path}")
    except Exception as e:
        print(f"  [DLQ] Fallback save ALSO failed: {e}")

# 6. 메시지 스키마
schema = StructType([
    StructField("event_time", StringType(), True),
    StructField("sensors", MapType(StringType(), FloatType()), True),
    StructField("pass_fail", IntegerType(), True),
])

# 7. Kafka readStream
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "secom-sensors") \
    .option("startingOffsets", "latest") \
    .load()

# 8. JSON 파싱
parsed_df = kafka_df \
    .selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), schema).alias("data")) \
    .select(
        col("data.event_time").cast("timestamp").alias("event_time"),
        col("data.sensors").alias("sensors"),
        col("data.pass_fail").alias("pass_fail"),
    )

# 9. foreachBatch 처리 (v3: try-catch + DLQ)
def process_batch(batch_df, batch_id):
    if batch_df.isEmpty():
        return

    batch_df.persist()
    count = batch_df.count()

    # Sink 1: raw_events
    raw_df = batch_df.select(
        col("event_time"),
        to_json(col("sensors")).alias("sensors"),
        col("pass_fail"),
    )

    try:
        raw_df.write \
            .option("stringtype", "unspecified") \
            .jdbc(JDBC_URL, "raw_events", mode="append", properties=JDBC_PROPERTIES)
    except Exception as e:
        print(f"Batch {batch_id}: [ERROR] raw_events write failed: {e}")
        save_to_fallback(raw_df, batch_id, "raw_events")
        batch_df.unpersist()
        return  # raw 실패하면 anomaly도 건너뜀

    # Sink 2: event_anomalies (모델 기반 이벤트 판정, 이벤트 단위 저장)
    scored_df = batch_df \
        .withColumn("model_result", score_event_with_model(col("sensors"))) \
        .select(
            col("event_time"),
            col("model_result.is_anomaly").alias("is_anomaly"),
            col("model_result.anomaly_score").alias("anomaly_score"),
            col("model_result.anomaly_type").alias("anomaly_type"),
            lit(model_version).alias("model_version"),
        )

    anomaly_count = scored_df.filter(col("is_anomaly") == True).count()
    try:
        scored_df.write \
            .jdbc(JDBC_URL, "event_anomalies", mode="append", properties=JDBC_PROPERTIES)
    except Exception as e:
        print(f"Batch {batch_id}: [ERROR] event_anomalies write failed: {e}")
        save_to_fallback(scored_df, batch_id, "event_anomalies")

    print(f"Batch {batch_id}: {count} rows scored -> {anomaly_count} anomalies")
    batch_df.unpersist()

# 10. 스트림 시작
print("Starting stream...")
query = parsed_df.writeStream \
    .foreachBatch(process_batch) \
    .option("checkpointLocation", SPARK_CHECKPOINT_LOCATION) \
    .start()

query.awaitTermination()
