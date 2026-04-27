import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_json, udf, explode
from pyspark.sql.types import (
    StringType, IntegerType, FloatType,
    StructType, StructField, MapType, ArrayType
)

# ============================================================
# SECOM Spark Streaming (v2 — UDF 분산 처리)
# Kafka -> raw_events + anomalies (PostgreSQL)
# collect() 제거 → Executor 단에서 분산 이상치 탐지
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

JDBC_URL = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
JDBC_PROPERTIES = {
    "user": POSTGRES_USER,
    "password": POSTGRES_PASSWORD,
    "driver": "org.postgresql.Driver",
}

# 3. 이상치 탐지용 통계값
SENSOR_STATS = {
    "0": {"mean": 3045.2, "std": 152.7},
    "1": {"mean": 2443.8, "std": 345.2},
    "2": {"mean": 2198.5, "std": 120.3},
    "5": {"mean": 99.8, "std": 2.1},
    "10": {"mean": 0.012, "std": 0.005},
}
SIGMA_THRESHOLD = 3
NAN_THRESHOLD = 100

# 4. 이상치 탐지 UDF (Executor에서 분산 실행)
anomaly_schema = ArrayType(StructType([
    StructField("sensor_id", StringType(), False),
    StructField("sensor_value", FloatType(), False),
    StructField("z_score", FloatType(), False),
    StructField("anomaly_type", StringType(), False),
    StructField("threshold_upper", FloatType(), False),
    StructField("threshold_lower", FloatType(), False),
]))

# closure로 통계값 캡처 → 각 Executor에 자동 전달
_stats = SENSOR_STATS
_sigma = SIGMA_THRESHOLD
_nan_threshold = NAN_THRESHOLD

@udf(returnType=anomaly_schema)
def detect_anomalies(sensors):
    if sensors is None:
        return []

    anomalies = []

    # NaN 체크
    nan_count = sum(1 for v in sensors.values() if v is None)
    if nan_count > _nan_threshold:
        anomalies.append(("ALL", 0.0, float(nan_count), "SENSOR_FAULT", 0.0, 0.0))
        return anomalies

    # 3시그마 체크
    for sensor_id, stat in _stats.items():
        value = sensors.get(sensor_id)
        if value is None:
            continue
        z_score = abs(value - stat["mean"]) / stat["std"]
        if z_score > _sigma:
            upper = stat["mean"] + _sigma * stat["std"]
            lower = stat["mean"] - _sigma * stat["std"]
            anomalies.append((
                sensor_id, float(value), round(z_score, 4),
                "3SIGMA_VIOLATION", round(upper, 4), round(lower, 4),
            ))

    return anomalies

# 5. 메시지 스키마
schema = StructType([
    StructField("event_time", StringType(), True),
    StructField("sensors", MapType(StringType(), FloatType()), True),
    StructField("pass_fail", IntegerType(), True),
])

# 6. Kafka readStream
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "secom-sensors") \
    .option("startingOffsets", "latest") \
    .load()

# 7. JSON 파싱
parsed_df = kafka_df \
    .selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), schema).alias("data")) \
    .select(
        col("data.event_time").cast("timestamp").alias("event_time"),
        col("data.sensors").alias("sensors"),
        col("data.pass_fail").alias("pass_fail"),
    )

# 8. foreachBatch 처리 (collect 없는 v2)
def process_batch(batch_df, batch_id):
    if batch_df.isEmpty():
        return

    batch_df.persist()
    count = batch_df.count()

    # Sink 1: raw_events (전부 저장)
    raw_df = batch_df.select(
        col("event_time"),
        to_json(col("sensors")).alias("sensors"),
        col("pass_fail"),
    )
    raw_df.write \
        .option("stringtype", "unspecified") \
        .jdbc(JDBC_URL, "raw_events", mode="append", properties=JDBC_PROPERTIES)

    # Sink 2: anomalies (UDF로 분산 처리 — collect 없음)
    anomaly_df = batch_df \
        .withColumn("anomaly_list", detect_anomalies(col("sensors"))) \
        .select(
            col("event_time"),
            explode(col("anomaly_list")).alias("anomaly")
        ) \
        .select(
            col("event_time"),
            col("anomaly.sensor_id"),
            col("anomaly.sensor_value"),
            col("anomaly.z_score"),
            col("anomaly.anomaly_type"),
            col("anomaly.threshold_upper"),
            col("anomaly.threshold_lower"),
        )

    anomaly_count = anomaly_df.count()
    if anomaly_count > 0:
        anomaly_df.write \
            .jdbc(JDBC_URL, "anomalies", mode="append", properties=JDBC_PROPERTIES)

    print(f"Batch {batch_id}: {count} rows -> {anomaly_count} anomalies")
    batch_df.unpersist()

# 9. 스트림 시작
print("Starting stream...")
query = parsed_df.writeStream \
    .foreachBatch(process_batch) \
    .option("checkpointLocation", SPARK_CHECKPOINT_LOCATION) \
    .start()

query.awaitTermination()
