import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_json
from pyspark.sql.types import StringType, IntegerType, FloatType, StructType, StructField, MapType

spark = SparkSession.builder \
    .appName("SECOM-Streaming") \
    .config("spark.sql.shuffle.partitions", "3") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
print("SparkSession created")

JDBC_URL = "jdbc:postgresql://postgres:5432/secom"
JDBC_PROPERTIES = {
    "user": "secom",
    "password": "secom123",
    "driver": "org.postgresql.Driver",
}

SENSOR_STATS = {
    "0": {"mean": 3045.2, "std": 152.7},
    "1": {"mean": 2443.8, "std": 345.2},
    "2": {"mean": 2198.5, "std": 120.3},
    "5": {"mean": 99.8, "std": 2.1},
    "10": {"mean": 0.012, "std": 0.005},
}
SIGMA_THRESHOLD = 3
NAN_THRESHOLD = 100

broadcast_stats = spark.sparkContext.broadcast(SENSOR_STATS)

schema = StructType([
    StructField("event_time", StringType(), True),
    StructField("sensors", MapType(StringType(), FloatType()), True),
    StructField("pass_fail", IntegerType(), True),
])

kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "secom-sensors") \
    .option("startingOffsets", "latest") \
    .load()

parsed_df = kafka_df \
    .selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), schema).alias("data")) \
    .select(
        col("data.event_time").cast("timestamp").alias("event_time"),
        col("data.sensors").alias("sensors"),
        col("data.pass_fail").alias("pass_fail"),
    )

def process_batch(batch_df, batch_id):
    if batch_df.isEmpty():
        return

    batch_df.persist()
    count = batch_df.count()

    raw_df = batch_df.select(
        col("event_time"),
        to_json(col("sensors")).alias("sensors"),
        col("pass_fail"),
    )
    raw_df.write \
        .option("stringtype", "unspecified") \
        .jdbc(JDBC_URL, "raw_events", mode="append", properties=JDBC_PROPERTIES)

    stats = broadcast_stats.value
    rows = batch_df.collect()
    anomalies = []

    for row in rows:
        sensors = row["sensors"]
        event_time = row["event_time"]
        if sensors is None:
            continue

        nan_count = sum(1 for v in sensors.values() if v is None)
        if nan_count > NAN_THRESHOLD:
            anomalies.append((
                event_time, "ALL", 0.0, float(nan_count),
                "SENSOR_FAULT", 0.0, 0.0,
            ))
            continue

        for sensor_id, stat in stats.items():
            value = sensors.get(sensor_id)
            if value is None:
                continue
            z_score = abs(value - stat["mean"]) / stat["std"]
            if z_score > SIGMA_THRESHOLD:
                upper = stat["mean"] + SIGMA_THRESHOLD * stat["std"]
                lower = stat["mean"] - SIGMA_THRESHOLD * stat["std"]
                anomalies.append((
                    event_time, sensor_id, float(value), round(z_score, 4),
                    "3SIGMA_VIOLATION", round(upper, 4), round(lower, 4),
                ))

    if anomalies:
        anomaly_df = spark.createDataFrame(
            anomalies,
            ["event_time", "sensor_id", "sensor_value", "z_score",
             "anomaly_type", "threshold_upper", "threshold_lower"]
        )
        anomaly_df.write \
            .jdbc(JDBC_URL, "anomalies", mode="append", properties=JDBC_PROPERTIES)
        print(f"Batch {batch_id}: {count} rows -> {len(anomalies)} anomalies")
    else:
        print(f"Batch {batch_id}: {count} rows -> 0 anomalies")

    batch_df.unpersist()

print("Starting stream...")
query = parsed_df.writeStream \
    .foreachBatch(process_batch) \
    .option("checkpointLocation", "/tmp/checkpoint/secom-streaming") \
    .start()

query.awaitTermination()
