from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, BooleanType

from transformations    import add_rolling_stats
from anomaly_detection  import get_anomaly_udf
from alert_publisher    import publish_alert
from storage.s3_writer  import write_to_s3
from storage.redshift_loader import write_to_postgres

KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC  = "vitals-stream"
S3_OUTPUT    = "s3a://vitals-stream-monitor/processed/"

# ── Schema ──────────────────────────────────────────────────────────────────

VITALS_SCHEMA = StructType([
    StructField("event_id",     StringType(),  True),
    StructField("patient_id",   StringType(),  True),
    StructField("timestamp",    StringType(),  True),
    StructField("heart_rate",   DoubleType(),  True),
    StructField("systolic_bp",  DoubleType(),  True),
    StructField("diastolic_bp", DoubleType(),  True),
    StructField("glucose_mg_dl",DoubleType(),  True),
    StructField("device_id",    StringType(),  True),
])

# ── Spark Session ────────────────────────────────────────────────────────────

spark = (SparkSession.builder
    .appName("PatientVitalsMonitor")
    .config("spark.sql.shuffle.partitions", "4")   # keep small for local mode
    .getOrCreate())

spark.sparkContext.setLogLevel("WARN")

# ── Read Stream from Kafka ───────────────────────────────────────────────────

raw_stream = (spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BROKER)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "latest")
    .load())

# Deserialize JSON payload
parsed = (raw_stream
    .selectExpr("CAST(value AS STRING) as json_str", "timestamp as kafka_ts")
    .select(F.from_json("json_str", VITALS_SCHEMA).alias("data"), "kafka_ts")
    .select("data.*", "kafka_ts")
    .withColumn("event_time", F.to_timestamp("timestamp")))

# ── Transformations & Rolling Stats ─────────────────────────────────────────

enriched = add_rolling_stats(parsed)   # adds rolling_mean_hr, rolling_std_hr, etc.

# ── Anomaly Detection UDF ────────────────────────────────────────────────────

anomaly_udf = get_anomaly_udf()        # loads trained Isolation Forest model

scored = enriched.withColumn(
    "anomaly_score",
    anomaly_udf(
        F.col("heart_rate"),
        F.col("systolic_bp"),
        F.col("glucose_mg_dl")
    )
).withColumn(
    "is_anomaly",
    F.col("anomaly_score") < -0.1      # threshold tuned on training data
)

# ── foreachBatch: alerts + storage ──────────────────────────────────────────

def process_batch(batch_df, batch_id):
    if batch_df.isEmpty():
        return

    print(f"\n[Batch {batch_id}] Processing {batch_df.count()} records ...")

    # 1. Send SNS alerts for anomalies
    anomalies = batch_df.filter(F.col("is_anomaly") == True)
    for row in anomalies.collect():
        publish_alert(row)

    # 2. Write all records to S3 as parquet
    write_to_s3(batch_df, S3_OUTPUT)

    # 3. Write to PostgreSQL (replaces Redshift in dev)
    write_to_postgres(batch_df)

query = (scored.writeStream
    .foreachBatch(process_batch)
    .option("checkpointLocation", "/tmp/vitals-checkpoint")
    .trigger(processingTime="10 seconds")
    .start())

query.awaitTermination()