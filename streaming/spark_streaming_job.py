import os
import sys
import json
import boto3
import pandas as pd
import io
from datetime import datetime
from dotenv import load_dotenv

import sys
sys.stdout.reconfigure(encoding='utf-8')

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, avg, window, to_timestamp,
    count, max as spark_max
)
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, BooleanType
)

# ─────────────────────────────────────────────────────────────────────────────
# LOAD ENVIRONMENT VARIABLES
# ─────────────────────────────────────────────────────────────────────────────
# Looks for .env in the project root (one level up from /streaming)

load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))

AWS_REGION          = os.getenv("AWS_REGION")
S3_RAW_BUCKET       = os.getenv("S3_RAW_BUCKET")
S3_PROCESSED_BUCKET = os.getenv("S3_PROCESSED_BUCKET")
SNS_TOPIC_ARN       = os.getenv("SNS_TOPIC_ARN")
WATERMARK_DELAY     = os.getenv("WATERMARK_DELAY")
WINDOW_DURATION     = os.getenv("WINDOW_DURATION")
SLIDE_DURATION      = os.getenv("SLIDE_DURATION")

HEART_RATE_MIN      = float(os.getenv("HEART_RATE_MIN"))
HEART_RATE_MAX      = float(os.getenv("HEART_RATE_MAX"))
GLUCOSE_MIN         = float(os.getenv("GLUCOSE_MIN"))
GLUCOSE_MAX         = float(os.getenv("GLUCOSE_MAX"))
BP_SYSTOLIC_MAX     = float(os.getenv("BP_SYSTOLIC_MAX"))
BP_DIASTOLIC_MAX    = float(os.getenv("BP_DIASTOLIC_MAX"))
SPO2_MIN            = float(os.getenv("SPO2_MIN"))

S3_INPUT_PATH   = f"s3a://{S3_RAW_BUCKET}/vitals/"
S3_OUTPUT_PATH  = f"s3a://{S3_PROCESSED_BUCKET}/processed/"
CHECKPOINT_PATH = f"s3a://{S3_PROCESSED_BUCKET}/checkpoints/spark/"

# ─────────────────────────────────────────────────────────────────────────────
# STEP 1 - CREATE SPARK SESSION
# ─────────────────────────────────────────────────────────────────────────────

print("=" * 60)
print("  Patient Vitals Monitoring - Spark Streaming Job")
print("=" * 60)

spark = SparkSession.builder \
    .appName("PatientVitalsMonitoring") \
    .config("spark.sql.shuffle.partitions", "4") \
    .config("spark.streaming.stopGracefullyOnShutdown", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
    .config("spark.hadoop.fs.s3a.connection.timeout", "60000") \
    .config("spark.hadoop.fs.s3a.socket.timeout", "60000") \
    .config("spark.local.dir", "C:/tmp/spark") \
    .config("spark.hadoop.hadoop.tmp.dir", "C:/tmp/hadoop") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
print("Spark session created")

# ─────────────────────────────────────────────────────────────────────────────
# STEP 2 — DEFINE SCHEMA
# ─────────────────────────────────────────────────────────────────────────────

vitals_schema = StructType([
    StructField("patient_id",    StringType(),  nullable=False),
    StructField("age_group",     StringType(),  nullable=True),
    StructField("heart_rate",    DoubleType(),  nullable=True),
    StructField("bp_systolic",   DoubleType(),  nullable=True),
    StructField("bp_diastolic",  DoubleType(),  nullable=True),
    StructField("glucose",       DoubleType(),  nullable=True),
    StructField("spo2",          DoubleType(),  nullable=True),
    StructField("timestamp",     StringType(),  nullable=False),
    StructField("device_id",     StringType(),  nullable=True),
    StructField("anomaly_type",  StringType(),  nullable=True),
])

print("Schema defined")

# ─────────────────────────────────────────────────────────────────────────────
# STEP 3 — READ STREAM FROM S3
# ─────────────────────────────────────────────────────────────────────────────

print(f"Connecting to S3 input path: {S3_INPUT_PATH}")

raw_stream = spark.readStream \
    .format("json") \
    .schema(vitals_schema) \
    .option("path", S3_INPUT_PATH) \
    .option("recursiveFileLookup", "true") \
    .option("maxFilesPerTrigger", 10) \
    .load()

parsed_stream = raw_stream \
    .withColumn("event_time", to_timestamp(col("timestamp"))) \
    .filter(col("patient_id").isNotNull()) \
    .filter(col("event_time").isNotNull())

print("Stream reader configured")

# ─────────────────────────────────────────────────────────────────────────────
# STEP 4 — WATERMARKING
# ─────────────────────────────────────────────────────────────────────────────

watermarked = parsed_stream \
    .withWatermark("event_time", WATERMARK_DELAY)

print("Watermark applied")

# ─────────────────────────────────────────────────────────────────────────────
# STEP 5 — ROLLING WINDOW AGGREGATION
# ─────────────────────────────────────────────────────────────────────────────

aggregated = watermarked \
    .groupBy(
        window(col("event_time"), WINDOW_DURATION, SLIDE_DURATION),
        col("patient_id"),
        col("age_group")
    ) \
    .agg(
        avg("heart_rate").alias("avg_hr"),
        avg("bp_systolic").alias("avg_bp_systolic"),
        avg("bp_diastolic").alias("avg_bp_diastolic"),
        avg("glucose").alias("avg_glucose"),
        avg("spo2").alias("avg_spo2"),
        count("*").alias("reading_count"),
        spark_max("heart_rate").alias("max_hr"),
        spark_max("glucose").alias("max_glucose"),
        spark_max("bp_systolic").alias("max_bp_systolic")
    ) \
    .select(
        col("patient_id"),
        col("age_group"),
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("avg_hr"),
        col("avg_bp_systolic"),
        col("avg_bp_diastolic"),
        col("avg_glucose"),
        col("avg_spo2"),
        col("reading_count"),
        col("max_hr"),
        col("max_glucose"),
        col("max_bp_systolic")
    )

print("Window aggregation configured")

# ─────────────────────────────────────────────────────────────────────────────
# ANOMALY DETECTION FUNCTIONS
# ─────────────────────────────────────────────────────────────────────────────

def apply_rule_based_detection(row):
    """
    Fast threshold checks run before ML.
    Catches obvious emergencies immediately.
    Returns anomaly type string if detected, else None.
    """
    hr      = row.get("avg_hr") or 0
    glucose = row.get("avg_glucose") or 0
    bp_sys  = row.get("avg_bp_systolic") or 0
    bp_dia  = row.get("avg_bp_diastolic") or 0
    spo2    = row.get("avg_spo2") or 100

    if hr > HEART_RATE_MAX:
        return f"Tachycardia (HR={round(hr,1)} bpm)"
    if hr < HEART_RATE_MIN:
        return f"Bradycardia (HR={round(hr,1)} bpm)"
    if glucose > GLUCOSE_MAX:
        return f"Hyperglycemia (Glucose={round(glucose,1)} mg/dL)"
    if glucose < GLUCOSE_MIN:
        return f"Hypoglycemia (Glucose={round(glucose,1)} mg/dL)"
    if bp_sys > BP_SYSTOLIC_MAX:
        return f"Hypertensive Crisis (BP={round(bp_sys,1)}/{round(bp_dia,1)} mmHg)"
    if spo2 < SPO2_MIN:
        return f"Hypoxia (SpO2={round(spo2,1)}%)"

    return None


def apply_isolation_forest(pdf):
    """
    Isolation Forest ML model to detect unusual combinations of vitals.
    Catches multivariate patterns that simple threshold rules miss.
    Returns DataFrame with 'is_ml_anomaly' and 'anomaly_score' columns added.
    """
    from sklearn.ensemble import IsolationForest
    from sklearn.preprocessing import StandardScaler

    FEATURES = ["avg_hr", "avg_glucose", "avg_bp_systolic", "avg_bp_diastolic", "avg_spo2"]

    if len(pdf) < 5:
        pdf["is_ml_anomaly"] = False
        pdf["anomaly_score"]  = 0.0
        return pdf

    feature_data = pdf[FEATURES].dropna()

    if len(feature_data) < 5:
        pdf["is_ml_anomaly"] = False
        pdf["anomaly_score"]  = 0.0
        return pdf

    scaler = StandardScaler()
    scaled = scaler.fit_transform(feature_data)

    model = IsolationForest(
        n_estimators=100,
        contamination=0.05,
        random_state=42,
        n_jobs=-1
    )
    preds  = model.fit_predict(scaled)
    scores = model.score_samples(scaled)

    pdf["is_ml_anomaly"] = pd.Series(preds == -1, index=feature_data.index) \
                             .reindex(pdf.index, fill_value=False)
    pdf["anomaly_score"]  = pd.Series(scores, index=feature_data.index) \
                             .reindex(pdf.index, fill_value=0.0)
    return pdf

# SNS ALERT FUNCTION

def send_sns_alert(row_dict):
    """
    Formats and sends a caregiver alert via AWS SNS.
    """
    sns = boto3.client("sns", region_name=AWS_REGION)

    patient_id    = row_dict.get("patient_id", "Unknown")
    anomaly_type  = row_dict.get("final_anomaly_type", "Abnormal Vitals")
    avg_hr        = round(row_dict.get("avg_hr") or 0, 1)
    avg_glucose   = round(row_dict.get("avg_glucose") or 0, 1)
    avg_bp_sys    = round(row_dict.get("avg_bp_systolic") or 0, 1)
    avg_bp_dia    = round(row_dict.get("avg_bp_diastolic") or 0, 1)
    avg_spo2      = round(row_dict.get("avg_spo2") or 0, 1)
    window_start  = row_dict.get("window_start", "")
    reading_count = row_dict.get("reading_count", 0)

    message = f"""
CRITICAL PATIENT VITALS ALERT
---------------------------------------
Patient ID    : {patient_id}
Alert Type    : {anomaly_type}
Window        : {window_start}
Readings Used : {reading_count} readings averaged

AVERAGED VITALS (5-min window):
  Heart Rate     : {avg_hr} bpm
  Blood Pressure : {avg_bp_sys}/{avg_bp_dia} mmHg
  Glucose        : {avg_glucose} mg/dL
  SpO2           : {avg_spo2}%

Please review patient immediately.
---------------------------------------
Automated alert - Patient Vitals Monitor
""".strip()

    try:
        response = sns.publish(
            TopicArn=SNS_TOPIC_ARN,
            Message=message,
            Subject=f"ALERT: {anomaly_type} — Patient {patient_id}"
        )
        print(f"  Alert sent for {patient_id} | MessageId: {response['MessageId']}")
    except Exception as e:
        print(f"  SNS alert failed for {patient_id}: {e}")

# ─────────────────────────────────────────────────────────────────────────────
# STEP 6 - PROCESS EACH MICRO-BATCH
# ─────────────────────────────────────────────────────────────────────────────

def process_batch(batch_df, batch_id):
    print(f"\n{'-'*50}")
    print(f"  Batch {batch_id} | {datetime.now().strftime('%H:%M:%S')}")
    print(f"{'-'*50}")

    pdf = batch_df.toPandas()
    print(f"  Records: {len(pdf)}")

    if pdf.empty:
        print("  Empty batch - skipping.")
        return

    # Rule-based detection — returns None or a string like "Tachycardia..."
    pdf["rule_anomaly"] = pdf.apply(
        lambda row: apply_rule_based_detection(row.to_dict()), axis=1
    )
    print(f"  Rule anomalies : {pdf['rule_anomaly'].notna().sum()}")

    # ML-based detection
    pdf = apply_isolation_forest(pdf)
    print(f"  ML anomalies   : {pdf['is_ml_anomaly'].sum()}")

    # Combine — MUST happen before fillna so None is still None here
    pdf["is_anomaly"] = pdf["rule_anomaly"].notna() | pdf["is_ml_anomaly"]

    # Build final label — use pd.notna() since rule_anomaly is still None/string here
    pdf["final_anomaly_type"] = pdf.apply(
        lambda row: row["rule_anomaly"] if pd.notna(row["rule_anomaly"])
        else ("ML-Detected Pattern Anomaly" if row["is_ml_anomaly"] else ""),
        axis=1
    )

    # NOW convert None to "" for Parquet compatibility — must be last
    pdf["rule_anomaly"] = pdf["rule_anomaly"].fillna("").astype(str)

    print(f"  Total anomalies: {pdf['is_anomaly'].sum()}")

    # Send alerts only for anomalies
    for _, row in pdf[pdf["is_anomaly"]].iterrows():
        send_sns_alert(row.to_dict())

    # Save to S3
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq

        pdf["processed_at"] = datetime.now().isoformat()
        pdf["batch_id"]     = batch_id
        pdf["window_start"] = pdf["window_start"].astype(str)
        pdf["window_end"]   = pdf["window_end"].astype(str)

        s3_client = boto3.client("s3", region_name=AWS_REGION)
        buffer    = io.BytesIO()
        pq.write_table(pa.Table.from_pandas(pdf), buffer)
        buffer.seek(0)

        s3_key = f"processed/{datetime.now().strftime('%Y/%m/%d')}/batch-{batch_id}.parquet"
        s3_client.put_object(
            Bucket=S3_PROCESSED_BUCKET,
            Key=s3_key,
            Body=buffer.getvalue()
        )
        print(f"  Saved to s3://{S3_PROCESSED_BUCKET}/{s3_key}")
    except Exception as e:
        print(f"  S3 write error: {e}")

    print(f"{'-'*50}\n")

# ─────────────────────────────────────────────────────────────────────────────
# STEP 7 — START THE STREAMING QUERY
# ─────────────────────────────────────────────────────────────────────────────

print("\nStarting streaming query...")
print(f"  Reading from : {S3_INPUT_PATH}")
print(f"  Writing to   : {S3_OUTPUT_PATH}")
print(f"  Trigger      : every 30 seconds")
print("\nWaiting for data... (Press Ctrl+C to stop)\n")

query = aggregated.writeStream \
    .trigger(processingTime="30 seconds") \
    .outputMode("update") \
    .foreachBatch(process_batch) \
    .option("checkpointLocation", CHECKPOINT_PATH) \
    .start()

# ─────────────────────────────────────────────────────────────────────────────
# STEP 8 — KEEP JOB RUNNING
# ─────────────────────────────────────────────────────────────────────────────

try:
    query.awaitTermination()
except KeyboardInterrupt:
    print("\nStopping streaming job...")
    query.stop()
    spark.stop()
    print("Done.")