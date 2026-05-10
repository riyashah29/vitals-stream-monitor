# Vitals Stream Monitor

Vitals Stream Monitor is a small end-to-end streaming project for simulated patient vitals. It generates vitals events, moves them through AWS SQS and S3, then runs a Spark Structured Streaming job to aggregate readings, detect anomalies, and publish alerts through SNS.

## What It Does

- Simulates patient vitals with a small chance of injected anomalies.
- Sends events to an AWS SQS queue.
- Pulls messages from SQS and writes raw batches to S3.
- Reads raw data from S3 with Spark Structured Streaming.
- Computes rolling window metrics.
- Flags anomalies with rule-based checks and Isolation Forest.
- Publishes alerts to AWS SNS.
- Writes processed output back to S3.

## Project Structure

- `ingestion/sqs_producer.py` - generates vitals and sends them to SQS.
- `ingestion/sqs_consumer.py` - reads SQS messages and saves raw batches to S3.
- `streaming/spark_streaming_job.py` - Spark streaming pipeline for aggregation, anomaly detection, and alerting.
- `streaming/anomaly_detection.py` - reusable anomaly detection helpers.
- `streaming/alert_publisher.py` - reusable SNS alert publisher.
- `streaming/transformations.py` - reusable Spark transformation helpers.
- `storage/` - storage-related utilities.
- `tests/` - test files.
- `requirements.txt` - Python dependencies.

## Requirements

- Python 3.10 or newer.
- AWS account and credentials with access to SQS, S3, and SNS.
- A configured SQS queue.
- An SNS topic for alerts.
- A local `.env` file in the project root.

## Install Dependencies

Create and activate a virtual environment, then install the project dependencies:

```bash
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
```

## Environment Variables

Create a `.env` file in the project root with values similar to the following:

```env
AWS_REGION=us-east-1
SQS_QUEUE_URL=https://sqs.us-east-1.amazonaws.com/123456789012/vitals-queue
S3_RAW_BUCKET=vitals-stream-raw
S3_PROCESSED_BUCKET=vitals-stream-processed
SNS_TOPIC_ARN=arn:aws:sns:us-east-1:123456789012:vitals-alerts

WATERMARK_DELAY=10 minutes
WINDOW_DURATION=5 minutes
SLIDE_DURATION=1 minute

HEART_RATE_MIN=50
HEART_RATE_MAX=120
GLUCOSE_MIN=70
GLUCOSE_MAX=180
BP_SYSTOLIC_MAX=140
BP_DIASTOLIC_MAX=90
SPO2_MIN=92
```
