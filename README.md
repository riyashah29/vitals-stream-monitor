# Vitals Stream Monitor

An end-to-end streaming pipeline for simulated patient vital-sign events. The project demonstrates ingestion, durable storage, stream processing with Apache Spark Structured Streaming, anomaly detection, and alerting using AWS messaging and notification services.

## Overview

This repository provides a HIPAA-aligned reference pipeline for ingesting encrypted wearable vital-sign data (via SQS and S3), performing near-real-time processing with Apache Spark Structured Streaming, and monitoring patient health at scale. The implementation focuses on heart rate, blood pressure, and glucose streams and combines rolling-window smoothing and aggregation with Isolation Forest models to detect clinically relevant anomalies (for example, arrhythmias or abrupt glucose excursions). When critical events are identified, the pipeline publishes caregiver notifications via AWS SNS. Longitudinal, de-identified records are persisted to Amazon Redshift with IAM-based access controls to enable secure, population-level analysis and predictive modeling.

## Key Capabilities

- Simulated vitals data generation with optional anomaly injection
- Reliable delivery via AWS SQS
- Raw and processed data persisted to S3
- Stream processing with Spark Structured Streaming (rolling-window aggregation)
- Anomaly detection using rule-based checks and machine-learning models
- Alert publishing via AWS SNS

## Architecture

1. A producer generates vitals events and sends them to an SQS queue.
2. A consumer reads messages from SQS and writes raw batches to S3 for durability.
3. A Spark Structured Streaming job reads raw records from S3, computes windowed metrics, performs anomaly detection, and writes processed results back to S3.
4. When anomalies are detected, alerts are published to an SNS topic.

## Repository Layout

- `ingestion/sqs_producer.py` — generates simulated vitals and publishes to SQS
- `ingestion/sqs_consumer.py` — consumes from SQS and writes raw batches to S3
- `streaming/spark_streaming_job.py` — Spark streaming pipeline for aggregation and anomaly detection
- `streaming/anomaly_detection.py` — anomaly detection utilities
- `streaming/alert_publisher.py` — SNS alert publishing helpers
- `streaming/transformations.py` — Spark transformation helpers
- `storage/` — storage-related utilities and helpers
- `tests/` — unit and integration tests
- `requirements.txt` — Python package dependencies

## Prerequisites

- Python 3.10 or newer
- Java (required by Spark) — compatible JDK for your Spark distribution
- Apache Spark (local or cluster) configured for Structured Streaming
- AWS account with permissions for SQS, S3, and SNS
- AWS CLI configured or environment variables with AWS credentials

## Setup

1. Create and activate a virtual environment:

```powershell
python -m venv .venv
.venv\Scripts\activate
```

2. Install Python dependencies:

```powershell
pip install -r requirements.txt
```

## Configuration

Create a `.env` file in the project root with the following variables (example values shown):

```env
AWS_REGION=us-east-1
SQS_QUEUE_URL=https://sqs.us-east-1.amazonaws.com/123456789012/vitals-queue
S3_RAW_BUCKET=vitals-stream-raw
S3_PROCESSED_BUCKET=vitals-stream-processed
SNS_TOPIC_ARN=arn:aws:sns:us-east-1:123456789012:vitals-alerts

# Spark streaming parameters
WATERMARK_DELAY=10 minutes
WINDOW_DURATION=5 minutes
SLIDE_DURATION=1 minute

# Simulation parameters (optional)
HEART_RATE_MIN=50
HEART_RATE_MAX=120
GLUCOSE_MIN=70
GLUCOSE_MAX=180
BP_SYSTOLIC_MAX=140
BP_DIASTOLIC_MAX=90
SPO2_MIN=92
```

Adjust values according to your environment and operational requirements.

## Running the components

- Producer (generate and send vitals):

```powershell
python ingestion/sqs_producer.py
```

- Consumer (read SQS and write raw batches to S3):

```powershell
python ingestion/sqs_consumer.py
```

- Spark streaming job (process data, detect anomalies, publish alerts):

```powershell
spark-submit --packages <required-packages> streaming/spark_streaming_job.py
```

Replace `<required-packages>` with any Spark connector or library packages you use (for example, the AWS SDK connector for S3).



