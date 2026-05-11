import boto3
import json
import time
import os
from datetime import datetime
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))

SQS_QUEUE_URL = os.getenv("SQS_QUEUE_URL")
S3_RAW_BUCKET = os.getenv("S3_RAW_BUCKET")
AWS_REGION    = os.getenv("AWS_REGION")

sqs = boto3.client("sqs", region_name=AWS_REGION)
s3  = boto3.client("s3",  region_name=AWS_REGION)

def poll_and_save():
    print("SQS Consumer running... Press Ctrl+C to stop.\n")
    while True:
        all_records = []

        for _ in range(10):
            response = sqs.receive_message(
                QueueUrl=SQS_QUEUE_URL,
                MaxNumberOfMessages=10,
                WaitTimeSeconds=2
            )
            messages = response.get("Messages", [])
            if not messages:
                break
            for msg in messages:
                all_records.append(json.loads(msg["Body"]))
                sqs.delete_message(
                    QueueUrl=SQS_QUEUE_URL,
                    ReceiptHandle=msg["ReceiptHandle"]
                )

        if all_records:
            key = f"vitals/batch-{datetime.now().strftime('%Y%m%d-%H%M%S')}.json"
            body = "\n".join(json.dumps(record) for record in all_records) + "\n"
            s3.put_object(
                Bucket=S3_RAW_BUCKET,
                Key=key,
                Body=body.encode("utf-8")
            )
            print(f"Saved {len(all_records)} records to s3a://{S3_RAW_BUCKET}/{key}")
        else:
            print(f"No messages yet... waiting")

        time.sleep(10)

if __name__ == "__main__":
    poll_and_save()