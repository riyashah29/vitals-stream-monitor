import boto3
import json
import time
import random
import os
from datetime import datetime, timezone
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))

SQS_QUEUE_URL = os.getenv("SQS_QUEUE_URL")
AWS_REGION    = os.getenv("AWS_REGION")

sqs = boto3.client("sqs", region_name=AWS_REGION)

PATIENTS = [
    {"patient_id": f"P{str(i).zfill(4)}",
     "age_group": random.choice(["adult", "elderly", "pediatric"])}
    for i in range(1, 11)
]

def generate_vitals(patient, anomalous=False):
    vitals = {
        "patient_id":   patient["patient_id"],
        "age_group":    patient["age_group"],
        "heart_rate":   round(random.uniform(60, 100), 1),
        "bp_systolic":  round(random.uniform(110, 130), 1),
        "bp_diastolic": round(random.uniform(70, 85), 1),
        "glucose":      round(random.uniform(80, 140), 1),
        "spo2":         round(random.uniform(95, 100), 1),
        "timestamp":    datetime.now(timezone.utc).isoformat(),
        "device_id":    f"DEV-{patient['patient_id']}",
        "anomaly_type": None
    }
    if anomalous:
        anomaly = random.choice(["heart", "glucose", "bp"])
        if anomaly == "heart":
            vitals["heart_rate"]   = round(random.uniform(160, 220), 1)
            vitals["anomaly_type"] = "Arrhythmia"
        elif anomaly == "glucose":
            vitals["glucose"]      = round(random.uniform(320, 500), 1)
            vitals["anomaly_type"] = "Glucose Spike"
        else:
            vitals["bp_systolic"]  = round(random.uniform(185, 220), 1)
            vitals["anomaly_type"] = "Hypertensive Crisis"
    return vitals

def main():
    print(f"Sending vitals to SQS queue...")
    print("Press Ctrl+C to stop.\n")
    count = 0
    while True:
        for patient in PATIENTS:
            anomalous = random.random() < 0.05
            vitals    = generate_vitals(patient, anomalous)
            sqs.send_message(
                QueueUrl=SQS_QUEUE_URL,
                MessageBody=json.dumps(vitals)
            )
            count += 1
            if anomalous:
                print(f"ANOMALY sent: {vitals['anomaly_type']} for {patient['patient_id']}")
            if count % 50 == 0:
                print(f"Sent {count} records total")
        time.sleep(5)

if __name__ == "__main__":
    main()