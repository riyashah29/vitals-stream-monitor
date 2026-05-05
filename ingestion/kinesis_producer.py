import json
import time
import random
import uuid
from datetime import datetime
from kafka import KafkaProducer

KAFKA_BROKER = "localhost:9092"
TOPIC_NAME   = "vitals-stream"

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

PATIENTS = [f"P{str(i).zfill(3)}" for i in range(1, 6)]  # P001 to P005

def generate_vitals(patient_id: str) -> dict:
    """
    Simulates realistic wearable vitals.
    Occasionally injects anomalies to trigger alerts.
    """
    is_anomaly = random.random() < 0.05  # 5% chance of anomaly

    heart_rate   = random.randint(180, 220) if is_anomaly else random.randint(60, 100)
    systolic_bp  = random.randint(170, 200) if is_anomaly else random.randint(110, 130)
    diastolic_bp = random.randint(100, 120) if is_anomaly else random.randint(70,  85)
    glucose      = random.randint(300, 450) if is_anomaly else random.randint(80, 140)

    return {
        "event_id"    : str(uuid.uuid4()),
        "patient_id"  : patient_id,
        "timestamp"   : datetime.utcnow().isoformat(),
        "heart_rate"  : heart_rate,
        "systolic_bp" : systolic_bp,
        "diastolic_bp": diastolic_bp,
        "glucose_mg_dl": glucose,
        "device_id"   : f"DEV-{patient_id}",
        "is_anomaly"  : is_anomaly   # label for testing only, not used by model
    }

def stream_vitals(interval_sec: float = 1.0):
    print(f"[Producer] Streaming vitals to topic '{TOPIC_NAME}' ...")
    try:
        while True:
            for patient_id in PATIENTS:
                record = generate_vitals(patient_id)
                producer.send(TOPIC_NAME, value=record, key=patient_id.encode())
                print(f"  → Sent: {record['patient_id']} | "
                      f"HR={record['heart_rate']} | "
                      f"Glucose={record['glucose_mg_dl']} | "
                      f"Anomaly={record['is_anomaly']}")
            producer.flush()
            time.sleep(interval_sec)
    except KeyboardInterrupt:
        print("\n[Producer] Stopped.")
    finally:
        producer.close()

if __name__ == "__main__":
    stream_vitals(interval_sec=1.0)