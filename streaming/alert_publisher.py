import boto3
import os
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))

SNS_TOPIC_ARN = os.getenv("SNS_TOPIC_ARN")
AWS_REGION    = os.getenv("AWS_REGION")

def publish_alert(patient_id, anomaly_type, vitals_dict):
    """
    Standalone alert publisher — can be imported and called
    from anywhere in the project.
    """
    sns = boto3.client("sns", region_name=AWS_REGION)

    hr      = round(vitals_dict.get("avg_hr") or 0, 1)
    glucose = round(vitals_dict.get("avg_glucose") or 0, 1)
    bp_sys  = round(vitals_dict.get("avg_bp_systolic") or 0, 1)
    bp_dia  = round(vitals_dict.get("avg_bp_diastolic") or 0, 1)
    spo2    = round(vitals_dict.get("avg_spo2") or 0, 1)

    message = f"""
PATIENT ALERT — {anomaly_type}
Patient  : {patient_id}
HR       : {hr} bpm
BP       : {bp_sys}/{bp_dia} mmHg
Glucose  : {glucose} mg/dL
SpO2     : {spo2}%
Please review patient immediately.
""".strip()

    sns.publish(
        TopicArn=SNS_TOPIC_ARN,
        Message=message,
        Subject=f"ALERT: {anomaly_type} — {patient_id}"
    )
    print(f"Alert published for {patient_id}: {anomaly_type}")