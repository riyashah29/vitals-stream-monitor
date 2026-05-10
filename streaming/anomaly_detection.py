import os
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))

HEART_RATE_MIN  = float(os.getenv("HEART_RATE_MIN"))
HEART_RATE_MAX  = float(os.getenv("HEART_RATE_MAX"))
GLUCOSE_MIN     = float(os.getenv("GLUCOSE_MIN"))
GLUCOSE_MAX     = float(os.getenv("GLUCOSE_MAX"))
BP_SYSTOLIC_MAX = float(os.getenv("BP_SYSTOLIC_MAX"))
BP_DIASTOLIC_MAX= float(os.getenv("BP_DIASTOLIC_MAX"))
SPO2_MIN        = float(os.getenv("SPO2_MIN"))

def rule_based_check(row):
    """Threshold checks. Returns anomaly label or None."""
    hr      = row.get("avg_hr") or 0
    glucose = row.get("avg_glucose") or 0
    bp_sys  = row.get("avg_bp_systolic") or 0
    bp_dia  = row.get("avg_bp_diastolic") or 0
    spo2    = row.get("avg_spo2") or 100

    if hr > HEART_RATE_MAX:      return f"Tachycardia (HR={round(hr,1)})"
    if hr < HEART_RATE_MIN:      return f"Bradycardia (HR={round(hr,1)})"
    if glucose > GLUCOSE_MAX:    return f"Hyperglycemia (Glucose={round(glucose,1)})"
    if glucose < GLUCOSE_MIN:    return f"Hypoglycemia (Glucose={round(glucose,1)})"
    if bp_sys > BP_SYSTOLIC_MAX: return f"Hypertensive Crisis (BP={round(bp_sys,1)})"
    if spo2 < SPO2_MIN:          return f"Hypoxia (SpO2={round(spo2,1)}%)"
    return None

def isolation_forest_check(pdf):
    """ML anomaly detection. Returns DataFrame with is_ml_anomaly column."""
    from sklearn.ensemble import IsolationForest
    from sklearn.preprocessing import StandardScaler

    FEATURES = ["avg_hr", "avg_glucose", "avg_bp_systolic", "avg_bp_diastolic", "avg_spo2"]

    if len(pdf) < 5:
        pdf["is_ml_anomaly"] = False
        return pdf

    feature_data = pdf[FEATURES].dropna()
    if len(feature_data) < 5:
        pdf["is_ml_anomaly"] = False
        return pdf

    scaled = StandardScaler().fit_transform(feature_data)
    preds  = IsolationForest(
        n_estimators=100, contamination=0.05, random_state=42
    ).fit_predict(scaled)

    import pandas as pd
    pdf["is_ml_anomaly"] = pd.Series(
        preds == -1, index=feature_data.index
    ).reindex(pdf.index, fill_value=False)

    return pdf