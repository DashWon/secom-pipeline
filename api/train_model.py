import pandas as pd
import numpy as np
from sklearn.ensemble import IsolationForest
import joblib
import os

CSV_PATH = os.getenv("SECOM_CSV_PATH", "data/uci-secom.csv")
MODEL_PATH = os.getenv("MODEL_PATH", "api/model.joblib")
MODEL_VERSION = os.getenv("MODEL_VERSION", "iforest-v2")

def train():
    print("Loading data...")
    df = pd.read_csv(CSV_PATH)

    # 센서 컬럼만 추출
    sensor_cols = [c for c in df.columns if c not in ("Time", "Pass/Fail")]
    X = df[sensor_cols].copy()

    # NaN → 컬럼 평균으로 채움
    X = X.fillna(X.mean())

    # Inf 제거
    X = X.replace([np.inf, -np.inf], np.nan).fillna(0)

    print(f"Training Isolation Forest on {X.shape[0]} rows, {X.shape[1]} features...")

    contamination = 0.05
    model = IsolationForest(
        n_estimators=100,       # 트리 100개
        contamination=contamination,  # 이상치 비율 약 5% 가정
        random_state=42,
        n_jobs=-1,              # CPU 코어 전부 사용
    )
    model.fit(X)

    # 운영 기준선을 모델 메타데이터로 함께 저장
    scores = model.decision_function(X)
    anomaly_threshold = float(np.percentile(scores, contamination * 100))

    # 모델 저장
    joblib.dump({
        "model": model,
        "feature_names": sensor_cols,
        "feature_means": X.mean().to_dict(),  # NaN 대체용
        "anomaly_threshold": anomaly_threshold,
        "model_version": MODEL_VERSION,
    }, MODEL_PATH)

    print(f"Model saved to {MODEL_PATH}")
    print(f"Model version: {MODEL_VERSION}")
    print(f"Anomaly threshold: {anomaly_threshold:.6f}")

    # 간단 검증
    preds = model.predict(X)
    anomaly_count = (preds == -1).sum()
    print(f"Anomalies detected: {anomaly_count}/{len(X)} ({anomaly_count/len(X)*100:.1f}%)")


if __name__ == "__main__":
    train()
