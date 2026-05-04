import pandas as pd
import numpy as np
from sklearn.ensemble import IsolationForest
import joblib
import os

CSV_PATH = os.getenv("SECOM_CSV_PATH", "data/uci-secom.csv")
MODEL_PATH = os.getenv("MODEL_PATH", "api/model.joblib")

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

    model = IsolationForest(
        n_estimators=100,       # 트리 100개
        contamination=0.1,      # 이상치 비율 약 10% 가정
        random_state=42,
        n_jobs=-1,              # CPU 코어 전부 사용
    )
    model.fit(X)

    # 모델 저장
    joblib.dump({
        "model": model,
        "feature_names": sensor_cols,
        "feature_means": X.mean().to_dict(),  # NaN 대체용
    }, MODEL_PATH)

    print(f"Model saved to {MODEL_PATH}")

    # 간단 검증
    scores = model.decision_function(X)
    preds = model.predict(X)
    anomaly_count = (preds == -1).sum()
    print(f"Anomalies detected: {anomaly_count}/{len(X)} ({anomaly_count/len(X)*100:.1f}%)")


if __name__ == "__main__":
    train()
