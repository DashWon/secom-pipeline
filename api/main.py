"""
SECOM Pipeline — FastAPI 서버 (v2: 커넥션 풀 적용)
====================================================
PostgreSQL 데이터 조회 + Isolation Forest 추론 API
- Gemini 피드백 반영: get_conn() → 커넥션 풀 방식
"""

import os
from datetime import date, datetime
from typing import Optional

import joblib
import numpy as np
import psycopg2
import psycopg2.pool
import psycopg2.extras
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

# ── 설정 ──────────────────────────────────────────────
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "secom")
POSTGRES_USER = os.getenv("POSTGRES_USER", "secom")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "secom123")
MODEL_PATH = os.getenv("MODEL_PATH", "/app/model.joblib")

# ── FastAPI 앱 ─────────────────────────────────────────
app = FastAPI(
    title="SECOM Pipeline API",
    description="반도체 공정 센서 이상 탐지 파이프라인 API",
    version="1.0.0",
)

# CORS: Streamlit에서 접근 허용
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# ── DB 커넥션 풀 ───────────────────────────────────────
conn_pool = None

@app.on_event("startup")
def init_db_pool():
    global conn_pool
    conn_pool = psycopg2.pool.SimpleConnectionPool(
        minconn=2,   # 최소 2개 연결 유지
        maxconn=10,  # 최대 10개까지 확장
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
    )
    print(f"DB connection pool created (min=2, max=10)")


@app.on_event("shutdown")
def close_db_pool():
    if conn_pool:
        conn_pool.closeall()
        print("DB connection pool closed")


def get_conn():
    """풀에서 커넥션 빌려오기"""
    return conn_pool.getconn()


def put_conn(conn):
    """풀에 커넥션 반납"""
    conn_pool.putconn(conn)


# ── 모델 로딩 (서버 시작 시 1회) ─────────────────────────
model_data = None

@app.on_event("startup")
def load_model():
    global model_data
    if os.path.exists(MODEL_PATH):
        model_data = joblib.load(MODEL_PATH)
        print(f"Model loaded from {MODEL_PATH}")
    else:
        print(f"Warning: Model not found at {MODEL_PATH}")


# ── Pydantic 스키마 ────────────────────────────────────
class PredictRequest(BaseModel):
    sensors: dict[str, Optional[float]]

    class Config:
        json_schema_extra = {
            "example": {
                "sensors": {"0": 3045.0, "1": 2500.0, "2": 2200.0, "5": 100.0, "10": 0.01}
            }
        }


class PredictResponse(BaseModel):
    is_anomaly: bool
    anomaly_score: float
    message: str


# ── 엔드포인트 ─────────────────────────────────────────

@app.get("/")
def root():
    return {"service": "SECOM Pipeline API", "status": "running"}


@app.get("/health")
def health_check():
    """DB 연결 상태 확인"""
    conn = None
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("SELECT 1")
        return {"status": "healthy", "database": "connected", "pool": "active"}
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"DB connection failed: {str(e)}")
    finally:
        if conn:
            put_conn(conn)


@app.get("/anomalies/latest")
def get_latest_anomalies(limit: int = Query(default=20, le=100)):
    """최근 이상치 목록 조회"""
    conn = get_conn()
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("""
            SELECT id, event_time AT TIME ZONE 'Asia/Seoul' AS event_time_kst,
                   sensor_id, sensor_value, z_score, anomaly_type,
                   threshold_upper, threshold_lower
            FROM anomalies
            ORDER BY event_time DESC
            LIMIT %s
        """, (limit,))
        rows = cur.fetchall()

        for row in rows:
            row["event_time_kst"] = row["event_time_kst"].isoformat()

        return {"count": len(rows), "anomalies": rows}
    finally:
        put_conn(conn)


@app.get("/anomalies/stats")
def get_anomaly_stats(target_date: Optional[date] = Query(default=None)):
    """일별 이상치 통계 조회"""
    conn = get_conn()
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        if target_date:
            cur.execute("""
                SELECT
                    sensor_id,
                    anomaly_type,
                    COUNT(*) AS count,
                    ROUND(AVG(z_score)::numeric, 2) AS avg_z_score,
                    ROUND(MAX(z_score)::numeric, 2) AS max_z_score
                FROM anomalies
                WHERE (event_time AT TIME ZONE 'Asia/Seoul')::date = %s
                GROUP BY sensor_id, anomaly_type
                ORDER BY count DESC
            """, (target_date,))
        else:
            cur.execute("""
                SELECT
                    sensor_id,
                    anomaly_type,
                    COUNT(*) AS count,
                    ROUND(AVG(z_score)::numeric, 2) AS avg_z_score,
                    ROUND(MAX(z_score)::numeric, 2) AS max_z_score
                FROM anomalies
                GROUP BY sensor_id, anomaly_type
                ORDER BY count DESC
            """)

        rows = cur.fetchall()
        total = sum(r["count"] for r in rows)

        return {"date": str(target_date) if target_date else "all", "total_anomalies": total, "by_sensor": rows}
    finally:
        put_conn(conn)


@app.get("/daily-agg")
def get_daily_agg(
    start_date: Optional[date] = Query(default=None),
    end_date: Optional[date] = Query(default=None),
):
    """일일 집계 데이터 조회"""
    conn = get_conn()
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        query = "SELECT * FROM daily_agg"
        params = []
        conditions = []

        if start_date:
            conditions.append("date >= %s")
            params.append(start_date)
        if end_date:
            conditions.append("date <= %s")
            params.append(end_date)

        if conditions:
            query += " WHERE " + " AND ".join(conditions)
        query += " ORDER BY date DESC"

        cur.execute(query, params)
        rows = cur.fetchall()

        for row in rows:
            row["date"] = row["date"].isoformat()
            if row.get("created_at"):
                row["created_at"] = row["created_at"].isoformat()

        return {"count": len(rows), "data": rows}
    finally:
        put_conn(conn)


@app.get("/raw-events/count")
def get_raw_events_count():
    """raw_events 총 건수 및 최근 이벤트 시간"""
    conn = get_conn()
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("""
            SELECT
                COUNT(*) AS total_count,
                MAX(event_time AT TIME ZONE 'Asia/Seoul') AS latest_event_kst,
                MIN(event_time AT TIME ZONE 'Asia/Seoul') AS earliest_event_kst
            FROM raw_events
        """)
        row = cur.fetchone()

        if row["latest_event_kst"]:
            row["latest_event_kst"] = row["latest_event_kst"].isoformat()
        if row["earliest_event_kst"]:
            row["earliest_event_kst"] = row["earliest_event_kst"].isoformat()

        return row
    finally:
        put_conn(conn)


@app.post("/predict", response_model=PredictResponse)
def predict(req: PredictRequest):
    """센서 데이터로 이상 여부 예측 (Isolation Forest)"""
    if model_data is None:
        raise HTTPException(status_code=503, detail="Model not loaded")

    model = model_data["model"]
    feature_names = model_data["feature_names"]
    feature_means = model_data["feature_means"]

    # 입력 센서값을 모델 feature 순서에 맞게 정렬
    values = []
    for col in feature_names:
        v = req.sensors.get(col)
        if v is None:
            # 입력에 없는 센서 → 학습 데이터 평균값으로 대체
            values.append(feature_means.get(col, 0.0))
        else:
            values.append(float(v))

    X = np.array(values).reshape(1, -1)

    # 추론
    prediction = model.predict(X)[0]       # 1=정상, -1=이상
    score = model.decision_function(X)[0]  # 낮을수록 이상

    is_anomaly = prediction == -1
    message = "ANOMALY DETECTED" if is_anomaly else "Normal"

    return PredictResponse(
        is_anomaly=is_anomaly,
        anomaly_score=round(float(score), 4),
        message=message,
    )
