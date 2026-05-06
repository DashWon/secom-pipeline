CREATE TABLE IF NOT EXISTS raw_events (
    id SERIAL PRIMARY KEY,
    event_time TIMESTAMPTZ NOT NULL,
    sensors JSONB NOT NULL,
    pass_fail INTEGER NOT NULL,
    inserted_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS anomalies (
    id SERIAL PRIMARY KEY,
    event_time TIMESTAMPTZ NOT NULL,
    sensor_id VARCHAR(10) NOT NULL,
    sensor_value FLOAT NOT NULL,
    z_score FLOAT NOT NULL,
    anomaly_type VARCHAR(50) NOT NULL,
    threshold_upper FLOAT,
    threshold_lower FLOAT,
    inserted_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS event_anomalies (
    id SERIAL PRIMARY KEY,
    event_time TIMESTAMPTZ NOT NULL,
    is_anomaly BOOLEAN NOT NULL,
    anomaly_score FLOAT NOT NULL,
    anomaly_type VARCHAR(50) NOT NULL,
    model_version VARCHAR(50) NOT NULL DEFAULT 'iforest-v1',
    inserted_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS daily_agg (
    date DATE PRIMARY KEY,
    total_events INTEGER,
    pass_count INTEGER,
    anomaly_count INTEGER,
    anomaly_rate FLOAT,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_raw_events_time ON raw_events(event_time);
CREATE INDEX IF NOT EXISTS idx_anomalies_time ON anomalies(event_time);
CREATE INDEX IF NOT EXISTS idx_event_anomalies_time ON event_anomalies(event_time);
CREATE INDEX IF NOT EXISTS idx_event_anomalies_flag ON event_anomalies(is_anomaly);

-- 기존 로컬 TIMESTAMP 데이터를 UTC 표준 TIMESTAMPTZ로 마이그레이션 (KST 기준 해석)
DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_name = 'raw_events'
          AND column_name = 'event_time'
          AND data_type = 'timestamp without time zone'
    ) THEN
        ALTER TABLE raw_events
        ALTER COLUMN event_time TYPE TIMESTAMPTZ
        USING event_time AT TIME ZONE 'Asia/Seoul';
    END IF;
END $$;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_name = 'anomalies'
          AND column_name = 'event_time'
          AND data_type = 'timestamp without time zone'
    ) THEN
        ALTER TABLE anomalies
        ALTER COLUMN event_time TYPE TIMESTAMPTZ
        USING event_time AT TIME ZONE 'Asia/Seoul';
    END IF;
END $$;

-- KST 조회용 뷰: 저장은 UTC, 조회는 한국시간
CREATE OR REPLACE VIEW raw_events_kst AS
SELECT
    id,
    event_time AT TIME ZONE 'Asia/Seoul' AS event_time_kst,
    sensors,
    pass_fail,
    inserted_at AT TIME ZONE 'Asia/Seoul' AS inserted_at_kst
FROM raw_events;

CREATE OR REPLACE VIEW anomalies_kst AS
SELECT
    id,
    event_time AT TIME ZONE 'Asia/Seoul' AS event_time_kst,
    sensor_id,
    sensor_value,
    z_score,
    anomaly_type,
    threshold_upper,
    threshold_lower,
    inserted_at AT TIME ZONE 'Asia/Seoul' AS inserted_at_kst
FROM anomalies;

CREATE OR REPLACE VIEW event_anomalies_kst AS
SELECT
    id,
    event_time AT TIME ZONE 'Asia/Seoul' AS event_time_kst,
    is_anomaly,
    anomaly_score,
    anomaly_type,
    model_version,
    inserted_at AT TIME ZONE 'Asia/Seoul' AS inserted_at_kst
FROM event_anomalies;
