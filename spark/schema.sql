CREATE TABLE IF NOT EXISTS raw_events (
    id SERIAL PRIMARY KEY,
    event_time TIMESTAMP NOT NULL,
    sensors JSONB NOT NULL,
    pass_fail INTEGER NOT NULL,
    inserted_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS anomalies (
    id SERIAL PRIMARY KEY,
    event_time TIMESTAMP NOT NULL,
    sensor_id VARCHAR(10) NOT NULL,
    sensor_value FLOAT NOT NULL,
    z_score FLOAT NOT NULL,
    anomaly_type VARCHAR(50) NOT NULL,
    threshold_upper FLOAT,
    threshold_lower FLOAT,
    inserted_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS daily_agg (
    date DATE PRIMARY KEY,
    total_events INTEGER,
    pass_count INTEGER,
    anomaly_count INTEGER,
    anomaly_rate FLOAT,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_raw_events_time ON raw_events(event_time);
CREATE INDEX IF NOT EXISTS idx_anomalies_time ON anomalies(event_time);
