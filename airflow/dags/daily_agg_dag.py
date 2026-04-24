from datetime import datetime, timedelta
from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.exceptions import AirflowSkipException
from airflow.providers.postgres.hooks.postgres import PostgresHook

# ============================================================
# SECOM Daily Aggregation DAG
# raw_events + anomalies → daily_agg (매일 자정)
# ============================================================

default_args = {
    "owner": "secom",
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
    "retry_exponential_backoff": 2.0,
    "max_retry_delay": timedelta(minutes=10),
}

def get_hook():
    return PostgresHook(postgres_conn_id="secom_postgres")

def check_data(**context):
    ds = context["ds"]
    hook = get_hook()
    sql = """
        SELECT COUNT(*)
        FROM raw_events
        WHERE (event_time AT TIME ZONE 'Asia/Seoul')::date = %s
    """
    result = hook.get_first(sql, parameters=(ds,))
    count = result[0]

    if count == 0:
        raise AirflowSkipException(f"No data for {ds}, skipping")

    print(f"[check_data] {ds}: {count} rows found")

def aggregate_and_load(**context):
    ds = context["ds"]
    hook = get_hook()

    # 단일 SQL문 = PostgreSQL이 자동으로 트랜잭션 보장 (Atomicity)
    # 실행 중 장애 시 WAL 기반 전체 롤백 → 부분 적재 없음
    sql = """
        INSERT INTO daily_agg (date, total_events, pass_count, anomaly_count, anomaly_rate)
        SELECT
            %(ds)s::date AS date,
            COUNT(*) AS total_events,
            COUNT(*) FILTER (WHERE pass_fail = -1) AS pass_count,
            (SELECT COUNT(*) FROM anomalies
             WHERE (event_time AT TIME ZONE 'Asia/Seoul')::date = %(ds)s::date
            ) AS anomaly_count,
            ROUND(
                (SELECT COUNT(*) FROM anomalies
                 WHERE (event_time AT TIME ZONE 'Asia/Seoul')::date = %(ds)s::date
                )::numeric / NULLIF(COUNT(*), 0), 4
            ) AS anomaly_rate
        FROM raw_events
        WHERE (event_time AT TIME ZONE 'Asia/Seoul')::date = %(ds)s::date
        ON CONFLICT (date) DO UPDATE SET
            total_events = EXCLUDED.total_events,
            pass_count = EXCLUDED.pass_count,
            anomaly_count = EXCLUDED.anomaly_count,
            anomaly_rate = EXCLUDED.anomaly_rate,
            created_at = NOW()
    """
    hook.run(sql, parameters={"ds": ds})
    print(f"[aggregate_and_load] {ds}: UPSERT complete")

def validate(**context):
    ds = context["ds"]
    hook = get_hook()

    sql = """
        SELECT total_events, pass_count, anomaly_count, anomaly_rate
        FROM daily_agg
        WHERE date = %s
    """
    result = hook.get_first(sql, parameters=(ds,))

    if result is None:
        raise ValueError(f"[validate] {ds}: daily_agg row not found after load!")

    total_events, pass_count, anomaly_count, anomaly_rate = result
    print(f"[validate] {ds}: total={total_events}, pass={pass_count}, "
          f"anomaly={anomaly_count}, rate={anomaly_rate}")

    if anomaly_rate and anomaly_rate > 0.5:
        raise ValueError(
            f"[validate] {ds}: anomaly_rate {anomaly_rate} > 0.5 — "
            f"possible sensor fault or data corruption!"
        )

    print(f"[validate] {ds}: OK")

with DAG(
    dag_id="secom_daily_aggregation",
    default_args=default_args,
    description="SECOM daily aggregation: raw_events + anomalies → daily_agg",
    schedule="@daily",
    start_date=datetime(2026, 4, 22),
    catchup=False,
    tags=["secom", "aggregation"],
) as dag:

    t_check = PythonOperator(
        task_id="check_data",
        python_callable=check_data,
    )

    t_aggregate = PythonOperator(
        task_id="aggregate_and_load",
        python_callable=aggregate_and_load,
    )

    t_validate = PythonOperator(
        task_id="validate",
        python_callable=validate,
    )

    t_check >> t_aggregate >> t_validate
