from datetime import timedelta
import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowSkipException
from airflow.providers.postgres.hooks.postgres import PostgresHook

default_args = {
    "owner": "secom",
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=10),
}

def get_hook():
    return PostgresHook(postgres_conn_id="secom_postgres")

def resolve_target_date(context, hook):
    """수동 실행은 최신 데이터 날짜(KST), 스케줄 실행은 ds 사용."""
    ds = context["ds"]
    dag_run = context.get("dag_run")
    run_type = str(getattr(dag_run, "run_type", "")).lower()

    if "manual" in run_type:
        latest = hook.get_first(
            "SELECT MAX((event_time AT TIME ZONE 'Asia/Seoul')::date) FROM raw_events"
        )
        if latest is None or latest[0] is None:
            raise AirflowSkipException("No data in raw_events, skipping")
        target_date = str(latest[0])
        print(f"[resolve_target_date] run_type=manual, ds={ds} -> target_date={target_date}")
        return target_date

    print(f"[resolve_target_date] run_type=scheduled, target_date={ds}")
    return ds

def check_data(**context):
    hook = get_hook()
    target_date = resolve_target_date(context, hook)
    result = hook.get_first(
        """SELECT COUNT(*) FROM raw_events
           WHERE event_time >= (%(target_date)s::date::timestamp AT TIME ZONE 'Asia/Seoul')
             AND event_time <  (((%(target_date)s::date + INTERVAL '1 day')::timestamp) AT TIME ZONE 'Asia/Seoul')""",
        parameters={"target_date": target_date},
    )
    count = result[0]
    if count == 0:
        raise AirflowSkipException(f"No data for target_date={target_date}, skipping")
    context["ti"].xcom_push(key="target_date", value=target_date)
    print(f"[check_data] target_date={target_date}: {count} rows found")

def aggregate_and_load(**context):
    ds = context["ds"]
    target_date = context["ti"].xcom_pull(task_ids="check_data", key="target_date") or ds
    hook = get_hook()
    sql = """
        WITH base AS (
            SELECT COUNT(*) AS total_events,
                   COUNT(*) FILTER (WHERE pass_fail = -1) AS pass_count
            FROM raw_events
            WHERE event_time >= (%(target_date)s::date::timestamp AT TIME ZONE 'Asia/Seoul')
              AND event_time <  (((%(target_date)s::date + INTERVAL '1 day')::timestamp) AT TIME ZONE 'Asia/Seoul')
        ),
        anom AS (
            SELECT COUNT(*) AS anomaly_count
            FROM event_anomalies
            WHERE event_time >= (%(target_date)s::date::timestamp AT TIME ZONE 'Asia/Seoul')
              AND event_time <  (((%(target_date)s::date + INTERVAL '1 day')::timestamp) AT TIME ZONE 'Asia/Seoul')
              AND is_anomaly = TRUE
        )
        INSERT INTO daily_agg (date, total_events, pass_count, anomaly_count, anomaly_rate)
        SELECT %(target_date)s::date, base.total_events, base.pass_count, anom.anomaly_count,
               ROUND(anom.anomaly_count::numeric / NULLIF(base.total_events, 0), 4)
        FROM base, anom
        ON CONFLICT (date) DO UPDATE SET
            total_events = EXCLUDED.total_events, pass_count = EXCLUDED.pass_count,
            anomaly_count = EXCLUDED.anomaly_count, anomaly_rate = EXCLUDED.anomaly_rate,
            created_at = NOW()
    """
    hook.run(sql, parameters={"target_date": target_date})
    print(f"[aggregate_and_load] target_date={target_date}: UPSERT complete")

def validate(**context):
    ds = context["ds"]
    target_date = context["ti"].xcom_pull(task_ids="check_data", key="target_date") or ds
    hook = get_hook()
    result = hook.get_first(
        "SELECT total_events, pass_count, anomaly_count, anomaly_rate FROM daily_agg WHERE date = %s",
        parameters=(target_date,),
    )
    if result is None:
        raise ValueError(f"[validate] target_date={target_date}: daily_agg row not found!")
    total_events, pass_count, anomaly_count, anomaly_rate = result
    print(f"[validate] target_date={target_date}: total={total_events}, pass={pass_count}, anomaly={anomaly_count}, rate={anomaly_rate}")
    if anomaly_rate and anomaly_rate > 0.5:
        raise ValueError(f"[validate] target_date={target_date}: anomaly_rate {anomaly_rate} > 0.5!")
    print(f"[validate] target_date={target_date}: OK")

with DAG(
    dag_id="secom_daily_aggregation",
    default_args=default_args,
    description="SECOM daily aggregation: raw_events + anomalies -> daily_agg",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 4, 22, tz="Asia/Seoul"),
    catchup=False,
    tags=["secom", "aggregation"],
) as dag:

    t_check = PythonOperator(task_id="check_data", python_callable=check_data)
    t_aggregate = PythonOperator(task_id="aggregate_and_load", python_callable=aggregate_and_load)
    t_validate = PythonOperator(task_id="validate", python_callable=validate)

    t_check >> t_aggregate >> t_validate
