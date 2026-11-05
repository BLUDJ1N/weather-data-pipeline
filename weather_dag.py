from datetime import timedelta

import pendulum
import psycopg
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import Connection, DAG

from weather_pipeline import run_pipeline


def load_daily_weather(data_interval_end, params):
    # Для расписания берём предыдущий день по московскому времени.
    weather_date = (
        data_interval_end.in_timezone("Europe/Moscow") - timedelta(days=1)
    ).date().isoformat()

    start_date = params.get("start_date") or weather_date
    end_date = params.get("end_date") or weather_date
    settings = Connection.get("weather_postgres")

    with psycopg.connect(
        host=settings.host,
        port=settings.port or 5432,
        dbname=settings.schema,
        user=settings.login,
        password=settings.password,
        connect_timeout=10,
    ) as connection:
        processed_count = run_pipeline(start_date, end_date, connection)

    print("Вставлено или обновлено записей:", processed_count)


with DAG(
    dag_id="weather_daily",
    description="Загрузка погоды трёх городов в PostgreSQL",
    start_date=pendulum.datetime(2026, 11, 1, tz="Europe/Moscow"),
    schedule="0 9 * * *",
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    params={"start_date": "", "end_date": ""},
    tags=["weather", "etl"],
) as dag:
    load_weather = PythonOperator(
        task_id="load_weather",
        python_callable=load_daily_weather,
    )
