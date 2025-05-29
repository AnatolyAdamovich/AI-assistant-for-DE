from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


PROJECT_DIR = "/opt/airflow/dbt"
DATA_PATH = f"{PROJECT_DIR}/sample"


DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}


def moving_data_from_source_to_dwh(**context):
    """
    Функция для извлечения данных из OpenMeteo API и загрузки их в аналитическое хранилище ClickHouse.

    Параметры:
        **context: Словарь контекста выполнения задачи Airflow, содержащий переменные, такие как дата выполнения.
    """
    import requests
    from airflow.models import Variable
    from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook

    # Получение параметров API из Airflow Variables
    api_url = Variable.get("openmeteo_api_url", default_var="https://api.open-meteo.com/v1/forecast")
    latitude = Variable.get("openmeteo_latitude", default_var="55.7558")  # Москва
    longitude = Variable.get("openmeteo_longitude", default_var="37.6173")  # Москва

    # Получение даты из контекста Airflow
    execution_date = context["data_interval_start"].to_date_string()

    # Формирование запроса к API
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "start_date": execution_date,
        "end_date": execution_date,
        "hourly": "temperature_2m,relative_humidity_2m,wind_speed_10m,precipitation"
    }

    response = requests.get(api_url, params=params)
    response.raise_for_status()
    data = response.json()

    # Преобразование данных для загрузки в ClickHouse
    hourly_data = data.get("hourly", {})
    timestamps = hourly_data.get("time", [])
    temperature_2m = hourly_data.get("temperature_2m", [])
    relative_humidity_2m = hourly_data.get("relative_humidity_2m", [])
    wind_speed_10m = hourly_data.get("wind_speed_10m", [])
    precipitation = hourly_data.get("precipitation", [])

    records = [
        (timestamps[i], temperature_2m[i], relative_humidity_2m[i], wind_speed_10m[i], precipitation[i])
        for i in range(len(timestamps))
    ]
    print(f"{len(records)} records")

    # Подключение к ClickHouse
    clickhouse_hook = ClickHouseHook(clickhouse_conn_id="dwh")

    # Удаление старых данных и создание таблицы
    clickhouse_hook.execute("DROP TABLE IF EXISTS raw_weather_data;")
    clickhouse_hook.execute(
        """
        CREATE TABLE IF NOT EXISTS raw_weather_data (
            timestamp String,
            temperature_2m Float32,
            relative_humidity_2m Float32,
            wind_speed_10m Float32,
            precipitation Float32
        ) ENGINE = MergeTree()
        ORDER BY timestamp;
        """
    )

    # Загрузка данных в ClickHouse
    clickhouse_hook.execute(
        "INSERT INTO raw_weather_data (timestamp, temperature_2m, relative_humidity_2m, wind_speed_10m, precipitation) VALUES",
        records
    )


with DAG(
    dag_id="weather_monitoring_moscow", 
    start_date=datetime(2025, 5, 1),
    schedule_interval="0 1 * * *",
    max_active_runs=1,
    catchup=True
) as dag:
    
    moving_data_from_source_to_dwh = PythonOperator(
        task_id="moving_data",
        python_callable=moving_data_from_source_to_dwh
    )

    build_staging_models = BashOperator(
        task_id="build_staging_models",
        bash_command=f"dbt run --profiles-dir {PROJECT_DIR} " \
                             f"--project-dir {PROJECT_DIR} " \
                             f"--select tag:stage " \
                             f"--no-version-check " \
    )
    
    build_intermediate_models = BashOperator(
        task_id="build_intermediate_models",
        bash_command=f"dbt run --profiles-dir {PROJECT_DIR} " \
                             f"--project-dir {PROJECT_DIR} " \
                             f"--select tag:core " \
                             f"--no-version-check " \

    )

    build_marts_models = BashOperator(
        task_id="build_marts_models",
        bash_command=f"dbt run --profiles-dir {PROJECT_DIR} " \
                             f"--project-dir {PROJECT_DIR} " \
                             f"--select tag:marts " \
                             f"--no-version-check " \
    )
  
    
    (
        moving_data_from_source_to_dwh
        >> build_staging_models
        >> build_intermediate_models
        >> build_marts_models
    )