from datetime import datetime, timedelta

from airflow.sdk import DAG
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


def moving_data_from_source_to_dwh(**context) -> None:
    '''
    Перенос метеоданных из OpenMeteo API в ClickHouse DWH.
    
    Извлекает данные за указанную дату выполнения DAG, преобразует в структурированный формат
    и загружает в сырой слой хранилища с TTL 365 дней.
    '''
    from airflow.providers.http.hooks.http import HttpHook
    from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook
    from airflow.models import Variable
    import json
    from datetime import datetime

    # Конфигурация API
    api_hook = HttpHook(method='GET', http_conn_id='openmeteo_api')
    base_url = Variable.get('OPENMETEO_BASE_URL', default_var='/v1/forecast')
    latitude = Variable.get('OPENMETEO_LATITUDE')
    longitude = Variable.get('OPENMETEO_LONGITUDE')
    
    # Параметры периода
    execution_date = context['ds']
    start_date = end_date = execution_date
    
    # Выполнение API-запроса
    endpoint = f'{base_url}?latitude={latitude}&longitude={longitude}&hourly=temperature_2m,relative_humidity_2m,wind_speed_10m,precipitation&start_date={start_date}&end_date={end_date}'
    response = api_hook.run(endpoint)
    data = json.loads(response.text)

    # Преобразование данных
    records = []
    for time, temp, humidity, wind, precip in zip(
        data['hourly']['time'],
        data['hourly']['temperature_2m'],
        data['hourly']['relative_humidity_2m'],
        data['hourly']['wind_speed_10m'],
        data['hourly']['precipitation']
    ):
        records.append((
            datetime.fromisoformat(time),
            float(temp),
            float(humidity),
            float(wind),
            float(precip)
        ))

    # Загрузка в ClickHouse
    ch_hook = ClickHouseHook(clickhouse_conn_id='clickhouse_dwh')
    
    # Создание таблицы с TTL
    create_table_sql = '''
    CREATE TABLE IF NOT EXISTS raw_weather_data (
        timestamp DateTime,
        temperature_2m Float32,
        relative_humidity_2m Float32,
        wind_speed_10m Float32,
        precipitation Float32
    )
    ENGINE = MergeTree()
    ORDER BY timestamp
    TTL timestamp + INTERVAL 365 DAY
    '''
    
    # Удаление данных за текущую дату
    delete_sql = f"ALTER TABLE raw_weather_data DELETE WHERE toDate(timestamp) = '{execution_date}'"
    
    ch_hook.run(create_table_sql)
    ch_hook.run(delete_sql)
    ch_hook.insert_rows(
        table='raw_weather_data',
        rows=records,
        columns=['timestamp', 'temperature_2m', 'relative_humidity_2m', 'wind_speed_10m', 'precipitation']
    )


with DAG(
    dag_id="pogoda_v_moskve", 
    start_date=datetime(2023, 10, 1),
    schedule_interval="0 1 * * *",
    max_active_runs=1,
    catchup=False
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
  
    # последовательность задач
    moving_data_from_source_to_dwh = moving_data_from_source_to_dwh()
    
    (
        moving_data_from_source_to_dwh
        >> build_staging_models
        >> build_intermediate_models
        >> build_marts_models
    )