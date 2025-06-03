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
    '''Функция для получения метеорологических данных из OpenMeteo API и загрузки их в ClickHouse.
    
    Извлекает данные о температуре, влажности, скорости ветра и осадках за текущую дату
    и сохраняет их в слой RAW аналитического хранилища.
    '''
    import requests
    from datetime import datetime
    from airflow.models import Variable
    from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook
    
    # Получаем дату из контекста
    ds = context['ds']
    
    # Параметры API запроса
    base_url = 'https://api.open-meteo.com/v1/forecast'
    params = {
        'latitude': Variable.get('weather_latitude', 55.75),  # Москва по умолчанию
        'longitude': Variable.get('weather_longitude', 37.62),
        'hourly': ['temperature_2m', 'relative_humidity_2m', 'wind_speed_10m', 'precipitation'],
        'start_date': ds,
        'end_date': ds
    }
    
    # Получаем данные из API
    response = requests.get(base_url, params=params)
    response.raise_for_status()
    data = response.json()
    
    # Подготавливаем данные для загрузки
    hourly_data = data['hourly']
    records = []
    for i in range(len(hourly_data['time'])):
        records.append((
            hourly_data['time'][i],
            hourly_data['temperature_2m'][i],
            hourly_data['relative_humidity_2m'][i],
            hourly_data['wind_speed_10m'][i],
            hourly_data['precipitation'][i]
        ))
    
    # Подключаемся к ClickHouse
    clickhouse = ClickHouseHook(clickhouse_conn_id='clickhouse_dwh')
    
    # Создаем таблицу, если она не существует
    create_table_sql = '''
    CREATE TABLE IF NOT EXISTS raw_weather (
        timestamp DateTime,
        temperature_2m Float32,
        relative_humidity_2m Float32,
        wind_speed_10m Float32,
        precipitation Float32
    ) ENGINE = MergeTree()
    ORDER BY timestamp
    TTL timestamp + INTERVAL 365 DAY;
    '''
    
    # Удаляем старые данные за эту дату
    delete_sql = f"ALTER TABLE raw_weather DELETE WHERE toDate(timestamp) = '{ds}'"
    
    # Выполняем SQL-запросы
    clickhouse.execute(create_table_sql)
    clickhouse.execute(delete_sql)
    
    # Загружаем новые данные
    insert_sql = 'INSERT INTO raw_weather VALUES'
    clickhouse.execute(insert_sql, records)


with DAG(
    dag_id="moscow_weather_monitoring", 
    start_date=datetime(2025, 5, 1),
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