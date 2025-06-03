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
    Перенос метеорологических данных из OpenMeteo API в ClickHouse.
    
    Args:
        context (dict): Контекст выполнения DAG
    
    Steps:
        1. Получение данных через API OpenMeteo за указанную дату
        2. Преобразование данных в формат для ClickHouse
        3. Очистка целевой таблицы
        4. Загрузка данных в DWH
    '''
    from airflow.providers.http.hooks.http import HttpHook
    from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook
    from airflow.models import Variable
    import logging
    from datetime import datetime

    # Конфигурация подключений
    http_hook = HttpHook(method='GET', http_conn_id='openmeteo_api')
    ch_hook = ClickHouseHook(clickhouse_conn_id='clickhouse_dwh')
    
    # Параметры запроса
    execution_date = context['ds']
    lat = Variable.get('openmeteo_latitude')
    lon = Variable.get('openmeteo_longitude')
    
    # Формирование API-запроса
    params = {
        'latitude': lat,
        'longitude': lon,
        'hourly': 'temperature_2m,relative_humidity_2m,wind_speed_10m,precipitation',
        'start_date': execution_date,
        'end_date': execution_date
    }
    
    # Выполнение HTTP-запроса
    response = http_hook.run(endpoint='/v1/forecast', data=params)
    if response.status_code != 200:
        raise ValueError(f'API Error: {response.text}')
    
    # Парсинг данных
    data = response.json()['hourly']
    records = [
        (
            datetime.strptime(ts, '%Y-%m-%dT%H:%M'),
            temp,
            humidity,
            wind_speed,
            precip
        )
        for ts, temp, humidity, wind_speed, precip in zip(
            data['time'],
            data['temperature_2m'],
            data['relative_humidity_2m'],
            data['wind_speed_10m'],
            data['precipitation']
        )
    ]
    
    # Создание таблицы
    create_table_sql = '''
    CREATE TABLE IF NOT EXISTS raw_weather_data (
        timestamp DateTime,
        temperature_2m Float32,
        relative_humidity_2m Float32,
        wind_speed_10m Float32,
        precipitation Float32
    ) ENGINE = MergeTree()
    ORDER BY timestamp
    TTL timestamp + INTERVAL 365 DAY
    '''
    
    # Очистка данных за текущую дату
    ch_hook.run('DELETE FROM raw_weather_data WHERE toDate(timestamp) = toDate(%(date)s)',
               parameters={'date': execution_date})
    
    # Вставка данных
    if records:
        ch_hook.insert_rows(
            table='raw_weather_data',
            rows=records,
            columns=['timestamp', 'temperature_2m', 'relative_humidity_2m', 'wind_speed_10m', 'precipitation']
        )
    logging.info(f'Successfully loaded {len(records)} records')


with DAG(
    dag_id="moscow_weather_monitoring", 
    start_date=2023-01-01,
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