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
    Функция перемещает данные из PostgreSQL в ClickHouse.
    Использует Airflow Connections для доступа к базам данных.
    Удаляет старую версию таблицы в ClickHouse перед загрузкой новых данных.
    '''

    from airflow.hooks.postgres_hook import PostgresHook
    from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook

    # Подключение к источнику данных (PostgreSQL)
    source = PostgresHook(postgres_conn_id='postgres_source')

    # Подключение к аналитическому хранилищу (ClickHouse)
    clickhouse_dwh = ClickHouseHook(clickhouse_conn_id='clickhouse_dwh')

    # Получение даты для фильтрации данных
    ds = context['ds']

    # SQL-запрос для извлечения данных из PostgreSQL
    query = f"SELECT * FROM orders WHERE order_date::date = '{ds}'"

    # Извлечение данных из PostgreSQL
    records = source.get_records(query)

    # Удаление старой версии таблицы в ClickHouse
    clickhouse_dwh.execute('DROP TABLE IF EXISTS orders')

    # Создание таблицы в ClickHouse
    create_table_query = '''
    CREATE TABLE IF NOT EXISTS orders (
        order_id Int32,
        order_date Date,
        ship_date Date,
        customer_id Int32,
        customer_name String,
        segment String,
        region String,
        city String,
        state String,
        postal_code String,
        product_id Int32,
        category String,
        sub_category String,
        product_name String,
        sales Float64,
        quantity Int32,
        discount Float64,
        profit Float64
    ) ENGINE = MergeTree()
    ORDER BY order_id
    '''
    clickhouse_dwh.execute(create_table_query)

    # Загрузка данных в ClickHouse
    clickhouse_dwh.insert_rows('orders', records)


with DAG(
    dag_id="Анализ продаж розничной сети", 
    start_date=datetime(year, month, day),
    schedule_interval="30 * * * *",
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