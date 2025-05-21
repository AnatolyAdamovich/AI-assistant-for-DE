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
    """
    Перемещение данных из источников PostgreSQL (orders и customers) в аналитическое хранилище ClickHouse.

    Аргументы:
        context: контекст выполнения DAG, содержащий переменные, такие как дата выполнения.
    """
    from airflow.hooks.postgres_hook import PostgresHook
    from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook

    # Инициализация подключения к источникам данных
    orders_source = PostgresHook(postgres_conn_id='postgres_orders')
    customers_source = PostgresHook(postgres_conn_id='postgres_customers')

    # Инициализация подключения к аналитическому хранилищу
    clickhouse_dwh = ClickHouseHook(clickhouse_conn_id='clickhouse_dwh')

    # Получение даты выполнения DAG из контекста
    execution_date = context["data_interval_start"].strftime("%Y-%m-%d")

    # SQL-запросы для извлечения данных из источников
    orders_query = f"""
        SELECT order_id, product_id, timestamp, customer_id, amount
        FROM orders
        WHERE DATE(timestamp) = '{execution_date}'
    """

    customers_query = """
        SELECT customer_id, name, region_id, age
        FROM customers
    """

    # Извлечение данных из PostgreSQL
    orders_data = orders_source.get_records(orders_query)
    customers_data = customers_source.get_records(customers_query)

    # Удаление старых таблиц в ClickHouse (если они существуют)
    clickhouse_dwh.run("DROP TABLE IF EXISTS orders_stage")
    clickhouse_dwh.run("DROP TABLE IF EXISTS customers_stage")

    # Создание новых таблиц в ClickHouse
    clickhouse_dwh.run("""
        CREATE TABLE orders_stage (
            order_id Int32,
            product_id Int32,
            timestamp DateTime,
            customer_id Int32,
            amount Float32
        ) ENGINE = MergeTree()
        ORDER BY order_id
    """)

    clickhouse_dwh.run("""
        CREATE TABLE customers_stage (
            customer_id Int32,
            name String,
            region_id Int32,
            age Int32
        ) ENGINE = MergeTree()
        ORDER BY customer_id
    """)

    # Загрузка данных в ClickHouse
    clickhouse_dwh.insert_rows(table="orders_stage", rows=orders_data)
    clickhouse_dwh.insert_rows(table="customers_stage", rows=customers_data)



with DAG(
    dag_id="analyze_sales_ecommerce", 
    start_date=datetime(2023, 10, 1),
    schedule_interval="0 3 * * *",
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