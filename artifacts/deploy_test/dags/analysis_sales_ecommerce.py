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
    Функция перемещает данные из источников данных (PostgreSQL) в аналитическое хранилище (ClickHouse).
    
    Шаги:
    1. Извлечение данных из таблиц orders и customers с использованием фильтрации по дате.
    2. Удаление предыдущей версии таблиц в ClickHouse.
    3. Создание новых таблиц в ClickHouse.
    4. Загрузка данных в ClickHouse.
    """
    from airflow.hooks.postgres_hook import PostgresHook
    from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook

    # Подключение к источникам данных
    postgres_orders = PostgresHook(postgres_conn_id='postgres_orders')
    postgres_customers = PostgresHook(postgres_conn_id='postgres_customers')

    # Подключение к ClickHouse
    clickhouse_dwh = ClickHouseHook(clickhouse_conn_id='clickhouse_dwh')

    # Получение даты из контекста
    ds = context["ds"]  # шаблонная переменная Airflow, представляющая текущую дату выполнения DAG

    # SQL-запросы для извлечения данных из PostgreSQL
    orders_query = f"""
        SELECT order_id, product_id, timestamp, customer_id, amount
        FROM orders
        WHERE DATE(timestamp) = '{ds}'
    """

    customers_query = """
        SELECT customer_id, name, region_id, age
        FROM customers
    """

    # Извлечение данных
    orders_data = postgres_orders.get_records(orders_query)
    customers_data = postgres_customers.get_records(customers_query)

    # Удаление предыдущих таблиц в ClickHouse
    clickhouse_dwh.run("DROP TABLE IF EXISTS orders")
    clickhouse_dwh.run("DROP TABLE IF EXISTS customers")

    # Создание новых таблиц в ClickHouse
    clickhouse_dwh.run(
        """
        CREATE TABLE orders (
            order_id Int32,
            product_id Int32,
            timestamp DateTime,
            customer_id Int32,
            amount Float64
        ) ENGINE = MergeTree() ORDER BY order_id
        """
    )

    clickhouse_dwh.run(
        """
        CREATE TABLE customers (
            customer_id Int32,
            name String,
            region_id Int32,
            age Int32
        ) ENGINE = MergeTree() ORDER BY customer_id
        """
    )

    # Загрузка данных в ClickHouse
    clickhouse_dwh.run("INSERT INTO orders VALUES", orders_data)
    clickhouse_dwh.run("INSERT INTO customers VALUES", customers_data)


with DAG(
    dag_id="analysis_sales_ecommerce", 
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