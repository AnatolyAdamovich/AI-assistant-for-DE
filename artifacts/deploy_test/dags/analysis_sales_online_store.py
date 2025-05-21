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
    Эта функция перемещает данные из двух таблиц PostgreSQL ('orders' и 'customers') в аналитическое хранилище ClickHouse.
    Данные извлекаются за определенный период, указанный в контексте выполнения DAG.
    """

    from airflow.hooks.postgres_hook import PostgresHook
    from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook

    # Инициализация подключений к источникам данных и DWH
    postgres_orders_hook = PostgresHook(postgres_conn_id='postgres_orders')
    postgres_customers_hook = PostgresHook(postgres_conn_id='postgres_customers')
    clickhouse_dwh_hook = ClickHouseHook(clickhouse_conn_id='clickhouse_dwh')

    # Получение даты выполнения DAG из контекста
    execution_date = context["data_interval_start"].strftime("%Y-%m-%d")

    # SQL-запросы для извлечения данных из PostgreSQL
    orders_query = f"""
        SELECT order_id, product_id, timestamp, customer_id, amount
        FROM orders
        WHERE timestamp::date = '{execution_date}'
    """

    customers_query = """
        SELECT customer_id, name, region_id, age
        FROM customers
    """

    # Извлечение данных из PostgreSQL
    orders_data = postgres_orders_hook.get_records(orders_query)
    customers_data = postgres_customers_hook.get_records(customers_query)

    # Удаление старых данных и создание таблиц в ClickHouse
    clickhouse_dwh_hook.run("DROP TABLE IF EXISTS orders")
    clickhouse_dwh_hook.run("DROP TABLE IF EXISTS customers")

    clickhouse_dwh_hook.run(
        """
        CREATE TABLE orders (
            order_id Int32,
            product_id Int32,
            timestamp DateTime,
            customer_id Int32,
            amount Float32
        ) ENGINE = MergeTree()
        ORDER BY order_id
        """
    )

    clickhouse_dwh_hook.run(
        """
        CREATE TABLE customers (
            customer_id Int32,
            name String,
            region_id Int32,
            age Int32
        ) ENGINE = MergeTree()
        ORDER BY customer_id
        """
    )

    # Загрузка данных в ClickHouse
    clickhouse_dwh_hook.run("INSERT INTO orders VALUES", orders_data)
    clickhouse_dwh_hook.run("INSERT INTO customers VALUES", customers_data)


with DAG(
    dag_id="analysis_sales_online_store", 
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