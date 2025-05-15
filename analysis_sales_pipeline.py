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


def move_data_to_dwh(**context) -> None:
    """
    Перемещает данные из PostgreSQL источников (orders и customers) в ClickHouse аналитическое хранилище.
    Данные извлекаются с учетом фильтра по дате (ds) и перезаписываются в соответствующие таблицы в DWH.
    """
    from airflow.hooks.postgres_hook import PostgresHook
    from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook
    import pandas as pd

    # Инициализация подключений
    orders_source = PostgresHook(postgres_conn_id='postgres_orders')
    customers_source = PostgresHook(postgres_conn_id='postgres_customers')
    clickhouse_dwh = ClickHouseHook(clickhouse_conn_id='clickhouse_dwh')

    # Получение даты из контекста Airflow
    ds = context["ds"]

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

    # Извлечение данных из источников
    orders_df = pd.DataFrame(orders_source.get_pandas_df(orders_query))
    customers_df = pd.DataFrame(customers_source.get_pandas_df(customers_query))

    # Загрузка данных в ClickHouse
    clickhouse_dwh.run("DROP TABLE IF EXISTS orders_stage;")
    clickhouse_dwh.run("""
        CREATE TABLE orders_stage (
            order_id Int32,
            product_id Int32,
            timestamp DateTime,
            customer_id Int32,
            amount Float64
        ) ENGINE = MergeTree()
        ORDER BY (order_id, timestamp);
    """)
    clickhouse_dwh.insert_df("orders_stage", orders_df)

    clickhouse_dwh.run("DROP TABLE IF EXISTS customers_stage;")
    clickhouse_dwh.run("""
        CREATE TABLE customers_stage (
            customer_id Int32,
            name String,
            region_id Int32,
            age Int32
        ) ENGINE = MergeTree()
        ORDER BY customer_id;
    """)
    clickhouse_dwh.insert_df("customers_stage", customers_df)

    print("Данные успешно перемещены в DWH")


with DAG(
    dag_id="analysis_sales_pipeline", 
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