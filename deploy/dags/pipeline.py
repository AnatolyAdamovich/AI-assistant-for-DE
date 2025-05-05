from datetime import datetime, timedelta

from airflow.decorators import dag, task


PROJECT_DIR = "/opt/airflow/dbt"
DATA_PATH = f"{PROJECT_DIR}/sample"
SEED_PATH = f"{PROJECT_DIR}/seeds"

DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}


@dag(
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    schedule_interval="0 0 * * *",
    start_date=datetime(2023, 10, 1),
    catchup=True
)
def airflow_pipeline():

    @task
    def moving_data_from_source_to_dwh(**context) -> None:
        """
        Функция перемещает данные из источника в аналитическое хранилище данных.
        Источником данных является база данных PostgreSQL, содержащая таблицы 'orders' и 'customers'.
        Данные из этих таблиц извлекаются и загружаются в аналитическое хранилище ClickHouse.
        """

        import pandas as pd
        from airflow.hooks.postgres_hook import PostgresHook
        from airflow_clickhouse_plugin.hooks.clickhouse_hook import ClickHouseHook

        # Подключение к источнику данных PostgreSQL
        orders_source = PostgresHook(postgres_conn_id='orders_source')
        customers_source = PostgresHook(postgres_conn_id='customers_source')

        # Подключение к аналитическому хранилищу ClickHouse
        clickhouse_dwh = ClickHouseHook(clickhouse_conn_id='clickhouse_dwh')

        # Извлечение данных из таблицы 'orders'
        orders_query = "SELECT * FROM orders"
        orders_df = orders_source.get_pandas_df(orders_query)

        # Извлечение данных из таблицы 'customers'
        customers_query = "SELECT * FROM customers"
        customers_df = customers_source.get_pandas_df(customers_query)

        # Загрузка данных в ClickHouse
        clickhouse_dwh.run("CREATE TABLE IF NOT EXISTS orders (order_id Int32, product_id Int32, timestamp DateTime, customer_id Int32, amount Float64) ENGINE = MergeTree() ORDER BY order_id")
        clickhouse_dwh.run("CREATE TABLE IF NOT EXISTS customers (customer_id Int32, name String, region_id Int32, age Int32) ENGINE = MergeTree() ORDER BY customer_id")

        clickhouse_dwh.insert_df('orders', orders_df)
        clickhouse_dwh.insert_df('customers', customers_df)

    @task.bash
    def build_staging_models() -> str:
        bash_command=f"dbt run --profiles-dir {PROJECT_DIR} " \
                             f"--project-dir {PROJECT_DIR} " \
                             f"--select tag:stage" \
                             f"--no-version-check " \
        
        return bash_command
    
    @task.bash
    def build_intermediate_models() -> str:
        bash_command=f"dbt run --profiles-dir {PROJECT_DIR} " \
                             f"--project-dir {PROJECT_DIR} " \
                             f"--select tag:intermediate" \
                             f"--no-version-check " \
        
        return bash_command

    @task.bash
    def build_marts_models() -> str:
        bash_command=f"dbt run --profiles-dir {PROJECT_DIR} " \
                             f"--project-dir {PROJECT_DIR} " \
                             f"--select tag:marts" \
                             f"--no-version-check " \
        
        return bash_command
  
    # последовательность задач
    moving_data_from_source_to_dwh = moving_data_from_source_to_dwh()
    build_staging_models = build_staging_models()
    build_intermediate_models = build_intermediate_models()
    build_marts_models = build_marts_models()
    
    (
        moving_data_from_source_to_dwh
        >> build_staging_models
        >> build_intermediate_models
        >> build_marts_models
    )


airflow_pipeline = airflow_pipeline()