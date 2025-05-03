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
    schedule_interval="",
    start_date=datetime(2024, 1, 1, 12, 0),
    catchup=True
)
def airflow_pipeline():

    @task
    def moving_data_from_source_to_dwh(**context) -> None:
        """
        Функция для переноса данных из исходной базы данных PostgreSQL в аналитическую базу данных ClickHouse.
        Данные извлекаются из таблицы 'orders' PostgreSQL и загружаются в ClickHouse для дальнейшего анализа.
        """
        from airflow.hooks.postgres_hook import PostgresHook
        from airflow.providers.clickhouse.hooks.clickhouse import ClickHouseHook
        import pandas as pd

        # Инициализация хуков для соединения с базами данных
        postgres_hook = PostgresHook(postgres_conn_id="postgres_source")
        clickhouse_hook = ClickHouseHook(clickhouse_conn_id="clickhouse_dwh")

        # SQL запрос для выборки данных из PostgreSQL
        query = """
        SELECT order_id, customer_id, timestamp, product_id, amount
        FROM orders
        WHERE timestamp >= CURRENT_DATE - INTERVAL '1 day'
        """

        # Использование хука для выполнения запроса и загрузки данных в DataFrame
        df = postgres_hook.get_pandas_df(sql=query)

        # Подготовка и выполнение запроса для вставки данных в ClickHouse
        clickhouse_hook.run("CREATE TABLE IF NOT EXISTS orders (order_id Int64, customer_id Int64, timestamp DateTime, product_id Int64, amount Float64) ENGINE = MergeTree() ORDER BY timestamp")
        clickhouse_hook.insert_dataframe("INSERT INTO orders VALUES", df)

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