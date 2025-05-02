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
    schedule_interval="0 1 * * *"0 * * * *",
    start_date=datetime(2025, 2, 1),
    catchup=True
)
def airflow_pipeline():

    @task
    def moving_data_from_source_to_dwh(**context) -> None:
        """
        Извлекает данные из таблицы 'orders' в источнике PostgreSQL
        и загружает их в таблицу 'orders' в ClickHouse DWH.

        Предполагается, что структура таблицы 'orders' в ClickHouse
        соответствует схеме источника:
        (order_id Int64, product_id Int64, timestamp DateTime, customer_id Int64, money Decimal).
        """
        import pandas as pd
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        from airflow.providers.clickhouse.hooks.clickhouse import ClickHouseHook

        # Define connection IDs and table names
        postgres_conn_id = "postgres_source"  # Replace with your actual Postgres connection ID
        clickhouse_conn_id = "clickhouse_dwh"
        source_table = "orders"
        target_table = "orders" # Assuming the target table has the same name

        # Define source schema explicitly for clarity, though hook handles types
        source_schema = {
            'order_id': 'Int64',
            'product_id': 'Int64',
            'timestamp': 'datetime64[ns]', # Pandas equivalent for datetime
            'customer_id': 'Int64',
            'money': 'object' # Pandas often reads numeric/decimal as object initially
        }
        target_columns = ['order_id', 'product_id', 'timestamp', 'customer_id', 'money']

        # Create hooks
        pg_hook = PostgresHook(postgres_conn_id=postgres_conn_id)
        ch_hook = ClickHouseHook(clickhouse_conn_id=clickhouse_conn_id)

        # Extract data from PostgreSQL
        sql = f"SELECT {', '.join(target_columns)} FROM {source_table}"
        source_conn = pg_hook.get_conn()

        try:
            # Use pandas read_sql for better type handling potential with numeric/decimal
            df = pd.read_sql(sql, source_conn)

            # Optional: Explicit type casting if needed, especially for decimals
            # df['money'] = pd.to_numeric(df['money']) # Cast if read as string/object
            # Ensure correct types based on source schema if read_sql inference isn't perfect
            # df = df.astype(source_schema, errors='ignore') # Apply schema types

        finally:
            if source_conn:
                source_conn.close()

        # Load data into ClickHouse
        if not df.empty:
            # Convert DataFrame to list of lists/tuples for insertion
            # Handle potential NaT values for datetime columns if necessary before conversion
            df['timestamp'] = df['timestamp'].astype(object).where(df['timestamp'].notnull(), None)
            rows_to_insert = df.values.tolist()

            ch_hook.insert_rows(
                table=target_table,
                rows=rows_to_insert,
                target_fields=target_columns
            )
        else:
            # Optionally log that no data was found
            pass # Or use context['ti'].log.info("No data extracted from source.")

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