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
    Перенос данных из таблицы orders PostgreSQL в ClickHouse с очисткой целевой таблицы.
    Использует фильтрацию по дате выполнения DAG для инкрементальной загрузки.
    '''
    from airflow.hooks.postgres_hook import PostgresHook
    from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook
    from datetime import datetime

    # Получаем временной интервал выполнения DAG
    data_interval_start = context['data_interval_start'].to_date_string()
    
    # Инициализируем соединение с источником данных
    pg_hook = PostgresHook(postgres_conn_id='source')
    ch_hook = ClickHouseHook(clickhouse_conn_id='dwh')

    # Формируем SQL-запрос с фильтрацией по дате заказа
    select_query = f'''
        SELECT order_id, order_date::text, ship_date::text, customer_id, 
               customer_name, segment, region, city, state, postal_code,
               product_id, category, sub_category, product_name,
               sales::float, quantity, discount::float, profit::float
        FROM orders
        WHERE order_date >= '{data_interval_start}'::date
    '''

    # Выполняем запрос и получаем данные
    connection = pg_hook.get_conn()
    cursor = connection.cursor()
    cursor.execute(select_query)
    records = cursor.fetchall()
    
    if records:
        # Создаем целевую таблицу в ClickHouse
        ch_hook.execute(
            '''
            DROP TABLE IF EXISTS orders
            ENGINE = MergeTree()
            ORDER BY (order_date, order_id)
            '''
        )

        # Вставляем данные пачкой
        ch_hook.insert_rows(
            table='orders',
            rows=records,
            column_types=[
                'Int32', 'Date', 'Date', 'Int32',
                'String', 'String', 'String', 'String',
                'String', 'String', 'Int32', 'String',
                'String', 'String', 'Float64', 'Int32',
                'Float64', 'Float64'
            ]
        )
    
    cursor.close()
    connection.close()


with DAG(
    dag_id="analyze_retail_sales", 
    start_date=datetime(2023, 7, 1),
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