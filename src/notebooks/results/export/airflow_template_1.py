def export_data_from_source(**context) -> None:
    """
    Экспортирует данные из таблицы customers в PostgreSQL в Parquet файл.
    Использует соединение Airflow postgres_source для подключения к БД.
    Сохраняет данные с учетом схемы данных в локальный файл customers.parquet.
    
    Args:
        **context: Контекст выполнения задачи Airflow
    """
    import pandas as pd
    from sqlalchemy import create_engine
    from airflow.hooks.base import BaseHook

    conn = BaseHook.get_connection('postgres_source')
    
    engine = create_engine(
        f'postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}'
    )

    df = pd.read_sql_query(
        'SELECT * FROM customers',
        engine,
        dtype={
            'customer_id': 'Int64',
            'name': 'string',
            'region_id': 'Int64', 
            'position': 'string',
            'Age': 'Int64'
        }
    )

    df.to_parquet(
        'customers.parquet',
        engine='pyarrow',
        compression='snappy',
        index=False
    )