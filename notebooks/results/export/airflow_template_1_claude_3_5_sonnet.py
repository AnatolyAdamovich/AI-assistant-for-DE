def export_data_from_source(**context) -> None:
    """
    Экспортирует данные из таблицы customers в PostgreSQL в CSV файл.
    Использует PostgresHook для подключения к БД.
    Сохраняет результат в CSV с учетом схемы данных.
    
    Args:
        **context: Контекст выполнения задачи Airflow
    """
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    import pandas as pd
    from datetime import datetime
    
    # Подключение к БД через hook
    pg_hook = PostgresHook(postgres_conn_id="postgres_source")
    
    # SQL запрос для получения данных
    sql = "SELECT customer_id, name, region_id, position, Age FROM customers"
    
    # Получение данных через pandas
    df = pd.read_sql_query(sql, pg_hook.get_sqlalchemy_engine())
    
    # Приведение типов данных согласно схеме
    df = df.astype({
        'customer_id': 'Int64',
        'name': 'string',
        'region_id': 'Int64', 
        'position': 'string',
        'Age': 'Int64'
    })
    
    # Формирование имени файла с текущей датой
    current_date = datetime.now().strftime('%Y%m%d')
    output_file = f"customers_export_{current_date}.csv"
    
    # Сохранение в CSV
    df.to_csv(output_file, index=False)