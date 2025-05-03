def export_data_from_source(**context) -> None:
    """
    Экспортирует данные из источника PostgreSQL с именем 'customers' в CSV файл.
    Использует Airflow Hook для подключения к базе данных и pandas для сохранения данных.
    """
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    import pandas as pd

    # Создаем подключение к базе данных
    hook = PostgresHook("customers_source")
    
    # SQL запрос для выборки данных
    sql = "SELECT * FROM customers"
    
    # Получаем данные в DataFrame
    df = hook.get_pandas_df(sql)
    
    # Сохраняем данные в CSV файл
    df.to_csv("/path/to/your/directory/customers_data.csv", index=False)