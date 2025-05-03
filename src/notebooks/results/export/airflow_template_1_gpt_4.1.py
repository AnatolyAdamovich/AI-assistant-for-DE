def export_data_from_source(**context) -> None:
    """
    Экспортирует данные из таблицы 'customers' PostgreSQL базы данных в CSV файл.
    Использует Airflow PostgresHook для подключения к базе данных.
    """
    from airflow.hooks.postgres_hook import PostgresHook
    import pandas as pd

    # Создаем подключение к источнику данных
    postgres_hook = PostgresHook("postgres_source")

    # Запрос для извлечения данных
    sql = "SELECT * FROM customers"

    # Получаем данные в DataFrame
    df = postgres_hook.get_pandas_df(sql)

    # Сохраняем данные в CSV файл
    df.to_csv("/path/to/your/directory/customers_data.csv", index=False)