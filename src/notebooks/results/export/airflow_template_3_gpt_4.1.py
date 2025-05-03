def export_data_from_source(**context) -> None:
    """
    Экспортирует данные из источника ClickHouse, используя соответствующий Airflow Hook.
    Данные из таблицы 'customers' сохраняются в формате CSV.
    """
    from airflow.providers.clickhouse.hooks.clickhouse import ClickHouseHook
    import pandas as pd

    # Создание подключения к ClickHouse
    hook = ClickHouseHook('clickhouse_default')

    # SQL запрос для выборки данных
    sql = "SELECT * FROM customers"

    # Выполнение запроса и загрузка данных в DataFrame
    df = hook.get_pandas_df(sql)

    # Сохранение данных в CSV файл
    df.to_csv('/path/to/output/customers_data.csv', index=False)