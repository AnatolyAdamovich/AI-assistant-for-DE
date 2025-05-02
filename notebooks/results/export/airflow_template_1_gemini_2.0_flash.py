import csv
import json
import logging
import os

import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook


def export_data_from_source(**context) -> None:
    """
    Функция для экспорта данных из PostgreSQL базы данных в CSV файл.
    Данные извлекаются из таблицы 'customers' и сохраняются в файл
    с именем 'customers.csv' в директории /tmp/.
    """
    try:
        postgres_hook = PostgresHook(postgres_conn_id="postgres_source")
        data = postgres_hook.get_records(sql="SELECT * FROM customers")
        df = pd.DataFrame.from_records(data, columns=['customer_id', 'name', 'region_id', 'position', 'Age'])
        filepath = "/tmp/customers.csv"
        df.to_csv(filepath, index=False)
        logging.info(f"Data successfully exported to {filepath}")

    except Exception as e:
        logging.error(f"Error exporting data: {e}")
        raise