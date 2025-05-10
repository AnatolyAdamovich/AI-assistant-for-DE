FROM apache/airflow:2.8.3-python3.10

RUN pip install --no-cache-dir \
    dbt-core==1.8.5 \
    SQLAlchemy \
    apache-airflow-providers-postgres \
    airflow-clickhouse-plugin \
    pydantic \
    pandas \
    dbt-clickhouse==1.8.0 \
    clickhouse-connect==0.7.19