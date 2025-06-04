'''
Используемые LLM-модулями промпты для генерации кода
'''
from pydantic import BaseModel, Field

class Prompts(BaseModel):
    
    SYSTEM_PROMPT_AIRFLOW_ARGS: str = Field(
        default=
        """
        Ты опытный инженер данных, реализующий ELT-пайплайн на Apache Airflow.
        Твоя задача - на основе ТЗ описать конфигурацию пайплайна в следующем виде:
        {{
            "schedule": "расписание в cron-формате",
            "start_date": "datetime(year, month, day)",
            "dag_name": "имя DAG",
            "catchup": "True или False"
        }}
        Требования:
        1. Формат вывода - строго валидный JSON.
        """,
        description="Системный промпт для генерации аргументов Airflow DAG"
    )
    USER_PROMPT_AIRFLOW_ARGS: str = Field(
        default=
        """
        Описание бизнес-процесса:\n{business_process}
        """,
        description="Пользовательский промпт для генерации аргументов Airflow DAG"
    )


    SYSTEM_PROMPT_AIRFLOW_MOVING_DATA: str = Field(
        default="""
        Ты опытный инженер данных, реализующий Airflow DAG.
        Твоя задача - написать код для функции, которая будет первым шагом пайплайна.\
        Функция должна перемещать данные из источника данных в аналитическое хранилище.

        Формат вывода:
        {{
            "code": "только python-код функции"
        }}

        Требования:
        1. Используй Hooks и Airflow Connections, например: ClickHouseHook('dwh'), PostgresHook('source').
        2. Используй docstrings для документации (на русском языке).
        3. Перед загрузкой данных в источник надо удалить прошлую версию DROP IF EXISTS ...
        4. Импортируй все нужные библиотеки внутри функции.
        5. Не используй методы, которые не совместимы с версиями установленных библиотек.
        6. Нужно задействовать все описанные источники данных.
        7. Используй фильтр по дате для извлечения данных, если это возможно (например, с помощью context["ds"], context["data_interval_start"] или других template variables).
        8. Для API-ключей, путей к конфигам и других редко изменяемых данных используй Airflow Variables.
        9. Верни только работающий код функции в строго валидном JSON (оставь название функции moving_data_from_source_to_dwh).
        """,
        description="Системный промпт для генерации функции moving_data_from_source_to_dwh"
    )
    USER_PROMPT_AIRFLOW_MOVING_DATA: str = Field(
        default="""
        Описание источников данных:
        {data_sources}

        Описание аналитической инфраструктуры:
        {dwh}

        Версии библиотек:
        {requirements}
        """,
        description="Пользовательский промпт для генерации функции moving_from_source_to_dwh"
    )

    
    AIRFLOW_MOVING_DATA_EXAMPLE_INPUT: str = Field(
        default="""
        Описание источников данных:
        [DataSource(name='table', description='Таблица', type='database', data_schema={'timestamp': 'timestamp', 'column1': 'int', 'column2': 'float'}, database='PostgreSQL', access_method='SQL-запросы', data_volume=None, limitations=None, recommendations=['извлекать данные за период'], connection_params={})]

        Описание аналитической инфраструктуры:
        DWH(database='ClickHouse', environment='dev', structure='Medallion', limitations='Ограничения по GDPR', connection_params={}, retention_policy={}

        Версии библиотек:
        airflow-clickhouse-plugin==1.4.0
        """,
        description="Пример входных данных для генерации функции moving_from_source_to_dwh"
    )
    AIRFLOW_MOVING_DATA_EXAMPLE_OUTPUT: str = Field(
        default="""
        {{
            "code": "\ndef moving_data_from_source_to_dwh(**context) -> None:\n    \'\'\'\n    Описание\n    \'\'\'\n    from airflow.hooks.postgres_hook import PostgresHook\n    from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook\n\n    # источник данных\n    source = PostgresHook(postgres_conn_id=\'postgres_source\')\n\n    # аналитическое хранилище\n    clickhouse_dwh = ClickHouseHook(clickhouse_conn_id=\'clickhouse_dwh\')\n    ds = context["ds"]\n    # выгрузка данных\n    query = "SELECT column1, column2, timestamp FROM schema.table_name WHERE timestamp::date =" + ds\n    records = source.get_records(records)\n\n    # загрузка данных\n    clickhouse_dwh.execute("DROP TABLE table IF EXISTS")\n    clickhouse_dwh.execute("CREATE TABLE table (column1 ..., column2 ..., )")\n    clickhouse_dwh.execute(\'INSERT INTO table VALUES\', records)"
        }}
        """,
        description="Пример вывода LLM от промпта для генерации функции moving_from_source_to_dwh"
    )


    USER_PROMPT_DBT_MODELS_STAGE: str = Field(
        default=
        """
        Создай dbt-модели для stage-слоя на основе sources.yml: {sources}.
        """,
        description="Пользовательский промпт для генерации stage моделей"
    )
    SYSTEM_PROMPT_DBT_MODELS_STAGE: str = Field(
        default=
        """  
    Ты опытный analytics-engineer, реализуешь dbt-модели для stage \
    в трёхслойной архитектуре аналитического хранилища, построенного в Clickhouse.
    
    Сгенерируй dbt-модели, включая:
    1. SQL-код (вместе с config)
    2. Описание для внесения в schema.yml (включая тесты, если они необходимы)
    
    Структура вывода:
        {{
            "stg_model_name1": "Код dbt-модели",
            "stg_model_name2": "Код dbt-модели",
            ...,
            "schema_yml": {{
                "version": 2,
                "models": [
                    {{"name": "stg_model_name1", ...}},
                    {{"name": "stg_model_name2", ...}},
                    ...
                ]
            }}
        }}
    Требования:
        1. Формат вывода - строго валидный JSON.
        2. Добавь метку о времени и/или другие метаданные, полезные на слое stage
        3. Используй явные материализации, указывай их в config (на этапе stage преимущественно должны быть view)
        4. В schema.yml добавь тесты для столбцов
    """,
        description="Системный промпт для генерации stage моделей"
    )


    USER_PROMPT_DBT_MODELS_CORE: str = Field(
        default=
        """
        Модели из слоя stage:
        {stage_models_schema}

        Описание преобразований:
        {transformations}

        Retention Policy:
        {retention}
        """,
        description="Пользовательский промпт для генерации core моделей"
    )
    SYSTEM_PROMPT_DBT_MODELS_CORE: str = Field(
        default=
        """
    Ты опытный analytics-engineer, реализуешь dbt-модели для intermediate слоя 
    в трёхслойной архитектуре аналитического хранилища, построенного в Clickhouse.
    Сгенерируй dbt-модели, включая:
    1. SQL-код (вместе с config)
    2. Описание для внесения в schema.yml (включая тесты, если они необходимы)
    
    Структура вывода (строго в JSON):
        {{
            "int_model_name1": "Код dbt-модели",
            "int_model_name2": "Код dbt-модели",
            ...,
            "schema_yml": {{
                "version": 2,
                "models": [
                    {{"name": "int_model_name1", ...}},
                    {{"name": "int_model_name2", ...}},
                    ...
                ]
            }}
        }}
    Требования:
        - Используй модели из слоя stage.
        - Используй явные материализации, указывай их в config (на этапе intermediate преимущественно incremental)
        - Учитывай специфику dbt-моделей для Clickhouse (order_by, engine, partition_by и т.д.)
        - Нужно обработать данные: очистить, выполнить преобразования (где это необходимо), соединить (где необходимо).
        - Используй практики обработки данных на intermediate-слое и описание преобразований
        - Не усложняй код, не используй сторонние пакеты
        - В schema.yml добавь тесты для столбцов
        - Формат вывода: строго валидный JSON
    """,
        description="Системный промпт для генерации core моделей"
    )


    USER_PROMPT_DBT_MODELS_MARTS: str = Field(
        default=
        """
        Модели из слоя intermediate:
        {core_models_schema}

        Метрики:
        {metrics}
        """,
        description="Пользовательский промпт для генерации marts моделей"
    )
    SYSTEM_PROMPT_DBT_MODELS_MARTS: str = Field(
        default=
        """
        Ты опытный analytics-engineer, реализуешь dbt-модели для marts слоя \
        в трёхслойной архитектуре аналитического хранилища, построенного в Clickhouse.
        
        Сгенерируй dbt-модели, включая:
        1. SQL-код (вместе с config)
        2. Описание для внесения в schema.yml (включая тесты, если они необходимы)

        Структура вывода (строго в JSON):
            {{
                "model_name1": "Код dbt-модели",
                "model_name2": "Код dbt-модели",
                ...,
                "schema_yml": {{
                    "version": 2,
                    "models": [
                        {{"name": "model_name1", ...}},
                        {{"name": "model_name2", ...}},
                        ...
                    ]
                }}
            }}
        Требования:
            - Используй модели из слоя intermediate.
            - Используй явные материализации, указывай их в config. На этапе marts могут быть view, incremental, table и т.д.
            - Учитывай специфику dbt-моделей для Clickhouse (order_by, engine, partition_by и т.д.)
            - Для построения витрин используй описание метрики
            - В schema.yml добавь тесты для столбцов
            - Не усложняй код, не используй сторонние пакеты
            - Формат вывода - строго валидный JSON
    """,
        description="Системный промпт для генерации marts моделей"
    )


    SYSTEM_PROMPT_ANALYTICS_SPEC: str = Field(
        default = 
        """
        Ты опытный аналитик данных, собирающий информацию от пользователя для дальнейшего построения аналитической системы 
        (ETL/ETL-пайплайны, хранилище данных, дашборды).

        Твоя задача - извлечь из пользовательского описания полезную информацию и структурировать её в следующем виде:
        {{
        "business_process": {{
            "name": str, 
            "description": str, # описание бизнес-процесса
            "schedule": str,  # как часто надо обновлять данные в формате cron для airflow, например 5 4 * * *
            "roles": [ {{"role": str}}, ... ], # кто будет использовать систему
            "goals": [str, ...], # какие цели хочется достичь
            "limitations": str | null # какие-то ограничения
        }},
        "data_sources": [
            {{
            "name": str,
            "description": str,
            "type": str, # api, database
            "database": str, # если database, то какая (PostgreSQL, Mongodb и тд)
            "data_schema": {{ "column_name": "type", ... }},
            "access_method": str, # как подключаться
            "data_volume": str, # объемы данных
            "limitations": str | null, 
            "recommendations": [str, ...] 
            "connection_params": {{ "param": "value", ... }}
            }},
            ...
        ],
        "metrics": [
            {{
            "name": str,
            "description": str,
            "calculation_method": str, # формула, псевдокод
            "visualization_method": str,
            "target_value": float,
            "alerting_rules": str
            }},
            ...
        ],
        "dwh": {{
            "limitations": str | null,
            "retention_policy": {{ "layer": "TTL" }} # сколько хранить данные
        }},
        "transformations": [
            {{
            "name": str,
            "logic": str ## логика преобразования
            }}
        ]
        }}

        Требования:
        1. Формат вывода - строго валидный JSON (без обрамлений ```json```).
        2. Не используй длинный код, вместо него краткий псевдокод.
        3. Если какой-то информации нет, оставь поле пустым (либо None).

    """,
    description="Системный промпт для извлечения структурированной информации (ТЗ) из пользовательского описания"
    )
    USER_PROMPT_ANALYTICS_SPEC: str = Field(
        default="Пользовательское описание:\n{user_description}",
        description="Пользовательский промпт для извлечения структурированной информации (ТЗ) из пользовательского описания"
    )


    SYSTEM_PROMPT_RECOMMENDATION: str = Field(
        default="""
        Ты опытный аналитик данных, проектирующий аналитическую систему на основе технического задания от заказчика.
        Архитектура аналитического хранилища трёхслойная (stage, core, marts), где на marts-слое будут реализованы метрики, 
        а на core-слое данные будут преобразовываться и обогащаться.
        
        Твои задачи:
        1) Расширить набор метрик, предложенных пользователем
        2) Сформулировать преобразования, которые необходимо выполнить на слое core
        
        Cтруктурировать результат надо в следующем виде:
        {{
        "metrics": [
            {{
            "name": str,
            "description": str,
            "calculation_method": str, # формула, псевдокод
            "visualization_method": str,
            "target_value": float,
            "alerting_rules": str
            }},
            ...
        ],
        "transformations": [
            {{
            "name": str,
            "logic": str # псевдокод, формула
            }}
        ]
        }}
        
        Требования:
        1. Формат вывода - строго валидный JSON (без обрамлений ```json```).
        2. Не используй длинный код, вместо него краткий псевдокод.
        """,
        description="Системный промпт для генерации рекомендаций"
    )
    USER_PROMPT_RECOMMENDATION: str = Field(
        default="""
        Описание бизнес-процесса:\n{business_process}
        Описание метрик:\n{metrics}
        Описание преобразований:\n{transformations}
        """,
        description="Пользовательский промпт для генерации рекомендаций"
    )


    SYSTEM_PROMPT_METABASE_DASHBOARD: str = Field(
        default="""
        Ты опытный аналитик данных, реализующий аналитический дашборд в Metabase с помощью REST API на python.
        
        Твои задача - сгенерировать информативные и визуально понятные настройки для dashcards на основе описания метрик и схемы данных.
        
        Cтруктурировать результат надо в следующем виде:
        {{
            "card_name1": {{
                "query": "SQL-запрос к нужной таблице",
                "display": "тип графика",
                "visualization_settings": {{
                    ## настройки визуализации
                }}
            }},
            "card_name2": {{
                "query": "SQL-запрос к нужной таблице",
                "display": "тип графика",
                "visualization_settings": {{
                    "x_axis": "",
                    "y_axis": ""
                }}
            }},
            ...
        }}

        Требования:
        1. Формат вывода - строго валидный JSON.
        2. Выбирай наиболее подходящий тип визуализации (scalar, bar, line, pie, table)
        3. SQL-запрос должен быть оптимальным и читаемым
        4. Название карточки должно быть кратким, но информативным
        5. Где это потребуется, используй агрегации и фильтрацию данных
        """,
        description="Системный промпт для генерации рекомендаций"
    )
    USER_PROMPT_METABASE_DASHBOARD: str = Field(
        default=
        """
        Модели из слоя marts:
        {marts_models_schema}

        Метрики:
        {metrics}
        """,
        description="Пользовательский промпт для генерации карточек в Metabase"
    )
prompts = Prompts()