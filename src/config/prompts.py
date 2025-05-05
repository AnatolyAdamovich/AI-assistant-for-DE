from pydantic import BaseModel, Field

class Prompts(BaseModel):
    
    USER_PROMPT_AIRFLOW_MOVING_DATA: str = Field(
        default="Business process:\n"
                 "- name: {name}\n"
                 "- schedule: {schedule}\n",
        description="Пользовательский промпт для генерации функции moving_from_source_to_dwh"
    )
    SYSTEM_PROMPT_AIRFLOW_ARGS: str = Field(
        default="",
        description="Системный промпт для генерации функции moving_from_source_to_dwh"
    )

    USER_PROMPT_DBT_MODELS_STAGE: str = Field(
        default=
        """
        Создай dbt-модели для stage-слоя на основе sources.yml: {sources}.
        
        Требования к dbt-моделям:
        - Добавь метку о времени и/или другие метаданные, полезные на слое stage
        - Используй явные материализации, указывай их в config. 
        На этапе stage преимущественно должны быть view.
        
        Формат вывода: valid JSON
        """,
        description="Пользовательский промпт для генерации stage моделей"
    )
    SYSTEM_PROMPT_DBT_MODELS_STAGE: str = Field(
        default=
        """  
    Ты опытный дата-инженер, реализуешь dbt-модели для stage 
    в трёхслойной архитектуре аналитического хранилища, построенного в Clickhouse.
    Сгенерируй dbt-модели, включая:
    1. SQL-код (вместе с config)
    2. Описание для внесения в schema.yml (включая тесты, если они необходимы)
    
    Структура вывода (строго в JSON):
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
    
    """,
        description="Системный промпт для генерации stage моделей"
    )

    USER_PROMPT_DBT_MODELS_CORE: str = Field(
        default=
        """
        Создай dbt-модели для intermediate-слоя.
        Требования:
        - Используй модели из слоя stage: {stage_models_schema}.
        - Используй явные материализации, указывай их в config. 
        На этапе intermediate преимущественно incremental
        - Учитывай специфику dbt-моделей для Clickhouse (order_by, engine, partition_by и т.д.)
        - Нужно обработать данные: очистить, выполнить преобразования (где это необходимо), соединить (где необходимо).
        Используй практики обработки данных на intermediate-слое и пользовательское описание преобразований: {transformations}
        - Не усложняй код, не используй сторонние пакеты
        
        Формат вывода: valid JSON
        """,
        description="Пользовательский промпт для генерации core моделей"
    )
    SYSTEM_PROMPT_DBT_MODELS_CORE: str = Field(
        default=
        """
    Ты опытный дата-инженер, реализуешь dbt-модели для intermediate слоя 
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
    """,
        description="Системный промпт для генерации core моделей"
    )

    USER_PROMPT_DBT_MODELS_MARTS: str = Field(
        default=
        """
        Создай dbt-модели для marts-слоя.
        Требования:
        - Используй модели из слоя intermediate: {core_models_schema}.
        - Используй явные материализации, указывай их в config. 
        На этапе marts могут быть view, incremental, table и т.д.
        - Учитывай специфику dbt-моделей для Clickhouse (order_by, engine, partition_by и т.д.)
        - Используй пользовательское описание нужных метрик: {metrics}
        - Если есть идеи, добавь 2-3 своих метрики
        - Не усложняй код, не используй сторонние пакеты
        
        Формат вывода: valid JSON
        """,
        description="Пользовательский промпт для генерации marts моделей"
    )
    SYSTEM_PROMPT_DBT_MODELS_MARTS: str = Field(
        default=
        """
        Ты опытный дата-инженер, реализуешь dbt-модели для marts слоя 
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
    """,
        description="Системный промпт для генерации marts моделей"
    )

    USER_PROMPT_ANALYTICS_SPEC: str = Field(
        default="{user_description}",
        description="Пользовательский промпт для извлечения структурированной информации (ТЗ) из пользовательского описания"
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
            "description": str,
            "schedule": "",  # как часто надо обновлять данные
            "roles": [ {{"role": str}}, ... ], # кто будет использовать систему
            "goals": [str, ...], # какие цели хочется достичь
            "limitations": str | null # какие-то ограничения
        }},
        "data_sources": [
            {{
            "name": str,
            "description": str,
            "data_schema": {{ "column_name": "type", ... }},
            "type": str,
            "database": str,
            "access_method": str | null,
            "limitations": str | null,
            "recommendations": [str, ...] | null,
            "connection_params": {{ "param": "value", ... }} | null
            }},
            ...
        ],
        "metrics": [
            {{
            "name": str,
            "description": str,
            "calculation_method": str | null,
            "visualization_method": str | null
            }},
            ...
        ],
        "dwh": {{
            "database": str,
            "structure": {{ "table": "description", ... }} | null,
            "limitations": str | null,
            "connection_params": {{ "param": "value", ... }} | null
        }},
        "transformations": [
            {{
            "name": str,
            "logic": str
            }}
        ]
        }}

        Требования:
        1) формат вывода - строго валидный JSON
        2) если какой-то информации нет, оставь поле пустым (либо None)

    """,
    description="Системный промпт для извлечения структурированной информации (ТЗ) из пользовательского описания"
    )
prompts = Prompts()