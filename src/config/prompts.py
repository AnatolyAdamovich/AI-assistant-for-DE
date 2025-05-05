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
        Формат вывода: JSON
        """,
        description="Пользовательский промпт для генерации stage моделей"
    )
    SYSTEM_PROMPT_DBT_MODELS_STAGE: str = Field(
        default=
        """  
    Ты опытный дата-инженер, реализуешь dbt-модели для stage 
    в трёхслойной архитектуре аналитического хранилища, построенного в Clickhouse.
    Сгенерируй dbt-модели, включая:
    1. SQL-код
    2. Описание для внесения в schema.yml (включая тесты, если они необходимы)
    
    Требования к dbt-моделям:
    - Добавь метку о времени и/или другие метаданные, полезные на слое stage
    - Используй явные материализации, указывай их в config. На этапе stage преимущественно view.

    Структура вывода (строго в JSON):
        {{
            "stg_model_name1": "Код dbt-модели",
            "stg_model_name2": "Код dbt-модели",
            ...,
            "schema_yml": {{
                'version': 2,
                'models': [
                    {{'name': "stg_model_name1", ...}},
                    {{"name": "stg_model_name2", ...}},
                    ...
                ]
            }}
        }}
    
    """,
        description="Системный промпт для генерации stage моделей"
    )

    USER_PROMPT_DBT_MODELS_CORE: str = Field(
        default="",
        description="Пользовательский промпт для генерации core моделей"
    )
    SYSTEM_PROMPT_DBT_MODELS_CORE: str = Field(
        default="",
        description="Системный промпт для генерации core моделей"
    )

    USER_PROMPT_DBT_MODELS_MARTS: str = Field(
        default="",
        description="Пользовательский промпт для генерации marts моделей"
    )
    SYSTEM_PROMPT_DBT_MODELS_MARTS: str = Field(
        default="",
        description="Системный промпт для генерации marts моделей"
    )

prompts = Prompts()