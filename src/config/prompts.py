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

prompts = Prompts()