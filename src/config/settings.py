from pydantic_settings import BaseSettings
from pydantic import Field
from pathlib import Path

class Settings(BaseSettings):
    # LLM
    OPENAI_API_KEY: str = Field(..., env="OPENAI_API_KEY")
    BASE_URL: str = Field(..., env="BASE_URL")
    
    LLM_MODEL_FOR_AIRFLOW_MOVING_DATA: str = "just-ai/openai-proxy/gpt-4o"
    TEMPERATURE_AIRFLOW_MOVING_DATA: float = 0
    
    LLM_MODEL_FOR_AIRFLOW_ARGS: str = "just-ai/openai-proxy/gpt-4o"
    TEMPERATURE_AIRFLOW_ARGS: float = 0

    LLM_MODEL_FOR_DBT_CONFIG: str = "just-ai/openai-proxy/gpt-4o"
    TEMPERATURE_DBT_CONFIG: float = 0

    LLM_MODEL_FOR_DBT_MODEL: str = "just-ai/openai-proxy/gpt-4o"
    TEMPERATURE_DBT_MODEL: float = 0


    # Пути к директориям для генерации файлов
    DEPLOY_DIR: Path = Path("deploy2")
    DAGS_DIR: Path = DEPLOY_DIR / "dags"
    OUTPUT_DAG_PATH: Path = DAGS_DIR / "pipeline.py"
    DBT_DIR: Path = DEPLOY_DIR / "dbt"
    DBT_MODELS_DIR: Path = DBT_DIR / "models"
    DOCKER_COMPOSE_PATH: Path = DEPLOY_DIR / "docker-compose.yml"

    # TEMPLATES
    TEMPLATE_DAG_PATH: Path = Path("templates") / "airflow_dag_template.py"

    # DBT
    DBT_TARGET: str = "dev"
    DBT_SOURCE_NAME: str = "exported_data"
    DBT_SOURCE_SCHEMA: str = "last"

    # DWH
    DWH_TYPE: str = Field(..., env="DWH_TYPE")
    DWH_HOST: str = Field(..., env="DWH_HOST")
    DWH_USER: str = Field(..., env="DWH_USER")
    DWH_PASS: str = Field(..., env="DWH_PASS")
    DWH_PORT: int = Field(..., env="DWH_PORT")
    DWH_DBNAME: str = Field(..., env="DWH_DBNAME")
    DWH_SCHEMA: str = Field(..., env="DWH_SCHEMA")
    DWH_THREADS: int = Field(..., env="DWH_THREADS")
    
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"

settings = Settings()