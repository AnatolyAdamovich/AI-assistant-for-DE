from pydantic_settings import BaseSettings
from pydantic import Field
from pathlib import Path

class Settings(BaseSettings):
    # LLM
    OPENAI_API_KEY: str = Field(..., env="OPENAI_API_KEY")
    BASE_URL: str = Field(..., env="BASE_URL")
    
    LLM_MODEL_FOR_AIRFLOW_MOVING_DATA: str = "just-ai/openai-proxy/gpt-4o"
    TEMPERATURE_AIRFLOW_MOVING_DATA: float = 0.5
    
    LLM_MODEL_FOR_AIRFLOW_ARGS: str = "just-ai/openai-proxy/gpt-4o"
    TEMPERATURE_AIRFLOW_ARGS: float = 0.5

    LLM_MODEL_FOR_DBT_CONFIG: str = "just-ai/openai-proxy/gpt-4o"
    TEMPERATURE_DBT_CONFIG: float = 0.5

    LLM_MODEL_FOR_DBT_MODEL: str = "just-ai/openai-proxy/gpt-4o"
    TEMPERATURE_DBT_MODEL: float = 0.5

    LLM_MODEL_FOR_ANALYTICS_SPEC: str = "just-ai/gigachat/GigaChat-2-Pro"
    TEMPERATURE_ANALYTICS_SPEC: float = 0.5
    
    LLM_MODEL_FOR_METABASE: str = "just-ai/deepseek/deepseek-r1"
    TEMPERATURE_METABASE: float = 0.5

    # Пути к директориям для генерации и сохранения файлов файлов
    PROJECT_ROOT: Path = Path(__file__).resolve().parent.parent.parent
    ARTIFACTS_DIRECTORY: Path = PROJECT_ROOT / "artifacts"
    LOGS_DIRECTORY: Path = ARTIFACTS_DIRECTORY / "logs"
    DEPLOY_DIR: Path = ARTIFACTS_DIRECTORY / "deploy_test"
    DAGS_DIR: Path = DEPLOY_DIR / "dags"
    DBT_DIR: Path = DEPLOY_DIR / "dbt"
    DBT_MODELS_DIR: Path = DBT_DIR / "models"

    # Инфраструктура
    DOCKER_COMPOSE_PATH: Path = PROJECT_ROOT / "infra" / "docker-compose.yml"
    REQUIREMENTS_PATH: Path = PROJECT_ROOT / "infra" / "requirements.txt"

    # TEMPLATES
    TEMPLATE_DAG_PATH: Path = PROJECT_ROOT / "src" / "templates" / "airflow_dag_template.py"

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
    
    # BI
    METABASE_URL: str = Field(..., env="METABASE_URL")
    METABASE_USERNAME: str = Field(..., env="METABASE_USERNAME")
    METABASE_PASSWORD: str = Field(..., env="METABASE_PASSWORD")


    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"

settings = Settings()