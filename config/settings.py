from pydantic_settings import BaseSettings
from pydantic import Field
from pathlib import Path

class Settings(BaseSettings):
    # OpenAI API
    openai_api_key: str = Field(..., env="OPENAI_API_KEY")
    openai_model: str = "gpt-4-turbo"

    # Пути к директориям для генерации файлов
    deploy_dir: Path = Path("deploy")
    dags_dir: Path = deploy_dir / "dags"
    dbt_dir: Path = deploy_dir / "dbt"
    dbt_models_dir: Path = deploy_dir / "dbt" / "models"
    docker_compose_path: Path = deploy_dir / "docker-compose.yml"

    # DBT
    dbt_target: str = "dev"

    # ClickHouse-DWH
    clickhouse_host: str = Field(..., env="CLICKHOUSE_HOST")
    clickhouse_port: int = Field(..., env="CLICKHOUSE_PORT")
    clickhouse_user: str = Field(..., env="CLICKHOUSE_USER")
    clickhouse_password: str = Field(..., env="CLICKHOUSE_USER")

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"

settings = Settings()