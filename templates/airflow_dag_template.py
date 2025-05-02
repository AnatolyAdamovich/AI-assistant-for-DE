from datetime import datetime, timedelta

from airflow.decorators import dag, task


PROJECT_DIR = "/opt/airflow/dbt"
DATA_PATH = f"{PROJECT_DIR}/sample"
SEED_PATH = f"{PROJECT_DIR}/seeds"

DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}


@dag(
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    schedule_interval="",
    start_date=datetime(2025, 1, 1),
    catchup=True
)
def airflow_pipeline():

    @task
    def moving_data_from_source_to_dwh(**context) -> None:
        pass

    @task.bash
    def build_staging_models() -> str:
        bash_command=f"dbt run --profiles-dir {PROJECT_DIR} " \
                             f"--project-dir {PROJECT_DIR} " \
                             f"--select tag:stage" \
                             f"--no-version-check " \
        
        return bash_command
    
    @task.bash
    def build_intermediate_models() -> str:
        bash_command=f"dbt run --profiles-dir {PROJECT_DIR} " \
                             f"--project-dir {PROJECT_DIR} " \
                             f"--select tag:intermediate" \
                             f"--no-version-check " \
        
        return bash_command

    @task.bash
    def build_marts_models() -> str:
        bash_command=f"dbt run --profiles-dir {PROJECT_DIR} " \
                             f"--project-dir {PROJECT_DIR} " \
                             f"--select tag:marts" \
                             f"--no-version-check " \
        
        return bash_command
  
    # последовательность задач
    moving_data_from_source_to_dwh = moving_data_from_source_to_dwh()
    build_staging_models = build_staging_models()
    build_intermediate_models = build_intermediate_models()
    build_marts_models = build_marts_models()
    
    (
        moving_data_from_source_to_dwh
        >> build_staging_models
        >> build_intermediate_models
        >> build_marts_models
    )


airflow_pipeline = airflow_pipeline()