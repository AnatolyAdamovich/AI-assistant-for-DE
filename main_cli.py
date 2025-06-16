from src.core.llm_generators.airflow import AirflowDagGenerator
from src.core.llm_generators.dbt import DbtGenerator
from src.core.llm_generators.specification import AnalyticsSpecGenerator
from src.core.llm_generators.dashboards import MetabaseDashboardGenerator
from src.config.settings import settings

if __name__ == "__main__":

    user_description = str(input("Введите описание желаемой аналитической системы:"))
    
    print("% Структурирование информации %")
    analytics_spec = AnalyticsSpecGenerator() \
                     .generate_spec(user_description)
    
    print("% Генерация Airflow DAG %")
    airflow_dag = AirflowDagGenerator(analytics_spec) \
                  .generate_dag()
    
    print("% Генерация DBT проекта %")
    dbt_project = DbtGenerator(analytics_spec) \
                  .generate_project()
    
    print("% Генерация Metabase Dashboard %")
    metabase_dash = MetabaseDashboardGenerator(
                        metabase_url=settings.METABASE_URL,
                        username=settings.METABASE_USERNAME,
                        password=settings.METABASE_PASSWORD) \
                    .generate_dashboard(marts_schema=dbt_project['marts'],
                                        metrics=analytics_spec.metrics)
    
    
    print("Аналитическая система готова!")