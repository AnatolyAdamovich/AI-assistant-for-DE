import sys
import os

# Абсолютный путь к src относительно текущего файла
src_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if src_path not in sys.path:
    sys.path.append(src_path)

import streamlit as st
from src.core.models.analytics import AnalyticsSpec
from src.config.settings import settings
from src.core.llm_generators.specification import AnalyticsSpecGenerator
from src.core.llm_generators.airflow import AirflowDagGenerator
from src.core.llm_generators.dbt import DbtGenerator
from src.core.llm_generators.dashboards import MetabaseDashboardGenerator



st.markdown(
    """
    <h1 style='display: flex; align-items: center;'>
        <img src='https://cdn-icons-png.flaticon.com/128/12122/12122378.png' width='70' style='margin-right: 10px'/>
        Data Platform Automation
    </h1>
    """,
    unsafe_allow_html=True
)

st.write(
    """
    Используйте меню слева для перехода между этапами.

    Для быстрого старта воспользуйтесь текущей вкладкой.

    **Пошаговый процесс:**
    1. Описываете вашу задачу и бизнес-процесс.
    2. AI автоматически генерирует техническое задание.
    3. AI автоматически генерирует DAG для Airflow.
    4. AI автоматически генерирует dbt-проект для моделирования данных.
    5. AI автоматически генерирует создает дашборда в Metabase.

    Просто опишите вашу задачу ниже — и AI всё сделает за вас!
    """
)

business_desc = st.text_area(
    "Опишите желаемую аналитическую систему",
    height=300,
    placeholder="Например: Аналитика по продажам интернет-магазина..."
)

if st.button("Запустить AI 🚀"):
    with st.spinner("Обработка ..."):
        analytics_spec = AnalyticsSpecGenerator().generate_spec(business_desc)
        airflow_dag = AirflowDagGenerator(analytics_spec).generate_dag()
        dbt_project = DbtGenerator(analytics_spec).generate_project()
        metabase_dash = MetabaseDashboardGenerator(
            metabase_url=settings.METABASE_URL,
            username=settings.METABASE_USERNAME,
            password=settings.METABASE_PASSWORD).generate_dashboard(marts_schema=dbt_project['marts'],
                                                                    metrics=analytics_spec.metrics)
    st.success("Система сгенерирована!")
    



    


