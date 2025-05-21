import streamlit as st
import yaml
from src.core.models.analytics import AnalyticsSpec
from src.core.llm_generators.specification import AnalyticsSpecGenerator
from src.core.llm_generators.airflow import AirflowDagGenerator
from src.core.llm_generators.dbt import DbtGenerator

st.set_page_config(page_title="Data Platform Automation", layout="wide")

st.title("Data Platform Automation")

# Навигация по этапам
tabs = st.tabs(["1. Analytics Spec", "2. Airflow DAG", "3. dbt Project"])

# 1. Analytics Spec
with tabs[0]:
    st.header("Шаг 1: Описание аналитической системы")
    # Пример простого ввода (можно расширить до пошагового мастера)
    business_desc = st.text_area("Описание")
    # ...и так далее для других полей
    if st.button("Сгенерировать AnalyticsSpec"):
        # Здесь вызываем генератор
        analytics_spec_generated = AnalyticsSpecGenerator().extract_info_from_users_desription(business_desc)
        st.success("AnalyticsSpec сгенерирован!")
        st.json(analytics_spec_generated.model_dump())

# 2. Airflow DAG
with tabs[1]:
    st.header("Шаг 2: Генерация Airflow DAG")
    # Можно загрузить AnalyticsSpec или выбрать из ранее сгенерированных
    uploaded_spec = st.file_uploader("Загрузите AnalyticsSpec (json/yaml)")
    if uploaded_spec:
        data = yaml.safe_load(uploaded_spec)
        analytics_spec = AnalyticsSpec(**data)
        if analytics_spec and st.button("Сгенерировать DAG"):
            # Пример: парсим spec и генерируем DAG
            AirflowDagGenerator(analytics_spec).generate_dag()
            st.success("Airflow Dag сгенерирован!")

# 3. dbt Project
with tabs[2]:
    st.header("Шаг 3: Генерация dbt-проекта")
    # Аналогично: загрузка spec, генерация dbt
    if st.button("Сгенерировать dbt-проект"):
        DbtGenerator(analytics_spec).fill_and_save_project()
        st.success("dbt-проект сгенерирован!")
        