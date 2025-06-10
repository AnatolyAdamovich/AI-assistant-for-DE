import sys
import os
import yaml

# Абсолютный путь к src относительно текущего файла
src_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if src_path not in sys.path:
    sys.path.append(src_path)

import streamlit as st
from src.core.llm_generators.airflow import AirflowDagGenerator
from src.core.models.analytics import AnalyticsSpec

st.markdown(
    "<h1 style='display: flex; align-items: center;'>"
    "<img src='https://cdn-icons-png.flaticon.com/128/4293/4293688.png' width='70' style='margin-right: 10px'/>"
    "Шаг 2: Генерация Airflow DAG"
    "</h1>",
    unsafe_allow_html=True
)

uploaded_spec = st.file_uploader("Загрузите AnalyticsSpec (yaml):")
if uploaded_spec:
    data = yaml.safe_load(uploaded_spec)
    analytics_spec = AnalyticsSpec(**data)
    if analytics_spec:
        if st.button("Запуск AI 🚀"):
            with st.spinner("Обработка ..."):
                # Пример: парсим spec и генерируем DAG
                dag_generator = AirflowDagGenerator(analytics_spec).generate_dag()
            st.success("Airflow Dag сгенерирован!")