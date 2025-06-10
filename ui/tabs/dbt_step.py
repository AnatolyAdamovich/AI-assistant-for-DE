import sys
import os
import yaml

# Абсолютный путь к src относительно текущего файла
src_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if src_path not in sys.path:
    sys.path.append(src_path)

import streamlit as st
from src.core.llm_generators.dbt import DbtGenerator
from src.core.models.analytics import AnalyticsSpec


st.markdown(
    "<h1 style='display: flex; align-items: center;'>"
    "<img src='https://cdn-icons-png.flaticon.com/128/9672/9672242.png' width='70' style='margin-right: 10px'/>"
    "Шаг 3: Генерация DBT-проекта"
    "</h1>",
    unsafe_allow_html=True
)

uploaded_spec = st.file_uploader("Загрузите AnalyticsSpec (yaml)")
if uploaded_spec:
    data = yaml.safe_load(uploaded_spec)
    analytics_spec = AnalyticsSpec(**data)
    if analytics_spec:
        if st.button("Запуск AI 🚀"):
            with st.spinner("Обработка ..."):
                dbt_generator = DbtGenerator(analytics_spec).fill_and_save_project()
            st.success("dbt-проект сгенерирован!")