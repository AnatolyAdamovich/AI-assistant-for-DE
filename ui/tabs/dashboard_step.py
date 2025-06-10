import sys
import os
import yaml

# Абсолютный путь к src относительно текущего файла
src_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if src_path not in sys.path:
    sys.path.append(src_path)

import streamlit as st
from src.core.llm_generators.dashboards import MetabaseDashboardGenerator
from src.core.models.analytics import AnalyticsSpec
from src.config.settings import settings


st.markdown(
    "<h1 style='display: flex; align-items: center;'>"
    "<img src='https://cdn-icons-png.flaticon.com/128/9074/9074033.png' width='70' style='margin-right: 10px'/>"
    "Шаг 4: Генерация Dashboard"
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
                dashboard_generator = MetabaseDashboardGenerator(
                    metabase_url=settings.METABASE_URL,
                    username=settings.METABASE_USERNAME,
                    password=settings.METABASE_PASSWORD,
                )
                
            st.success("Dashboard сгенерирован!")