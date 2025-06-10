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

# Функция для отображения содержимого модели
def show_model(name, content, is_yaml=False):
    label = f"{name}.yml" if is_yaml else f"{name}.sql"
    with st.expander(label):
        if is_yaml:
            if isinstance(content, bytes):
                content_str = content.decode('utf-8')
            else:
                content_str = yaml.dump(content, sort_keys=False, allow_unicode=True)
            st.code(content_str, language="yaml")
        else:
            st.code(content, language="sql")

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
                dbt_project = DbtGenerator(analytics_spec).generate_project()
            st.success("dbt-проект сгенерирован!")

            # Проходим по всем разделам проекта
            for section in ['stage', 'core', 'marts']:
                st.markdown(f"### Слой: {section}")
                models = dbt_project.get(section, {})
                for model_name, model_content in models.items():
                    if model_name == 'schema_yml':
                        show_model("schema", model_content, is_yaml=True)
                    else:
                        show_model(model_name, model_content, is_yaml=False)