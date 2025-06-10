import streamlit as st


st.set_page_config(page_title="Data Platform Automation", 
                   page_icon="🚀", 
                   layout="wide")


# Внизу сайдбара — контакты и информация о проекте
st.sidebar.markdown(
    """
    **Контакты:**  

    📧 tolik.frnk@yandex.ru

    📱 tg: @ffrankusha 

    ---
    © 2025 Data Platform Automation
    """
)

pg = st.navigation([
    st.Page("tabs/quick_start.py", title="Быстрый старт", icon="⚡"),
    st.Page("tabs/requirements_step.py", title="Требования"),
    st.Page("tabs/airflow_step.py", title="ELT-процесс"),
    st.Page("tabs/dbt_step.py", title="Хранилище данных"),
    st.Page("tabs/dashboard_step.py", title="Визуализация")
])

pg.run()

