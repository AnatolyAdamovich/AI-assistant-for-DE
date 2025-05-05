from src.core.llm_generators.airflow import AirflowDagGenerator
from src.core.llm_generators.dbt import DbtGenerator
from src.core.llm_generators.spec import AnalyticsSpecGenerator

if __name__ == "__main__":

    user_description = """
    Мы хотим построить аналитику для интернет-магазина.
    Основная цель — анализировать продажи и поведение покупателей, чтобы повысить выручку и оптимизировать маркетинговые кампании.
    Пользователи аналитики — менеджеры по продажам и маркетологи.

    Данные хранятся в PostgreSQL. Есть две основные таблицы:

    orders: содержит информацию о заказах (order_id, product_id, timestamp, customer_id, amount)
    customers: содержит информацию о клиентах (customer_id, name, region_id, age)
    Ключевые метрики:

    * Общая сумма продаж по дням
    * Количество уникальных покупателей по регионам
    * Средний чек

    Данные должны обновляться ежедневно, желательно ночью в промежуток между 00:00 и 05:00.
    В дальнейшем планируется добавить витрины для анализа повторных покупок и сегментации клиентов.
    Важно учитывать возможные ограничения по GDPR.
    """
    
    analytics_spec = AnalyticsSpecGenerator() \
                     .extract_info_from_users_desription(user_description)
    
    AirflowDagGenerator(analytics_spec).fill_and_save_template()
    DbtGenerator(analytics_spec).fill_and_save_project()

    print("Проект готов! Файлы сохранены в папке deploy/")