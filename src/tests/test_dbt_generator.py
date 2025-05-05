from src.core.llm_generators.dbt import DbtGenerator
from src.core.specs import (
    BusinessProcess, 
    DataSource, 
    Metric, 
    DWH, 
    Transformation, 
    AnalyticsSpec
)


bp = BusinessProcess(
    name="Анализ продаж",
    description="Анализ продаж интернет-магазина",
    roles=[{"аналитик": "анализирует данные в DWH"},
            {"менеджер": "следит за основными показателями"}],
    goals=["увеличение продаж", "оптимизация маркетинга"],
    schedule="ежедневно в промежуток между 12:00-16:00",
    limitations=None
)
ds_orders = DataSource(
    name="orders",
    description="Таблица заказов",
    data_schema={"order_id": "Int64",
                 "product_id": "Int64",
                 "timestamp": "datetime",
                 "customer_id": "Int64",
                 "money": "numeric"},
    type="table",
    database="PostgreSQL",
    access_method=None,
    recommendations=["использовать фильтрацию по дате (timestamp)"]
)

ds_customers = DataSource(
    name="customers",
    description="Таблица клиентов",
    data_schema={"region_id": "Int64",
                 "registration_date": "datetime",
                 "customer_id": "Int64"},
    type="table",
    database="PostgreSQL",
    access_method=None,
)

metric1 = Metric(
    name="total_sales",
    description="Общая сумма продаж в динамике по дням/неделям",
    calculation_method="SUM(amount)",
    visualization_method="line_chart"
)


metric2 = Metric(
    name="total_sales_per_region",
    description="Общая сумма продаж по регионам",
    calculation_method="SUM(amount)",
    visualization_method="line_chart"
)


dwh = DWH(
    database="ClickHouse",
    structure={"orders": "Таблица заказов"}
)

tr = Transformation(
    name="enrich_orders",
    logic="Объединить информацию о заказах и покупателях"
)

spec = AnalyticsSpec(
    business_process=bp,
    data_sources=[ds_orders, ds_customers],
    metrics=[metric1, metric2],
    dwh=dwh,
    transformations=[tr]
)

dbt_llm = DbtGenerator(analytics_specification=spec)

#dbt_llm._generate_sources()
dbt_llm.fill_and_save_project()