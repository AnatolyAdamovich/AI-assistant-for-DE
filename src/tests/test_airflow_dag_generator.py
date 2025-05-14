from src.core.llm_generators.airflow import AirflowDagGenerator
from src.core.models.analytics import (
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
    data_schema={"order_id": "Int64", 'customer_id': "Int64", 
                 "timestamp": "datetime", "product_id": "Int64", "amount": "Float64"},
    type="table",
    database="PostgreSQL",
    recommendations=["экспортировать данные за период используя поле timestamp"]
)

metric = Metric(
    name="total_sales",
    description="Общая сумма продаж в динамике",
    calculation_method="SUM(amount)",
    visualization_method="line_chart"
)

dwh = DWH(
    database="ClickHouse",
    structure={"orders": "Таблица заказов"}
)

tr = Transformation(
    name="enrich_orders",
    logic="JOIN orders и customers по customer_id"
)

spec = AnalyticsSpec(
    business_process=bp,
    data_source=ds_orders,
    metrics=[metric],
    dwh=dwh,
    transformations=[tr]
)

airflow_llm = AirflowDagGenerator(analytics_specification=spec)

airflow_llm.fill_and_save_template()