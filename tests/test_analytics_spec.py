import pytest
from pydantic import ValidationError
from core.specs import (
    BusinessProcess, 
    DataSource, 
    Metric, 
    DWH, 
    Transformation, 
    AnalyticsSpec
)


def test_analytics_spec_valid():
    bp = BusinessProcess(
        name="Анализ продаж",
        description="Анализ продаж интернет-магазина",
        roles=[{"аналитик": "анализирует данные в DWH"},
               {"менеджер": "следит за основными показателями"}],
        goals=["увеличение продаж", "оптимизация маркетинга"],
        schedule="daily",
        limitations=None
    )
    ds_orders = DataSource(
        name="orders",
        description="Таблица заказов",
        data_schema={"order_id": "Int64", 'customer_id': "Int64", 
                     "product_id": "Int64", "amount": "Float64"},
        type="table",
        database="PostgreSQL",
        access_method=None,
        limitations=None,
        recommendations=["экспортировать данные ночью"],
        connection_params={"host": "localhost", "port": "9000"}
    )
    ds_customers = DataSource(
        name="customers",
        description="Таблица клиентов",
        data_schema={"customer_id": "Int64", "name": "Text", "region_id": "Int64", 
                     "position": "Text", "Age": "Int64"},
        type="table",
        database="PostgreSQL",
        access_method=None,
        limitations=None,
        recommendations=["экспортировать данные ночью"],
        connection_params={"host": "localhost", "port": "5123"}
    )
    metric = Metric(
        name="total_sales",
        description="Общая сумма продаж в динамике",
        calculation_method="SUM(amount)",
        visualization_method="line_chart"
    )
    dwh = DWH(
        database="ClickHouse",
        structure={"orders": "Таблица заказов"},
        limitations=None,
        connection_params={"host": "localhost", "port": "9000"}
    )
    tr = Transformation(
        name="enrich_orders",
        logic="JOIN orders и customers по customer_id"
    )
    spec = AnalyticsSpec(
        business_process=bp,
        data_sources=[ds_orders, ds_customers],
        metrics=[metric],
        dwh=dwh,
        transformations=[tr]
    )
    assert spec.business_process.name == "Анализ продаж"
    assert spec.data_sources[0].database == "PostgreSQL"
    assert spec.metrics[0].visualization_method == "line_chart"
    assert spec.dwh.database == "ClickHouse"
    assert spec.transformations[0].name == "enrich_orders"

def test_missing_required_field():
    with pytest.raises(ValidationError):
        DataSource(
            name="customers",
            description="Таблица клиентов",
            data_schema={"id": "Int64"},
            # type пропущен
            database="PostgreSQL"
        )

def test_optional_fields():
    ds_customers = DataSource(
        name="customers",
        description="Таблица клиентов",
        data_schema={"id": "Int64"},
        type="table",
        database="ClickHouse"
    )
    assert ds_customers.access_method is None
    assert ds_customers.recommendations is None
    assert ds_customers.connection_params is None
    assert ds_customers.limitations is None