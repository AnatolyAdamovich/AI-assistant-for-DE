'''
Pydantic-классы, описывающие структуру технического задания
'''
from typing import List, Dict, Optional
from pydantic import BaseModel


class DataSource(BaseModel):
    name: str
    description: str
    type: str  # 'table', 'csv', 'api', etc.
    data_schema: Dict[str, str] # column_name: type;
    database: str | None = None
    access_method: str | None = None
    data_volume: str | None = None
    limitations: List[str] | None = None
    recommendations: List[str] | None = None
    connection_params: Dict[str, str] | None = None

class Metric(BaseModel):
    name: str
    description: str
    calculation_method: str | None = None  # SQL or pseudo-code or formula
    visualization_method: str | None = None # e.g. barchart, "гистограмма" и т.д.
    target_value: float | None = None
    alerting_rules: str | None = None

class DWH(BaseModel):
    database: str = "ClickHouse"
    environment: str = "dev"
    structure: str = "Medallion"
    limitations: str | None = None
    connection_params: Dict[str, str] | None = None
    retention_policy: Dict[str, str] | None = None

class BusinessProcess(BaseModel):
    name: str
    description: str
    schedule: str = "0 0 * * *"
    roles: List[Dict[str, str]] | None = None
    goals: List[str] | None = None
    limitations: Optional[str] | None = None

class Transformation(BaseModel):
    name: str
    logic: str # SQL or pseudo-code

class AnalyticsSpec(BaseModel):
    business_process: BusinessProcess
    data_sources: List[DataSource]
    metrics: List[Metric]
    dwh: DWH
    transformations: List[Transformation]
