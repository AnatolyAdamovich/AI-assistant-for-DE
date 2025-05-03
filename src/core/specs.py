from typing import List, Dict, Optional
from pydantic import BaseModel


class DataSource(BaseModel):
    name: str
    description: str
    data_schema: Dict[str, str] # column_name: type
    type: str  # 'table', 'csv', 'api', etc.
    database: str
    access_method: str | None = None
    limitations: Optional[str] | None = None
    recommendations: List[str] | None = None
    connection_params: Dict[str, str] | None = None

class Metric(BaseModel):
    name: str
    description: str
    calculation_method: str | None = None  # SQL or pseudo-code
    visualization_method: str | None = None # e.g. barchart, "гистограмма" и т.д.

class DWH(BaseModel):
    database: str
    structure: Dict[str, str] | None = None
    limitations: Optional[str] | None = None
    connection_params: Dict[str, str] | None = None

class BusinessProcess(BaseModel):
    name: str
    description: str
    schedule: str
    roles: List[Dict[str, str]] | None = None
    goals: List[str] | None = None
    limitations: Optional[str] | None = None

class Transformation(BaseModel):
    name: str
    logic: str # SQL or pseudo-code

class AnalyticsSpec(BaseModel):
    business_process: BusinessProcess
    data_source: DataSource
    metrics: List[Metric]
    dwh: DWH
    transformations: List[Transformation]