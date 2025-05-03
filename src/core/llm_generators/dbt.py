import re
from typing import List
from langchain.prompts import ChatPromptTemplate
from langchain_openai import ChatOpenAI
from src.config.settings import settings
from src.core.specs import AnalyticsSpec


class DbtGenerator:
    def __init__(self, analytics_specification: AnalyticsSpec):
        
        self.data_source = analytics_specification.data_source
        self.metrics = analytics_specification.metrics

        self.llm_for_configs = ChatOpenAI(
                                model=settings.LLM_MODEL_FOR_AIRFLOW_MOVING_DATA,
                                temperature=settings.TEMPERATURE_AIRFLOW_MOVING_DATA,
                                max_tokens=None,
                                timeout=None,
                                max_retries=2,
                                api_key=settings.OPENAI_API_KEY,
                                base_url=settings.BASE_URL
                            )
        self.llm_for_models = ChatOpenAI(
                                model=settings.LLM_MODEL_FOR_AIRFLOW_ARGS,
                                temperature=settings.TEMPERATURE_AIRFLOW_ARGS,
                                max_tokens=None,
                                timeout=None,
                                max_retries=2,
                                api_key=settings.OPENAI_API_KEY,
                                base_url=settings.BASE_URL
                            )
    def _generate_dbt_project(self):
        pass
    
    def _generate_profiles(self):
        pass

    def _generate_sources(self):
        pass
    
    def _generate_schemas(self):
        pass

    def _generate_stage_models(self):
        pass

    def _generate_intermediate_models(self) -> List[str]:
        pass

    def _generate_marts(self):
        pass

    def fill_and_save_project(self):
        pass