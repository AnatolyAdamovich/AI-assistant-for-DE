import yaml
from typing import List
from langchain.prompts import ChatPromptTemplate
from langchain_core.output_parsers import JsonOutputParser
from langchain_openai import ChatOpenAI
from src.config.settings import settings
from src.config.prompts import prompts
from src.core.models.analytics import DataSource, Metric, Transformation, BusinessProcess, DWH, AnalyticsSpec


class AnalyticsSpecGenerator:
    def __init__(self):
        self.llm = ChatOpenAI(
                model=settings.LLM_MODEL_FOR_ANALYTICS_SPEC,
                temperature=settings.TEMPERATURE_ANALYTICS_SPEC,
                max_tokens=None,
                timeout=None,
                max_retries=2,
                api_key=settings.OPENAI_API_KEY,
                base_url=settings.BASE_URL
        )

        self.parser = JsonOutputParser()
        
    def extract_info_from_users_desription(self, user_description: str) -> dict:
        '''
        Извлечь данные из пользовательского описания (ТЗ)
        '''
        system_template = prompts.SYSTEM_PROMPT_ANALYTICS_SPEC
        user_template = prompts.USER_PROMPT_ANALYTICS_SPEC

        prompt_template = ChatPromptTemplate.from_messages(
               [("system", system_template),
                ("user", user_template)]
        )

        chain = prompt_template | self.llm | self.parser
                
        result = chain.invoke(
            {"user_description": user_description}
        )

        return self._from_dict_to_analytics_spec(result)
    # extract_info_dwh
    # extract_info_metrics
    # extract_info_transformations
    # extract_info_business_process
    # extract_info_data_sources: _from_erd _from_text, _from_query _from_api from_docs
    # recommendation_bp
    # recommendation_metrics
    # recommendation_dwh
    # save_spec_to_tyml

    @staticmethod
    def _from_dict_to_analytics_spec(content: dict) -> AnalyticsSpec:
        '''
        Заполнение объекта AnalyticsSpec из словаря
        
        '''
        business_process = BusinessProcess(**content['business_process'])
        dwh = DWH(**content['dwh'])
        data_sources = [DataSource(**ds) for ds in content['data_sources']]
        metrics = [Metric(**m) for m in content['metrics']]
        transformations = [Transformation(**t) for t in content['transformations']]

        spec = AnalyticsSpec(business_process=business_process,
                             data_sources=data_sources,
                             metrics=metrics,
                             dwh=dwh,
                             transformations=transformations)
        return spec
        
