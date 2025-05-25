import os
import yaml
import re
import logging
from typing import List
from langchain.prompts import ChatPromptTemplate
from langchain_core.output_parsers import JsonOutputParser
from langchain_openai import ChatOpenAI
from src.config.settings import settings
from src.config.prompts import prompts
from src.core.models.analytics import AnalyticsSpec


logger = logging.getLogger(name="DBT")

class DbtGenerator:
    def __init__(self, analytics_specification: AnalyticsSpec):
        
        self.data_sources = analytics_specification.data_sources
        self.metrics = analytics_specification.metrics
        self.transformations = analytics_specification.transformations
        self.dwh = analytics_specification.dwh

        self.parser = JsonOutputParser()
        self.llm_for_configs = ChatOpenAI(
                                model=settings.LLM_MODEL_FOR_DBT_CONFIG,
                                temperature=settings.TEMPERATURE_DBT_CONFIG,
                                max_tokens=None,
                                timeout=None,
                                max_retries=2,
                                api_key=settings.OPENAI_API_KEY,
                                base_url=settings.BASE_URL
                            )
        self.llm_for_models = ChatOpenAI(
                                model=settings.LLM_MODEL_FOR_DBT_MODEL,
                                temperature=settings.TEMPERATURE_DBT_MODEL,
                                max_tokens=None,
                                timeout=None,
                                max_retries=2,
                                api_key=settings.OPENAI_API_KEY,
                                base_url=settings.BASE_URL
                            )
    
    def _generate_profiles(self) -> dict:
        '''
        Генерация файла profiles.yml
        '''
        profiles = {
            "airflow": {
                "target": settings.DBT_TARGET,
                "outputs": {
                    settings.DBT_TARGET: {
                        "type": settings.DWH_TYPE,
                        "host": settings.DWH_HOST,
                        "user": settings.DWH_USER,
                        "pass": settings.DWH_PASS,
                        "port": settings.DWH_PORT,
                        "dbname": settings.DWH_DBNAME,
                        "schema": settings.DWH_SCHEMA,
                        "threads": settings.DWH_THREADS                    
                    }
                }
            }
        }
        logging.info("profiles.yml сгенерирован")

        return profiles

    def _generate_sources(self) -> dict:
        '''
        Генерация файла models/source.yml
        '''
        
        sources = {
            "sources": [
                {
                    "name": settings.DBT_SOURCE_NAME,
                    "schema": settings.DBT_SOURCE_SCHEMA,
                    "tables": []
                }
            ]
        }
        for ds in self.data_sources:
            table_dict = {
                "name": ds.name,
                "identifier": ds.name + "_last_data",
                "description": ds.description,
                "columns": [
                    {
                        "name": key,
                        "data_type": data_type
                    } 
                    for key, data_type in ds.data_schema.items()]
            }
            sources['sources'][0]['tables'].append(table_dict)
        
        logging.info("sources.yml сгенерирован")
        return sources

    def _generate_stage_models(self, sources: dict) -> dict:
        system_template = prompts.SYSTEM_PROMPT_DBT_MODELS_STAGE
        user_template = prompts.USER_PROMPT_DBT_MODELS_STAGE
        
        prompt_template = ChatPromptTemplate.from_messages(
                [("system", system_template),
                 ("user", user_template)]
        )

        chain = prompt_template | self.llm_for_models | self.parser
                
        result = chain.invoke(
            {
                "sources": sources
            }
        )
        logging.info("Stage-модели сгенерированы")
        return result

    def _generate_intermediate_models(self, stage_models_schema) -> List[str]:
        system_template = prompts.SYSTEM_PROMPT_DBT_MODELS_CORE
        user_template = prompts.USER_PROMPT_DBT_MODELS_CORE

        prompt_template = ChatPromptTemplate.from_messages(
                [("system", system_template),
                 ("user", user_template)]
        )

        chain = prompt_template | self.llm_for_models | self.parser
                
        result = chain.invoke(
            {
                "stage_models_schema": stage_models_schema,
                "transformations": self.transformations,
                "retention": self.dwh.retention_policy
            }
        )
        logging.info("Core-модели сгенерированы")
        return result

    def _generate_marts(self, core_models_schema):
        system_template = prompts.SYSTEM_PROMPT_DBT_MODELS_MARTS
        user_template = prompts.USER_PROMPT_DBT_MODELS_MARTS

        prompt_template = ChatPromptTemplate.from_messages(
               [("system", system_template),
                ("user", user_template)]
        )

        chain = prompt_template | self.llm_for_models | self.parser
                
        result = chain.invoke(
            {
                "core_models_schema": core_models_schema,
                "metrics": self.metrics
            }
        )
        logging.info("Marts-модели сгенерированы")

        return result

    def fill_and_save_project(self):
        # sources.yml
        sources = self._generate_sources()
        self._save_yml_from_dict(content=sources,
                                 file_path=settings.DBT_MODELS_DIR / "source.yml")
        
        # profiles.yml
        profiles = self._generate_profiles()
        self._save_yml_from_dict(content=profiles,
                                 file_path=settings.DBT_DIR / "profiles.yml")
        
        # stage/
        stage_models = self._generate_stage_models(sources)
        self._save_yml_from_dict(content=stage_models['schema_yml'],
                                 file_path=settings.DBT_MODELS_DIR / "stage" / "schema.yml")
        for model_name, sql_code in stage_models.items():
            if model_name != 'schema_yml':
                self._save_sql_model(content=sql_code,
                                     file_path=settings.DBT_MODELS_DIR / "stage" / f"{model_name}.sql")


        # core/
        core_models = self._generate_intermediate_models(stage_models_schema=stage_models['schema_yml'])
        self._save_yml_from_dict(content=core_models['schema_yml'],
                                 file_path=settings.DBT_MODELS_DIR / "core" / "schema.yml")
        for model_name, sql_code in core_models.items():
            if model_name != 'schema_yml':
                self._save_sql_model(content=sql_code,
                                     file_path=settings.DBT_MODELS_DIR / "core" / f"{model_name}.sql")

        # marts/
        marts_models = self._generate_marts(core_models_schema=core_models['schema_yml'])
        self._save_yml_from_dict(content=marts_models['schema_yml'],
                                 file_path=settings.DBT_MODELS_DIR / "marts" / "schema.yml")
        for model_name, sql_code in marts_models.items():
            if model_name != 'schema_yml':
                self._save_sql_model(content=sql_code,
                                     file_path=settings.DBT_MODELS_DIR / "marts" / f"{model_name}.sql")
            
    @staticmethod
    def _save_yml_from_str(content: str, file_path: str) -> None:
        dir_path = os.path.dirname(file_path)
        if dir_path and not os.path.exists(dir_path):
            os.makedirs(dir_path)

        with open(file_path, "w", encoding="utf-8") as f:
            f.write(content)
        logger.info(f"Файл сохранен {file_path}")
    
    @staticmethod
    def _save_yml_from_dict(content: dict, file_path: str) -> None:
        dir_path = os.path.dirname(file_path)
        if dir_path and not os.path.exists(dir_path):
            os.makedirs(dir_path)

        with open(file_path, 'w', encoding='utf-8') as f:
            yaml.dump(content, f, sort_keys=False, allow_unicode=True)
        logger.info(f"Файл сохранен {file_path}")
    
    @staticmethod
    def _save_sql_model(content: str, file_path: str) -> None:
        dir_path = os.path.dirname(file_path)
        if dir_path and not os.path.exists(dir_path):
            os.makedirs(dir_path)

        with open(file_path, "w", encoding="utf-8") as f:
            f.write(content)
        logger.info(f"Файл сохранен {file_path}")
    
    @staticmethod
    def _clean_sql_code(code_str: str) -> str:
        # убрать обрамление ``` или ```sql и оставить только содержимое
        pattern = r"```(?:sql)?\n(.*?)```"
        matches = re.findall(pattern, code_str, re.DOTALL)
        if matches:
            # если несколько блоков, объединяем их через 2 перевода строки
            return "\n\n".join(match.strip() for match in matches)
        return code_str.strip()


