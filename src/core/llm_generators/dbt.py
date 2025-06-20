'''
LLM-модуль для генерации аналитического дашборда в BI-системе Metabase
'''
import os
import yaml
import re
import logging
import time
from typing import List, Any
from langchain.prompts import ChatPromptTemplate
from langchain_core.output_parsers import JsonOutputParser
from langchain_openai import ChatOpenAI
from langchain.callbacks import get_openai_callback
from src.config.settings import settings
from src.config.prompts import prompts
from src.core.models.analytics import AnalyticsSpec


logger = logging.getLogger(name="DBT")

class DbtGenerator:
    def __init__(self, 
                 analytics_specification: AnalyticsSpec,
                 with_metrics: bool = False,
                 model: str = settings.LLM_MODEL_FOR_DBT_MODEL,
                 temperature: float = settings.TEMPERATURE_DBT_MODEL):
        '''
        Инициализация генератора dbt-проекта
        
        Parameters
        ----------
        analytics_specification: AnalyticsSpec
            Спецификация аналитики, на основе которой будет генерироваться DAG
        with_metrics: bool, optional
            Флаг, указывающий, нужно ли включать подсчёт метрик при использовании LLM (по умолчанию False)
        model: str, optional
            Название модели LLM (по умолчанию settings.LLM_MODEL_FOR_ANALYTICS_SPEC).
        temperature: float, optional
            Параметр температуры для модели LLM (по умолчанию settings.TEMPERATURE_ANALYTICS_SPEC).
        '''

        self.with_metrics = with_metrics

        self.data_sources = analytics_specification.data_sources
        self.metrics = analytics_specification.metrics
        self.transformations = analytics_specification.transformations
        self.dwh = analytics_specification.dwh

        self.parser = JsonOutputParser()
        
        self.llm_for_models = ChatOpenAI(
                                model=model,
                                temperature=temperature,
                                max_tokens=None,
                                timeout=None,
                                max_retries=2,
                                api_key=settings.OPENAI_API_KEY,
                                base_url=settings.BASE_URL
                            )
        logger.info(f"Используется {self.llm_for_models.model_name} с температурой {self.llm_for_models.temperature}")
    
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

    def _generate_stage_models(self, 
                               sources: dict[str, Any]) -> dict[str, Any]:
        '''
        Генерация dbt-моделей на stage-слое

        Parameters
        ----------
        sources: dict[str, Any]
            Описание источников данных в dbt-проекте (файл source.yml)
        '''
        system_template = prompts.SYSTEM_PROMPT_DBT_MODELS_STAGE
        user_template = prompts.USER_PROMPT_DBT_MODELS_STAGE
        
        prompt_template = ChatPromptTemplate.from_messages(
                [("system", system_template),
                 ("user", user_template)]
        )

        chain = prompt_template | self.llm_for_models | self.parser
        if self.with_metrics:
            with get_openai_callback() as cb:
                start_time = time.time()
                result = chain.invoke(
                    {
                        "sources": sources
                    }
                )
                end_time = time.time()
                generation_time = end_time - start_time
                
                total_tokens = cb.total_tokens
                prompt_tokens = cb.prompt_tokens
                completion_tokens = cb.completion_tokens
                
                total_cost = self._count_cost(prompt_tokens, completion_tokens,
                                              self.llm_for_models.model_name)

                logger.info(
                    "Токены: всего=%d, prompt=%d, completion=%d; Стоимость=%.3f ₽; Время=%.2f сек",
                    total_tokens, prompt_tokens, completion_tokens, total_cost, generation_time
                )
        else:
            result = chain.invoke(
                {
                    "sources": sources
                }
            )
        
        logging.info("Stage-модели сгенерированы")
        return result
    
    def _generate_intermediate_models(self, 
                                      stage_models_schema: dict[str, Any]) -> dict[str, Any]:
        '''
        Генерация dbt-моделей на core-слое

        Parameters
        ----------
        stage_models_schema: dict[str, Any]
            Описание структуры dbt-моделей на stage-слое
        '''
        system_template = prompts.SYSTEM_PROMPT_DBT_MODELS_CORE
        user_template = prompts.USER_PROMPT_DBT_MODELS_CORE

        prompt_template = ChatPromptTemplate.from_messages(
                [("system", system_template),
                 ("user", user_template)]
        )

        chain = prompt_template | self.llm_for_models | self.parser

        if self.with_metrics:
            with get_openai_callback() as cb:
                start_time = time.time()
                result = chain.invoke(
                    {
                        "stage_models_schema": stage_models_schema,
                        "transformations": self.transformations,
                        "retention": self.dwh.retention_policy
                    }
                )
                end_time = time.time()
                generation_time = end_time - start_time
                total_tokens = cb.total_tokens
                prompt_tokens = cb.prompt_tokens
                completion_tokens = cb.completion_tokens
                
                total_cost = self._count_cost(prompt_tokens, completion_tokens,
                                              self.llm_for_models.model_name)
                
                logger.info(
                    "Токены: всего=%d, prompt=%d, completion=%d; Стоимость=%.3f ₽; Время=%.2f сек",
                    total_tokens, prompt_tokens, completion_tokens, total_cost, generation_time
                )
        else:
            result = chain.invoke(
                {
                    "stage_models_schema": stage_models_schema,
                    "transformations": self.transformations,
                    "retention": self.dwh.retention_policy
                }
            )


        logging.info("Core-модели сгенерированы")
        return result

    def _generate_marts(self, 
                        core_models_schema: dict[str, Any]) -> dict[str, Any]:
        '''
        Генерация dbt-моделей на marts-слое

        Parameters
        ----------
        core_models_schema: dict[str, Any]
            Описание структуры dbt-моделей на core-слое
        '''
        system_template = prompts.SYSTEM_PROMPT_DBT_MODELS_MARTS
        user_template = prompts.USER_PROMPT_DBT_MODELS_MARTS

        prompt_template = ChatPromptTemplate.from_messages(
               [("system", system_template),
                ("user", user_template)]
        )

        chain = prompt_template | self.llm_for_models | self.parser
        
        if self.with_metrics:
            with get_openai_callback() as cb:
                start_time = time.time()
                result = chain.invoke(
                    {
                        "core_models_schema": core_models_schema,
                        "metrics": self.metrics
                    }
                )
                end_time = time.time()
                generation_time = end_time - start_time
                total_tokens = cb.total_tokens
                prompt_tokens = cb.prompt_tokens
                completion_tokens = cb.completion_tokens
                
                total_cost = self._count_cost(prompt_tokens, completion_tokens,
                                              self.llm_for_models.model_name)

                logger.info(
                    "Токены: всего=%d, prompt=%d, completion=%d; Стоимость=%.3f ₽; Время=%.2f сек",
                    total_tokens, prompt_tokens, completion_tokens, total_cost, generation_time
                )
        else:
            result = chain.invoke(
                {
                    "core_models_schema": core_models_schema,
                    "metrics": self.metrics
                }
            )
        
        logging.info("Marts-модели сгенерированы")

        return result

    def generate_project(self) -> dict[str, Any]:
        '''
        Полный цикл генерации проекта (profiles + sources + stage + core + marts)
        '''
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
        return {'stage': stage_models, 'core': core_models, 'marts': marts_models}

    @staticmethod
    def _save_yml_from_str(content: str, 
                           file_path: str) -> None:
        '''
        Сохранение строки в файл с расширением yml

        Parameters
        ----------
        content: str
            Строка, которую надо сохранить
        file_path: str
            Путь, куда сохранять файл
        '''
        dir_path = os.path.dirname(file_path)
        if dir_path and not os.path.exists(dir_path):
            os.makedirs(dir_path)

        with open(file_path, "w", encoding="utf-8") as f:
            f.write(content)
        logger.info(f"Файл сохранен {file_path}")
    
    @staticmethod
    def _save_yml_from_dict(content: dict, 
                            file_path: str) -> None:
        '''
        Сохранение словаря в файл с расширением yml

        Parameters
        ----------
        content: dict
            Словарь, который надо сохранить
        file_path: str
            Путь, куда сохранять файл
        '''
        dir_path = os.path.dirname(file_path)
        if dir_path and not os.path.exists(dir_path):
            os.makedirs(dir_path)

        with open(file_path, 'w', encoding='utf-8') as f:
            yaml.dump(content, f, sort_keys=False, allow_unicode=True)
        logger.info(f"Файл сохранен {file_path}")
    
    @staticmethod
    def _save_sql_model(content: str, 
                        file_path: str) -> None:
        '''
        Сохранение строки в sql-модель

        Parameters
        ----------
        content: str
            Строка, которую надо сохранить
        file_path: str
            Путь, куда сохранять файл
        '''
        dir_path = os.path.dirname(file_path)
        if dir_path and not os.path.exists(dir_path):
            os.makedirs(dir_path)

        with open(file_path, "w", encoding="utf-8") as f:
            f.write(content)
        logger.info(f"Файл сохранен {file_path}")
    
    @staticmethod
    def _clean_sql_code(code_str: str) -> str:
        '''
        Очистить сгенерированную строку от обрамления
        
        Parameters
        ----------
        code_str: str
            Код, который надо почистить
        '''
        # убрать обрамление ``` или ```sql и оставить только содержимое
        pattern = r"```(?:sql)?\n(.*?)```"
        matches = re.findall(pattern, code_str, re.DOTALL)
        if matches:
            # если несколько блоков, объединяем их через 2 перевода строки
            return "\n\n".join(match.strip() for match in matches)
        return code_str.strip()

    @staticmethod
    def _count_cost(prompt_tokens: int, 
                    completion_tokens: int,
                    model_name: str):
        '''
        Подсчитать стоимость запроса

        Parameters
        ----------
        prompt_tokens : int
            Количество входящих токенов
        completion_tokens: int
            Количество выходящих токенов
        model_name : str
            LLM-модель
        '''
        pricing = settings.LLM_PRICING[model_name]
        return (prompt_tokens * pricing["input"]) / 1000 + (completion_tokens * pricing["output"]) / 1000
