'''
LLM-модуль для извлечения структурированного ТЗ из пользовательского описания
'''
import os
import yaml
import logging
import time
import re
from langchain.prompts import ChatPromptTemplate
from langchain_core.output_parsers import JsonOutputParser
from langchain_openai import ChatOpenAI
from langchain.callbacks import get_openai_callback
from src.config.settings import settings
from src.config.prompts import prompts
from src.core.models.analytics import DataSource, Metric, Transformation, BusinessProcess, DWH, AnalyticsSpec

logger = logging.getLogger(name="SPECIFICATION")

class AnalyticsSpecGenerator:
    def __init__(self, 
                 with_metrics: bool = False,
                 model: str = settings.LLM_MODEL_FOR_ANALYTICS_SPEC,
                 temperature: float = settings.TEMPERATURE_ANALYTICS_SPEC):
        '''
        Инициализация генератора ТЗ на базе пользовательского описания
        
        Parameters
        ----------
        with_metrics: bool, optional
            Флаг, указывающий, нужно ли включать подсчёт метрик при использовании LLM (по умолчанию False)
        model: str, optional
            Название модели LLM (по умолчанию settings.LLM_MODEL_FOR_ANALYTICS_SPEC).
        temperature: float, optional
            Параметр температуры для модели LLM (по умолчанию settings.TEMPERATURE_ANALYTICS_SPEC).
        '''
        
        self.with_metrics = with_metrics
        self.llm = ChatOpenAI(
            model=model,
            temperature=temperature,
            max_tokens=None,
            timeout=None,
            max_retries=2,
            api_key=settings.OPENAI_API_KEY,
            base_url=settings.BASE_URL
        )
        self.parser = JsonOutputParser()
        logger.info(f"Используется {self.llm.model_name} с температурой {self.llm.temperature}")
        
    def extract_info_from_users_desription(self, 
                                           user_description: str) -> dict:
        '''
        Извлечь данные в структурированном виде (см. src.models.analytics) 
        из пользовательского описания на естественном языке.

        Parameters
        ----------
        user_description: str
            Пользовательское описание на естественном языке
        '''
        system_template = prompts.SYSTEM_PROMPT_ANALYTICS_SPEC
        user_template = prompts.USER_PROMPT_ANALYTICS_SPEC
        
        prompt_template = ChatPromptTemplate.from_messages(
               [("system", system_template),
                ("user", user_template)]
        )

        chain = prompt_template | self.llm | self.parser
        
        if self.with_metrics:
            with get_openai_callback() as cb:
                start_time = time.time()
                result = chain.invoke(
                    {
                        "user_description": user_description
                    }
                )
                end_time = time.time()
                generation_time = end_time - start_time
                total_tokens = cb.total_tokens
                prompt_tokens = cb.prompt_tokens
                completion_tokens = cb.completion_tokens

                total_cost = self._count_cost(prompt_tokens, completion_tokens,
                                              self.llm.model_name)

                logger.info(
                    "Токены: всего=%d, prompt=%d, completion=%d; Стоимость=%.3f ₽; Время=%.2f сек",
                    total_tokens, prompt_tokens, completion_tokens, total_cost, generation_time
                )
        else:
            result = chain.invoke(
                    {
                        "user_description": user_description
                    }
            )
        
        logger.info("ТЗ извлечено из пользовательского описания")
        
        return result
        
    def extract_info_from_users_answers_legacy(self, 
                                        user_answers: dict[str, str]) -> AnalyticsSpec:
        '''
        [DEPRECATED] Извлечь данные в структурированном виде (см. src.models.analytics) 
        из пользовательских ответов на вопросы.

        Parameters
        ----------
        user_answers: dict[str, str]
            Ответы пользователя на вопросы бота об аналитической системе
        '''
        system_template = prompts.SYSTEM_PROMPT_ANALYTICS_SPEC_ANSWERS
        user_template = prompts.USER_PROMPT_ANALYTICS_SPEC_ANSWERS
        
        prompt_template = ChatPromptTemplate.from_messages(
               [("system", system_template),
                ("user", user_template)]
        )

        chain = prompt_template | self.llm | self.parser
        
        result = chain.invoke(
            {"user_answers": user_answers}
        )
        # сохранение в файл
        deployment_path = settings.ARTIFACTS_DIRECTORY / "analytics_spec.yml"
        self._save_dict_to_yml(content=result, filepath=deployment_path)

        # парсинг в объект класса AnalyticsSpec
        spec = self._from_dict_to_analytics_spec(result)

        return spec
      
    def recommendations(self, 
                        business_process: BusinessProcess, 
                        transformations: list[Transformation],
                        metrics: list[Metric]) -> list[Metric]:
        '''
        Генерация рекомендаций по метрикам и преобразованиям 
        на основе бизнес-процесса и пользовательских метрик
        
        Parameters
        ----------
        business_process: BusinessProcess
            Описание бизнес-процесса
        transformations: list[Transformation]
            Массив с описанием преобразований
        metrics: list[Metric]
            Массив с описанием метрик
        '''
        system_template = prompts.SYSTEM_PROMPT_RECOMMENDATION
        user_template = prompts.USER_PROMPT_RECOMMENDATION
        
        prompt_template = ChatPromptTemplate.from_messages(
               [("system", system_template),
                ("user", user_template)]
        )

        chain = prompt_template | self.llm | self.parser
        if self.with_metrics:
            with get_openai_callback() as cb:
                start_time = time.time()
                result = chain.invoke(
                    {
                        "business_process": business_process,
                        "transformations": transformations,
                        "metrics": metrics
                    }
                ) 
                end_time = time.time()
                generation_time = end_time - start_time
                total_tokens = cb.total_tokens
                prompt_tokens = cb.prompt_tokens
                completion_tokens = cb.completion_tokens

                total_cost = self._count_cost(prompt_tokens, completion_tokens,
                                              self.llm.model_name)

                logger.info(
                    "Токены: всего=%d, prompt=%d, completion=%d; Стоимость=%.3f ₽; Время=%.2f сек",
                    total_tokens, prompt_tokens, completion_tokens, total_cost, generation_time
                )
        else:
            result = chain.invoke(
                {
                    "business_process": business_process,
                    "transformations": transformations,
                    "metrics": metrics
                }
            )   
        
        logger.info("Сгенерированы рекомендации")
        
        return result
    
    def generate_spec(self, 
                      user_description: str,
                      with_recommendations: bool = True) -> AnalyticsSpec:
        '''
        Полный цикл создания структурированного ТЗ

        Parameters
        ----------
        user_description: str
            Пользовательское описание на естественном языке
        with_recommendations: bool, optional
            Флаг, указывающий, нужно ли дополнительно генерировать рекоммендации по метрикам
            и преобразованиям (по умолчанию True)
        '''
        
        result = self.extract_info_from_users_desription(user_description)
        # парсинг в объект класса AnalyticsSpec
        spec = self._from_dict_to_analytics_spec(result)

        if with_recommendations:
            recommendations = self.recommendations(metrics=spec.metrics,
                                                   business_process=spec.business_process,
                                                   transformations=spec.transformations)
            result.update(recommendations)
            spec = self._from_dict_to_analytics_spec(result)
        
        # сохранение в файл
        deployment_path = settings.ARTIFACTS_DIRECTORY / "analytics_spec.yml"
        self._save_dict_to_yml(content=result, filepath=deployment_path)
    
        return spec
    
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
    
    @staticmethod
    def _clean_json(json_str: str) -> str:
        '''
        Убрать обрамление ```json (если LLM генерирует markdown)

        Parameters
        ----------
        json_str : str
            Строка в json-формате с возможным обрамлением
        '''
        json_str = json_str.content
        pattern = r"```(?:json)?\n(.*?)```"
        matches = re.findall(pattern, json_str, re.DOTALL)
        if matches:
            # если несколько блоков, объединяем их через 2 перевода строки
            return "\n\n".join(match.strip() for match in matches)
        return json_str.strip()
    
    @staticmethod
    def _from_dict_to_analytics_spec(content: dict) -> AnalyticsSpec:
        '''
        Заполнение объекта AnalyticsSpec из словаря

        Parameters
        ----------
        content : dict
            Словарь, из которого будет сформирован объект класса AnalyticsSpec.
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
        
        logger.info(f"Объект AnalyticsSpec загружен из словаря")
        return spec

    @staticmethod
    def _from_yml_to_analytics_spec(filepath: str) -> AnalyticsSpec:
        '''
        Заполнение объекта AnalyticsSpec из файла yml
        
        Parameters
        ----------
        filepath : str
            Путь к yml файлу
        '''
        with open(filepath, encoding='utf-8') as f:
            data_from_file = yaml.safe_load(f)
        spec = AnalyticsSpec(**data_from_file)
        
        logger.info(f"Объект AnalyticsSpec загружен из файла {filepath}")
        return spec
    
    @staticmethod
    def _save_dict_to_yml(content: dict, 
                          filepath: str) -> None:
        '''
        Сохранение словаря в yml-файл
        
        Parameters
        ----------
        content : dict
            Словарь, который требуется сохранить
        filepath: str
            Путь для сохранения
        '''
        dir_path = os.path.dirname(filepath)
        if dir_path and not os.path.exists(dir_path):
            os.makedirs(dir_path)

        with open(filepath, "w", encoding="utf-8") as f:
            yaml.dump(content, f, allow_unicode=True, sort_keys=False)
        
        logger.info(f'Извлеченное ТЗ сохранено в {filepath}')