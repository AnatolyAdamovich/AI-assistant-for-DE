import yaml
import logging
from langchain.prompts import ChatPromptTemplate
from langchain_core.output_parsers import JsonOutputParser
from langchain_openai import ChatOpenAI
from src.config.settings import settings
from src.config.prompts import prompts
from src.core.models.analytics import DataSource, Metric, Transformation, BusinessProcess, DWH, AnalyticsSpec

logger = logging.getLogger(name="SPECIFICATION")

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

    def extract_info_from_users_desription(self, 
                                           user_description: str,
                                           with_reccomendations: bool = True) -> AnalyticsSpec:
        '''
        Извлечь данные в структурированном виде (см. src.models.analytics) 
        из пользовательского описания на естественном языке.

        Parameters
        ----------
        user_description : str
            Пользовательское описание на естественном языке
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
        logger.info("ТЗ извлечено из пользовательского описания")
        
        if with_reccomendations:
            recommendations = self.recommendations()
            result.update(recommendations)
        # сохранение в файл
        deployment_path = settings.ARTIFACTS_DIRECTORY / "analytics_spec.yml"
        self._save_dict_to_yml(content=result, filepath=deployment_path)

        # парсинг в объект класса AnalyticsSpec
        spec = self._from_dict_to_analytics_spec(result)

        return spec
    
    def extract_info_from_users_answers(self, 
                                        user_answers: dict[str, str]) -> AnalyticsSpec:
        '''
        Извлечь данные в структурированном виде (см. src.models.analytics) 
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
        
        result = chain.invoke(
            {"business_process": business_process,
             "transformations": transformations,
             "metrics": metrics}
        )

        logger.info("Сгенерированы рекомендации")
        return result

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
    def _save_dict_to_yml(content: dict, filepath: str) -> None:
        '''
        Сохранение словаря в yml-файл
        
        Parameters
        ----------
        content : dict
            Словарь, который требуется сохранить
        filepath: str
            Путь для сохранения
        '''
        
        with open(filepath, "w", encoding="utf-8") as f:
            yaml.dump(content, f, allow_unicode=True, sort_keys=False)
        
        logger.info(f'Извлеченное ТЗ сохранено в {filepath}')