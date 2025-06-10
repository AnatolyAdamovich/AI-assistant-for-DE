'''
LLM-модуль для генерации ETL/ELT-процессов
'''
import os
import logging
import re
import time
from jinja2 import Template
from langchain.prompts import ChatPromptTemplate, FewShotChatMessagePromptTemplate
from langchain_openai import ChatOpenAI
from langchain.callbacks import get_openai_callback
from langchain_core.output_parsers import JsonOutputParser
from src.config.settings import settings
from src.config.prompts import prompts
from src.core.models.analytics import AnalyticsSpec


logger = logging.getLogger(name="AIRFLOW")

class AirflowDagGenerator:
    def __init__(self, 
                 analytics_specification: AnalyticsSpec,
                 template_path: str = settings.TEMPLATE_DAG_PATH,
                 requirements_path: str = settings.REQUIREMENTS_PATH,
                 with_metrics: bool = False,
                 model: str = settings.LLM_MODEL_FOR_AIRFLOW_MOVING_DATA,
                 temperature: float = settings.TEMPERATURE_AIRFLOW_MOVING_DATA):
        '''
        Инициализация генератора Airflow DAG.

        Parameters
        ----------
        analytics_specification: AnalyticsSpec
            Спецификация аналитики, на основе которой будет генерироваться DAG
        template_path: str, optional
            Путь к шаблону DAG (по умолчанию settings.TEMPLATE_DAG_PATH)
        requirements_path: str, optional
            Путь к файлу с зависимостями (по умолчанию settings.REQUIREMENTS_PATH)
        with_metrics: bool, optional
            Флаг, указывающий, нужно ли включать подсчёт метрик при использовании LLM (по умолчанию False)
        model: str, optional
            Название модели LLM (по умолчанию settings.LLM_MODEL_FOR_AIRFLOW_MOVING_DATA).
        temperature: float, optional
            Параметр температуры для модели LLM (по умолчанию settings.TEMPERATURE_AIRFLOW_MOVING_DATA).
        '''
        self.with_metrics = with_metrics

        self.data_sources = analytics_specification.data_sources
        self.business_process = analytics_specification.business_process
        self.dwh = analytics_specification.dwh
        
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

        with open(template_path, "r", encoding="utf-8") as f:
            self.pipeline_template = f.read()
        
        with open(requirements_path, "r", encoding="utf-8") as f:
            self.requirements = f.read()
         
    def _generate_dag_args(self) -> dict[str, str]:
        '''
        Генерация аргументов для Airflow DAG.
        '''
        system_template = prompts.SYSTEM_PROMPT_AIRFLOW_ARGS
        user_template = prompts.USER_PROMPT_AIRFLOW_ARGS
        
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
                        "business_process": self.business_process
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
                    "business_process": self.business_process
                }
            )

        logger.info("Аргументы для DAG сгенерированы")
        return result

    def _generate_moving_data_function(self) -> str:
        '''
        Генерация функции перемещения данных из источника в хранилище
        '''
        system_template = prompts.SYSTEM_PROMPT_AIRFLOW_MOVING_DATA
        user_template = prompts.USER_PROMPT_AIRFLOW_MOVING_DATA

        example = [
            {"input": prompts.AIRFLOW_MOVING_DATA_EXAMPLE_INPUT,
             "output": prompts.AIRFLOW_MOVING_DATA_EXAMPLE_OUTPUT},
        ]
        example_prompt = ChatPromptTemplate.from_messages(
            [
                ("user", "{input}"),
                ("ai", "{output}"),
            ]
        )
        few_shot_prompt = FewShotChatMessagePromptTemplate(
            example_prompt=example_prompt,
            examples=example,
        )
        prompt_template = ChatPromptTemplate.from_messages(
               [("system", system_template),
                few_shot_prompt,
                ("user", user_template)]
        )

        chain = prompt_template | self.llm | self.parser
        
        if self.with_metrics:
            with get_openai_callback() as cb:
                start_time = time.time()
                result = chain.invoke(
                    {
                        "data_sources": self.data_sources,
                        "dwh": self.dwh,
                        "requirements": self.requirements
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
                    "data_sources": self.data_sources,
                    "dwh": self.dwh,
                    "requirements": self.requirements
                }
            )

        logger.info("Функция moving_data_from_source_to_dwh сгенерирована")
        return result

    def _generate_dag_args_legacy(self) -> str:
        '''
        [DEPRECATED] Функция для генерации аргументов Airflow DAG
        '''
        
        system_template = prompts.SYSTEM_PROMPT_AIRFLOW_ARGS
        user_template = prompts.USER_PROMPT_AIRFLOW_ARGS
        
        prompt_template = ChatPromptTemplate.from_messages([
            ("system", system_template),
            ("user", user_template)
        ])
        chain = prompt_template | self.llm

        result = chain.invoke({
            "name": getattr(self.business_process, "name", "Анализ"),
            "schedule": getattr(self.business_process, "schedule", "0 0 * * *"),
        })
        
        return self._clean_code(result.content)

    def _generate_moving_data_function_legacy(self) -> str:
        '''
        [DEPRECATED] Функция для генерация таски перемещения данных из источника в хранилище
        '''
        system_template = prompts.SYSTEM_PROMPT_AIRFLOW_MOVING_DATA
        user_template = prompts.USER_PROMPT_AIRFLOW_MOVING_DATA
        

        prompt_template = ChatPromptTemplate.from_messages(
            [("system", system_template),
             ("user", user_template)]
        )

        chain = prompt_template | self.llm

        result = chain.invoke(
            {"data_sources": self.data_sources}
        )
        cleaned_code = self._clean_code(result.content)
        return self._indent_code_block(cleaned_code, indent=4)

    def generate_dag(self) -> None:
        '''
        Полный цикл генерации (аргументы + функция перемещения данных + заполнение template)
        '''
        dag_args = self._generate_dag_args()
        moving_function_code = self._generate_moving_data_function()
        
        arguments = dag_args | moving_function_code
        dag_code = self._render_dag(
            pipeline_template=self.pipeline_template,
            arguments=arguments
        )
        logger.info("Шаблон DAG заполнен")
        self._save_code_to_file(code=dag_code, name=dag_args["dag_name"] + ".py")
        return dag_code

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
            Название LLM-модели
        '''
        pricing = settings.LLM_PRICING[model_name]
        return (prompt_tokens * pricing["input"]) / 1000 + (completion_tokens * pricing["output"]) / 1000

    @staticmethod
    def _clean_code(code_str: str) -> str:
        '''
        Убрать обрамление ``` или ```python (LLM периодически генерит markdown)

        Parameters
        ----------
        code_str: str
            Сгенерированный код в формате строки
        '''
        pattern = r"```(?:python)?\n(.*?)```"
        matches = re.findall(pattern, code_str, re.DOTALL)
        if matches:
            # если несколько блоков, объединяем их через 2 перевода строки
            return "\n\n".join(match.strip() for match in matches)
        return code_str.strip()
    
    @staticmethod
    def _indent_code_block(code_str: str, indent: int) -> str:
        '''
        Внести отступы в блоки кода
        
        Parameters
        ----------
        code_str: str
            Код
        indent: int
            Количество пробелов для отступа
        '''
        lines = code_str.splitlines()
        if not lines:
            return ""
        first_line = lines[0]
        indented_lines = [(" " * indent) + line if line.strip() else "" for line in lines[1:]]
        return "\n".join([first_line] + indented_lines)
    
    @staticmethod
    def _render_dag(pipeline_template: str,
                    arguments: dict[str, str]) -> str:
        '''
        Функция для рендера: шаблон DAG заполняется значениями, 
        которые сгенерировала LLM

        Parameters
        ----------
        pipeline_template: str
            Шаблон пайплайна
        arguments: dict[str, str]
            Сгенерированные "детали" пайплайна
        '''
        template = Template(pipeline_template)

        dag_code = template.render(dag_name=arguments["dag_name"],
                                   schedule=arguments["schedule"],
                                   start_date=arguments["start_date"],
                                   catchup=arguments["catchup"],
                                   moving_data_from_source_to_dwh=arguments["code"])
        
        return dag_code

    @staticmethod
    def _save_code_to_file(code: str, name: str) -> None:
        '''
        Сохранение кода в python-скрипт

        Parameters
        ----------
        code: str
            Код пайплайна
        name: str
            Имя сохраняемого файла
        '''
        
        if not os.path.exists(settings.DAGS_DIR):
            os.makedirs(settings.DAGS_DIR)

        output_path = settings.DAGS_DIR / name
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(code)        
        logger.info(f"DAG сохранен в {output_path}")