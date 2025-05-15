## TO DO: вынести промпты в другое место
## TO DO: адаптировать под множественный источник   
## TO DO: рассмотреть использование string.Template
## TO DO: обогатить класс
## TO DO: добавить docstrings

import re
from jinja2 import Template
from langchain.prompts import ChatPromptTemplate
from langchain_openai import ChatOpenAI
from langchain_core.output_parsers import JsonOutputParser
from src.config.settings import settings
from src.config.prompts import prompts
from src.core.models.analytics import AnalyticsSpec


class AirflowDagGenerator:
    def __init__(self, analytics_specification: AnalyticsSpec,
                 template_path: str = settings.TEMPLATE_DAG_PATH):
        
        self.data_sources = analytics_specification.data_sources
        self.business_process = analytics_specification.business_process
        
        self.llm_for_moving = ChatOpenAI(
                                model=settings.LLM_MODEL_FOR_AIRFLOW_MOVING_DATA,
                                temperature=settings.TEMPERATURE_AIRFLOW_MOVING_DATA,
                                max_tokens=None,
                                timeout=None,
                                max_retries=2,
                                api_key=settings.OPENAI_API_KEY,
                                base_url=settings.BASE_URL
                            )
        self.llm_for_args = ChatOpenAI(
                                model=settings.LLM_MODEL_FOR_AIRFLOW_ARGS,
                                temperature=settings.TEMPERATURE_AIRFLOW_ARGS,
                                max_tokens=None,
                                timeout=None,
                                max_retries=2,
                                api_key=settings.OPENAI_API_KEY,
                                base_url=settings.BASE_URL
                            )
        self.parser = JsonOutputParser()
        
        with open(template_path, "r", encoding='utf-8') as f:
            self.pipeline_template = f.read()
        
    def _generate_dag_args(self) -> dict[str, str]:
        '''
        Генерация аргументов для airflow DAG.
        '''
        system_template = prompts.SYSTEM_PROMPT_AIRFLOW_ARGS
        user_template = prompts.USER_PROMPT_AIRFLOW_ARGS
        
        prompt_template = ChatPromptTemplate.from_messages(
               [("system", system_template),
                ("user", user_template)]
        )

        chain = prompt_template | self.llm_for_args | self.parser
        
        # result = chain.invoke(
        #     {"": }
        # )
        # return 

    def _generate_dag_args_legacy(self) -> str:
        
        system_template = prompts.SYSTEM_PROMPT_AIRFLOW_ARGS
        user_template = prompts.USER_PROMPT_AIRFLOW_ARGS
        
        prompt_template = ChatPromptTemplate.from_messages([
            ("system", system_template),
            ("user", user_template)
        ])
        chain = prompt_template | self.llm_for_args

        result = chain.invoke({
            "name": getattr(self.business_process, "name", "Анализ"),
            "schedule": getattr(self.business_process, "schedule", "0 0 * * *"),
        })
        
        return self._clean_code(result.content)

    def _generate_moving_data_function_legacy(self) -> str:
        system_template = prompts.SYSTEM_PROMPT_AIRFLOW_MOVING_DATA
        user_template = prompts.USER_PROMPT_AIRFLOW_MOVING_DATA
        

        prompt_template = ChatPromptTemplate.from_messages(
            [("system", system_template),
             ("user", user_template)]
        )

        chain = prompt_template | self.llm_for_moving

        result = chain.invoke(
            {"data_sources": self.data_sources}
        )
        cleaned_code = self._clean_code(result.content)
        return self._indent_code_block(cleaned_code, indent=4)

    def generate_dag(self) -> None:
        dag_args = self._generate_dag_args()
        moving_function_code = self._generate_moving_data_function()
        
        dag_code = self._render_dag(
            pipeline_template=self.pipeline_template,
            dag_name=dag_args["dag_name"],
            start_date=dag_args["start_date"],
            schedule=dag_args["schedule"],
            moving_data_from_source_to_dwh=moving_function_code
        )

        self._save_code_to_file(code=dag_code, name=dag_args["dag_name"])

    @staticmethod
    def _clean_code(code_str: str) -> str:
        # убрать обрамление ``` или ```python (LLM генерит markdown)
        pattern = r"```(?:python)?\n(.*?)```"
        matches = re.findall(pattern, code_str, re.DOTALL)
        if matches:
            # если несколько блоков, объединяем их через 2 перевода строки
            return "\n\n".join(match.strip() for match in matches)
        return code_str.strip()
    
    @staticmethod
    def _indent_code_block(code_str: str, indent: int) -> str:
        lines = code_str.splitlines()
        if not lines:
            return ""
        first_line = lines[0]
        indented_lines = [(" " * indent) + line if line.strip() else "" for line in lines[1:]]
        return "\n".join([first_line] + indented_lines)
    
    @staticmethod
    def _render_dag(pipeline_template: str, 
                    dag_name: str, 
                    schedule: str, 
                    start_date: str, 
                    moving_data_from_source_to_dwh: str) -> str:
        '''
        Функция для рендера: шаблон DAG заполняется значениями, 
        которые сгенерировала LLM

        Parameters
        ----------
        pipeline_template: str
            Шаблон пайплайна
        dag_name: str
            Сгенерированное имя пайплайна
        schedule: str
            Сгенерированное расписание в формате крон
        start_date: str
            Сгенерированная дата старта работы пайплайна
        moving_data_from_source_to_dwh: str
            Сгенерированная функция перемещения данных из источника в хранилище
        '''
        template = Template(pipeline_template)

        dag_code = template.render(dag_name=dag_name,
                                   schedule=schedule,
                                   start_date=start_date,
                                   moving_data_from_source_to_dwh=moving_data_from_source_to_dwh)
        
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
        
        output_path = settings.DAGS_DIR / name
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(code)        