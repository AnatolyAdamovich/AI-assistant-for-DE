## TO DO: вынести промпты в другое место
## TO DO: адаптировать под множественный источник   
## TO DO: рассмотреть использование string.Template
## TO DO: обогатить класс
## TO DO: добавить docstrings

import re
from langchain.prompts import ChatPromptTemplate
from langchain_openai import ChatOpenAI
from src.config.settings import settings
from src.config.prompts import prompts
from src.core.specs import AnalyticsSpec


class AirflowDagGenerator:
    def __init__(self, analytics_specification: AnalyticsSpec):
        
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
        
    def _generate_dag_args(self) -> str:
        # Генерируем schedule_interval и start_date на основе business_process
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

    def _generate_moving_data_function(self) -> str:
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

    def fill_and_save_template(self) -> None:
        dag_args_code = self._generate_dag_args()
        moving_function_code = self._generate_moving_data_function()
        
        with open(settings.TEMPLATE_DAG_PATH, "r", encoding="utf-8") as f:
            template = f.read()

        # подстановка args
        for line in dag_args_code.splitlines():
            if "schedule_interval" in line:
                template = re.sub(r'schedule_interval\s*=\s*["\']{0,1}["\']{0,1}', line, template)
            if "start_date" in line:
                template = re.sub(r'start_date\s*=\s*datetime\([^)]+\)', line, template)
        
        # подстановка moving
        template = re.sub(
           r"def moving_data_from_source_to_dwh\(\*\*context\) -> None:\n\s*pass",
            moving_function_code,
            template
        )
        
        # сохранение итогового файла
        with open(settings.OUTPUT_DAG_PATH, "w", encoding="utf-8") as f:
            f.write(template)

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

        