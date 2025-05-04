import yaml
from typing import List
from langchain.prompts import ChatPromptTemplate
from langchain_openai import ChatOpenAI
from src.config.settings import settings
from src.config.prompts import prompts
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

        return profiles

    def _generate_sources(self) -> dict:
        '''
        Генерация файла models/source.yml
        '''
        sources = {
            "sources": [
                {
                    "name": "exported_data",
                    "schema": "last",
                    "tables": [
                        {
                            "name": self.data_source.name,
                            "identifier": self.data_source.name + "_last_data",
                            "description": self.data_source.description
                        }
                    ]
                }
            ]
        }
        return sources
    
    def _generate_schemas(self) -> dict:
        pass

    def _generate_stage_models(self, sources: dict) -> dict:
        system_template = prompts.SYSTEM_PROMPT_DBT_MODELS_STAGE
        user_template = prompts.USER_PROMPT_DBT_MODELS_STAGE

        prompt_template = ChatPromptTemplate.from_messages(
                [("system", system_template),
                 ("user", user_template)]
        )

        chain = prompt_template | self.llm_for_models

        result = chain.invoke(
            {"sources": yaml.dump(sources, allow_unicode=True)}
        )

        return self._clean_sql_code(result.content)

    def _generate_intermediate_models(self) -> List[str]:
        system_template = prompts.SYSTEM_PROMPT_DBT_MODELS_CORE
        user_template = prompts.USER_PROMPT_DBT_MODELS_CORE

        prompt_template = ChatPromptTemplate.from_messages(
                [("system", system_template),
                 ("user", user_template)]
        )

        chain = prompt_template | self.llm_for_models

        result = chain.invoke(
            {}
        )

        return self._clean_sql_code(result.content)

    def _generate_marts(self):
        system_template = prompts.SYSTEM_PROMPT_DBT_MODELS_MARTS
        user_template = prompts.USER_PROMPT_DBT_MODELS_MARTS

        prompt_template = ChatPromptTemplate.from_messages(
                [("system", system_template),
                 ("user", user_template)]
        )

        chain = prompt_template | self.llm_for_models

        result = chain.invoke(
            {}
        )

        return self._clean_sql_code(result.content)

    def fill_and_save_project(self):
        sources = self._generate_sources()
        profiles = self._generate_profiles()

        stage_models = self._generate_stage_models(sources)
        # TO DO: вероятно, core будет зависеть от stage, а marts от core и metrics
        core_models = self._generate_intermediate_models()
        marts_models = self._generate_marts()
        
        # TO DO: схема должна на что-то опираться
        schemas = self._generate_schemas()

        self._save_yml_from_dict(content=sources,
                                 file_path=settings.DBT_MODELS_DIR / "source.yml")
        self._save_yml_from_dict(content=profiles,
                                 file_path=settings.DBT_DIR / "profiles.yml")
        self._save_yml_from_dict(content=schemas,
                                 file_path=settings.DBT_MODELS_DIR / "schema.yml")
        
        for model_name, sql_code in stage_models.items():
            self._save_sql_model(content=sql_code,
                                 file_path=settings.DBT_MODELS_DIR / "stage" / f"stg_{model_name}.sql")

        for model_name, sql_code in core_models.items():
            self._save_sql_model(content=sql_code,
                                 file_path=settings.DBT_MODELS_DIR / "core" / f"int_{model_name}.sql")
        
        for model_name, sql_code in marts_models.items():
            self._save_sql_model(content=sql_code,
                                 file_path=settings.DBT_MODELS_DIR / "marts" / f"{model_name}.sql")
            
    @staticmethod
    def _save_yml_from_str(content: str, file_path: str) -> None:
        with open(file_path, "w", encoding="utf-8") as f:
            f.write(content)
    
    @staticmethod
    def _save_yml_from_dict(content: dict, file_path: str) -> None:
        with open(file_path, 'w', encoding='utf-8') as f:
            yaml.dump(content, f, sort_keys=False, allow_unicode=True)
    
    @staticmethod
    def _save_sql_model(content: str, file_path: str) -> None:
        with open(file_path, "w", encoding="utf-8") as f:
            f.write(content)
    
    @staticmethod
    def _clean_sql_code(code_str: str) -> str:
        # убрать обрамление ``` или ```sql и оставить только содержимое
        pattern = r"```(?:sql)?\n(.*?)```"
        matches = re.findall(pattern, code_str, re.DOTALL)
        if matches:
            # если несколько блоков, объединяем их через 2 перевода строки
            return "\n\n".join(match.strip() for match in matches)
        return code_str.strip()


