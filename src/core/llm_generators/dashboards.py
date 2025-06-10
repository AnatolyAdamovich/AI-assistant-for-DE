'''
LLM-модуль для генерации аналитического дашборда в BI-системе Metabase
'''
import requests
import logging
import time
from typing import Any
from langchain.prompts import ChatPromptTemplate
from langchain_openai import ChatOpenAI
from langchain.callbacks import get_openai_callback
from langchain_core.output_parsers import JsonOutputParser
from src.config.settings import settings
from src.config.prompts import prompts
from src.core.models.analytics import Metric


logger = logging.getLogger(name="DASHBOARD")

class MetabaseDashboardGenerator:
    def __init__(self, 
                 metabase_url: str, 
                 username: str, 
                 password: str,
                 with_metrics: bool = False,
                 model: str = settings.LLM_MODEL_FOR_METABASE,
                 temperature: float = settings.TEMPERATURE_METABASE):
        '''
        Инициализация генератора визуализаций в Metabase
        
        Parameters
        ----------
        metabase_url: str
            Адрес, на котором хостится Metabase
        username: str
            Логин для авторизации
        password: str
            Пароль для авторизации
        with_metrics: bool, optional
            Флаг, указывающий, нужно ли включать подсчёт метрик при использовании LLM (по умолчанию False)
        model: str, optional
            Название модели LLM (по умолчанию settings.LLM_MODEL_FOR_METABASE).
        temperature: float, optional
            Параметр температуры для модели LLM (по умолчанию settings.TEMPERATURE_METABASE).

        '''
        self.with_metrics = with_metrics
        
        self.metabase_url = metabase_url.rstrip('/')
        self.username = username
        self.password = password
        self.session_id = self._authenticate()

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

    def _authenticate(self) -> str:
        '''
        Аутентификация в Metabase
        '''
        url = f"{self.metabase_url}/api/session"
        response = requests.post(url, json={
            "username": self.username,
            "password": self.password
        })
        response.raise_for_status()
        logger.info("Аутентификация в Metabase прошла успешно!")

        return response.json()['id']
    
    def _headers(self) -> dict[str, str]:
        '''
        Заголовки для REST API
        '''
        return {
            "Content-Type": "application/json",
            "X-Metabase-Session": self.session_id
        }

    def create_card(self, 
                    name: str, 
                    visualization_settings: dict[str, Any], 
                    query: dict[str, Any], 
                    display: str = "table") -> int:
        '''
        Создание карточки (график/таблица/скаляр)
        
        Parameters
        ----------
        name: str
            Название метрики
        visualization_settings: dict[str, Any]
            Настройки визуализации
        query: dict[str, Any]
            Запрос
        display: str, optional
            Вид отображения (по умолчанию "table")
        '''
        url = f"{self.metabase_url}/api/card"
        payload = {
            "name": name,
            "display": display,
            "visualization_settings": visualization_settings,
            "dataset_query": {
                "type": "native",
                "native": {
                    "query": query
                },
                "database": 2
            }
        }
        response = requests.post(url, json=payload, headers=self._headers())
        response.raise_for_status()
        logger.info(f"Карточка '{name}' создана!")

        return response.json()['id']

    def get_dashboard_data(self, dashboard_id: int) -> dict:
        '''
        Получение информации об отчёте (дашборде)

        Parameters
        ----------
        dashboard_id: int
            Идентификатор отчёта
        '''
        url = f"{self.metabase_url}/api/dashboard/{dashboard_id}"
        resp = requests.get(
            url,
            headers=self._headers()
        )
        dashboard_data = resp.json()
        return dashboard_data

    def create_dashboard(self, 
                         name: str, 
                         description: str = "") -> int:
        '''
        Создание отчёта

        Parameters
        ----------
        name: str
            Название отчёта
        description: str, optional
            Описание отчёта (по умолчанию "")
        '''
        
        url = f"{self.metabase_url}/api/dashboard"
        payload = {
            "name": name,
            "description": description
        }
        response = requests.post(url, json=payload, headers=self._headers())
        response.raise_for_status()
        logger.info(f"Дашборд '{name}' создан!")

        return response.json()['id']
    
    def add_cards_to_dashboard(self, 
                               dashboard_id: int, 
                               cards_ids: list[dict[str, Any]]) -> int:
        '''
        Создание отчёта и добавление карточек (dashcards)
        
        Parameters
        ----------
        dashboard_id: int
            Идентификатор дашборда, на который добавляем карточки
        cards_ids: list[dict[str, Any]]
            Массив с описанием карточек 
        '''
        dashboard_data = self.get_dashboard_data(dashboard_id)

        N_dashcards_per_row = 2
        size_x = 12
        size_y = 7

        for i, card_id in enumerate(cards_ids):
            new_dashcard = {
                "id": card_id,
                "card_id": card_id,
                "col": (i % N_dashcards_per_row) * size_x,
                "row": (i // N_dashcards_per_row) * size_y,
                "size_x": size_x,
                "size_y": size_y,
                "series": [],
                "parameter_mappings": []
            }
            dashboard_data["dashcards"].append(new_dashcard)
            logger.info(f"Карточка №{card_id} добавлена на дашборд №{dashboard_id}")
        
        url = f"{self.metabase_url}/api/dashboard/{dashboard_id}"
        response = requests.put(url, json=dashboard_data, headers=self._headers())
        response.raise_for_status()
        return response.json()
    
    def generate_cards_data(self, 
                            marts_schema: dict[str, Any], 
                            metrics: list[dict[Metric]]) -> dict[str, Any]:
        '''
        Генерация payload для карточек в Metabase на основе описания схемы витрин и метрик

        Parameters
        ----------
        marts_schema: dict[str, Any]
            Описание структуры marts-слоя в DWH
        metrics: list[Metrics]
            Метрики согласно извлечённому ТЗ (см. src.core.models.analytic.Metrics)
        '''
        system_template = prompts.SYSTEM_PROMPT_METABASE_DASHBOARD
        user_template = prompts.USER_PROMPT_METABASE_DASHBOARD
        
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
                        "metrics": metrics,
                        "marts_models_schema": marts_schema
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
                    "metrics": metrics,
                    "marts_models_schema": marts_schema
                }
            )

        logger.info(f'Сгенерированы настройки для {len(result)} карточек')
        return result
    
    def generate_dashboard(self, 
                           marts_schema: dict[str, Any], 
                           metrics: list[dict[Metric]]) -> dict[str, Any]:
        '''
        Генерация дашборда с нужными графиками

        Parameters
        ----------
        marts_schema: dict[str, Any]
            Схема витрин на слое marts
        metrics: list[Metric]
            Метрики, заданные пользователем (и сгенерированные LLM)
        '''
        # генерация настроек для карточек
        cards_data = self.generate_cards_data(marts_schema=marts_schema, 
                                              metrics=metrics)

        # создание карточек
        cards_ids = []
        for card_name, card_data in cards_data.items():
            new_card_id = self.create_card(name=card_name, 
                                           visualization_settings=card_data['visualization_settings'],
                                           query=card_data['query'],
                                           display=card_data['display'])
            cards_ids.append(new_card_id)
        
        # создание дашборда
        dashboard_id = self.create_dashboard(name="Analytics Dashboard")
        
        # добавление карточек на дашборд
        self.add_cards_to_dashboard(dashboard_id=dashboard_id,
                                    cards_ids=cards_ids)
        
        logger.info("Аналитический дашборд готов к использованию")
        return cards_data

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