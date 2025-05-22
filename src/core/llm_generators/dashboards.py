import requests
from typing import Any
from jinja2 import Template

from langchain.prompts import ChatPromptTemplate
from langchain_openai import ChatOpenAI
from langchain_core.output_parsers import JsonOutputParser
from src.config.settings import settings
from src.config.prompts import prompts
from src.core.models.analytics import AnalyticsSpec

class MetabaseDashboardGenerator:
    def __init__(self, metabase_url: str, username: str, password: str):
        '''
        Инициализация объекта
        
        Parameters
        ----------
        metabase_url: str
            Адрес, на котором хостится Metabase
        username: str
            Логин для авторизации
        password: str
            Пароль для авторизации
        '''
        self.metabase_url = metabase_url.rstrip('/')
        self.username = username
        self.password = password
        self.session_id = self._authenticate()

    def _authenticate(self) -> str:
        '''
        Аутентификация
        '''
        url = f"{self.metabase_url}/api/session"
        response = requests.post(url, json={
            "username": self.username,
            "password": self.password
        })
        response.raise_for_status()
        return response.json()['id']
    
    def _headers(self) -> dict[str, str]:
        return {
            "Content-Type": "application/json",
            "X-Metabase-Session": self.session_id
        }

    def create_card(self, name: str, visualization_settings: dict[str, Any], query: dict[str, Any], display: str = "table") -> int:
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
        display: str = "table"
            Вид отображения
        '''
        url = f"{self.metabase_url}/api/card"
        payload = {
            "name": name,
            "dataset_query": query,
            "display": display,
            "visualization_settings": visualization_settings
        }
        response = requests.post(url, json=payload, headers=self._headers())
        response.raise_for_status()
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

    def create_dashboard(self, name: str, description: str = "") -> int:
        '''
        Создание отчёта

        Parameters
        ----------
        name: str
            Название отчёта
        description: str
            Описание отчёта
        '''
        
        url = f"{self.metabase_url}/api/dashboard"
        payload = {
            "name": name,
            "description": description
        }
        response = requests.post(url, json=payload, headers=self._headers())
        response.raise_for_status()
        return response.json()['id']
    
    def add_cards_to_dashboard(self, dashboard_id: int, cards_ids: list[dict[str, Any]]) -> int:
        '''
        Создание отчёта и добавление карточек
        
        Parameters
        ----------
        cards_ids: list[dict[str, Any]]
            Массив с описанием карточек 
        '''
        dashboard_data = self.get_dashboard(dashboard_id)

        N_dashcards_per_row = 3
        size_x = 4
        size_y = 3

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
        
        url = f"{self.metabase_url}/api/dashboard/{dashboard_id}"
        response = requests.put(url, json=dashboard_data, headers=self._headers())
        response.raise_for_status()
        return response.json()
