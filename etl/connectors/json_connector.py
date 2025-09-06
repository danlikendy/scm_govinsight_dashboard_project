"""
Коннектор для работы с JSON API
"""

import requests
import pandas as pd
from typing import Dict, Any, Optional, List
import time
from .base_connector import BaseConnector


class JSONConnector(BaseConnector):
    """Коннектор для извлечения данных из JSON API"""
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.base_url = config.get('endpoint', '')
        self.session = requests.Session()
        self.session.headers.update({
            'Content-Type': 'application/json',
            'User-Agent': 'SCM-Dashboard/1.0'
        })
        
        # Настройки аутентификации
        auth_config = config.get('auth', {})
        if auth_config.get('type') == 'bearer':
            self.session.headers['Authorization'] = f"Bearer {auth_config['token']}"
        elif auth_config.get('type') == 'api_key':
            self.session.headers[auth_config['header']] = auth_config['key']
    
    def extract(self, params: Optional[Dict] = None, **kwargs) -> pd.DataFrame:
        """Извлечение данных из JSON API"""
        self.logger.info(f"Extracting data from {self.base_url}")
        
        try:
            # Подготовка параметров запроса
            request_params = params or {}
            request_params.update(kwargs)
            
            # Извлечение данных с пагинацией
            all_data = self._fetch_all_pages(request_params)
            
            # Преобразование в DataFrame
            df = pd.json_normalize(all_data)
            
            # Добавление метаданных
            df = self.add_metadata(df)
            
            # Валидация
            schema = self.config.get('schema', [])
            df = self.validate_data(df, schema)
            
            self.log_extraction_stats(df)
            return df
            
        except Exception as e:
            self.logger.error(f"Error extracting data from {self.base_url}: {str(e)}")
            raise
    
    def _fetch_all_pages(self, params: Dict) -> List[Dict]:
        """Получение всех страниц данных"""
        all_data = []
        page = 1
        limit = params.get('limit', 1000)
        
        while True:
            # Подготовка параметров для текущей страницы
            page_params = params.copy()
            page_params.update({
                'page': page,
                'limit': limit
            })
            
            # Запрос данных
            response = self.session.get(self.base_url, params=page_params, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            
            # Извлечение элементов данных
            items = self._extract_items(data)
            if not items:
                break
                
            all_data.extend(items)
            self.logger.info(f"Fetched page {page}, {len(items)} items")
            
            # Проверка наличия следующей страницы
            if not self._has_next_page(data, page):
                break
                
            page += 1
            time.sleep(0.5)  # Задержка между запросами
        
        return all_data
    
    def _extract_items(self, data: Dict) -> List[Dict]:
        """Извлечение элементов данных из ответа API"""
        # Поддержка различных структур ответа
        if 'items' in data:
            return data['items']
        elif 'data' in data:
            return data['data']
        elif 'results' in data:
            return data['results']
        elif isinstance(data, list):
            return data
        else:
            return [data]  # Одиночный объект
    
    def _has_next_page(self, data: Dict, current_page: int) -> bool:
        """Проверка наличия следующей страницы"""
        # Различные способы определения наличия следующей страницы
        if 'next_page' in data:
            return data['next_page'] is not None
        elif 'has_next' in data:
            return data['has_next']
        elif 'pagination' in data:
            pagination = data['pagination']
            return pagination.get('current_page', 0) < pagination.get('total_pages', 0)
        else:
            # Если нет явной информации о пагинации, проверяем количество элементов
            items = self._extract_items(data)
            return len(items) >= 1000  # Предполагаем, что если меньше 1000, то это последняя страница
    
    def _make_request(self, url: str, params: Dict) -> Dict:
        """Выполнение HTTP запроса с обработкой ошибок"""
        try:
            response = self.session.get(url, params=params, timeout=30)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Request failed: {str(e)}")
            raise
    
    def close(self):
        """Закрытие сессии"""
        self.session.close()
