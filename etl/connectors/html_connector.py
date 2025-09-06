"""
Коннектор для парсинга HTML страниц
"""

import requests
import pandas as pd
from bs4 import BeautifulSoup
from typing import Dict, Any, Optional
import time
from urllib.parse import urljoin, urlparse
from .base_connector import BaseConnector


class HTMLConnector(BaseConnector):
    """Коннектор для извлечения данных из HTML страниц"""
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.base_url = config.get('endpoint', '')
        self.parsing_config = config.get('parsing', {})
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
    
    def extract(self, url: Optional[str] = None, **kwargs) -> pd.DataFrame:
        """Извлечение данных из HTML"""
        target_url = url or self.base_url
        self.logger.info(f"Extracting data from {target_url}")
        
        try:
            # Загрузка страницы
            response = self.session.get(target_url, timeout=30)
            response.raise_for_status()
            
            # Парсинг HTML
            soup = BeautifulSoup(response.text, 'lxml')
            
            # Извлечение данных согласно конфигурации
            data = self._extract_table_data(soup)
            
            # Обработка пагинации
            if self.parsing_config.get('pagination'):
                data = self._handle_pagination(soup, data, target_url)
            
            df = pd.DataFrame(data)
            
            # Добавление метаданных
            df = self.add_metadata(df)
            
            # Валидация
            schema = self.config.get('schema', [])
            df = self.validate_data(df, schema)
            
            self.log_extraction_stats(df)
            return df
            
        except Exception as e:
            self.logger.error(f"Error extracting data from {target_url}: {str(e)}")
            raise
    
    def _extract_table_data(self, soup: BeautifulSoup) -> List[Dict]:
        """Извлечение данных из таблицы"""
        selector = self.parsing_config.get('selector', 'table tr')
        rows = soup.select(selector)
        
        if not rows:
            self.logger.warning("No rows found with selector: " + selector)
            return []
        
        data = []
        data_extraction = self.parsing_config.get('data_extraction', {})
        
        for i, row in enumerate(rows):
            if i == 0 and self.parsing_config.get('skip_header', True):
                continue
                
            row_data = {}
            cells = row.select('td')
            
            for field, selector in data_extraction.items():
                if selector.startswith('td:nth-child('):
                    # Извлечение по позиции столбца
                    try:
                        col_num = int(selector.split('(')[1].split(')')[0])
                        if col_num <= len(cells):
                            value = cells[col_num - 1].get_text(strip=True)
                            row_data[field] = self._clean_value(value, field)
                    except (ValueError, IndexError):
                        pass
                else:
                    # Извлечение по CSS селектору
                    element = row.select_one(selector)
                    if element:
                        value = element.get_text(strip=True)
                        row_data[field] = self._clean_value(value, field)
            
            if row_data:  # Добавляем только непустые строки
                data.append(row_data)
        
        return data
    
    def _clean_value(self, value: str, field: str) -> Any:
        """Очистка и преобразование значений"""
        if not value or value == '-':
            return None
        
        # Преобразование дат
        if 'date' in field.lower():
            try:
                return pd.to_datetime(value, format='%d.%m.%Y').date()
            except:
                try:
                    return pd.to_datetime(value).date()
                except:
                    return value
        
        # Преобразование булевых значений
        if field == 'is_domestic':
            return value.lower() in ['да', 'true', '1', 'yes', '✓']
        
        # Преобразование чисел
        if value.replace('.', '').replace(',', '').replace('-', '').isdigit():
            try:
                return float(value.replace(',', '.'))
            except:
                pass
        
        return value.strip()
    
    def _handle_pagination(self, soup: BeautifulSoup, data: List[Dict], base_url: str) -> List[Dict]:
        """Обработка пагинации"""
        pagination_config = self.parsing_config['pagination']
        pagination_type = pagination_config.get('type', 'next_page_link')
        
        if pagination_type == 'next_page_link':
            next_link = soup.select_one(pagination_config.get('selector', 'a.next-page'))
            if next_link and next_link.get('href'):
                next_url = urljoin(base_url, next_link['href'])
                self.logger.info(f"Following pagination to {next_url}")
                
                # Рекурсивный вызов для следующей страницы
                time.sleep(1)  # Задержка между запросами
                next_df = self.extract(url=next_url)
                data.extend(next_df.to_dict('records'))
        
        return data
    
    def close(self):
        """Закрытие сессии"""
        self.session.close()
