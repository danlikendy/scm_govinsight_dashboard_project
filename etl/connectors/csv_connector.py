"""
Коннектор для работы с CSV файлами
"""

import requests
import pandas as pd
import io
import hashlib
from typing import Dict, Any, Optional
from .base_connector import BaseConnector


class CSVConnector(BaseConnector):
    """Коннектор для извлечения данных из CSV файлов"""
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'SCM-Dashboard/1.0'
        })
    
    def extract(self, url: Optional[str] = None, **kwargs) -> pd.DataFrame:
        """Извлечение данных из CSV файла"""
        target_url = url or self.config.get('endpoint', '')
        self.logger.info(f"Extracting data from {target_url}")
        
        try:
            # Загрузка файла
            response = self.session.get(target_url, timeout=60)
            response.raise_for_status()
            
            # Получение содержимого файла
            content = response.content
            file_hash = hashlib.sha256(content).hexdigest()
            self.logger.info(f"Downloaded file, hash: {file_hash[:16]}...")
            
            # Парсинг конфигурации
            parsing_config = self.config.get('parsing', {})
            encoding = parsing_config.get('encoding', 'utf-8')
            delimiter = parsing_config.get('delimiter', ',')
            quote_char = parsing_config.get('quote_char', '"')
            
            # Чтение CSV
            df = pd.read_csv(
                io.BytesIO(content),
                encoding=encoding,
                delimiter=delimiter,
                quotechar=quote_char,
                low_memory=False
            )
            
            # Применение маппинга полей
            field_mapping = parsing_config.get('data_mapping', {})
            if field_mapping:
                df = df.rename(columns=field_mapping)
            
            # Добавление метаданных
            df = self.add_metadata(df)
            df['_file_hash'] = file_hash
            
            # Валидация
            schema = self.config.get('schema', [])
            df = self.validate_data(df, schema)
            
            self.log_extraction_stats(df)
            return df
            
        except Exception as e:
            self.logger.error(f"Error extracting data from {target_url}: {str(e)}")
            raise
    
    def extract_from_file(self, file_path: str, **kwargs) -> pd.DataFrame:
        """Извлечение данных из локального CSV файла"""
        self.logger.info(f"Extracting data from local file {file_path}")
        
        try:
            # Парсинг конфигурации
            parsing_config = self.config.get('parsing', {})
            encoding = parsing_config.get('encoding', 'utf-8')
            delimiter = parsing_config.get('delimiter', ',')
            quote_char = parsing_config.get('quote_char', '"')
            
            # Чтение CSV
            df = pd.read_csv(
                file_path,
                encoding=encoding,
                delimiter=delimiter,
                quotechar=quote_char,
                low_memory=False
            )
            
            # Применение маппинга полей
            field_mapping = parsing_config.get('data_mapping', {})
            if field_mapping:
                df = df.rename(columns=field_mapping)
            
            # Добавление метаданных
            df = self.add_metadata(df)
            
            # Валидация
            schema = self.config.get('schema', [])
            df = self.validate_data(df, schema)
            
            self.log_extraction_stats(df)
            return df
            
        except Exception as e:
            self.logger.error(f"Error extracting data from {file_path}: {str(e)}")
            raise
    
    def close(self):
        """Закрытие сессии"""
        self.session.close()
