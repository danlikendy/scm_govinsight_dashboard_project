"""
Коннектор для работы с XLSX файлами
"""

import requests
import pandas as pd
import io
import hashlib
from typing import Dict, Any, Optional
from .base_connector import BaseConnector


class XLSXConnector(BaseConnector):
    """Коннектор для извлечения данных из XLSX файлов"""
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'SCM-Dashboard/1.0'
        })
    
    def extract(self, url: Optional[str] = None, **kwargs) -> pd.DataFrame:
        """Извлечение данных из XLSX файла"""
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
            sheet_name = parsing_config.get('sheet_name', 0)
            header_row = parsing_config.get('header_row', 0)
            
            # Чтение XLSX
            df = pd.read_excel(
                io.BytesIO(content),
                sheet_name=sheet_name,
                header=header_row,
                engine='openpyxl'
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
        """Извлечение данных из локального XLSX файла"""
        self.logger.info(f"Extracting data from local file {file_path}")
        
        try:
            # Парсинг конфигурации
            parsing_config = self.config.get('parsing', {})
            sheet_name = parsing_config.get('sheet_name', 0)
            header_row = parsing_config.get('header_row', 0)
            
            # Чтение XLSX
            df = pd.read_excel(
                file_path,
                sheet_name=sheet_name,
                header=header_row,
                engine='openpyxl'
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
    
    def extract_multiple_sheets(self, url: Optional[str] = None, **kwargs) -> Dict[str, pd.DataFrame]:
        """Извлечение данных из всех листов XLSX файла"""
        target_url = url or self.config.get('endpoint', '')
        self.logger.info(f"Extracting data from all sheets in {target_url}")
        
        try:
            # Загрузка файла
            response = self.session.get(target_url, timeout=60)
            response.raise_for_status()
            
            content = response.content
            file_hash = hashlib.sha256(content).hexdigest()
            
            # Чтение всех листов
            all_sheets = pd.read_excel(
                io.BytesIO(content),
                sheet_name=None,  # Все листы
                engine='openpyxl'
            )
            
            result = {}
            for sheet_name, df in all_sheets.items():
                if df.empty:
                    continue
                    
                # Применение маппинга полей
                field_mapping = self.config.get('parsing', {}).get('data_mapping', {})
                if field_mapping:
                    df = df.rename(columns=field_mapping)
                
                # Добавление метаданных
                df = self.add_metadata(df)
                df['_file_hash'] = file_hash
                df['_sheet_name'] = sheet_name
                
                result[sheet_name] = df
                self.logger.info(f"Extracted {len(df)} rows from sheet '{sheet_name}'")
            
            return result
            
        except Exception as e:
            self.logger.error(f"Error extracting data from {target_url}: {str(e)}")
            raise
    
    def close(self):
        """Закрытие сессии"""
        self.session.close()
