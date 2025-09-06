"""
Базовый класс для всех коннекторов
"""

import logging
from abc import ABC, abstractmethod
from typing import Dict, Any, Optional, List
import pandas as pd
from datetime import datetime
import hashlib

logger = logging.getLogger(__name__)


class BaseConnector(ABC):
    """Базовый класс для всех коннекторов данных"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.source_name = config.get('source_name', 'unknown')
        self.logger = logging.getLogger(f"{__name__}.{self.source_name}")
        
    @abstractmethod
    def extract(self, **kwargs) -> pd.DataFrame:
        """Извлечение данных из источника"""
        pass
    
    def validate_data(self, df: pd.DataFrame, schema: List[Dict]) -> pd.DataFrame:
        """Валидация данных согласно схеме"""
        self.logger.info(f"Validating {len(df)} rows for {self.source_name}")
        
        for field in schema:
            field_name = field['name']
            required = field.get('required', False)
            
            if required and field_name not in df.columns:
                raise ValueError(f"Required field {field_name} not found in data")
                
            if field_name in df.columns:
                # Проверка на null для обязательных полей
                if required and df[field_name].isnull().any():
                    null_count = df[field_name].isnull().sum()
                    self.logger.warning(f"Found {null_count} null values in required field {field_name}")
        
        return df
    
    def add_metadata(self, df: pd.DataFrame) -> pd.DataFrame:
        """Добавление метаданных к данным"""
        df = df.copy()
        df['_source'] = self.source_name
        df['_extracted_at'] = datetime.now()
        df['_data_hash'] = self._calculate_hash(df)
        return df
    
    def _calculate_hash(self, df: pd.DataFrame) -> str:
        """Расчет хэша данных для контроля изменений"""
        data_str = df.to_string()
        return hashlib.sha256(data_str.encode()).hexdigest()
    
    def log_extraction_stats(self, df: pd.DataFrame):
        """Логирование статистики извлечения"""
        self.logger.info(f"Extracted {len(df)} rows from {self.source_name}")
        self.logger.info(f"Columns: {list(df.columns)}")
        if not df.empty:
            self.logger.info(f"Date range: {df.get('date', pd.Series()).min()} - {df.get('date', pd.Series()).max()}")
