"""
Коннекторы для извлечения данных из различных источников
"""

from .base_connector import BaseConnector
from .html_connector import HTMLConnector
from .json_connector import JSONConnector
from .csv_connector import CSVConnector
from .xlsx_connector import XLSXConnector
from .document_connector import DocumentConnector

__all__ = [
    'BaseConnector',
    'HTMLConnector', 
    'JSONConnector',
    'CSVConnector',
    'XLSXConnector',
    'DocumentConnector'
]
