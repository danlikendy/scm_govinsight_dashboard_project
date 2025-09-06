"""
Реальные источники данных для SCM Dashboard
Парсинг актуальных данных из государственных источников
"""

import requests
import pandas as pd
from bs4 import BeautifulSoup
import json
from datetime import datetime, timedelta
import time
import logging

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ReestrPOConnector:
    """Коннектор для Реестра российского ПО (Минцифры)"""
    
    def __init__(self):
        self.base_url = "https://reestr.digital.gov.ru"
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
    
    def get_scm_solutions(self):
        """Получение SCM-решений из реестра"""
        try:
            # Поиск по категории "Управление цепями поставок"
            search_url = f"{self.base_url}/reestr/search/"
            params = {
                'category': 'supply_chain_management',
                'status': 'active'
            }
            
            response = self.session.get(search_url, params=params, timeout=30)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            solutions = []
            
            # Парсинг таблицы решений
            table = soup.find('table', class_='solutions-table')
            if table:
                rows = table.find_all('tr')[1:]  # Пропускаем заголовок
                
                for row in rows:
                    cells = row.find_all('td')
                    if len(cells) >= 6:
                        solution = {
                            'name': cells[0].get_text(strip=True),
                            'vendor': cells[1].get_text(strip=True),
                            'version': cells[2].get_text(strip=True),
                            'registration_date': cells[3].get_text(strip=True),
                            'status': cells[4].get_text(strip=True),
                            'category': 'SCM',
                            'is_domestic': True,
                            'source': 'reestr_po'
                        }
                        solutions.append(solution)
            
            logger.info(f"Получено {len(solutions)} SCM-решений из реестра ПО")
            return solutions
            
        except Exception as e:
            logger.error(f"Ошибка получения данных из реестра ПО: {e}")
            return []

class EISConnector:
    """Коннектор для ЕИС (Единая информационная система в сфере закупок)"""
    
    def __init__(self):
        self.base_url = "https://zakupki.gov.ru"
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
    
    def get_scm_procurements(self, days_back=30):
        """Получение закупок SCM-решений"""
        try:
            # Поиск по ключевым словам SCM
            keywords = [
                "управление цепями поставок",
                "SCM",
                "WMS",
                "TMS", 
                "логистическое программное обеспечение",
                "складское управление"
            ]
            
            all_procurements = []
            
            for keyword in keywords:
                search_url = f"{self.base_url}/epz/opendata/search/results.html"
                params = {
                    'searchString': keyword,
                    'morphology': 'on',
                    'search-filter': 'Дате+размещения',
                    'sortBy': 'BY_RELEVANCE_DESC',
                    'pageNumber': 1,
                    'sortDirection': 'false',
                    'recordsPerPage': '_10',
                    'showLots': 'on',
                    'fz44': 'on',
                    'fz223': 'on',
                    'ppRf615': 'on',
                    'fz94': 'on'
                }
                
                response = self.session.get(search_url, params=params, timeout=30)
                response.raise_for_status()
                
                soup = BeautifulSoup(response.content, 'html.parser')
                
                # Парсинг результатов поиска
                procurement_items = soup.find_all('div', class_='search-registry-entry-block')
                
                for item in procurement_items:
                    try:
                        title_elem = item.find('a', class_='registry-entry__body-title')
                        if not title_elem:
                            continue
                            
                        procurement = {
                            'title': title_elem.get_text(strip=True),
                            'url': f"{self.base_url}{title_elem.get('href', '')}",
                            'customer': item.find('div', class_='registry-entry__body-value').get_text(strip=True) if item.find('div', class_='registry-entry__body-value') else '',
                            'price': self._extract_price(item),
                            'publication_date': self._extract_date(item),
                            'keyword': keyword,
                            'source': 'eis'
                        }
                        all_procurements.append(procurement)
                        
                    except Exception as e:
                        logger.warning(f"Ошибка парсинга закупки: {e}")
                        continue
                
                time.sleep(1)  # Задержка между запросами
            
            logger.info(f"Получено {len(all_procurements)} закупок SCM из ЕИС")
            return all_procurements
            
        except Exception as e:
            logger.error(f"Ошибка получения данных из ЕИС: {e}")
            return []
    
    def _extract_price(self, item):
        """Извлечение цены из элемента закупки"""
        price_elem = item.find('div', class_='price-block__value')
        if price_elem:
            price_text = price_elem.get_text(strip=True)
            # Очистка и конвертация цены
            price_text = price_text.replace(' ', '').replace('₽', '').replace(',', '.')
            try:
                return float(price_text)
            except:
                return 0
        return 0
    
    def _extract_date(self, item):
        """Извлечение даты из элемента закупки"""
        date_elem = item.find('div', class_='data-block__value')
        if date_elem:
            return date_elem.get_text(strip=True)
        return ''

class FedstatConnector:
    """Коннектор для Федстат (ЕМИСС)"""
    
    def __init__(self):
        self.base_url = "https://fedstat.ru"
        self.api_url = "https://fedstat.ru/api"
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
    
    def get_it_indicators(self):
        """Получение показателей ИТ-отрасли"""
        try:
            # Индикаторы развития ИТ
            indicators = [
                'IT_INVESTMENT',  # Инвестиции в ИТ
                'SOFTWARE_PRODUCTION',  # Производство ПО
                'DIGITAL_ECONOMY',  # Цифровая экономика
                'IT_EMPLOYMENT'  # Занятость в ИТ
            ]
            
            data = []
            
            for indicator in indicators:
                url = f"{self.api_url}/indicator/{indicator}"
                response = self.session.get(url, timeout=30)
                response.raise_for_status()
                
                indicator_data = response.json()
                
                for item in indicator_data.get('data', []):
                    data.append({
                        'indicator': indicator,
                        'year': item.get('year'),
                        'value': item.get('value'),
                        'unit': item.get('unit'),
                        'region': item.get('region', 'Российская Федерация'),
                        'source': 'fedstat'
                    })
            
            logger.info(f"Получено {len(data)} показателей из Федстат")
            return data
            
        except Exception as e:
            logger.error(f"Ошибка получения данных из Федстат: {e}")
            return []

class GISPConnector:
    """Коннектор для ГИСП (Государственная информационная система промышленности)"""
    
    def __init__(self):
        self.base_url = "https://gisp.gov.ru"
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
    
    def get_support_measures(self):
        """Получение мер поддержки для ИТ-отрасли"""
        try:
            # Поиск мер поддержки для ИТ
            search_url = f"{self.base_url}/measures"
            params = {
                'category': 'it',
                'status': 'active'
            }
            
            response = self.session.get(search_url, params=params, timeout=30)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            measures = []
            
            # Парсинг карточек мер поддержки
            measure_cards = soup.find_all('div', class_='measure-card')
            
            for card in measure_cards:
                try:
                    title_elem = card.find('h3', class_='measure-title')
                    if not title_elem:
                        continue
                    
                    measure = {
                        'title': title_elem.get_text(strip=True),
                        'description': self._extract_description(card),
                        'amount': self._extract_amount(card),
                        'deadline': self._extract_deadline(card),
                        'requirements': self._extract_requirements(card),
                        'source': 'gisp'
                    }
                    measures.append(measure)
                    
                except Exception as e:
                    logger.warning(f"Ошибка парсинга меры поддержки: {e}")
                    continue
            
            logger.info(f"Получено {len(measures)} мер поддержки из ГИСП")
            return measures
            
        except Exception as e:
            logger.error(f"Ошибка получения данных из ГИСП: {e}")
            return []
    
    def _extract_description(self, card):
        """Извлечение описания меры поддержки"""
        desc_elem = card.find('div', class_='measure-description')
        return desc_elem.get_text(strip=True) if desc_elem else ''
    
    def _extract_amount(self, card):
        """Извлечение суммы поддержки"""
        amount_elem = card.find('div', class_='measure-amount')
        if amount_elem:
            amount_text = amount_elem.get_text(strip=True)
            # Очистка и конвертация суммы
            amount_text = amount_text.replace(' ', '').replace('₽', '').replace(',', '.')
            try:
                return float(amount_text)
            except:
                return 0
        return 0
    
    def _extract_deadline(self, card):
        """Извлечение срока подачи заявок"""
        deadline_elem = card.find('div', class_='measure-deadline')
        return deadline_elem.get_text(strip=True) if deadline_elem else ''
    
    def _extract_requirements(self, card):
        """Извлечение требований"""
        req_elem = card.find('div', class_='measure-requirements')
        return req_elem.get_text(strip=True) if req_elem else ''

class DataAggregator:
    """Агрегатор данных из всех источников"""
    
    def __init__(self):
        self.reestr = ReestrPOConnector()
        self.eis = EISConnector()
        self.fedstat = FedstatConnector()
        self.gisp = GISPConnector()
    
    def collect_all_data(self):
        """Сбор данных из всех источников"""
        logger.info("Начинаем сбор данных из всех источников...")
        
        all_data = {
            'solutions': [],
            'procurements': [],
            'indicators': [],
            'support_measures': []
        }
        
        try:
            # Сбор данных из реестра ПО
            logger.info("Сбор данных из реестра ПО...")
            all_data['solutions'] = self.reestr.get_scm_solutions()
            
            # Сбор данных из ЕИС
            logger.info("Сбор данных из ЕИС...")
            all_data['procurements'] = self.eis.get_scm_procurements()
            
            # Сбор данных из Федстат
            logger.info("Сбор данных из Федстат...")
            all_data['indicators'] = self.fedstat.get_it_indicators()
            
            # Сбор данных из ГИСП
            logger.info("Сбор данных из ГИСП...")
            all_data['support_measures'] = self.gisp.get_support_measures()
            
            logger.info("Сбор данных завершен успешно")
            return all_data
            
        except Exception as e:
            logger.error(f"Ошибка при сборе данных: {e}")
            return all_data
    
    def save_to_database(self, conn, data):
        """Сохранение собранных данных в базу"""
        try:
            cursor = conn.cursor()
            
            # Сохранение решений
            for solution in data['solutions']:
                cursor.execute('''
                    INSERT OR REPLACE INTO real_solutions 
                    (name, vendor, version, registration_date, status, category, is_domestic, source)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    solution['name'],
                    solution['vendor'],
                    solution['version'],
                    solution['registration_date'],
                    solution['status'],
                    solution['category'],
                    solution['is_domestic'],
                    solution['source']
                ))
            
            # Сохранение закупок
            for procurement in data['procurements']:
                cursor.execute('''
                    INSERT OR REPLACE INTO real_procurements
                    (title, url, customer, price, publication_date, keyword, source)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                ''', (
                    procurement['title'],
                    procurement['url'],
                    procurement['customer'],
                    procurement['price'],
                    procurement['publication_date'],
                    procurement['keyword'],
                    procurement['source']
                ))
            
            # Сохранение индикаторов
            for indicator in data['indicators']:
                cursor.execute('''
                    INSERT OR REPLACE INTO real_indicators
                    (indicator, year, value, unit, region, source)
                    VALUES (?, ?, ?, ?, ?, ?)
                ''', (
                    indicator['indicator'],
                    indicator['year'],
                    indicator['value'],
                    indicator['unit'],
                    indicator['region'],
                    indicator['source']
                ))
            
            # Сохранение мер поддержки
            for measure in data['support_measures']:
                cursor.execute('''
                    INSERT OR REPLACE INTO real_support_measures
                    (title, description, amount, deadline, requirements, source)
                    VALUES (?, ?, ?, ?, ?, ?)
                ''', (
                    measure['title'],
                    measure['description'],
                    measure['amount'],
                    measure['deadline'],
                    measure['requirements'],
                    measure['source']
                ))
            
            conn.commit()
            logger.info("Данные успешно сохранены в базу")
            
        except Exception as e:
            logger.error(f"Ошибка сохранения данных: {e}")

def create_real_data_tables(conn):
    """Создание таблиц для реальных данных"""
    cursor = conn.cursor()
    
    # Таблица реальных решений
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS real_solutions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT,
            vendor TEXT,
            version TEXT,
            registration_date TEXT,
            status TEXT,
            category TEXT,
            is_domestic BOOLEAN,
            source TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    # Таблица реальных закупок
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS real_procurements (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT,
            url TEXT,
            customer TEXT,
            price REAL,
            publication_date TEXT,
            keyword TEXT,
            source TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    # Таблица реальных индикаторов
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS real_indicators (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            indicator TEXT,
            year INTEGER,
            value REAL,
            unit TEXT,
            region TEXT,
            source TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    # Таблица реальных мер поддержки
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS real_support_measures (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT,
            description TEXT,
            amount REAL,
            deadline TEXT,
            requirements TEXT,
            source TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    conn.commit()
