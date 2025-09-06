"""
DAG для нормализации данных в staging слой
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable
import sys
import os

# Добавляем путь к модулям ETL
sys.path.append('/opt/airflow/etl')

import pandas as pd
import logging
from sqlalchemy import create_engine
import yaml

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Конфигурация DAG
default_args = {
    'owner': 'scm-dashboard',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'staging_normalize',
    default_args=default_args,
    description='Нормализация данных в staging слой',
    schedule_interval='0 5 * * *',  # Ежедневно в 5:00
    catchup=False,
    tags=['staging', 'normalize']
)

def get_db_connection():
    """Получение подключения к БД"""
    db_url = Variable.get("database_url", "postgresql://scm_user:scm_password@postgres:5432/scm_dashboard")
    return create_engine(db_url)

def normalize_reestr_po(**context):
    """Нормализация данных реестра ПО"""
    logger.info("Starting normalization of reestr_po data")
    
    try:
        # Загрузка данных из RAW
        raw_files = [f for f in os.listdir('/opt/airflow/data/raw/') if f.startswith('reestr_po_')]
        if not raw_files:
            logger.warning("No raw reestr_po files found")
            return "No data to normalize"
        
        latest_file = max(raw_files)
        df = pd.read_parquet(f'/opt/airflow/data/raw/{latest_file}')
        
        # Нормализация данных
        df_normalized = df.copy()
        
        # Очистка и типизация полей
        df_normalized['solution_code'] = df_normalized['solution_code'].astype(str).str.strip()
        df_normalized['solution_name'] = df_normalized['solution_name'].astype(str).str.strip()
        df_normalized['vendor_inn'] = df_normalized['vendor_inn'].astype(str).str.replace(r'[^\d]', '', regex=True)
        df_normalized['vendor_name'] = df_normalized['vendor_name'].astype(str).str.strip()
        df_normalized['is_domestic'] = df_normalized['is_domestic'].astype(bool)
        
        # Преобразование дат
        df_normalized['register_decision_date'] = pd.to_datetime(df_normalized['register_decision_date'], errors='coerce').dt.date
        
        # Классификация SCM
        scm_keywords = {
            'WMS': ['склад', 'warehouse', 'wms', 'складской'],
            'TMS': ['транспорт', 'transport', 'tms', 'логистика'],
            'S&OP': ['планирование', 'planning', 's&op', 'sop'],
            'APS': ['планирование', 'planning', 'aps', 'advanced'],
            'OMS': ['заказ', 'order', 'oms', 'управление заказами'],
            'Procurement': ['закупки', 'procurement', 'закуп', 'снабжение']
        }
        
        def classify_scm(name):
            name_lower = name.lower()
            for class_name, keywords in scm_keywords.items():
                if any(keyword in name_lower for keyword in keywords):
                    return class_name
            return 'Other'
        
        df_normalized['class_scm'] = df_normalized['solution_name'].apply(classify_scm)
        
        # Сохранение в staging
        staging_file = f'/opt/airflow/data/staging/reestr_po_normalized_{datetime.now().strftime("%Y%m%d_%H%M%S")}.parquet'
        df_normalized.to_parquet(staging_file, index=False)
        
        logger.info(f"Normalized {len(df_normalized)} rows from reestr_po")
        return f"Successfully normalized {len(df_normalized)} rows"
        
    except Exception as e:
        logger.error(f"Error normalizing reestr_po: {str(e)}")
        raise

def normalize_eis_procurement(**context):
    """Нормализация данных ЕИС"""
    logger.info("Starting normalization of eis_procurement data")
    
    try:
        # Загрузка данных из RAW
        raw_files = [f for f in os.listdir('/opt/airflow/data/raw/') if f.startswith('eis_procurement_')]
        if not raw_files:
            logger.warning("No raw eis_procurement files found")
            return "No data to normalize"
        
        latest_file = max(raw_files)
        df = pd.read_parquet(f'/opt/airflow/data/raw/{latest_file}')
        
        # Нормализация данных
        df_normalized = df.copy()
        
        # Очистка и типизация полей
        df_normalized['notice_id'] = df_normalized['notice_id'].astype(str).str.strip()
        df_normalized['customer_inn'] = df_normalized['customer_inn'].astype(str).str.replace(r'[^\d]', '', regex=True)
        df_normalized['supplier_inn'] = df_normalized['supplier_inn'].astype(str).str.replace(r'[^\d]', '', regex=True)
        df_normalized['okpd2'] = df_normalized['okpd2'].astype(str).str.strip()
        df_normalized['subject'] = df_normalized['subject'].astype(str).str.strip()
        
        # Преобразование сумм
        df_normalized['sum_rub'] = pd.to_numeric(df_normalized['sum_rub'], errors='coerce')
        
        # Преобразование дат
        df_normalized['publish_date'] = pd.to_datetime(df_normalized['publish_date'], errors='coerce').dt.date
        df_normalized['contract_date'] = pd.to_datetime(df_normalized['contract_date'], errors='coerce').dt.date
        
        # Определение SCM-связанных закупок
        scm_keywords = ['scm', 'wms', 'tms', 'erp', 'склад', 'транспорт', 'логистика', 'планирование', 'закупки']
        df_normalized['is_scm_related'] = df_normalized['subject'].str.lower().str.contains('|'.join(scm_keywords), na=False)
        
        # Сохранение в staging
        staging_file = f'/opt/airflow/data/staging/eis_procurement_normalized_{datetime.now().strftime("%Y%m%d_%H%M%S")}.parquet'
        df_normalized.to_parquet(staging_file, index=False)
        
        logger.info(f"Normalized {len(df_normalized)} rows from eis_procurement")
        return f"Successfully normalized {len(df_normalized)} rows"
        
    except Exception as e:
        logger.error(f"Error normalizing eis_procurement: {str(e)}")
        raise

def normalize_fedstat_macro(**context):
    """Нормализация данных ЕМИСС"""
    logger.info("Starting normalization of fedstat_macro data")
    
    try:
        # Загрузка данных из RAW
        raw_files = [f for f in os.listdir('/opt/airflow/data/raw/') if f.startswith('fedstat_macro_')]
        if not raw_files:
            logger.warning("No raw fedstat_macro files found")
            return "No data to normalize"
        
        latest_file = max(raw_files)
        df = pd.read_parquet(f'/opt/airflow/data/raw/{latest_file}')
        
        # Нормализация данных
        df_normalized = df.copy()
        
        # Очистка и типизация полей
        df_normalized['indicator_code'] = df_normalized['indicator_code'].astype(str).str.strip()
        df_normalized['indicator_name'] = df_normalized['indicator_name'].astype(str).str.strip()
        df_normalized['region_code'] = df_normalized['region_code'].astype(str).str.strip()
        df_normalized['unit'] = df_normalized['unit'].astype(str).str.strip()
        
        # Преобразование значений
        df_normalized['value'] = pd.to_numeric(df_normalized['value'], errors='coerce')
        
        # Преобразование дат
        df_normalized['period'] = pd.to_datetime(df_normalized['period'], errors='coerce').dt.date
        
        # Классификация типов показателей
        indicator_types = {
            'production': ['производство', 'production', 'индекс'],
            'employment': ['занятость', 'employment', 'работа'],
            'investment': ['инвестиции', 'investment', 'вложения'],
            'revenue': ['выручка', 'revenue', 'доход'],
            'logistics': ['логистика', 'logistics', 'транспорт']
        }
        
        def classify_indicator_type(name):
            name_lower = name.lower()
            for type_name, keywords in indicator_types.items():
                if any(keyword in name_lower for keyword in keywords):
                    return type_name
            return 'other'
        
        df_normalized['indicator_type'] = df_normalized['indicator_name'].apply(classify_indicator_type)
        
        # Сохранение в staging
        staging_file = f'/opt/airflow/data/staging/fedstat_macro_normalized_{datetime.now().strftime("%Y%m%d_%H%M%S")}.parquet'
        df_normalized.to_parquet(staging_file, index=False)
        
        logger.info(f"Normalized {len(df_normalized)} rows from fedstat_macro")
        return f"Successfully normalized {len(df_normalized)} rows"
        
    except Exception as e:
        logger.error(f"Error normalizing fedstat_macro: {str(e)}")
        raise

def normalize_gisp_support(**context):
    """Нормализация данных ГИСП"""
    logger.info("Starting normalization of gisp_support data")
    
    try:
        # Загрузка данных из RAW
        raw_files = [f for f in os.listdir('/opt/airflow/data/raw/') if f.startswith('gisp_support_')]
        if not raw_files:
            logger.warning("No raw gisp_support files found")
            return "No data to normalize"
        
        latest_file = max(raw_files)
        df = pd.read_parquet(f'/opt/airflow/data/raw/{latest_file}')
        
        # Нормализация данных
        df_normalized = df.copy()
        
        # Очистка и типизация полей
        df_normalized['support_id'] = df_normalized['support_id'].astype(str).str.strip()
        df_normalized['program_code'] = df_normalized['program_code'].astype(str).str.strip()
        df_normalized['measure_type'] = df_normalized['measure_type'].astype(str).str.strip()
        df_normalized['recipient_inn'] = df_normalized['recipient_inn'].astype(str).str.replace(r'[^\d]', '', regex=True)
        df_normalized['conditions'] = df_normalized['conditions'].astype(str).str.strip()
        
        # Преобразование сумм
        df_normalized['amount_rub'] = pd.to_numeric(df_normalized['amount_rub'], errors='coerce')
        df_normalized['kpi_planned'] = pd.to_numeric(df_normalized['kpi_planned'], errors='coerce')
        df_normalized['kpi_actual'] = pd.to_numeric(df_normalized['kpi_actual'], errors='coerce')
        
        # Преобразование дат
        df_normalized['approval_date'] = pd.to_datetime(df_normalized['approval_date'], errors='coerce').dt.date
        df_normalized['disbursement_date'] = pd.to_datetime(df_normalized['disbursement_date'], errors='coerce').dt.date
        
        # Определение SCM-связанной поддержки
        scm_keywords = ['scm', 'wms', 'tms', 'erp', 'склад', 'транспорт', 'логистика', 'планирование', 'закупки']
        df_normalized['is_scm_related'] = df_normalized['conditions'].str.lower().str.contains('|'.join(scm_keywords), na=False)
        
        # Сохранение в staging
        staging_file = f'/opt/airflow/data/staging/gisp_support_normalized_{datetime.now().strftime("%Y%m%d_%H%M%S")}.parquet'
        df_normalized.to_parquet(staging_file, index=False)
        
        logger.info(f"Normalized {len(df_normalized)} rows from gisp_support")
        return f"Successfully normalized {len(df_normalized)} rows"
        
    except Exception as e:
        logger.error(f"Error normalizing gisp_support: {str(e)}")
        raise

# Задачи нормализации
normalize_reestr_po_task = PythonOperator(
    task_id='normalize_reestr_po',
    python_callable=normalize_reestr_po,
    dag=dag
)

normalize_eis_procurement_task = PythonOperator(
    task_id='normalize_eis_procurement',
    python_callable=normalize_eis_procurement,
    dag=dag
)

normalize_fedstat_macro_task = PythonOperator(
    task_id='normalize_fedstat_macro',
    python_callable=normalize_fedstat_macro,
    dag=dag
)

normalize_gisp_support_task = PythonOperator(
    task_id='normalize_gisp_support',
    python_callable=normalize_gisp_support,
    dag=dag
)

# Задача проверки качества нормализации
check_normalization_quality = BashOperator(
    task_id='check_normalization_quality',
    bash_command='''
    echo "Checking normalization quality..."
    ls -la /opt/airflow/data/staging/
    echo "Staging data files count: $(ls /opt/airflow/data/staging/*.parquet | wc -l)"
    ''',
    dag=dag
)

# Определение зависимостей
[
    normalize_reestr_po_task,
    normalize_eis_procurement_task,
    normalize_fedstat_macro_task,
    normalize_gisp_support_task
] >> check_normalization_quality
