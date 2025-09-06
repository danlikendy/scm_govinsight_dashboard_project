"""
DAG для извлечения данных из источников
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
import sys
import os

# Добавляем путь к модулям ETL
sys.path.append('/opt/airflow/etl')

from connectors import HTMLConnector, JSONConnector, CSVConnector, XLSXConnector, DocumentConnector
import yaml
import pandas as pd
import logging

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
    'ingest_sources',
    default_args=default_args,
    description='Извлечение данных из источников',
    schedule_interval='0 4 * * *',  # Ежедневно в 4:00
    catchup=False,
    tags=['ingest', 'sources']
)

def load_contract(contract_name):
    """Загрузка контракта источника"""
    contract_path = f'/opt/airflow/contracts/{contract_name}.yaml'
    with open(contract_path, 'r', encoding='utf-8') as f:
        return yaml.safe_load(f)

def extract_reestr_po(**context):
    """Извлечение данных из реестра ПО"""
    logger.info("Starting extraction from reestr_po")
    
    contract = load_contract('reestr_po')
    connector = HTMLConnector(contract)
    
    try:
        df = connector.extract()
        logger.info(f"Extracted {len(df)} rows from reestr_po")
        
        # Сохранение в RAW слой
        df.to_parquet(
            f'/opt/airflow/data/raw/reestr_po_{datetime.now().strftime("%Y%m%d_%H%M%S")}.parquet',
            index=False
        )
        
        return f"Successfully extracted {len(df)} rows"
        
    except Exception as e:
        logger.error(f"Error extracting reestr_po: {str(e)}")
        raise
    finally:
        connector.close()

def extract_eis_procurement(**context):
    """Извлечение данных из ЕИС"""
    logger.info("Starting extraction from eis_procurement")
    
    contract = load_contract('eis_procurement')
    connector = CSVConnector(contract)
    
    try:
        df = connector.extract()
        logger.info(f"Extracted {len(df)} rows from eis_procurement")
        
        # Сохранение в RAW слой
        df.to_parquet(
            f'/opt/airflow/data/raw/eis_procurement_{datetime.now().strftime("%Y%m%d_%H%M%S")}.parquet',
            index=False
        )
        
        return f"Successfully extracted {len(df)} rows"
        
    except Exception as e:
        logger.error(f"Error extracting eis_procurement: {str(e)}")
        raise
    finally:
        connector.close()

def extract_fedstat_macro(**context):
    """Извлечение данных из ЕМИСС"""
    logger.info("Starting extraction from fedstat_macro")
    
    contract = load_contract('fedstat_macro')
    connector = JSONConnector(contract)
    
    try:
        df = connector.extract()
        logger.info(f"Extracted {len(df)} rows from fedstat_macro")
        
        # Сохранение в RAW слой
        df.to_parquet(
            f'/opt/airflow/data/raw/fedstat_macro_{datetime.now().strftime("%Y%m%d_%H%M%S")}.parquet',
            index=False
        )
        
        return f"Successfully extracted {len(df)} rows"
        
    except Exception as e:
        logger.error(f"Error extracting fedstat_macro: {str(e)}")
        raise
    finally:
        connector.close()

def extract_gisp_support(**context):
    """Извлечение данных из ГИСП"""
    logger.info("Starting extraction from gisp_support")
    
    contract = load_contract('gisp_support')
    connector = DocumentConnector(contract)
    
    try:
        df = connector.extract()
        logger.info(f"Extracted {len(df)} rows from gisp_support")
        
        # Сохранение в RAW слой
        df.to_parquet(
            f'/opt/airflow/data/raw/gisp_support_{datetime.now().strftime("%Y%m%d_%H%M%S")}.parquet',
            index=False
        )
        
        return f"Successfully extracted {len(df)} rows"
        
    except Exception as e:
        logger.error(f"Error extracting gisp_support: {str(e)}")
        raise
    finally:
        connector.close()

# Создание директорий
create_dirs = BashOperator(
    task_id='create_directories',
    bash_command='mkdir -p /opt/airflow/data/raw /opt/airflow/data/staging /opt/airflow/data/core',
    dag=dag
)

# Задачи извлечения данных
extract_reestr_po_task = PythonOperator(
    task_id='extract_reestr_po',
    python_callable=extract_reestr_po,
    dag=dag
)

extract_eis_procurement_task = PythonOperator(
    task_id='extract_eis_procurement',
    python_callable=extract_eis_procurement,
    dag=dag
)

extract_fedstat_macro_task = PythonOperator(
    task_id='extract_fedstat_macro',
    python_callable=extract_fedstat_macro,
    dag=dag
)

extract_gisp_support_task = PythonOperator(
    task_id='extract_gisp_support',
    python_callable=extract_gisp_support,
    dag=dag
)

# Задача проверки качества извлечения
check_extraction_quality = BashOperator(
    task_id='check_extraction_quality',
    bash_command='''
    echo "Checking extraction quality..."
    ls -la /opt/airflow/data/raw/
    echo "Raw data files count: $(ls /opt/airflow/data/raw/*.parquet | wc -l)"
    ''',
    dag=dag
)

# Определение зависимостей
create_dirs >> [
    extract_reestr_po_task,
    extract_eis_procurement_task,
    extract_fedstat_macro_task,
    extract_gisp_support_task
] >> check_extraction_quality
