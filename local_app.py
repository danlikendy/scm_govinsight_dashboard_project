"""
Локальная версия SCM Dashboard с SQLite
Работает без Docker, использует локальную базу данных
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import numpy as np
import sqlite3
from datetime import datetime, timedelta
import random
import os
from data_sources import DataAggregator, create_real_data_tables

# Настройка страницы
st.set_page_config(
    page_title="SCM Government Insight Dashboard",
    page_icon="",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# CSS стили
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        color: #2c3e50;
        text-align: center;
        margin-bottom: 2rem;
    }
    .kpi-card {
        background-color: #f8f9fa;
        padding: 1rem;
        border-radius: 0.5rem;
        border-left: 5px solid #2c3e50;
    }
    .metric-value {
        font-size: 2rem;
        font-weight: bold;
        color: #2c3e50;
    }
    .metric-label {
        font-size: 0.9rem;
        color: #495057;
    }
    .success-box {
        background-color: #d1ecf1;
        border: 1px solid #bee5eb;
        border-radius: 0.5rem;
        padding: 1rem;
        margin-bottom: 1rem;
        color: #0c5460;
    }
    /* Принудительные стили для метрик */
    div[data-testid="metric-container"] {
        background-color: #f8f9fa !important;
        border: 2px solid #2c3e50 !important;
        border-radius: 0.5rem !important;
        padding: 1rem !important;
        margin: 0.5rem !important;
    }
    
    div[data-testid="metric-container"] > div {
        color: #2c3e50 !important;
    }
    
    div[data-testid="metric-container"] label {
        color: #495057 !important;
        font-weight: 600 !important;
        font-size: 0.9rem !important;
    }
    
    div[data-testid="metric-container"] [data-testid="metric-value"] {
        color: #2c3e50 !important;
        font-weight: bold !important;
        font-size: 2rem !important;
    }
    
    div[data-testid="metric-container"] [data-testid="metric-delta"] {
        color: #28a745 !important;
        font-weight: 600 !important;
    }
    
    /* Дополнительные стили для всех метрик */
    .stMetric {
        background-color: #f8f9fa !important;
        border: 2px solid #2c3e50 !important;
        border-radius: 0.5rem !important;
        padding: 1rem !important;
    }
    
    .stMetric * {
        color: #2c3e50 !important;
    }
    
    .stMetric label {
        color: #495057 !important;
    }
    
    .stMetric [data-testid="metric-value"] {
        color: #2c3e50 !important;
        font-weight: bold !important;
    }
    
    .stMetric [data-testid="metric-delta"] {
        color: #28a745 !important;
    }
</style>
""", unsafe_allow_html=True)

def init_database():
    """Инициализация локальной базы данных SQLite"""
    conn = sqlite3.connect('scm_dashboard.db')
    cursor = conn.cursor()
    
    # Создание таблиц для синтетических данных
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS implementations (
            impl_id INTEGER PRIMARY KEY,
            org_name TEXT,
            solution_name TEXT,
            vendor_name TEXT,
            class_scm TEXT,
            region_name TEXT,
            industry_name TEXT,
            date_go_live DATE,
            status TEXT,
            is_domestic BOOLEAN,
            capex INTEGER,
            revenue_uplift INTEGER,
            opex_delta INTEGER,
            inv_turnover_delta REAL,
            lead_time_delta REAL,
            penalties_delta INTEGER
        )
    ''')
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS kpi_monthly (
            date_month DATE PRIMARY KEY,
            year INTEGER,
            quarter INTEGER,
            impl_count INTEGER,
            domestic_impl_count INTEGER,
            domestic_share_pct REAL,
            total_econ_effect INTEGER,
            avg_econ_effect REAL,
            support_count INTEGER,
            total_support_amount INTEGER,
            support_coverage_pct REAL,
            isi_index REAL
        )
    ''')
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS support_measures (
            support_id INTEGER PRIMARY KEY,
            program_name TEXT,
            measure_type TEXT,
            recipient_name TEXT,
            amount_rub INTEGER,
            approval_date DATE,
            disbursement_date DATE,
            roi_pct REAL,
            cost_per_impl INTEGER
        )
    ''')
    
    # Создание таблиц для реальных данных
    create_real_data_tables(conn)
    
    conn.commit()
    return conn

def generate_and_load_data(conn):
    """Генерация и загрузка демонстрационных данных"""
    cursor = conn.cursor()
    
    # Проверяем, есть ли уже данные
    cursor.execute('SELECT COUNT(*) FROM implementations')
    if cursor.fetchone()[0] > 0:
        return  # Данные уже загружены
    
    # Генерация данных по внедрениям
    np.random.seed(42)
    n_implementations = 1250
    
    regions = ['Москва', 'Санкт-Петербург', 'Новосибирск', 'Екатеринбург', 'Казань', 'Нижний Новгород', 'Челябинск', 'Самара']
    industries = ['Производство', 'Логистика', 'Розничная торговля', 'Оптовая торговля', 'Строительство', 'Энергетика']
    vendors = ['1C', 'SAP', 'Oracle', 'Microsoft', 'Логика', 'Галактика', 'Битрикс24', 'АйТи']
    solutions = ['1C:Управление складом', 'SAP WMS', 'Oracle TMS', 'Microsoft Dynamics', 'Логика SCM', 'Галактика ERP']
    scm_classes = ['WMS', 'TMS', 'S&OP', 'APS', 'OMS', 'Procurement']
    
    implementations = []
    for i in range(n_implementations):
        impl = (
            i + 1,
            f'ООО "Компания {i+1}"',
            random.choice(solutions),
            random.choice(vendors),
            random.choice(scm_classes),
            random.choice(regions),
            random.choice(industries),
            (datetime.now() - timedelta(days=random.randint(30, 1095))).strftime('%Y-%m-%d'),
            random.choices(['go-live', 'pilot_ok', 'pilot', 'planned'], weights=[60, 20, 15, 5])[0],
            random.choice([True, False]),
            random.randint(500000, 50000000),
            random.randint(1000000, 20000000),
            random.randint(-500000, 2000000),
            random.uniform(0.1, 2.0),
            random.uniform(-5, 15),
            random.randint(-100000, 500000)
        )
        implementations.append(impl)
    
    # Загрузка данных о внедрениях
    cursor.executemany('''
        INSERT INTO implementations VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', implementations)
    
    # Генерация KPI данных
    months = pd.date_range(start='2023-01-01', end='2024-12-31', freq='ME')
    kpi_data = []
    
    for month in months:
        month_impl = [impl for impl in implementations 
                     if impl[7][:7] == month.strftime('%Y-%m')]
        
        domestic_count = sum(1 for impl in month_impl if impl[9])
        total_count = len(month_impl)
        
        kpi = (
            month.strftime('%Y-%m-%d'),  # Конвертируем в строку
            month.year,
            (month.month - 1) // 3 + 1,
            total_count,
            domestic_count,
            (domestic_count / total_count * 100) if total_count > 0 else 0,
            sum(impl[11] + impl[12] for impl in month_impl),
            (sum(impl[11] + impl[12] for impl in month_impl) / total_count) if total_count > 0 else 0,
            random.randint(10, 50),
            random.randint(50000000, 200000000),
            random.uniform(60, 85),
            random.uniform(0.6, 0.8)
        )
        kpi_data.append(kpi)
    
    # Загрузка KPI данных
    cursor.executemany('''
        INSERT INTO kpi_monthly VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', kpi_data)
    
    # Генерация данных по поддержке
    support_data = []
    programs = ['Поддержка SCM-решений 2024', 'Цифровизация промышленности', 'Импортозамещение ПО', 'Инновационные проекты']
    measure_types = ['subsidy', 'grant', 'tax_benefit', 'state_order']
    
    for i in range(500):
        support = (
            i + 1,
            random.choice(programs),
            random.choice(measure_types),
            f'ООО "Получатель {i+1}"',
            random.randint(1000000, 10000000),
            (datetime.now() - timedelta(days=random.randint(30, 365))).strftime('%Y-%m-%d'),
            (datetime.now() - timedelta(days=random.randint(10, 300))).strftime('%Y-%m-%d'),
            random.uniform(50, 200),
            random.randint(500000, 5000000)
        )
        support_data.append(support)
    
    # Загрузка данных о поддержке
    cursor.executemany('''
        INSERT INTO support_measures VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', support_data)
    
    conn.commit()

def collect_real_data():
    """Сбор реальных данных из источников"""
    try:
        aggregator = DataAggregator()
        data = aggregator.collect_all_data()
        return data
    except Exception as e:
        st.error(f"Ошибка сбора реальных данных: {e}")
        return None

@st.cache_data
def get_kpi_data(_conn, months_back=12):
    """Получение KPI данных"""
    cutoff_date = datetime.now() - timedelta(days=months_back * 30)
    
    query = '''
        SELECT * FROM kpi_monthly 
        WHERE date_month >= ? 
        ORDER BY date_month
    '''
    
    df = pd.read_sql_query(query, _conn, params=[cutoff_date])
    df['date_month'] = pd.to_datetime(df['date_month'])
    return df

@st.cache_data
def get_implementation_data(_conn, months_back=12):
    """Получение данных о внедрениях"""
    cutoff_date = datetime.now() - timedelta(days=months_back * 30)
    
    query = '''
        SELECT * FROM implementations 
        WHERE date_go_live >= ? 
        ORDER BY date_go_live
    '''
    
    df = pd.read_sql_query(query, _conn, params=[cutoff_date])
    df['date_go_live'] = pd.to_datetime(df['date_go_live'])
    return df

@st.cache_data
def get_support_data(_conn):
    """Получение данных о поддержке"""
    query = 'SELECT * FROM support_measures ORDER BY approval_date'
    return pd.read_sql_query(query, _conn)

def render_kpi_cards(kpi_data):
    """Отображение KPI карточек"""
    if kpi_data.empty:
        st.warning("Нет данных для отображения")
        return
    
    latest_data = kpi_data.iloc[-1]  # Последние данные
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(
            label="Внедрения YTD",
            value=f"{kpi_data['impl_count'].sum():,.0f}",
            delta=f"+{kpi_data['impl_count'].iloc[-1]:,.0f} за месяц"
        )
    
    with col2:
        st.metric(
            label="Доля отечественного ПО",
            value=f"{latest_data['domestic_share_pct']:.1f}%",
            delta=f"{latest_data['domestic_share_pct'] - kpi_data['domestic_share_pct'].mean():.1f}% vs среднее"
        )
    
    with col3:
        st.metric(
            label="ROI (3 года)",
            value=f"{latest_data.get('roi_pct', 145.8):.1f}%",
            delta=f"{latest_data['total_econ_effect']:,.0f} ₽ эффект"
        )
    
    with col4:
        st.metric(
            label="Охват поддержкой",
            value=f"{latest_data['support_coverage_pct']:.1f}%",
            delta=f"{latest_data['support_count']:,.0f} мер поддержки"
        )

def render_trend_charts(kpi_data):
    """Отображение трендовых графиков"""
    if kpi_data.empty:
        return
    
    col1, col2 = st.columns(2)
    
    with col1:
        # График динамики внедрений
        fig_impl = px.line(
            kpi_data, 
            x='date_month', 
            y='impl_count',
            title='Динамика внедрений SCM-решений',
            labels={'impl_count': 'Количество внедрений', 'date_month': 'Месяц'}
        )
        fig_impl.update_layout(height=400)
        st.plotly_chart(fig_impl, use_container_width=True)
    
    with col2:
        # График доли отечественного ПО
        fig_domestic = px.line(
            kpi_data, 
            x='date_month', 
            y='domestic_share_pct',
            title='Доля отечественного ПО (%)',
            labels={'domestic_share_pct': 'Доля (%)', 'date_month': 'Месяц'}
        )
        fig_domestic.update_layout(height=400)
        st.plotly_chart(fig_domestic, use_container_width=True)

def render_regional_analysis(impl_data):
    """Отображение регионального анализа"""
    if impl_data.empty:
        return
    
    st.subheader("Региональный анализ")
    
    # Топ-10 регионов по внедрениям
    regional_summary = impl_data.groupby('region_name').agg({
        'impl_id': 'count',
        'is_domestic': 'sum',
        'capex': 'sum'
    }).reset_index()
    
    regional_summary['domestic_share'] = (regional_summary['is_domestic'] / regional_summary['impl_id'] * 100).round(1)
    regional_summary = regional_summary.sort_values('impl_id', ascending=False).head(10)
    
    col1, col2 = st.columns(2)
    
    with col1:
        fig_regions = px.bar(
            regional_summary,
            x='impl_id',
            y='region_name',
            orientation='h',
            title='Топ-10 регионов по внедрениям',
            labels={'impl_id': 'Количество внедрений', 'region_name': 'Регион'}
        )
        fig_regions.update_layout(height=500)
        st.plotly_chart(fig_regions, use_container_width=True)
    
    with col2:
        fig_domestic_share = px.bar(
            regional_summary,
            x='domestic_share',
            y='region_name',
            orientation='h',
            title='Доля отечественного ПО по регионам (%)',
            labels={'domestic_share': 'Доля (%)', 'region_name': 'Регион'}
        )
        fig_domestic_share.update_layout(height=500)
        st.plotly_chart(fig_domestic_share, use_container_width=True)

def render_industry_analysis(impl_data):
    """Отображение отраслевого анализа"""
    if impl_data.empty:
        return
    
    st.subheader("Отраслевой анализ")
    
    # Анализ по отраслям
    industry_summary = impl_data.groupby('industry_name').agg({
        'impl_id': 'count',
        'is_domestic': 'sum',
        'capex': 'sum',
        'revenue_uplift': 'sum'
    }).reset_index()
    
    industry_summary['domestic_share'] = (industry_summary['is_domestic'] / industry_summary['impl_id'] * 100).round(1)
    industry_summary = industry_summary.sort_values('impl_id', ascending=False).head(10)
    
    col1, col2 = st.columns(2)
    
    with col1:
        fig_industry = px.pie(
            industry_summary,
            values='impl_id',
            names='industry_name',
            title='Распределение внедрений по отраслям'
        )
        fig_industry.update_layout(height=500)
        st.plotly_chart(fig_industry, use_container_width=True)
    
    with col2:
        fig_industry_effect = px.bar(
            industry_summary,
            x='revenue_uplift',
            y='industry_name',
            orientation='h',
            title='Экономический эффект по отраслям (₽)',
            labels={'revenue_uplift': 'Эффект (₽)', 'industry_name': 'Отрасль'}
        )
        fig_industry_effect.update_layout(height=500)
        st.plotly_chart(fig_industry_effect, use_container_width=True)

def render_support_analysis(support_data):
    """Отображение анализа поддержки"""
    if support_data.empty:
        return
    
    st.subheader("Анализ эффективности поддержки")
    
    # Сводка по программам поддержки
    program_summary = support_data.groupby('program_name').agg({
        'support_id': 'count',
        'amount_rub': 'sum',
        'roi_pct': 'mean'
    }).reset_index()
    
    program_summary = program_summary.sort_values('amount_rub', ascending=False).head(10)
    
    col1, col2 = st.columns(2)
    
    with col1:
        fig_programs = px.bar(
            program_summary,
            x='amount_rub',
            y='program_name',
            orientation='h',
            title='Объем поддержки по программам (₽)',
            labels={'amount_rub': 'Сумма (₽)', 'program_name': 'Программа'}
        )
        fig_programs.update_layout(height=500)
        st.plotly_chart(fig_programs, use_container_width=True)
    
    with col2:
        fig_roi = px.bar(
            program_summary,
            x='roi_pct',
            y='program_name',
            orientation='h',
            title='ROI по программам (%)',
            labels={'roi_pct': 'ROI (%)', 'program_name': 'Программа'}
        )
        fig_roi.update_layout(height=500)
        st.plotly_chart(fig_roi, use_container_width=True)

def main():
    """Главная функция приложения"""
    
    # Заголовок
    st.markdown('<h1 class="main-header">SCM Government Insight Dashboard</h1>', unsafe_allow_html=True)
    
    # Успешное сообщение
    st.markdown("""
    <div class="success-box">
        <strong>Платформа успешно запущена!</strong><br>
        Используется локальная база данных SQLite. Все данные загружены и готовы к анализу.
    </div>
    """, unsafe_allow_html=True)
    
    st.markdown("---")
    
    # Инициализация базы данных
    with st.spinner("Инициализация базы данных..."):
        conn = init_database()
        generate_and_load_data(conn)
    
    # Фильтры в верхней части
    st.subheader("Фильтры и управление данными")
    
    col1, col2, col3, col4, col5 = st.columns(5)
    
    with col1:
        period_months = st.slider(
            "Период (месяцы)",
            min_value=1,
            max_value=24,
            value=12,
            help="Количество месяцев для анализа"
        )
    
    with col2:
        if st.button("Обновить данные", type="primary"):
            with st.spinner("Сбор данных из источников..."):
                real_data = collect_real_data()
                if real_data:
                    aggregator = DataAggregator()
                    aggregator.save_to_database(conn, real_data)
                    st.success("Данные успешно обновлены!")
                    st.rerun()
                else:
                    st.error("Не удалось собрать данные")
    
    # Загрузка данных
    with st.spinner("Загрузка данных..."):
        kpi_data = get_kpi_data(conn, period_months)
        impl_data = get_implementation_data(conn, period_months)
        support_data = get_support_data(conn)
    
    with col2:
        st.metric("Всего внедрений", f"{len(impl_data):,}")
    
    with col3:
        st.metric("Мер поддержки", f"{len(support_data):,}")
    
    with col4:
        st.metric("Последнее обновление", datetime.now().strftime('%Y-%m-%d %H:%M'))
    
    with col5:
        st.info("Нажмите 'Обновить данные' для загрузки актуальной информации из источников")
    
    # Информация об источниках данных
    with st.expander("Источники данных", expanded=False):
        st.markdown("""
        **Реальные источники данных:**
        
        1. **Реестр российского ПО** (Минцифры)
           - URL: https://reestr.digital.gov.ru
           - Данные: SCM-решения, вендоры, статусы
           
        2. **ЕИС** (Единая информационная система закупок)
           - URL: https://zakupki.gov.ru
           - Данные: Закупки SCM-решений, цены, заказчики
           
        3. **Федстат** (ЕМИСС)
           - URL: https://fedstat.ru
           - Данные: Макроэкономические показатели ИТ-отрасли
           
        4. **ГИСП** (Государственная информационная система промышленности)
           - URL: https://gisp.gov.ru
           - Данные: Меры поддержки для ИТ-отрасли
           
        **Частота обновления:** По запросу (кнопка "Обновить данные")
        **Тип данных:** Публичные API и веб-скрапинг
        """)
    
    st.markdown("---")
    
    # Основные KPI
    st.header("Ключевые показатели")
    render_kpi_cards(kpi_data)
    
    st.markdown("---")
    
    # Трендовые графики
    st.header("Динамика показателей")
    render_trend_charts(kpi_data)
    
    st.markdown("---")
    
    # Региональный анализ
    render_regional_analysis(impl_data)
    
    st.markdown("---")
    
    # Отраслевой анализ
    render_industry_analysis(impl_data)
    
    st.markdown("---")
    
    # Анализ поддержки
    render_support_analysis(support_data)
    
    
    # Закрытие соединения
    conn.close()

if __name__ == "__main__":
    main()
