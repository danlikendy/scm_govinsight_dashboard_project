"""
SCM Government Insight Dashboard
Главное приложение Streamlit для мониторинга SCM-решений
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import sqlalchemy as sa
from sqlalchemy import create_engine, text
import os
from datetime import datetime, timedelta
import logging

# Настройка страницы
st.set_page_config(
    page_title="SCM Government Insight Dashboard",
    page_icon="",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# CSS стили
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        color: #1f77b4;
        text-align: center;
        margin-bottom: 2rem;
    }
    .kpi-card {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 0.5rem;
        border-left: 5px solid #1f77b4;
    }
    .metric-value {
        font-size: 2rem;
        font-weight: bold;
        color: #1f77b4;
    }
    .metric-label {
        font-size: 0.9rem;
        color: #666;
    }
</style>
""", unsafe_allow_html=True)

@st.cache_data
def get_db_connection():
    """Получение подключения к БД с кэшированием"""
    database_url = os.getenv('DATABASE_URL', 'postgresql://scm_user:scm_password@postgres:5432/scm_dashboard')
    return create_engine(database_url)

@st.cache_data(ttl=300)  # Кэш на 5 минут
def load_kpi_data(period_months=12):
    """Загрузка KPI данных"""
    try:
        engine = get_db_connection()
        
        query = f"""
        SELECT 
            date_month,
            year,
            quarter,
            impl_count,
            domestic_impl_count,
            domestic_share_pct,
            total_econ_effect,
            avg_econ_effect,
            support_count,
            total_support_amount,
            support_coverage_pct,
            isi_index
        FROM mart.mart_dashboard_kpi
        WHERE date_month >= CURRENT_DATE - INTERVAL '{period_months} months'
        ORDER BY date_month DESC
        """
        
        df = pd.read_sql(query, engine)
        return df
    except Exception as e:
        st.error(f"Ошибка загрузки данных: {str(e)}")
        return pd.DataFrame()

@st.cache_data(ttl=300)
def load_implementation_summary(period_months=12):
    """Загрузка сводки по внедрениям"""
    try:
        engine = get_db_connection()
        
        query = f"""
        SELECT 
            date_month,
            year,
            quarter,
            region_name,
            industry_name,
            solution_name,
            vendor_name,
            class_scm,
            domestic_installs,
            installs_all,
            capex_total,
            opex_delta_total,
            revenue_uplift_total
        FROM mart.mart_impl_summary
        WHERE date_month >= CURRENT_DATE - INTERVAL '{period_months} months'
        ORDER BY date_month DESC
        """
        
        df = pd.read_sql(query, engine)
        return df
    except Exception as e:
        st.error(f"Ошибка загрузки данных: {str(e)}")
        return pd.DataFrame()

@st.cache_data(ttl=300)
def load_support_effectiveness(period_months=12):
    """Загрузка данных по эффективности поддержки"""
    try:
        engine = get_db_connection()
        
        query = f"""
        SELECT 
            date_month,
            year,
            quarter,
            region_name,
            industry_name,
            program_name,
            measure_type,
            support_count,
            total_amount,
            supported_impls,
            roi_pct,
            cost_per_impl
        FROM mart.mart_support_effectiveness
        WHERE date_month >= CURRENT_DATE - INTERVAL '{period_months} months'
        ORDER BY date_month DESC
        """
        
        df = pd.read_sql(query, engine)
        return df
    except Exception as e:
        st.error(f"Ошибка загрузки данных: {str(e)}")
        return pd.DataFrame()

def render_kpi_cards(kpi_data):
    """Отображение KPI карточек"""
    if kpi_data.empty:
        st.warning("Нет данных для отображения")
        return
    
    latest_data = kpi_data.iloc[0]
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(
            label="Внедрения YTD",
            value=f"{latest_data['impl_count']:,.0f}",
            delta=f"+{kpi_data['impl_count'].sum():,.0f} за период"
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
            value=f"{latest_data.get('roi_pct', 0):.1f}%",
            delta=f"{latest_data.get('total_econ_effect', 0):,.0f} ₽ эффект"
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
        'installs_all': 'sum',
        'domestic_installs': 'sum',
        'capex_total': 'sum'
    }).reset_index()
    
    regional_summary['domestic_share'] = (regional_summary['domestic_installs'] / regional_summary['installs_all'] * 100).round(1)
    regional_summary = regional_summary.sort_values('installs_all', ascending=False).head(10)
    
    col1, col2 = st.columns(2)
    
    with col1:
        fig_regions = px.bar(
            regional_summary,
            x='installs_all',
            y='region_name',
            orientation='h',
            title='Топ-10 регионов по внедрениям',
            labels={'installs_all': 'Количество внедрений', 'region_name': 'Регион'}
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
        'installs_all': 'sum',
        'domestic_installs': 'sum',
        'capex_total': 'sum',
        'revenue_uplift_total': 'sum'
    }).reset_index()
    
    industry_summary['domestic_share'] = (industry_summary['domestic_installs'] / industry_summary['installs_all'] * 100).round(1)
    industry_summary = industry_summary.sort_values('installs_all', ascending=False).head(10)
    
    col1, col2 = st.columns(2)
    
    with col1:
        fig_industry = px.pie(
            industry_summary,
            values='installs_all',
            names='industry_name',
            title='Распределение внедрений по отраслям'
        )
        fig_industry.update_layout(height=500)
        st.plotly_chart(fig_industry, use_container_width=True)
    
    with col2:
        fig_industry_effect = px.bar(
            industry_summary,
            x='revenue_uplift_total',
            y='industry_name',
            orientation='h',
            title='Экономический эффект по отраслям (₽)',
            labels={'revenue_uplift_total': 'Эффект (₽)', 'industry_name': 'Отрасль'}
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
        'support_count': 'sum',
        'total_amount': 'sum',
        'supported_impls': 'sum',
        'roi_pct': 'mean'
    }).reset_index()
    
    program_summary = program_summary.sort_values('total_amount', ascending=False).head(10)
    
    col1, col2 = st.columns(2)
    
    with col1:
        fig_programs = px.bar(
            program_summary,
            x='total_amount',
            y='program_name',
            orientation='h',
            title='Объем поддержки по программам (₽)',
            labels={'total_amount': 'Сумма (₽)', 'program_name': 'Программа'}
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
    st.markdown("---")
    
    # Боковая панель с фильтрами
    st.sidebar.header("Фильтры")
    
    period_months = st.sidebar.slider(
        "Период (месяцы)",
        min_value=1,
        max_value=36,
        value=12,
        help="Количество месяцев для анализа"
    )
    
    # Загрузка данных
    with st.spinner("Загрузка данных..."):
        kpi_data = load_kpi_data(period_months)
        impl_data = load_implementation_summary(period_months)
        support_data = load_support_effectiveness(period_months)
    
    if kpi_data.empty and impl_data.empty and support_data.empty:
        st.error("Нет данных для отображения. Проверьте подключение к базе данных.")
        return
    
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
    
    # Информация о данных
    st.sidebar.markdown("---")
    st.sidebar.markdown("### Информация о данных")
    st.sidebar.info(f"Последнее обновление: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    st.sidebar.info(f"Период анализа: {period_months} месяцев")

if __name__ == "__main__":
    main()
