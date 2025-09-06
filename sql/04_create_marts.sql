-- Создание витрин данных (marts) для BI

-- Витрина "Сводка по внедрениям"
CREATE MATERIALIZED VIEW IF NOT EXISTS mart.mart_impl_summary AS
SELECT 
    d.date_month,
    d.year,
    d.quarter,
    r.region_name,
    i.industry_name,
    s.name as solution_name,
    v.name as vendor_name,
    s.class_scm,
    SUM(i.is_domestic::int) as domestic_installs,
    COUNT(*) as installs_all,
    SUM(i.capex) as capex_total,
    SUM(i.opex_delta) as opex_delta_total,
    SUM(i.inv_turnover_delta) as inv_turnover_delta_total,
    SUM(i.lead_time_delta) as lead_time_delta_total,
    SUM(i.penalties_delta) as penalties_delta_total,
    SUM(i.revenue_uplift) as revenue_uplift_total,
    AVG(i.capex) as avg_capex,
    AVG(i.opex_delta) as avg_opex_delta,
    AVG(i.inv_turnover_delta) as avg_inv_turnover_delta,
    AVG(i.lead_time_delta) as avg_lead_time_delta,
    AVG(i.penalties_delta) as avg_penalties_delta,
    AVG(i.revenue_uplift) as avg_revenue_uplift
FROM core.f_implementations i
JOIN core.d_date d ON i.date_go_live = d.date
LEFT JOIN core.d_region r ON i.region_id = r.region_id
LEFT JOIN core.d_industry ind ON i.industry_id = ind.industry_id
LEFT JOIN core.d_solution s ON i.solution_id = s.solution_id
LEFT JOIN core.d_vendor v ON i.vendor_id = v.vendor_id
WHERE i.status IN ('go-live', 'pilot_ok')
GROUP BY 1,2,3,4,5,6,7,8;

-- Создание индексов для mart_impl_summary
CREATE INDEX IF NOT EXISTS idx_mart_impl_date ON mart.mart_impl_summary(date_month);
CREATE INDEX IF NOT EXISTS idx_mart_impl_region ON mart.mart_impl_summary(region_name);
CREATE INDEX IF NOT EXISTS idx_mart_impl_industry ON mart.mart_impl_summary(industry_name);
CREATE INDEX IF NOT EXISTS idx_mart_impl_solution ON mart.mart_impl_summary(solution_name);
CREATE INDEX IF NOT EXISTS idx_mart_impl_vendor ON mart.mart_impl_summary(vendor_name);
CREATE INDEX IF NOT EXISTS idx_mart_impl_class ON mart.mart_impl_summary(class_scm);

-- Витрина "Эффективность поддержки"
CREATE MATERIALIZED VIEW IF NOT EXISTS mart.mart_support_effectiveness AS
SELECT 
    d.date_month,
    d.year,
    d.quarter,
    r.region_name,
    i.industry_name,
    sm.program_name,
    sm.measure_type,
    COUNT(DISTINCT s.support_id) as support_count,
    SUM(s.amount_rub) as total_amount,
    AVG(s.amount_rub) as avg_amount,
    COUNT(DISTINCT i.impl_id) as supported_impls,
    SUM(i.capex) as total_capex_supported,
    SUM(i.revenue_uplift) as total_revenue_uplift,
    SUM(i.opex_delta + i.inv_turnover_delta + i.penalties_delta) as total_op_effect,
    -- ROI расчет (упрощенный)
    CASE 
        WHEN SUM(s.amount_rub) > 0 THEN 
            (SUM(i.revenue_uplift) + SUM(i.opex_delta + i.inv_turnover_delta + i.penalties_delta)) / SUM(s.amount_rub) * 100
        ELSE 0 
    END as roi_pct,
    -- Cost per implementation
    CASE 
        WHEN COUNT(DISTINCT i.impl_id) > 0 THEN 
            SUM(s.amount_rub) / COUNT(DISTINCT i.impl_id)
        ELSE 0 
    END as cost_per_impl
FROM core.f_support s
JOIN core.d_support_measure sm ON s.support_measure_id = sm.support_measure_id
JOIN core.d_date d ON s.approval_date = d.date
LEFT JOIN core.d_region r ON s.region_id = r.region_id
LEFT JOIN core.d_organization o ON s.org_id = o.org_id
LEFT JOIN core.d_industry ind ON o.industry_id = ind.industry_id
LEFT JOIN core.f_implementations i ON s.support_id = i.support_id
GROUP BY 1,2,3,4,5,6,7;

-- Создание индексов для mart_support_effectiveness
CREATE INDEX IF NOT EXISTS idx_mart_support_date ON mart.mart_support_effectiveness(date_month);
CREATE INDEX IF NOT EXISTS idx_mart_support_region ON mart.mart_support_effectiveness(region_name);
CREATE INDEX IF NOT EXISTS idx_mart_support_program ON mart.mart_support_effectiveness(program_name);
CREATE INDEX IF NOT EXISTS idx_mart_support_type ON mart.mart_support_effectiveness(measure_type);

-- Витрина "Рыночная аналитика"
CREATE MATERIALIZED VIEW IF NOT EXISTS mart.mart_market_analytics AS
SELECT 
    d.date_month,
    d.year,
    d.quarter,
    r.region_name,
    i.industry_name,
    s.class_scm,
    v.name as vendor_name,
    v.is_domestic,
    COUNT(*) as market_activity,
    SUM(i.capex) as market_volume,
    AVG(i.capex) as avg_contract_value,
    SUM(i.is_domestic::int) as domestic_activity,
    COUNT(*) - SUM(i.is_domestic::int) as foreign_activity,
    -- Доля отечественного ПО
    CASE 
        WHEN COUNT(*) > 0 THEN 
            SUM(i.is_domestic::int)::float / COUNT(*) * 100
        ELSE 0 
    END as domestic_share_pct,
    -- Индекс импортозамещения (упрощенный)
    CASE 
        WHEN COUNT(*) > 0 THEN 
            (SUM(i.is_domestic::int)::float / COUNT(*) * 0.6 + 
             AVG(CASE WHEN v.is_domestic THEN 1.0 ELSE 0.0 END) * 0.4)
        ELSE 0 
    END as isi_index
FROM core.f_implementations i
JOIN core.d_date d ON i.date_go_live = d.date
LEFT JOIN core.d_region r ON i.region_id = r.region_id
LEFT JOIN core.d_organization o ON i.org_id = o.org_id
LEFT JOIN core.d_industry ind ON o.industry_id = ind.industry_id
LEFT JOIN core.d_solution s ON i.solution_id = s.solution_id
LEFT JOIN core.d_vendor v ON i.vendor_id = v.vendor_id
WHERE i.status IN ('go-live', 'pilot_ok')
GROUP BY 1,2,3,4,5,6,7,8;

-- Создание индексов для mart_market_analytics
CREATE INDEX IF NOT EXISTS idx_mart_market_date ON mart.mart_market_analytics(date_month);
CREATE INDEX IF NOT EXISTS idx_mart_market_region ON mart.mart_market_analytics(region_name);
CREATE INDEX IF NOT EXISTS idx_mart_market_industry ON mart.mart_market_analytics(industry_name);
CREATE INDEX IF NOT EXISTS idx_mart_market_class ON mart.mart_market_analytics(class_scm);
CREATE INDEX IF NOT EXISTS idx_mart_market_domestic ON mart.mart_market_analytics(is_domestic);

-- Витрина "KPI дашборда"
CREATE MATERIALIZED VIEW IF NOT EXISTS mart.mart_dashboard_kpi AS
SELECT 
    d.date_month,
    d.year,
    d.quarter,
    -- Основные KPI
    COUNT(*) as impl_count,
    SUM(i.is_domestic::int) as domestic_impl_count,
    CASE 
        WHEN COUNT(*) > 0 THEN 
            SUM(i.is_domestic::int)::float / COUNT(*) * 100
        ELSE 0 
    END as domestic_share_pct,
    -- Экономический эффект
    SUM(i.revenue_uplift + i.opex_delta + i.inv_turnover_delta + i.penalties_delta) as total_econ_effect,
    AVG(i.revenue_uplift + i.opex_delta + i.inv_turnover_delta + i.penalties_delta) as avg_econ_effect,
    -- Поддержка
    COUNT(DISTINCT s.support_id) as support_count,
    SUM(COALESCE(s.amount_rub, 0)) as total_support_amount,
    CASE 
        WHEN COUNT(*) > 0 THEN 
            COUNT(DISTINCT s.support_id)::float / COUNT(*) * 100
        ELSE 0 
    END as support_coverage_pct,
    -- Индекс импортозамещения
    CASE 
        WHEN COUNT(*) > 0 THEN 
            (SUM(i.is_domestic::int)::float / COUNT(*) * 0.6 + 
             AVG(CASE WHEN v.is_domestic THEN 1.0 ELSE 0.0 END) * 0.4)
        ELSE 0 
    END as isi_index
FROM core.f_implementations i
JOIN core.d_date d ON i.date_go_live = d.date
LEFT JOIN core.d_vendor v ON i.vendor_id = v.vendor_id
LEFT JOIN core.f_support s ON i.support_id = s.support_id
WHERE i.status IN ('go-live', 'pilot_ok')
GROUP BY 1,2,3;

-- Создание индексов для mart_dashboard_kpi
CREATE INDEX IF NOT EXISTS idx_mart_kpi_date ON mart.mart_dashboard_kpi(date_month);
CREATE INDEX IF NOT EXISTS idx_mart_kpi_year ON mart.mart_dashboard_kpi(year);
CREATE INDEX IF NOT EXISTS idx_mart_kpi_quarter ON mart.mart_dashboard_kpi(quarter);

-- Функция для обновления материализованных представлений
CREATE OR REPLACE FUNCTION mart.refresh_all_marts()
RETURNS void AS $$
BEGIN
    REFRESH MATERIALIZED VIEW mart.mart_impl_summary;
    REFRESH MATERIALIZED VIEW mart.mart_support_effectiveness;
    REFRESH MATERIALIZED VIEW mart.mart_market_analytics;
    REFRESH MATERIALIZED VIEW mart.mart_dashboard_kpi;
    
    RAISE NOTICE 'All materialized views refreshed successfully';
END;
$$ LANGUAGE plpgsql;

-- Комментарии к витринам
COMMENT ON MATERIALIZED VIEW mart.mart_impl_summary IS 'Сводка по внедрениям SCM-решений';
COMMENT ON MATERIALIZED VIEW mart.mart_support_effectiveness IS 'Эффективность государственной поддержки';
COMMENT ON MATERIALIZED VIEW mart.mart_market_analytics IS 'Рыночная аналитика и конкуренция';
COMMENT ON MATERIALIZED VIEW mart.mart_dashboard_kpi IS 'KPI для дашборда';
COMMENT ON FUNCTION mart.refresh_all_marts() IS 'Обновление всех материализованных представлений';
