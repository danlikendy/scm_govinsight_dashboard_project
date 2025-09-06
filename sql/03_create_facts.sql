-- Создание фактов (fact tables) для звёздной схемы

-- Факт "Внедрения"
CREATE TABLE IF NOT EXISTS core.f_implementations (
    impl_id SERIAL PRIMARY KEY,
    org_id INTEGER NOT NULL REFERENCES core.d_organization(org_id),
    solution_id INTEGER NOT NULL REFERENCES core.d_solution(solution_id),
    vendor_id INTEGER NOT NULL REFERENCES core.d_vendor(vendor_id),
    region_id INTEGER REFERENCES core.d_region(region_id),
    industry_id INTEGER REFERENCES core.d_industry(industry_id),
    support_id INTEGER REFERENCES core.f_support(support_id),
    contract_id INTEGER REFERENCES core.d_contract(contract_id),
    date_go_live DATE NOT NULL,
    status VARCHAR(50) NOT NULL, -- planned, pilot, go-live, pilot_ok, cancelled
    capex DECIMAL(15,2),
    opex_delta DECIMAL(15,2), -- изменение OPEX (годовое)
    inv_turnover_delta DECIMAL(10,2), -- изменение оборачиваемости запасов
    lead_time_delta DECIMAL(10,2), -- изменение времени выполнения заказа (дни)
    penalties_delta DECIMAL(15,2), -- изменение штрафов (годовое)
    revenue_uplift DECIMAL(15,2), -- рост выручки (годовой)
    is_domestic BOOLEAN NOT NULL,
    is_control_group BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Создание индексов для f_implementations
CREATE INDEX IF NOT EXISTS idx_f_impl_org ON core.f_implementations(org_id);
CREATE INDEX IF NOT EXISTS idx_f_impl_solution ON core.f_implementations(solution_id);
CREATE INDEX IF NOT EXISTS idx_f_impl_vendor ON core.f_implementations(vendor_id);
CREATE INDEX IF NOT EXISTS idx_f_impl_date ON core.f_implementations(date_go_live);
CREATE INDEX IF NOT EXISTS idx_f_impl_status ON core.f_implementations(status);
CREATE INDEX IF NOT EXISTS idx_f_impl_domestic ON core.f_implementations(is_domestic);
CREATE INDEX IF NOT EXISTS idx_f_impl_control ON core.f_implementations(is_control_group);

-- Факт "Поддержка"
CREATE TABLE IF NOT EXISTS core.f_support (
    support_id SERIAL PRIMARY KEY,
    support_measure_id INTEGER NOT NULL REFERENCES core.d_support_measure(support_measure_id),
    org_id INTEGER NOT NULL REFERENCES core.d_organization(org_id),
    region_id INTEGER REFERENCES core.d_region(region_id),
    amount_rub DECIMAL(15,2) NOT NULL,
    approval_date DATE NOT NULL,
    disbursement_date DATE,
    conditions_met_flag BOOLEAN DEFAULT FALSE,
    kpi_planned DECIMAL(15,2),
    kpi_actual DECIMAL(15,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Создание индексов для f_support
CREATE INDEX IF NOT EXISTS idx_f_support_measure ON core.f_support(support_measure_id);
CREATE INDEX IF NOT EXISTS idx_f_support_org ON core.f_support(org_id);
CREATE INDEX IF NOT EXISTS idx_f_support_approval ON core.f_support(approval_date);
CREATE INDEX IF NOT EXISTS idx_f_support_disbursement ON core.f_support(disbursement_date);

-- Факт "Производительность"
CREATE TABLE IF NOT EXISTS core.f_performance (
    performance_id SERIAL PRIMARY KEY,
    org_id INTEGER NOT NULL REFERENCES core.d_organization(org_id),
    solution_id INTEGER REFERENCES core.d_solution(solution_id),
    region_id INTEGER REFERENCES core.d_region(region_id),
    industry_id INTEGER REFERENCES core.d_industry(industry_id),
    period_start DATE NOT NULL,
    period_end DATE NOT NULL,
    metric_name VARCHAR(100) NOT NULL,
    metric_value DECIMAL(15,4) NOT NULL,
    metric_unit VARCHAR(50),
    is_baseline BOOLEAN DEFAULT FALSE, -- базовый период
    is_after_impl BOOLEAN DEFAULT FALSE, -- после внедрения
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Создание индексов для f_performance
CREATE INDEX IF NOT EXISTS idx_f_perf_org ON core.f_performance(org_id);
CREATE INDEX IF NOT EXISTS idx_f_perf_solution ON core.f_performance(solution_id);
CREATE INDEX IF NOT EXISTS idx_f_perf_period ON core.f_performance(period_start, period_end);
CREATE INDEX IF NOT EXISTS idx_f_perf_metric ON core.f_performance(metric_name);
CREATE INDEX IF NOT EXISTS idx_f_perf_baseline ON core.f_performance(is_baseline);
CREATE INDEX IF NOT EXISTS idx_f_perf_after ON core.f_performance(is_after_impl);

-- Факт "Рынок"
CREATE TABLE IF NOT EXISTS core.f_market (
    market_id SERIAL PRIMARY KEY,
    region_id INTEGER REFERENCES core.d_region(region_id),
    industry_id INTEGER REFERENCES core.d_industry(industry_id),
    solution_id INTEGER REFERENCES core.d_solution(solution_id),
    period_start DATE NOT NULL,
    period_end DATE NOT NULL,
    market_size_rub DECIMAL(15,2),
    market_share_pct DECIMAL(5,2),
    avg_contract_value DECIMAL(15,2),
    competition_index DECIMAL(5,2),
    growth_rate_pct DECIMAL(5,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Создание индексов для f_market
CREATE INDEX IF NOT EXISTS idx_f_market_region ON core.f_market(region_id);
CREATE INDEX IF NOT EXISTS idx_f_market_industry ON core.f_market(industry_id);
CREATE INDEX IF NOT EXISTS idx_f_market_solution ON core.f_market(solution_id);
CREATE INDEX IF NOT EXISTS idx_f_market_period ON core.f_market(period_start, period_end);

-- Факт "Макроэкономические показатели"
CREATE TABLE IF NOT EXISTS core.f_macro (
    macro_id SERIAL PRIMARY KEY,
    region_id INTEGER REFERENCES core.d_region(region_id),
    industry_id INTEGER REFERENCES core.d_industry(industry_id),
    indicator_code VARCHAR(50) NOT NULL,
    indicator_name VARCHAR(200) NOT NULL,
    period_start DATE NOT NULL,
    period_end DATE NOT NULL,
    value DECIMAL(15,4) NOT NULL,
    unit VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Создание индексов для f_macro
CREATE INDEX IF NOT EXISTS idx_f_macro_region ON core.f_macro(region_id);
CREATE INDEX IF NOT EXISTS idx_f_macro_industry ON core.f_macro(industry_id);
CREATE INDEX IF NOT EXISTS idx_f_macro_indicator ON core.f_macro(indicator_code);
CREATE INDEX IF NOT EXISTS idx_f_macro_period ON core.f_macro(period_start, period_end);

-- Комментарии к таблицам фактов
COMMENT ON TABLE core.f_implementations IS 'Факт "Внедрения SCM-решений"';
COMMENT ON TABLE core.f_support IS 'Факт "Государственная поддержка"';
COMMENT ON TABLE core.f_performance IS 'Факт "Показатели производительности"';
COMMENT ON TABLE core.f_market IS 'Факт "Рыночные показатели"';
COMMENT ON TABLE core.f_macro IS 'Факт "Макроэкономические показатели"';

-- Ограничения целостности
ALTER TABLE core.f_implementations 
ADD CONSTRAINT chk_f_impl_status 
CHECK (status IN ('planned', 'pilot', 'go-live', 'pilot_ok', 'cancelled'));

ALTER TABLE core.f_support 
ADD CONSTRAINT chk_f_support_amount 
CHECK (amount_rub > 0);

ALTER TABLE core.f_performance 
ADD CONSTRAINT chk_f_perf_period 
CHECK (period_end >= period_start);

ALTER TABLE core.f_market 
ADD CONSTRAINT chk_f_market_share 
CHECK (market_share_pct >= 0 AND market_share_pct <= 100);
