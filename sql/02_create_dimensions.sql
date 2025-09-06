-- Создание размерностей (dimensions) для звёздной схемы

-- Размерность "Дата"
CREATE TABLE IF NOT EXISTS core.d_date (
    date_id SERIAL PRIMARY KEY,
    date DATE NOT NULL UNIQUE,
    year INTEGER NOT NULL,
    quarter INTEGER NOT NULL,
    month INTEGER NOT NULL,
    month_name VARCHAR(20) NOT NULL,
    week INTEGER NOT NULL,
    day_of_year INTEGER NOT NULL,
    day_of_week INTEGER NOT NULL,
    day_name VARCHAR(20) NOT NULL,
    is_weekend BOOLEAN NOT NULL,
    is_holiday BOOLEAN NOT NULL DEFAULT FALSE,
    fiscal_year INTEGER,
    fiscal_quarter INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Создание индексов для d_date
CREATE INDEX IF NOT EXISTS idx_d_date_date ON core.d_date(date);
CREATE INDEX IF NOT EXISTS idx_d_date_year_month ON core.d_date(year, month);
CREATE INDEX IF NOT EXISTS idx_d_date_quarter ON core.d_date(quarter);

-- Размерность "Организация"
CREATE TABLE IF NOT EXISTS core.d_organization (
    org_id SERIAL PRIMARY KEY,
    inn VARCHAR(12) NOT NULL UNIQUE,
    ogrn VARCHAR(15),
    name VARCHAR(500) NOT NULL,
    short_name VARCHAR(200),
    legal_form VARCHAR(100),
    region_code VARCHAR(11),
    region_name VARCHAR(200),
    industry_code VARCHAR(10),
    industry_name VARCHAR(300),
    size_category VARCHAR(20), -- SMB, Enterprise
    is_government BOOLEAN DEFAULT FALSE,
    is_domestic BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    valid_from DATE DEFAULT CURRENT_DATE,
    valid_to DATE DEFAULT '9999-12-31',
    is_current BOOLEAN DEFAULT TRUE
);

-- Создание индексов для d_organization
CREATE INDEX IF NOT EXISTS idx_d_org_inn ON core.d_organization(inn);
CREATE INDEX IF NOT EXISTS idx_d_org_region ON core.d_organization(region_code);
CREATE INDEX IF NOT EXISTS idx_d_org_industry ON core.d_organization(industry_code);
CREATE INDEX IF NOT EXISTS idx_d_org_current ON core.d_organization(is_current);

-- Размерность "Вендор"
CREATE TABLE IF NOT EXISTS core.d_vendor (
    vendor_id SERIAL PRIMARY KEY,
    inn VARCHAR(12) NOT NULL,
    ogrn VARCHAR(15),
    name VARCHAR(500) NOT NULL,
    short_name VARCHAR(200),
    country_code VARCHAR(3),
    country_name VARCHAR(100),
    is_domestic BOOLEAN DEFAULT FALSE,
    is_resident BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    valid_from DATE DEFAULT CURRENT_DATE,
    valid_to DATE DEFAULT '9999-12-31',
    is_current BOOLEAN DEFAULT TRUE
);

-- Создание индексов для d_vendor
CREATE INDEX IF NOT EXISTS idx_d_vendor_inn ON core.d_vendor(inn);
CREATE INDEX IF NOT EXISTS idx_d_vendor_domestic ON core.d_vendor(is_domestic);
CREATE INDEX IF NOT EXISTS idx_d_vendor_current ON core.d_vendor(is_current);

-- Размерность "Решение"
CREATE TABLE IF NOT EXISTS core.d_solution (
    solution_id SERIAL PRIMARY KEY,
    solution_code VARCHAR(50) NOT NULL UNIQUE,
    name VARCHAR(500) NOT NULL,
    vendor_id INTEGER REFERENCES core.d_vendor(vendor_id),
    class_scm VARCHAR(50), -- WMS, TMS, S&OP, APS, OMS, Procurement
    version VARCHAR(50),
    is_domestic BOOLEAN DEFAULT FALSE,
    license_type VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    valid_from DATE DEFAULT CURRENT_DATE,
    valid_to DATE DEFAULT '9999-12-31',
    is_current BOOLEAN DEFAULT TRUE
);

-- Создание индексов для d_solution
CREATE INDEX IF NOT EXISTS idx_d_solution_code ON core.d_solution(solution_code);
CREATE INDEX IF NOT EXISTS idx_d_solution_vendor ON core.d_solution(vendor_id);
CREATE INDEX IF NOT EXISTS idx_d_solution_class ON core.d_solution(class_scm);
CREATE INDEX IF NOT EXISTS idx_d_solution_domestic ON core.d_solution(is_domestic);
CREATE INDEX IF NOT EXISTS idx_d_solution_current ON core.d_solution(is_current);

-- Размерность "Регион"
CREATE TABLE IF NOT EXISTS core.d_region (
    region_id SERIAL PRIMARY KEY,
    region_code VARCHAR(11) NOT NULL UNIQUE, -- ОКТМО
    region_name VARCHAR(200) NOT NULL,
    federal_district VARCHAR(100),
    federal_district_code VARCHAR(2),
    okato_code VARCHAR(11),
    timezone VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Создание индексов для d_region
CREATE INDEX IF NOT EXISTS idx_d_region_code ON core.d_region(region_code);
CREATE INDEX IF NOT EXISTS idx_d_region_federal_district ON core.d_region(federal_district);

-- Размерность "Отрасль"
CREATE TABLE IF NOT EXISTS core.d_industry (
    industry_id SERIAL PRIMARY KEY,
    okved_code VARCHAR(10) NOT NULL UNIQUE,
    okved_name VARCHAR(300) NOT NULL,
    naics_code VARCHAR(10),
    naics_name VARCHAR(300),
    parent_okved VARCHAR(10),
    level INTEGER NOT NULL,
    is_manufacturing BOOLEAN DEFAULT FALSE,
    is_logistics BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Создание индексов для d_industry
CREATE INDEX IF NOT EXISTS idx_d_industry_code ON core.d_industry(okved_code);
CREATE INDEX IF NOT EXISTS idx_d_industry_manufacturing ON core.d_industry(is_manufacturing);
CREATE INDEX IF NOT EXISTS idx_d_industry_logistics ON core.d_industry(is_logistics);

-- Размерность "Мера поддержки"
CREATE TABLE IF NOT EXISTS core.d_support_measure (
    support_measure_id SERIAL PRIMARY KEY,
    program_code VARCHAR(50) NOT NULL,
    program_name VARCHAR(500) NOT NULL,
    measure_type VARCHAR(50) NOT NULL, -- subsidy, grant, tax_benefit, state_order
    measure_name VARCHAR(500) NOT NULL,
    min_amount DECIMAL(15,2),
    max_amount DECIMAL(15,2),
    conditions TEXT,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    valid_from DATE DEFAULT CURRENT_DATE,
    valid_to DATE DEFAULT '9999-12-31',
    is_current BOOLEAN DEFAULT TRUE
);

-- Создание индексов для d_support_measure
CREATE INDEX IF NOT EXISTS idx_d_support_program ON core.d_support_measure(program_code);
CREATE INDEX IF NOT EXISTS idx_d_support_type ON core.d_support_measure(measure_type);
CREATE INDEX IF NOT EXISTS idx_d_support_active ON core.d_support_measure(is_active);
CREATE INDEX IF NOT EXISTS idx_d_support_current ON core.d_support_measure(is_current);

-- Размерность "Контракт"
CREATE TABLE IF NOT EXISTS core.d_contract (
    contract_id SERIAL PRIMARY KEY,
    notice_id VARCHAR(100) NOT NULL,
    contract_number VARCHAR(100),
    procurement_type VARCHAR(20) NOT NULL, -- 44-FZ, 223-FZ
    customer_inn VARCHAR(12) NOT NULL,
    supplier_inn VARCHAR(12),
    okpd2_code VARCHAR(20),
    okpd2_name VARCHAR(500),
    region_code VARCHAR(11),
    is_scm_related BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Создание индексов для d_contract
CREATE INDEX IF NOT EXISTS idx_d_contract_notice ON core.d_contract(notice_id);
CREATE INDEX IF NOT EXISTS idx_d_contract_customer ON core.d_contract(customer_inn);
CREATE INDEX IF NOT EXISTS idx_d_contract_supplier ON core.d_contract(supplier_inn);
CREATE INDEX IF NOT EXISTS idx_d_contract_scm ON core.d_contract(is_scm_related);

-- Комментарии к таблицам
COMMENT ON TABLE core.d_date IS 'Размерность "Дата" с календарными атрибутами';
COMMENT ON TABLE core.d_organization IS 'Размерность "Организация" (SCD Type-2)';
COMMENT ON TABLE core.d_vendor IS 'Размерность "Вендор" (SCD Type-2)';
COMMENT ON TABLE core.d_solution IS 'Размерность "Решение" (SCD Type-2)';
COMMENT ON TABLE core.d_region IS 'Размерность "Регион"';
COMMENT ON TABLE core.d_industry IS 'Размерность "Отрасль"';
COMMENT ON TABLE core.d_support_measure IS 'Размерность "Мера поддержки" (SCD Type-2)';
COMMENT ON TABLE core.d_contract IS 'Размерность "Контракт"';
