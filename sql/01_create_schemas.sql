-- Создание схем для SCM Dashboard
-- Схемы: raw (сырые данные), staging (очищенные), core (3НФ), mart (звёздная схема)

-- Схема для сырых данных
CREATE SCHEMA IF NOT EXISTS raw;

-- Схема для промежуточной обработки
CREATE SCHEMA IF NOT EXISTS staging;

-- Схема для нормализованных данных (3НФ)
CREATE SCHEMA IF NOT EXISTS core;

-- Схема для витрин данных (звёздная схема)
CREATE SCHEMA IF NOT EXISTS mart;

-- Схема для справочников
CREATE SCHEMA IF NOT EXISTS ref;

-- Схема для метаданных и логирования
CREATE SCHEMA IF NOT EXISTS metadata;

-- Комментарии к схемам
COMMENT ON SCHEMA raw IS 'Сырые данные из источников без изменений';
COMMENT ON SCHEMA staging IS 'Промежуточная обработка и очистка данных';
COMMENT ON SCHEMA core IS 'Нормализованные данные в 3НФ';
COMMENT ON SCHEMA mart IS 'Витрины данных для BI (звёздная схема)';
COMMENT ON SCHEMA ref IS 'Справочники и классификаторы';
COMMENT ON SCHEMA metadata IS 'Метаданные, логи и мониторинг';
