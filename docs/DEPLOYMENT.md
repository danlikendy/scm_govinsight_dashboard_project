# Руководство по развёртыванию SCM Dashboard

## Быстрый старт

### 1. Предварительные требования

- Docker и Docker Compose
- Git
- Минимум 8GB RAM
- 20GB свободного места на диске

### 2. Клонирование и запуск

```bash
# Клонирование репозитория
git clone <repository-url>
cd scm_govinsight_dashboard_project

# Запуск всех сервисов
cd infra
docker-compose up -d

# Проверка статуса
docker-compose ps
```

### 3. Доступ к сервисам

- **Streamlit Dashboard**: http://localhost:8501
- **Airflow UI**: http://localhost:8080 (admin/admin)
- **Grafana**: http://localhost:3000 (admin/admin123)
- **Prometheus**: http://localhost:9090

## Детальная настройка

### 1. Настройка базы данных

```bash
# Подключение к PostgreSQL
docker exec -it infra_postgres_1 psql -U scm_user -d scm_dashboard

# Выполнение миграций
\i /docker-entrypoint-initdb.d/01_create_schemas.sql
\i /docker-entrypoint-initdb.d/02_create_dimensions.sql
\i /docker-entrypoint-initdb.d/03_create_facts.sql
\i /docker-entrypoint-initdb.d/04_create_marts.sql
```

### 2. Настройка Airflow

```bash
# Инициализация Airflow
docker exec -it infra_airflow-webserver_1 airflow db init

# Создание пользователя
docker exec -it infra_airflow-webserver_1 airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com \
    --password admin

# Запуск DAGs
docker exec -it infra_airflow-webserver_1 airflow dags unpause ingest_sources
docker exec -it infra_airflow-webserver_1 airflow dags unpause staging_normalize
```

### 3. Настройка источников данных

#### Реестр ПО (Минцифры)
- URL: https://reestr.digital.gov.ru
- Тип: HTML парсинг
- Частота: Ежемесячно
- Конфигурация: `contracts/reestr_po.yaml`

#### Госзакупки (ЕИС)
- URL: https://zakupki.gov.ru/opendata
- Тип: CSV загрузка
- Частота: Ежедневно
- Конфигурация: `contracts/eis_procurement.yaml`

#### Статистика (ЕМИСС)
- URL: https://fedstat.ru/api
- Тип: JSON API
- Частота: Ежемесячно
- Конфигурация: `contracts/fedstat_macro.yaml`

#### Поддержка (ГИСП)
- URL: https://gisp.gov.ru
- Тип: Документы (PDF/DOCX)
- Частота: Ежемесячно
- Конфигурация: `contracts/gisp_support.yaml`

## Мониторинг и обслуживание

### 1. Логи

```bash
# Просмотр логов всех сервисов
docker-compose logs -f

# Логи конкретного сервиса
docker-compose logs -f streamlit
docker-compose logs -f airflow-webserver
docker-compose logs -f postgres
```

### 2. Резервное копирование

```bash
# Бэкап базы данных
docker exec infra_postgres_1 pg_dump -U scm_user scm_dashboard > backup_$(date +%Y%m%d).sql

# Восстановление
docker exec -i infra_postgres_1 psql -U scm_user scm_dashboard < backup_20240101.sql
```

### 3. Обновление данных

```bash
# Ручной запуск DAG
docker exec -it infra_airflow-webserver_1 airflow dags trigger ingest_sources

# Обновление витрин
docker exec -it infra_airflow-webserver_1 airflow dags trigger build_marts
```

## Устранение неполадок

### 1. Проблемы с подключением к БД

```bash
# Проверка статуса PostgreSQL
docker exec infra_postgres_1 pg_isready -U scm_user

# Перезапуск БД
docker-compose restart postgres
```

### 2. Проблемы с Airflow

```bash
# Очистка метаданных Airflow
docker exec infra_airflow-webserver_1 airflow db reset

# Перезапуск Airflow
docker-compose restart airflow-webserver airflow-scheduler
```

### 3. Проблемы с данными

```bash
# Проверка качества данных
docker exec infra_airflow-webserver_1 python /opt/airflow/etl/dq_tests.py

# Очистка кэша Streamlit
docker-compose restart streamlit
```

## Масштабирование

### 1. Горизонтальное масштабирование

```yaml
# docker-compose.override.yml
version: '3.8'
services:
  streamlit:
    deploy:
      replicas: 3
  airflow-webserver:
    deploy:
      replicas: 2
```

### 2. Вертикальное масштабирование

```yaml
# docker-compose.override.yml
version: '3.8'
services:
  postgres:
    deploy:
      resources:
        limits:
          memory: 4G
        reservations:
          memory: 2G
```

## Безопасность

### 1. Настройка SSL

```bash
# Генерация сертификатов
openssl req -x509 -newkey rsa:4096 -keyout key.pem -out cert.pem -days 365 -nodes

# Обновление docker-compose.yml
# Добавить volumes для сертификатов
```

### 2. Настройка аутентификации

```python
# streamlit/config.toml
[server]
enableCORS = false
enableXsrfProtection = true
```

### 3. Ограничение доступа

```bash
# Настройка firewall
ufw allow 8501/tcp
ufw allow 8080/tcp
ufw deny 5432/tcp  # Закрыть прямой доступ к БД
```

## Производительность

### 1. Оптимизация БД

```sql
-- Создание индексов
CREATE INDEX CONCURRENTLY idx_f_impl_date_status ON core.f_implementations(date_go_live, status);
CREATE INDEX CONCURRENTLY idx_f_impl_org_solution ON core.f_implementations(org_id, solution_id);

-- Анализ статистики
ANALYZE core.f_implementations;
```

### 2. Оптимизация витрин

```sql
-- Обновление статистики материализованных представлений
REFRESH MATERIALIZED VIEW CONCURRENTLY mart.mart_dashboard_kpi;
```

### 3. Мониторинг производительности

- Grafana дашборды: http://localhost:3000
- Prometheus метрики: http://localhost:9090
- Airflow метрики: http://localhost:8080/admin/metrics

## Обновления

### 1. Обновление кода

```bash
# Получение обновлений
git pull origin main

# Пересборка образов
docker-compose build --no-cache

# Перезапуск сервисов
docker-compose up -d
```

### 2. Обновление данных

```bash
# Миграция схемы БД
docker exec -i infra_postgres_1 psql -U scm_user scm_dashboard < migrations/new_schema.sql

# Обновление витрин
docker exec infra_airflow-webserver_1 airflow dags trigger build_marts
```

## Поддержка

- Документация: `/docs/`
- Логи: `docker-compose logs`
- Мониторинг: Grafana + Prometheus
- Контакты: support@example.com
