# SCM Government Insight Dashboard

Аналитическая платформа для мониторинга внедрений SCM-ПО, доли отечественных решений и эффективности мер господдержки.

## Цель проекта

Дать государственным стейкхолдерам, вендорам и интеграторам прозрачную, replicable-аналитику по:
- Динамике внедрений SCM-ПО
- Доле отечественных решений в установочной базе  
- Эффективности мер господдержки через ROI, окупаемость, косвенный макроэффект

## Архитектура

- **Хранилище**: PostgreSQL + витрины (звёздная схема)
- **Оркестрация**: Apache Airflow
- **Визуализация**: Streamlit (MVP) + Power BI (prod)
- **Качество данных**: Great Expectations
- **ML/Аналитика**: statsmodels, causalml, linearmodels
- **Мониторинг**: Prometheus + Grafana

## Структура проекта

```
/infra          # Docker Compose, K8s манифесты, мониторинг
/airflow        # DAG'и, operators, тесты
/etl            # Коннекторы, трансформации, реестры
/sql            # DDL/DML: core, marts, индексы
/streamlit      # UI-страницы, компоненты
/ml             # DID/PSM/SCM ноутбуки и пайплайны
/docs           # SLA, метрики, дата-контракты
/contracts      # Дата-контракты источников
```

## Быстрый старт

### 1. Локальный запуск (рекомендуется)

```bash
# Активация виртуального окружения
source venv/bin/activate

# Запуск приложения
streamlit run local_app.py --server.port 8501
```

Или используйте скрипт:

```bash
# Запуск через скрипт
./run_app.sh
```

**Доступ:** http://localhost:8501

### 2. Запуск с Docker (полная версия)

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

### 3. Инициализация данных

```bash
# Выполнение миграций БД
docker exec -i infra_postgres_1 psql -U scm_user scm_dashboard < sql/01_create_schemas.sql
docker exec -i infra_postgres_1 psql -U scm_user scm_dashboard < sql/02_create_dimensions.sql
docker exec -i infra_postgres_1 psql -U scm_user scm_dashboard < sql/03_create_facts.sql
docker exec -i infra_postgres_1 psql -U scm_user scm_dashboard < sql/04_create_marts.sql

# Запуск ETL процессов
docker exec infra_airflow-webserver_1 airflow dags unpause ingest_sources
docker exec infra_airflow-webserver_1 airflow dags unpause staging_normalize
```

## KPI и метрики

| Метрика | Описание | Формула |
|---------|----------|---------|
| **impl_count** | Количество внедрений | `∑1{status∈['go-live','pilot_ok'] ∧ date_go_live∈t}` |
| **domestic_share** | Доля отечественного ПО (%) | `installs_domestic(t) / installs_all(t) × 100` |
| **support_coverage** | Охват господдержкой (%) | `impl_with_support(t) / impl_all(t) × 100` |
| **ROI** | Рентабельность (3/5 лет) | `(∑effects - grant_cost) / grant_cost × 100` |
| **ISI** | Индекс импортозамещения | `w₁×domestic_share + w₂×local_components + w₃×R&D_share` |

## Источники данных

| Источник | Тип | Частота | Описание |
|----------|-----|---------|----------|
| **Реестр ПО** | HTML парсинг | Ежемесячно | Единый реестр российского ПО (Минцифры) |
| **ЕИС** | CSV загрузка | Ежедневно | Госзакупки 44-ФЗ/223-ФЗ |
| **ЕМИСС** | JSON API | Ежемесячно | Статистика Росстата |
| **ГИСП** | Документы | Ежемесячно | Меры промышленной поддержки |
| **Опросы** | Формы/S3 | По событию | Отчётность компаний |

## Методология оценки эффекта

### Difference-in-Differences (DID)
- **Treatment**: внедрения с поддержкой
- **Control**: сопоставимые внедрения без поддержки
- **Балансировка**: PSM по размеру, отрасли, региону

### Propensity Score Matching (PSM)
- Матчинг по ковариатам: размер компании, отрасль, выручка, регион
- Оценка ΔΔ по KPI: lead time, оборот запасов, штрафы, маржинальность

### Synthetic Control Method (SCM)
- Формирование синтетического "контроля" из не получавших поддержку
- Верификация устойчивости через placebo-тесты

## Дашборд страницы

1. **Executive Overview** - KPI-карточки, тренды, heatmap по регионам
2. **Доля отечественного ПО** - разрезы по отрасли, региону, классу SCM
3. **Эффективность поддержки** - DID-карты, ROI по программам
4. **Рынок и конкуренция** - доли вендоров, средние чеки, "горячие" сегменты
5. **Сценарии (What-if)** - ползунки бюджета, пересчёт метрик
6. **DataOps** - статус DAG'ов, качество данных, алерты

## Разработка

### Установка зависимостей

```bash
# Python зависимости
pip install -r streamlit/requirements.txt

# Дополнительные зависимости для ML
pip install -r ml/requirements.txt
```

### Запуск в режиме разработки

```bash
# Streamlit
cd streamlit
streamlit run app.py

# Airflow (локально)
cd airflow
airflow webserver --port 8080
airflow scheduler
```

### Тестирование

```bash
# Тесты качества данных
python etl/dq_tests.py

# Каузальный анализ
python ml/causal_analysis.py
```

## Документация

- [Руководство по развёртыванию](docs/DEPLOYMENT.md)
- [API Документация](docs/API.md)
- [Дата-контракты](contracts/)
- [SQL схемы](sql/)

## Технические детали

### ETL Контур
```
RAW → STAGING → CORE → MART
 ↓       ↓       ↓      ↓
MinIO  Normalize 3NF   Star Schema
```

### Качество данных
- **Great Expectations** для валидации
- **DQ-правила**: уникальность, диапазоны, референциальная целостность
- **Алерты** в Slack/Email при нарушениях

### Безопасность
- **RBAC**: роли viewer, analyst, data_steward, admin
- **RLS**: ограничения по регионам/ведомствам
- **Аудит**: журналирование доступа и выгрузок

## Мониторинг

- **Grafana**: дашборды производительности
- **Prometheus**: метрики системы
- **Airflow**: статус DAG'ов и задач
- **Great Expectations**: отчёты качества данных

## Вклад в проект

1. Fork репозитория
2. Создайте feature branch (`git checkout -b feature/amazing-feature`)
3. Commit изменения (`git commit -m 'Add amazing feature'`)
4. Push в branch (`git push origin feature/amazing-feature`)
5. Откройте Pull Request

## Лицензия

MIT License - см. [LICENSE](LICENSE) файл

## Поддержка

- **Документация**: `/docs/`
- **Issues**: GitHub Issues
- **Контакты**: support@example.com

---

**Статус**: Production Ready | **Версия**: 1.0.0 | **Последнее обновление**: 2024-01-15
