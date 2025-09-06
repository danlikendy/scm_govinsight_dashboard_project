# API Документация SCM Dashboard

## Обзор

SCM Dashboard предоставляет REST API для доступа к данным и метрикам. API построен на основе FastAPI и интегрирован с PostgreSQL.

## Базовый URL

```
http://localhost:8501/api/v1
```

## Аутентификация

Все запросы требуют API ключ в заголовке:

```http
Authorization: Bearer your-api-key
```

## Endpoints

### 1. KPI Метрики

#### GET /kpi/dashboard

Получение основных KPI для дашборда.

**Параметры:**
- `period_months` (int, optional): Период в месяцах (по умолчанию 12)
- `region` (string, optional): Фильтр по региону
- `industry` (string, optional): Фильтр по отрасли

**Пример запроса:**
```http
GET /api/v1/kpi/dashboard?period_months=6&region=Москва
```

**Ответ:**
```json
{
  "impl_count": 1250,
  "domestic_share_pct": 65.2,
  "roi_3y": 145.8,
  "support_coverage_pct": 78.5,
  "isi_index": 0.72,
  "total_econ_effect": 2500000000,
  "period_start": "2024-01-01",
  "period_end": "2024-06-30"
}
```

### 2. Внедрения

#### GET /implementations

Получение списка внедрений с фильтрацией.

**Параметры:**
- `page` (int): Номер страницы (по умолчанию 1)
- `limit` (int): Количество записей на странице (по умолчанию 100)
- `status` (string): Фильтр по статусу (planned, pilot, go-live, pilot_ok, cancelled)
- `is_domestic` (boolean): Фильтр по отечественному ПО
- `region` (string): Фильтр по региону
- `industry` (string): Фильтр по отрасли
- `date_from` (date): Начальная дата
- `date_to` (date): Конечная дата

**Пример запроса:**
```http
GET /api/v1/implementations?status=go-live&is_domestic=true&limit=50
```

**Ответ:**
```json
{
  "data": [
    {
      "impl_id": 1,
      "org_name": "ООО Пример",
      "solution_name": "1C:Управление складом",
      "vendor_name": "1C",
      "class_scm": "WMS",
      "date_go_live": "2024-01-15",
      "status": "go-live",
      "is_domestic": true,
      "capex": 5000000,
      "revenue_uplift": 15000000
    }
  ],
  "pagination": {
    "page": 1,
    "limit": 50,
    "total": 1250,
    "pages": 25
  }
}
```

### 3. Поддержка

#### GET /support

Получение данных о государственной поддержке.

**Параметры:**
- `page` (int): Номер страницы
- `limit` (int): Количество записей на странице
- `program_code` (string): Фильтр по программе
- `measure_type` (string): Фильтр по типу меры
- `region` (string): Фильтр по региону
- `date_from` (date): Начальная дата
- `date_to` (date): Конечная дата

**Пример запроса:**
```http
GET /api/v1/support?program_code=SCM-2024&measure_type=subsidy
```

**Ответ:**
```json
{
  "data": [
    {
      "support_id": 1,
      "program_name": "Поддержка SCM-решений 2024",
      "measure_type": "subsidy",
      "recipient_name": "ООО Пример",
      "amount_rub": 2000000,
      "approval_date": "2024-01-10",
      "disbursement_date": "2024-01-15",
      "roi_pct": 145.8
    }
  ],
  "pagination": {
    "page": 1,
    "limit": 100,
    "total": 500,
    "pages": 5
  }
}
```

### 4. Аналитика

#### GET /analytics/trends

Получение трендовых данных.

**Параметры:**
- `metric` (string): Метрика (impl_count, domestic_share, roi, support_coverage)
- `period_months` (int): Период в месяцах
- `granularity` (string): Детализация (month, quarter, year)
- `region` (string): Фильтр по региону
- `industry` (string): Фильтр по отрасли

**Пример запроса:**
```http
GET /api/v1/analytics/trends?metric=domestic_share&period_months=24&granularity=quarter
```

**Ответ:**
```json
{
  "metric": "domestic_share",
  "granularity": "quarter",
  "data": [
    {
      "period": "2024-Q1",
      "value": 62.5,
      "change_pct": 5.2
    },
    {
      "period": "2024-Q2",
      "value": 65.2,
      "change_pct": 4.3
    }
  ]
}
```

#### GET /analytics/regional

Региональный анализ.

**Параметры:**
- `metric` (string): Метрика для анализа
- `limit` (int): Количество топ-регионов
- `period_months` (int): Период в месяцах

**Пример запроса:**
```http
GET /api/v1/analytics/regional?metric=impl_count&limit=10
```

**Ответ:**
```json
{
  "data": [
    {
      "region_name": "Москва",
      "impl_count": 450,
      "domestic_share_pct": 68.5,
      "total_capex": 2250000000,
      "rank": 1
    }
  ]
}
```

### 5. Каузальный анализ

#### POST /analytics/causal

Запуск каузального анализа эффекта поддержки.

**Тело запроса:**
```json
{
  "method": "psm",
  "outcome_col": "revenue_uplift",
  "treatment_col": "received_support",
  "covariate_cols": ["org_size", "industry", "region"],
  "parameters": {
    "caliper": 0.1,
    "n_neighbors": 1
  }
}
```

**Ответ:**
```json
{
  "method": "psm",
  "treatment_effect": 1250000,
  "t_statistic": 3.45,
  "p_value": 0.001,
  "confidence_interval": [850000, 1650000],
  "n_treated": 150,
  "n_control": 300,
  "roi": {
    "roi_percent": 145.8,
    "discounted_effect": 2500000,
    "support_amount": 1000000,
    "payback_period": 0.8
  }
}
```

### 6. Экспорт данных

#### GET /export/csv

Экспорт данных в CSV формате.

**Параметры:**
- `table` (string): Таблица для экспорта (implementations, support, kpi)
- `filters` (object): Фильтры в JSON формате
- `columns` (array): Список колонок для экспорта

**Пример запроса:**
```http
GET /api/v1/export/csv?table=implementations&columns=org_name,solution_name,date_go_live,is_domestic
```

**Ответ:** CSV файл

#### GET /export/excel

Экспорт данных в Excel формате.

**Параметры:** Аналогично CSV экспорту

**Ответ:** Excel файл (.xlsx)

## Коды ошибок

| Код | Описание |
|-----|----------|
| 200 | Успешный запрос |
| 400 | Неверный запрос |
| 401 | Не авторизован |
| 403 | Доступ запрещен |
| 404 | Ресурс не найден |
| 422 | Ошибка валидации |
| 500 | Внутренняя ошибка сервера |

## Примеры использования

### Python

```python
import requests
import pandas as pd

# Настройка
API_BASE = "http://localhost:8501/api/v1"
API_KEY = "your-api-key"
headers = {"Authorization": f"Bearer {API_KEY}"}

# Получение KPI
response = requests.get(f"{API_BASE}/kpi/dashboard", headers=headers)
kpi_data = response.json()

# Получение внедрений
response = requests.get(f"{API_BASE}/implementations?limit=1000", headers=headers)
impl_data = response.json()
df = pd.DataFrame(impl_data['data'])

# Каузальный анализ
causal_request = {
    "method": "psm",
    "outcome_col": "revenue_uplift",
    "treatment_col": "received_support",
    "covariate_cols": ["org_size", "industry", "region"]
}
response = requests.post(f"{API_BASE}/analytics/causal", 
                        json=causal_request, headers=headers)
causal_result = response.json()
```

### JavaScript

```javascript
const API_BASE = 'http://localhost:8501/api/v1';
const API_KEY = 'your-api-key';

// Получение KPI
fetch(`${API_BASE}/kpi/dashboard`, {
  headers: {
    'Authorization': `Bearer ${API_KEY}`
  }
})
.then(response => response.json())
.then(data => console.log(data));

// Получение трендов
fetch(`${API_BASE}/analytics/trends?metric=domestic_share&period_months=12`, {
  headers: {
    'Authorization': `Bearer ${API_KEY}`
  }
})
.then(response => response.json())
.then(data => {
  // Построение графика
  const chart = new Chart(ctx, {
    type: 'line',
    data: {
      labels: data.data.map(d => d.period),
      datasets: [{
        label: 'Доля отечественного ПО (%)',
        data: data.data.map(d => d.value)
      }]
    }
  });
});
```

### cURL

```bash
# Получение KPI
curl -H "Authorization: Bearer your-api-key" \
     "http://localhost:8501/api/v1/kpi/dashboard?period_months=6"

# Получение внедрений
curl -H "Authorization: Bearer your-api-key" \
     "http://localhost:8501/api/v1/implementations?status=go-live&limit=50"

# Экспорт в CSV
curl -H "Authorization: Bearer your-api-key" \
     "http://localhost:8501/api/v1/export/csv?table=implementations" \
     -o implementations.csv
```

## Лимиты и ограничения

- Максимум 1000 записей на страницу
- Максимум 100 запросов в минуту на API ключ
- Таймаут запроса: 30 секунд
- Максимальный размер экспорта: 100MB

## Версионирование

API использует семантическое версионирование (SemVer).

- Текущая версия: v1.0.0
- Обратная совместимость гарантируется в рамках мажорной версии
- Новые функции добавляются в минорных версиях
- Критические изменения только в мажорных версиях
