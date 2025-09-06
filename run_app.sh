#!/bin/bash

# Скрипт для запуска SCM Dashboard

echo "Запуск SCM Government Insight Dashboard..."

# Активация виртуального окружения
source venv/bin/activate

# Проверка зависимостей
echo "Проверка зависимостей..."
python -c "import streamlit, pandas, plotly, sqlite3; print('Все зависимости установлены')"

# Запуск приложения
echo "Запуск веб-приложения на http://localhost:8501"
echo "Для остановки нажмите Ctrl+C"
echo ""

streamlit run local_app.py --server.port 8501
