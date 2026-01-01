#!/bin/bash

# Скрипт для запуска SCM Dashboard

echo "Запуск SCM Government Insight Dashboard..."

# Активация виртуального окружения
source venv/bin/activate

# Проверка зависимостей
echo "Проверка зависимостей..."
python -c "import streamlit, pandas, plotly, sqlite3; print('Все зависимости установлены')"

# Запуск приложения
echo "Запуск веб-приложения..."
echo ""
echo "Локальный доступ: http://localhost:8501"
echo ""
echo "Для доступа с других устройств:"
echo "1. Убедитесь, что другое устройство подключено к той же сети:"
echo "   - Wi-Fi: к той же Wi-Fi сети"
echo "   - Мобильная сеть: к той же точке доступа телефона"
echo ""
echo "2. IP-адрес этого компьютера:"
if [[ "$OSTYPE" == "darwin"* ]]; then
    # Проверяем все сетевые интерфейсы
    IP_EN0=$(ipconfig getifaddr en0 2>/dev/null)
    IP_EN1=$(ipconfig getifaddr en1 2>/dev/null)
    IP_UTUN=$(ipconfig getifaddr utun0 2>/dev/null || ipconfig getifaddr utun1 2>/dev/null)
    
    if [ -n "$IP_EN0" ]; then
        echo "   Wi-Fi (en0): $IP_EN0"
        IP=$IP_EN0
    fi
    if [ -n "$IP_EN1" ]; then
        echo "   Ethernet (en1): $IP_EN1"
        if [ -z "$IP" ]; then IP=$IP_EN1; fi
    fi
    if [ -n "$IP_UTUN" ]; then
        echo "   Мобильная сеть/точка доступа: $IP_UTUN"
        if [ -z "$IP" ]; then IP=$IP_UTUN; fi
    fi
    
    if [ -z "$IP" ]; then
        echo "   IP-адрес не найден автоматически"
        echo "   Выполните: ifconfig | grep 'inet '"
    fi
elif [[ "$OSTYPE" == "linux-gnu"* ]]; then
    IP=$(hostname -I | awk '{print $1}' 2>/dev/null || echo "не найден")
    echo "   IP-адрес: $IP"
else
    echo "   Выполните: ipconfig (Windows) или ifconfig (Linux/Mac)"
fi
echo ""
echo "3. Откройте в браузере на другом устройстве:"
if [ -n "$IP" ] && [ "$IP" != "не найден" ]; then
    echo "   http://$IP:8501"
    echo ""
    echo "   Если не работает, попробуйте другие IP-адреса выше"
else
    echo "   http://ВАШ_IP_АДРЕС:8501"
fi
echo ""
echo "Для остановки нажмите Ctrl+C"
echo ""

streamlit run local_app.py --server.port 8501 --server.address 0.0.0.0
