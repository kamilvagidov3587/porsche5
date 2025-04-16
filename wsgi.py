#!/usr/bin/env python3
"""
WSGI-файл для запуска приложения на продакшн-сервере
Используйте с gunicorn: gunicorn -w 4 -k gevent wsgi:app
"""

import threading
from app import app, run_scheduler, init_backup_settings

# Инициализация настроек резервного копирования
init_backup_settings()

# Запуск планировщика резервного копирования в отдельном потоке
backup_thread = threading.Thread(target=run_scheduler, daemon=True)
backup_thread.start()

# Экспортируем приложение для WSGI сервера
application = app

if __name__ == "__main__":
    app.run() 