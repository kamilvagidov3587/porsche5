#!/usr/bin/env python3
# Файл для запуска приложения на локальном сервере

import os
import threading
from app import app, run_scheduler, init_backup_settings, start_backup_scheduler

if __name__ == "__main__":
    # Инициализация настроек резервного копирования
    init_backup_settings()
    
    # Запуск планировщика резервного копирования в отдельном потоке
    start_backup_scheduler()
    print("Планировщик резервного копирования запущен успешно")
    
    # Получение порта из переменных окружения для совместимости с облачными платформами
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, threaded=True, debug=True) 