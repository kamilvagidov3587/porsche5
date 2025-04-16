#!/usr/bin/env python3
import sys
import os

# Установка путей для проекта
INTERP = os.path.expanduser("~/venv/bin/python3")
if sys.executable != INTERP:
    os.execl(INTERP, INTERP, *sys.argv)

# Добавляем текущую директорию в Python path
cwd = os.getcwd()
sys.path.append(cwd)

# Импортируем Flask-приложение
from app import app as application

# WSGI-точка входа
if __name__ == '__main__':
    application.run() 