from flask import Flask, render_template, request, redirect, url_for, flash, session, jsonify, send_file, make_response
import os
import json
from datetime import datetime, timedelta
from math import ceil
import requests
import io
import xlsxwriter
from werkzeug.middleware.proxy_fix import ProxyFix
from functools import lru_cache, wraps
import threading
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
import schedule
import time
import copy
import multiprocessing
import random
import traceback
from urllib.parse import quote

# Определение декоратора login_required для защиты административных маршрутов
def login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if not session.get('admin'):
            flash('Пожалуйста, войдите в систему', 'danger')
            return redirect(url_for('admin_login'))
        return f(*args, **kwargs)
    return decorated_function

app = Flask(__name__)
app.secret_key = os.getenv('SECRET_KEY', 'secret_key_for_session')
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024  # Ограничение загрузки файлов до 16 МБ

# Настройка для работы за прокси-сервером
app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1)

# Путь к файлу с настройками
SETTINGS_FILE = os.environ.get('SETTINGS_FILE', os.path.join(os.path.dirname(__file__), 'settings.json'))

# Добавляем блокировку для безопасной работы с данными при конкурентном доступе
data_lock = threading.Lock()
settings_lock = threading.Lock()

# Создаем файл настроек, если он не существует
if not os.path.exists(SETTINGS_FILE):
    with settings_lock:
        # Создаем директорию для данных, если её нет
        os.makedirs(os.path.dirname(SETTINGS_FILE), exist_ok=True)
        with open(SETTINGS_FILE, 'w', encoding='utf-8') as f:
            json.dump({
                "whatsapp_link": "https://chat.whatsapp.com/EIa4wkifsVQDttzjOKlOY3"
            }, f, ensure_ascii=False, indent=4)

# Кэш для участников
PARTICIPANTS_CACHE = None
PARTICIPANTS_CACHE_TTL = 60  # 60 секунд

# Кэш для настроек с временем жизни
settings_cache = {
    'data': None,
    'timestamp': 0
}
SETTINGS_CACHE_TTL = 60  # 60 секунд

# Настройки для резервного копирования
BACKUP_SETTINGS = {
    'enabled': True,
    'interval': 'daily',  # daily, hourly, custom
    'yandex_token': 'y0__xDy1a_hARjblgMguuSn6xJXlhubBW4-LmJ7Gq8ZG8kwV-zyIw',  # OAuth-токен Яндекс.Диска
    'last_backup': None,
    'custom_value': 24,    # Значение для произвольного интервала
    'custom_unit': 'hours' # Единица измерения: seconds, minutes, hours, days, weeks
}

# Добавляем событие для сигнализации об изменении настроек планировщика
scheduler_event = threading.Event()

def load_settings():
    """Загрузка настроек из файла с кэшированием"""
    global settings_cache
    current_time = datetime.now().timestamp()
    
    # Если есть актуальные данные в кэше, возвращаем их
    if settings_cache['data'] is not None and current_time - settings_cache['timestamp'] < SETTINGS_CACHE_TTL:
        return settings_cache['data']
    
    # Иначе загружаем из файла
    with settings_lock:
        try:
            with open(SETTINGS_FILE, 'r', encoding='utf-8') as f:
                settings = json.load(f)
                # Обновляем кэш
                settings_cache['data'] = settings
                settings_cache['timestamp'] = current_time
                return settings
        except:
            # В случае ошибки возвращаем настройки по умолчанию
            default_settings = {
                "whatsapp_link": "https://chat.whatsapp.com/EIa4wkifsVQDttzjOKlOY3"
            }
            settings_cache['data'] = default_settings
            settings_cache['timestamp'] = current_time
            return default_settings

def save_settings(settings_data):
    """Сохранение настроек в файл"""
    with settings_lock:
        with open(SETTINGS_FILE, 'w', encoding='utf-8') as f:
            json.dump(settings_data, f, ensure_ascii=False, indent=4)
        
        # Обновляем кэш
        settings_cache['data'] = settings_data
        settings_cache['timestamp'] = datetime.now().timestamp()

# Список допустимых городов и районов
ALLOWED_CITIES = [
    # Основные города
    'махачкала', 'каспийск',
    
    # Районы Махачкалы
    'кировский район', 'ленинский район', 'советский район',
    
    # Посёлки городского типа Кировского района
    'ленинкент', 'семендер', 'сулак', 'шамхал',
    
    # Сёла Кировского района
    'богатырёвка', 'красноармейское', 'остров чечень', 'шамхал-термен',
    
    # Посёлки и сёла Ленинского района
    'новый кяхулай', 'новый хушет', 'талги',
    
    # Посёлки Советского района
    'альбурикент', 'кяхулай', 'тарки',
    
    # Микрорайоны и районы
    '5-й посёлок', '5 посёлок',
    
    # Дополнительные микрорайоны и кварталы
    'каменный карьер', 'афган-городок', 'кемпинг', 'кирпичный', 
    'ккоз', 'тау', 'центральный', 'южный', 'рекреационная зона', 'финский квартал',
    
    # Пригородные районы
    'турали'
]

# Для тестирования на хостинге - разрешаем все города, если установлена переменная окружения
if os.environ.get('ALLOW_ALL_LOCATIONS') == 'true':
    def check_location_allowed(city):
        return True
else:
    def check_location_allowed(city):
        return city in ALLOWED_CITIES

# Кэш для данных о местоположении по IP
ip_location_cache = {}

# Время жизни кэша местоположения (1 час)
IP_CACHE_TTL = 3600

@lru_cache(maxsize=128)
def get_location_from_ip(ip_address):
    """Получение информации о местоположении по IP-адресу"""
    # Проверяем кэш
    current_time = datetime.now().timestamp()
    if ip_address in ip_location_cache:
        cache_entry = ip_location_cache[ip_address]
        if current_time - cache_entry['timestamp'] < IP_CACHE_TTL:
            return cache_entry['data']
    
    try:
        response = requests.get(f"http://ip-api.com/json/{ip_address}", timeout=3)
        data = response.json()
        if data.get('status') == 'success':
            result = {
                'city': data.get('city', '').lower(),
                'region': data.get('regionName', ''),
                'country': data.get('country', '')
            }
            # Сохраняем в кэш
            ip_location_cache[ip_address] = {
                'data': result,
                'timestamp': current_time
            }
            return result
        return None
    except Exception as e:
        print(f"Ошибка при определении местоположения: {e}")
        return None

@lru_cache(maxsize=128)
def get_location_from_coordinates(lat, lng):
    """Получение информации о местоположении по координатам"""
    try:
        response = requests.get(
            f"https://nominatim.openstreetmap.org/reverse?format=json&lat={lat}&lon={lng}&zoom=18&addressdetails=1",
            headers={'User-Agent': 'CarRaffle/1.0'},
            timeout=3
        )
        data = response.json()
        if 'address' in data:
            city = data['address'].get('city', '').lower()
            if not city:
                city = data['address'].get('town', '').lower()
            if not city:
                city = data['address'].get('village', '').lower()
            
            return {
                'city': city,
                'region': data['address'].get('state', ''),
                'country': data['address'].get('country', '')
            }
        return None
    except Exception as e:
        print(f"Ошибка при определении местоположения по координатам: {e}")
        return None

def load_participants(force_reload=False):
    """Загружает данные участников из файла JSON или с Яндекс.Диска"""
    global PARTICIPANTS_CACHE
    
    # Если данные уже загружены и не требуется принудительная перезагрузка, возвращаем кэш
    if PARTICIPANTS_CACHE is not None and not force_reload:
        return PARTICIPANTS_CACHE
    
    try:
        # Получаем токен Яндекс.Диска из настроек
        settings = load_settings()
        yandex_token = settings.get('backup_settings', {}).get('yandex_token')
        
        participants = []
        
        # Сначала пробуем загрузить с Яндекс.Диска если есть токен
        if yandex_token:
            try:
                # Проверяем существование файла
                url = "https://cloud-api.yandex.net/v1/disk/resources"
                headers = {"Authorization": f"OAuth {yandex_token}"}
                params = {"path": "app:/participants.json"}
                
                file_check = requests.get(url, headers=headers, params=params)
                
                if file_check.status_code == 200:
                    # URL файла на Яндекс.Диске для скачивания
                    download_url = "https://cloud-api.yandex.net/v1/disk/resources/download"
                    download_params = {"path": "app:/participants.json"}
                    
                    response = requests.get(download_url, headers=headers, params=download_params)
                    
                    if response.status_code == 200:
                        # Получаем ссылку на скачивание
                        download_link = response.json().get("href")
                        
                        # Скачиваем файл с данными с явным указанием UTF-8
                        data_response = requests.get(
                            download_link, 
                            headers={'Accept-Charset': 'utf-8'}
                        )
                        
                        if data_response.status_code == 200:
                            # Устанавливаем кодировку UTF-8 для ответа
                            data_response.encoding = 'utf-8'
                            # Парсим JSON-данные
                            participants = data_response.json()
                            app.logger.info(f"Загружено {len(participants)} участников с Яндекс.Диска")
            except Exception as e:
                app.logger.error(f"Ошибка при загрузке с Яндекс.Диска: {str(e)}")
        
        # Если не удалось загрузить с Яндекс.Диска или нет токена, пробуем локальный файл
        if not participants and os.path.exists('data/participants.json'):
            with open('data/participants.json', 'r', encoding='utf-8') as file:
                participants = json.load(file)
                app.logger.info(f"Загружено {len(participants)} участников из локального файла")
        
        # Проверяем и исправляем кодировку для всех текстовых полей
        for participant in participants:
            for key, value in participant.items():
                if isinstance(value, str):
                    if key == 'full_name':  # Особая обработка для имени
                        participant[key] = fix_cyrillic(value)
                    else:
                        try:
                            value.encode('utf-8').decode('utf-8')
                        except UnicodeError:
                            try:
                                participant[key] = value.encode('latin1').decode('utf-8')
                            except:
                                participant[key] = value.encode('utf-8', errors='replace').decode('utf-8')
                
                # Проверяем вложенные структуры
                elif isinstance(value, dict):
                    for nested_key, nested_value in value.items():
                        if isinstance(nested_value, str):
                            if nested_key == 'city':  # Особая обработка для города
                                nested_value = fix_cyrillic(nested_value)
                            else:
                                try:
                                    nested_value.encode('utf-8').decode('utf-8')
                                except UnicodeError:
                                    try:
                                        value[nested_key] = nested_value.encode('latin1').decode('utf-8')
                                    except:
                                        value[nested_key] = nested_value.encode('utf-8', errors='replace').decode('utf-8')
        
        # Обновляем кэш и возвращаем данные
        PARTICIPANTS_CACHE = participants
        return participants
                
    except Exception as e:
        app.logger.error(f"Ошибка при загрузке данных участников: {str(e)}")
        PARTICIPANTS_CACHE = []
        return []

def save_participant(data):
    """Сохраняет информацию об участнике в файл данных и на Яндекс.Диск"""
    try:
        # Загружаем текущих участников
        participants = load_participants()
        
        # Проверяем и корректируем кодировку всех текстовых полей участника
        for key, value in data.items():
            if isinstance(value, str):
                if key == 'full_name':  # Для имени используем специальную функцию
                    data[key] = fix_cyrillic(value)
                else:
                    try:
                        # Проверяем корректность UTF-8
                        value.encode('utf-8').decode('utf-8')
                    except UnicodeError:
                        # Если есть проблемы, пробуем исправить
                        try:
                            data[key] = value.encode('latin1').decode('utf-8')
                        except:
                            data[key] = value.encode('utf-8', errors='replace').decode('utf-8')
        
        # Проверяем вложенные структуры (координаты, местоположение)
        for nested_key in ['coordinates', 'location']:
            if nested_key in data and isinstance(data[nested_key], dict):
                for key, value in data[nested_key].items():
                    if isinstance(value, str):
                        if key == 'city':  # Для города используем специальную функцию
                            data[nested_key][key] = fix_cyrillic(value)
                        else:
                            try:
                                value.encode('utf-8').decode('utf-8')
                            except UnicodeError:
                                try:
                                    data[nested_key][key] = value.encode('latin1').decode('utf-8')
                                except:
                                    data[nested_key][key] = value.encode('utf-8', errors='replace').decode('utf-8')
        
        # Добавляем нового участника
        participants.append(data)
        
        # Обновляем глобальный кэш участников
        global PARTICIPANTS_CACHE
        PARTICIPANTS_CACHE = participants
        
        # Проверяем, существует ли каталог data
        if not os.path.exists('data'):
            os.makedirs('data')
            
        # Сохраняем локально с корректной кодировкой
        with open('data/participants.json', 'w', encoding='utf-8') as file:
            json.dump(participants, file, ensure_ascii=False, indent=4)
            
        # Получаем токен Яндекс.Диска из настроек
        settings = load_settings()
        yandex_token = settings.get('backup_settings', {}).get('yandex_token')
        
        if yandex_token:
            try:
                # Сохраняем данные на Яндекс.Диск
                # Сначала получаем ссылку для загрузки
                headers = {"Authorization": f"OAuth {yandex_token}"}
                upload_url = "https://cloud-api.yandex.net/v1/disk/resources/upload"
                params = {"path": "app:/participants.json", "overwrite": "true"}
                
                response = requests.get(upload_url, headers=headers, params=params)
                
                if response.status_code == 200:
                    # Получаем ссылку для загрузки
                    upload_link = response.json().get("href")
                    
                    # Подготавливаем данные для отправки
                    json_data = json.dumps(participants, ensure_ascii=False, indent=4)
                    
                    # Отправляем данные с явным указанием UTF-8
                    upload_response = requests.put(
                        upload_link, 
                        data=json_data.encode('utf-8'),
                        headers={'Content-Type': 'application/json; charset=utf-8'}
                    )
                    
                    if upload_response.status_code == 201 or upload_response.status_code == 200:
                        app.logger.info(f"Данные успешно сохранены на Яндекс.Диск. Всего участников: {len(participants)}")
                    else:
                        app.logger.error(f"Ошибка при сохранении данных на Яндекс.Диск: {upload_response.status_code}")
                else:
                    app.logger.error(f"Ошибка при получении ссылки для загрузки: {response.status_code}")
            except Exception as e:
                app.logger.error(f"Ошибка при сохранении на Яндекс.Диск: {str(e)}")
        
        # Если включено резервное копирование, запускаем его в фоновом режиме
        if settings.get('backup_settings', {}).get('enabled', False):
            backup_thread = threading.Thread(target=create_backup, daemon=True)
            backup_thread.start()
            
        return True
    except Exception as e:
        app.logger.error(f"Ошибка при сохранении данных участника: {str(e)}")
        return False

def is_phone_registered(phone):
    """Проверка, зарегистрирован ли уже данный номер телефона"""
    participants = load_participants()
    # Нормализуем телефон для сравнения (удаляем все, кроме цифр)
    normalized_phone = ''.join(filter(str.isdigit, phone))
    
    for participant in participants:
        normalized_participant_phone = ''.join(filter(str.isdigit, participant['phone']))
        if normalized_participant_phone == normalized_phone:
            return True
    return False

def get_ticket_by_phone(phone):
    """Получение данных участника по номеру телефона"""
    participants = load_participants()
    # Нормализуем телефон для сравнения (удаляем все, кроме цифр)
    normalized_phone = ''.join(filter(str.isdigit, phone))
    
    for participant in participants:
        # Убедимся, что у участника есть номер телефона
        if not participant.get('phone'):
            continue
            
        normalized_participant_phone = ''.join(filter(str.isdigit, participant['phone']))
        
        # Сначала проверяем точное совпадение
        if normalized_participant_phone == normalized_phone:
            return {
                'ticket_number': participant.get('ticket_number'),
                'full_name': participant.get('full_name')
            }
        
        # Если номера имеют разную длину, но последние 10 цифр совпадают 
        # (разные форматы записи российских номеров: +7/8 в начале)
        if (len(normalized_participant_phone) >= 10 and len(normalized_phone) >= 10 and 
            normalized_participant_phone[-10:] == normalized_phone[-10:]):
            return {
                'ticket_number': participant.get('ticket_number'),
                'full_name': participant.get('full_name')
            }
    
    return None

# Функция для генерации уникального 4-значного номера
def generate_unique_ticket_number():
    """Генерация последовательного номера участника (1, 2, 3, ...)"""
    participants = load_participants()
    
    # Если список участников пуст, начинаем с 1
    if not participants:
        return 1
    
    # Находим максимальный существующий номер
    max_number = 0
    for participant in participants:
        ticket_number = participant.get('ticket_number', 0)
        if isinstance(ticket_number, (int, float)) and ticket_number > max_number:
            max_number = ticket_number
    
    # Получаем следующий номер (просто увеличиваем максимальный на 1)
    next_number = max_number + 1
    
    return next_number

@app.route('/')
def index():
    """Главная страница с формой регистрации"""
    settings = load_settings()
    return render_template('index.html', whatsapp_link=settings.get('whatsapp_link'))

@app.route('/check-coordinates')
def check_coordinates():
    """Проверка местоположения пользователя по координатам"""
    lat = request.args.get('lat')
    lng = request.args.get('lng')
    
    if not lat or not lng:
        return jsonify({"status": "error", "message": "Не указаны координаты"})
    
    location = get_location_from_coordinates(lat, lng)
    if not location:
        return jsonify({"status": "error", "message": "Не удалось определить местоположение по координатам"})
    
    city = location.get('city', '').lower()
    allowed = check_location_allowed(city)
    
    return jsonify({
        "status": "success", 
        "allowed": allowed,
        "city": city
    })

@app.route('/check-location')
def check_location():
    """Проверка местоположения пользователя по IP"""
    ip_address = request.remote_addr
    
    # Для локальной разработки используем внешний IP
    if ip_address == '127.0.0.1':
        # Для тестирования можно использовать любой публичный IP из Махачкалы
        # Это только для разработки
        return jsonify({"status": "success", "allowed": True, "city": "махачкала (тестовый режим)"})
    
    location = get_location_from_ip(ip_address)
    if not location:
        return jsonify({"status": "error", "message": "Не удалось определить местоположение"})
    
    city = location.get('city', '').lower()
    allowed = check_location_allowed(city)
    
    return jsonify({
        "status": "success", 
        "allowed": allowed,
        "city": city
    })

@app.route('/check-phone')
def check_phone():
    """Проверка существования номера телефона в базе данных"""
    phone = request.args.get('phone')
    
    if not phone:
        return jsonify({"exists": False})
    
    # Проверяем, зарегистрирован ли уже данный номер телефона
    if is_phone_registered(phone):
        return jsonify({
            "exists": True, 
            "message": "Этот номер телефона уже зарегистрирован в розыгрыше. Регистрация возможна только один раз."
        })
    
    return jsonify({"exists": False})

@app.route('/register', methods=['POST'])
def register():
    """Обработка регистрации нового участника"""
    try:
        # Устанавливаем кодировку для запроса
        request.encoding = 'utf-8'
        
        # Получаем данные от пользователя
        full_name_raw = request.form.get('full_name', '').strip()
        phone = request.form.get('phone', '').strip()
        age = int(request.form.get('age', 0))
        gender = request.form.get('gender', 'male')
        latitude = request.form.get('latitude', '')
        longitude = request.form.get('longitude', '')
        ip_address = request.remote_addr
        
        # Применяем специальную функцию для исправления кодировки имени
        full_name = fix_cyrillic(full_name_raw)
        
        # Если имя все еще выглядит как кракозябры, пробуем другие методы
        if not any(ord('А') <= ord(c) <= ord('я') for c in full_name) and any(c in full_name for c in 'ÐÑ'):
            # Последняя попытка - предполагаем, что это UTF-8 в неправильной кодировке
            try:
                full_name = full_name_raw.encode('latin1').decode('utf-8', errors='replace')
            except:
                # Если ничего не сработало, оставляем как есть
                full_name = full_name_raw
        
        # Нормализуем телефон (оставляем только цифры)
        normalized_phone = ''.join(filter(str.isdigit, phone))
        
        # Проверяем, что номер телефона полный (не менее 11 цифр для российского номера)
        if len(normalized_phone) < 11:
            return jsonify({
                'success': False,
                'message': 'Пожалуйста, введите полный номер телефона'
            })
        
        # Если номер начинается с 8, заменяем на 7 для стандартизации
        if normalized_phone.startswith('8') and len(normalized_phone) == 11:
            normalized_phone = '7' + normalized_phone[1:]
        
        # Проверяем, что пользователь с таким телефоном еще не зарегистрирован
        if is_phone_registered(normalized_phone):
            return jsonify({
                'success': False,
                'message': 'Этот номер телефона уже зарегистрирован в розыгрыше'
            })
        
        # Создаем данные о местоположении
        location = None
        if ip_address:
            location = get_location_from_ip(ip_address)
            # Исправляем кодировку города, если он есть
            if location and 'city' in location and location['city']:
                location['city'] = fix_cyrillic(location['city'])
        
        # Создаем данные о координатах
        coordinates = None
        if latitude and longitude:
            try:
                lat_float = float(latitude)
                lng_float = float(longitude)
                coordinates = get_location_from_coordinates(lat_float, lng_float)
                # Исправляем кодировку города, если он есть
                if coordinates and 'city' in coordinates and coordinates['city']:
                    coordinates['city'] = fix_cyrillic(coordinates['city'])
            except (ValueError, TypeError):
                pass
        
        # Генерируем уникальный номер участника
        ticket_number = generate_unique_ticket_number()
        
        # Сохраняем данные участника
        participant_data = {
            'ticket_number': ticket_number,
            'full_name': full_name,
            'phone': normalized_phone,
            'age': age,
            'gender': gender,
            'registration_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'ip_address': ip_address
        }
        
        # Добавляем информацию о местоположении, если она доступна
        if location:
            participant_data['location'] = location
        
        # Добавляем информацию о координатах, если она доступна
        if coordinates:
            participant_data['coordinates'] = coordinates
        
        # Сохраняем данные участника
        if save_participant(participant_data):
            # Сохраняем номер участника в сессии для показа на странице успеха
            session['ticket_number'] = ticket_number
            session['full_name'] = full_name
            
            return jsonify({
                'success': True,
                'message': 'Регистрация прошла успешно',
                'ticket_number': ticket_number
            })
        else:
            return jsonify({
                'success': False,
                'message': 'Произошла ошибка при сохранении данных'
            })
    except Exception as e:
        app.logger.error(f"Ошибка при регистрации: {str(e)}")
        return jsonify({
            'success': False,
            'message': 'Произошла ошибка при регистрации'
        })

@app.route('/success')
def success():
    """Страница успешной регистрации"""
    return render_template('success.html')

@app.route('/get-ticket-number')
def get_ticket_number():
    """Получение номера участника из сессии"""
    ticket_number = session.get('ticket_number')
    if ticket_number:
        return jsonify({'success': True, 'ticket_number': ticket_number})
    else:
        return jsonify({'success': False, 'message': 'Номер не найден. Возможно, вы еще не зарегистрировались или сессия истекла.'}), 404

@app.route('/admin')
@login_required
def admin_panel():
    """Админ-панель"""
    # Загружаем свежие данные о участниках, игнорируя кэш
    participants = load_participants(force_reload=True)
    
    # Получаем статистику
    stats = {
        'total': len(participants),
        'today': sum(1 for p in participants if datetime.strptime(p.get('registration_time', '1970-01-01 00:00:00'), '%Y-%m-%d %H:%M:%S').date() == datetime.now().date()),
        'week': sum(1 for p in participants if (datetime.now().date() - datetime.strptime(p.get('registration_time', '1970-01-01 00:00:00'), '%Y-%m-%d %H:%M:%S').date()).days <= 7),
        'month': sum(1 for p in participants if (datetime.now().date() - datetime.strptime(p.get('registration_time', '1970-01-01 00:00:00'), '%Y-%m-%d %H:%M:%S').date()).days <= 30),
    }
    
    # Загружаем настройки
    settings = load_settings()
    
    # Получаем информацию о следующем бэкапе
    next_backup_time = get_next_backup_info()
    
    return render_template('admin.html', 
                           participants=participants, 
                           stats=stats, 
                           settings=settings,
                           next_backup_time=next_backup_time)

@app.route('/admin-login', methods=['POST'])
def admin_login():
    """Обработка входа администратора"""
    password = request.form.get('password')
    secure_password = "kvdarit_avto35"  # Пароль администратора
    
    if password == secure_password:
        session['admin'] = True
        return redirect(url_for('admin_panel'))
    else:
        flash('Неверный пароль!', 'danger')
        response = make_response(render_template('admin_login.html'))
        response.headers['Content-Type'] = 'text/html; charset=utf-8'
        return response

@app.route('/delete-participants', methods=['POST'])
def delete_participants():
    # Проверка, что пользователь является администратором
    if not session.get('admin'):
        return jsonify({'success': False, 'message': 'Доступ запрещен'}), 403
    
    try:
        # Получаем токен Яндекс.Диска из настроек
        settings = load_settings()
        yandex_token = settings.get('backup_settings', {}).get('yandex_token')
        
        if not yandex_token:
            return jsonify({'success': False, 'message': 'Не найден токен Яндекс.Диска'}), 500
        
        # Обновляем кэш
        global PARTICIPANTS_CACHE
        PARTICIPANTS_CACHE = []
        
        # Загружаем пустой массив на Яндекс.Диск
        headers = {"Authorization": f"OAuth {yandex_token}"}
        upload_url = "https://cloud-api.yandex.net/v1/disk/resources/upload"
        params = {"path": "app:/participants.json", "overwrite": "true"}
        
        response = requests.get(upload_url, headers=headers, params=params)
        
        if response.status_code == 200:
            # Получаем ссылку для загрузки
            upload_link = response.json().get("href")
            
            # Отправляем пустой массив
            upload_response = requests.put(upload_link, data="[]".encode('utf-8'))
            
            if upload_response.status_code == 201 or upload_response.status_code == 200:
                app.logger.info("Все данные участников успешно удалены с Яндекс.Диска")
                return jsonify({'success': True})
            else:
                error_msg = f"Ошибка при удалении данных на Яндекс.Диске: {upload_response.status_code}"
                app.logger.error(error_msg)
                return jsonify({'success': False, 'message': error_msg}), 500
        else:
            error_msg = f"Ошибка при получении ссылки для загрузки: {response.status_code}"
            app.logger.error(error_msg)
            return jsonify({'success': False, 'message': error_msg}), 500
            
    except Exception as e:
        error_msg = f"Ошибка при удалении данных участников: {str(e)}"
        app.logger.error(error_msg)
        return jsonify({'success': False, 'message': error_msg}), 500

@app.route('/delete-participant/<int:index>', methods=['POST'])
def delete_participant(index):
    # Проверка, что пользователь является администратором
    if not session.get('admin'):
        return jsonify({'success': False, 'message': 'Доступ запрещен'}), 403
    
    try:
        # Загрузка списка участников
        participants = load_participants()
        
        # Проверка валидности индекса
        if index < 0 or index >= len(participants):
            return jsonify({'success': False, 'message': 'Участник не найден'}), 404
        
        # Удаление участника
        del participants[index]
        
        # Получаем токен Яндекс.Диска из настроек
        settings = load_settings()
        yandex_token = settings.get('backup_settings', {}).get('yandex_token')
        
        if not yandex_token:
            return jsonify({'success': False, 'message': 'Не найден токен Яндекс.Диска'}), 500
        
        # Обновляем кэш
        global PARTICIPANTS_CACHE
        PARTICIPANTS_CACHE = participants
        
        # Загружаем обновленный список на Яндекс.Диск
        headers = {"Authorization": f"OAuth {yandex_token}"}
        upload_url = "https://cloud-api.yandex.net/v1/disk/resources/upload"
        params = {"path": "app:/participants.json", "overwrite": "true"}
        
        response = requests.get(upload_url, headers=headers, params=params)
        
        if response.status_code == 200:
            # Получаем ссылку для загрузки
            upload_link = response.json().get("href")
            
            # Подготавливаем данные для отправки
            json_data = json.dumps(participants, ensure_ascii=False, indent=4)
            
            # Отправляем данные
            upload_response = requests.put(upload_link, data=json_data.encode('utf-8'))
            
            if upload_response.status_code == 201 or upload_response.status_code == 200:
                app.logger.info(f"Участник успешно удален. Осталось участников: {len(participants)}")
                return jsonify({'success': True})
            else:
                error_msg = f"Ошибка при обновлении данных на Яндекс.Диске: {upload_response.status_code}"
                app.logger.error(error_msg)
                return jsonify({'success': False, 'message': error_msg}), 500
        else:
            error_msg = f"Ошибка при получении ссылки для загрузки: {response.status_code}"
            app.logger.error(error_msg)
            return jsonify({'success': False, 'message': error_msg}), 500
            
    except Exception as e:
        error_msg = f"Ошибка при удалении участника: {str(e)}"
        app.logger.error(error_msg)
        return jsonify({'success': False, 'message': error_msg}), 500

@app.route('/export-to-excel', methods=['GET'])
def export_to_excel():
    """Генерация Excel-файла с данными участников"""
    # Проверка, что пользователь является администратором
    if not session.get('admin'):
        flash('Доступ запрещен. Пожалуйста, войдите как администратор.', 'danger')
        return redirect(url_for('admin'))
    
    try:
        # Загрузка данных участников
        participants = load_participants()
        
        # Создание объекта для записи Excel-файла
        output = io.BytesIO()
        workbook = xlsxwriter.Workbook(output)
        worksheet = workbook.add_worksheet('Участники')
        
        # Форматирование
        header_format = workbook.add_format({
            'bold': True,
            'bg_color': '#007bff',
            'font_color': 'white',
            'border': 1
        })
        
        cell_format = workbook.add_format({
            'border': 1
        })
        
        # Установка ширины столбцов
        worksheet.set_column('A:A', 25)  # Имя
        worksheet.set_column('B:B', 10)  # Номер участника
        worksheet.set_column('C:C', 20)  # Телефон
        worksheet.set_column('D:D', 10)  # Возраст
        worksheet.set_column('E:E', 15)  # Пол
        worksheet.set_column('F:F', 20)  # Город
        worksheet.set_column('G:G', 20)  # Регион
        worksheet.set_column('H:H', 20)  # Страна
        worksheet.set_column('I:I', 25)  # Время регистрации
        worksheet.set_column('J:J', 30)  # Координаты
        worksheet.set_column('K:K', 20)  # IP-адрес
        
        # Заголовки столбцов
        headers = [
            'Имя', 'Номер участника', 'Телефон', 'Возраст', 'Пол', 'Город', 'Регион', 'Страна', 
            'Время регистрации', 'Координаты', 'IP-адрес'
        ]
        
        for col, header in enumerate(headers):
            worksheet.write(0, col, header, header_format)
        
        # Заполнение данными
        for i, participant in enumerate(participants):
            row = i + 1
            
            # Безопасное извлечение данных
            full_name = str(participant.get('full_name', ''))
            ticket_number = str(participant.get('ticket_number', ''))
            phone = str(participant.get('phone', ''))
            age = str(participant.get('age', ''))
            gender = 'Мужской' if str(participant.get('gender', '')) == 'male' else 'Женский'
            
            # Безопасное извлечение данных о местоположении
            city = ''
            region = ''
            country = ''
            
            # Получение города из координат (если они есть)
            coordinates = participant.get('coordinates', {})
            if coordinates and isinstance(coordinates, dict):
                city_from_coords = coordinates.get('city', '')
                if city_from_coords:
                    city = city_from_coords
            
            # Если город не определен из координат, пробуем получить его из location
            if not city:
                location = participant.get('location', {})
                if location and isinstance(location, dict):
                    city = location.get('city', '')
                    region = location.get('region', '')
                    country = location.get('country', '')
            
            # Форматирование координат
            coords = ''
            if coordinates and isinstance(coordinates, dict):
                lat = coordinates.get('latitude', '')
                lng = coordinates.get('longitude', '')
                if lat and lng:
                    coords = f"{lat}, {lng}"
            
            # IP-адрес
            ip_address = str(participant.get('ip_address', ''))
            
            # Время регистрации
            reg_time = str(participant.get('registration_time', ''))
            
            # Капитализация строк
            if city:
                city = city.capitalize()
            if region:
                region = region.capitalize()
            if country:
                country = country.capitalize()
            
            # Данные для записи
            data = [
                full_name,
                ticket_number,
                phone,
                age,
                gender,
                city,
                region,
                country,
                reg_time,
                coords,
                ip_address
            ]
            
            # Запись данных в Excel
            for col, value in enumerate(data):
                worksheet.write(row, col, value, cell_format)
        
        # Закрытие и возврат Excel-файла
        workbook.close()
        output.seek(0)
        
        # Формирование имени файла с текущей датой
        current_date = datetime.now().strftime('%Y-%m-%d')
        filename = f'participants_{current_date}.xlsx'
        
        return send_file(
            output, 
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet', 
            as_attachment=True, 
            download_name=filename
        )
    except Exception as e:
        import traceback
        print(traceback.format_exc())  # Печать полного трейсбека ошибки в консоль
        flash(f'Ошибка при создании Excel-файла: {str(e)}', 'danger')
        return redirect(url_for('admin'))

@app.route('/update-whatsapp-link', methods=['POST'])
def update_whatsapp_link():
    """Обновление ссылки на WhatsApp-сообщество"""
    # Проверка, что пользователь является администратором
    if not session.get('admin'):
        return jsonify({'success': False, 'message': 'Доступ запрещен'}), 403
    
    try:
        new_link = request.form.get('whatsapp_link', '').strip()
        if not new_link:
            return jsonify({'success': False, 'message': 'Ссылка не может быть пустой'}), 400
        
        # Загрузка текущих настроек
        settings = load_settings()
        
        # Обновление ссылки
        settings['whatsapp_link'] = new_link
        
        # Сохранение обновленных настроек
        save_settings(settings)
        
        return jsonify({'success': True, 'message': 'Ссылка успешно обновлена'})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500

@app.route('/update-backup-settings', methods=['POST'])
def update_backup_settings():
    """Обновление настроек резервного копирования"""
    # Проверка, что пользователь является администратором
    if not session.get('admin'):
        return jsonify({'success': False, 'message': 'Доступ запрещен'}), 403
    
    try:
        # Получаем данные из формы
        backup_enabled = request.form.get('backup_enabled') == 'true'
        yandex_token = request.form.get('yandex_token', '').strip()
        backup_interval = request.form.get('backup_interval', 'daily')
        
        # Получаем настройки произвольного расписания
        custom_value = request.form.get('custom_value', '24')
        custom_unit = request.form.get('custom_unit', 'hours')
        
        # Проверяем, что значение интервала является положительным числом
        try:
            custom_value = int(custom_value)
            if custom_value <= 0:
                return jsonify({'success': False, 'message': 'Интервал должен быть положительным числом'}), 400
        except ValueError:
            return jsonify({'success': False, 'message': 'Интервал должен быть числом'}), 400
        
        if backup_enabled and not yandex_token:
            return jsonify({'success': False, 'message': 'Укажите токен Яндекс.Диска для резервного копирования'}), 400
        
        # Загрузка текущих настроек
        settings = load_settings()
        
        # Обновление настроек резервного копирования
        if 'backup_settings' not in settings:
            settings['backup_settings'] = copy.deepcopy(BACKUP_SETTINGS)
        
        # Сохраняем предыдущие значения для проверки, были ли изменения
        old_enabled = settings['backup_settings'].get('enabled', False)
        old_interval = settings['backup_settings'].get('interval', 'daily')
        old_value = settings['backup_settings'].get('custom_value', 24)
        old_unit = settings['backup_settings'].get('custom_unit', 'hours')
        
        # Обновляем настройки
        settings['backup_settings']['enabled'] = backup_enabled
        settings['backup_settings']['yandex_token'] = yandex_token
        settings['backup_settings']['interval'] = backup_interval
        settings['backup_settings']['custom_value'] = custom_value
        settings['backup_settings']['custom_unit'] = custom_unit
        
        # Сохранение обновленных настроек
        save_settings(settings)
        
        # Сигнализируем планировщику, что настройки изменились и нужно перезапустить расчёты
        # Особенно если включили резервное копирование или изменили настройки интервала
        if (not old_enabled and backup_enabled) or \
           (old_interval != backup_interval) or \
           (backup_interval == 'custom' and (old_value != custom_value or old_unit != custom_unit)):
            # Устанавливаем флаг события, чтобы планировщик пересчитал время
            scheduler_event.set()
        
        # Формируем информационное сообщение о следующей резервной копии
        next_backup_message = ""
        if backup_enabled:
            if backup_interval == 'daily':
                next_backup_message = " Следующая копия будет создана в 03:00."
            elif backup_interval == 'hourly':
                next_backup_message = " Следующая копия будет создана в начале следующего часа."
            elif backup_interval == 'custom':
                # Используем фактически сохраненные значения из настроек
                value = settings['backup_settings']['custom_value']
                unit = settings['backup_settings']['custom_unit']
                
                unit_name = ""
                if unit == 'seconds':
                    unit_name = "секунд"
                elif unit == 'minutes':
                    unit_name = "минут"
                elif unit == 'hours':
                    unit_name = "часов"
                elif unit == 'days':
                    unit_name = "дней"
                elif unit == 'weeks':
                    unit_name = "недель"
                
                next_backup_message = f" Следующая копия будет создана через {value} {unit_name} после последнего резервного копирования."
        
        return jsonify({'success': True, 'message': 'Настройки резервного копирования обновлены.' + next_backup_message})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500

@app.route('/create-backup', methods=['POST'])
def manual_backup():
    """Ручное создание резервной копии"""
    # Проверка, что пользователь является администратором
    if not session.get('admin'):
        return jsonify({'success': False, 'message': 'Доступ запрещен'}), 403
    
    try:
        # Получаем данные участников
        participants = load_participants()
        if not participants:
            return jsonify({'success': False, 'message': 'Нет данных для резервного копирования'}), 400
        
        # Загрузка настроек
        settings = load_settings()
        
        # Получаем токен Яндекс.Диска
        yandex_token = request.form.get('yandex_token') or settings.get('backup_settings', {}).get('yandex_token')
        if not yandex_token:
            return jsonify({'success': False, 'message': 'Не указан токен Яндекс.Диска для резервного копирования'}), 400
        
        # Создаем и отправляем резервную копию
        success = send_backup_to_yadisk(participants, yandex_token)
        
        if success:
            # Обновляем время последнего резервного копирования
            if 'backup_settings' not in settings:
                settings['backup_settings'] = copy.deepcopy(BACKUP_SETTINGS)
            
            settings['backup_settings']['last_backup'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            save_settings(settings)
            
            return jsonify({'success': True, 'message': 'Резервная копия успешно загружена на Яндекс.Диск'})
        else:
            return jsonify({'success': False, 'message': 'Не удалось создать резервную копию'}), 500
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500

# Добавляем настройку для сжатия ответов
@app.after_request
def add_header(response):
    # Кэширование статических файлов
    if 'Cache-Control' not in response.headers:
        if request.path.startswith('/static/'):
            # Кэшировать статические файлы на 1 год
            response.headers['Cache-Control'] = 'public, max-age=31536000'
        else:
            # Не кэшировать HTML-страницы
            response.headers['Cache-Control'] = 'no-store'
    
    # Явно указываем кодировку UTF-8 для всех ответов
    if 'Content-Type' in response.headers:
        if 'text/html' in response.headers['Content-Type']:
            response.headers['Content-Type'] = 'text/html; charset=utf-8'
        elif 'application/json' in response.headers['Content-Type']:
            response.headers['Content-Type'] = 'application/json; charset=utf-8'
        elif 'text/css' in response.headers['Content-Type']:
            response.headers['Content-Type'] = 'text/css; charset=utf-8'
        elif 'application/javascript' in response.headers['Content-Type']:
            response.headers['Content-Type'] = 'application/javascript; charset=utf-8'
    
    return response

# Функция для создания и загрузки резервной копии на Яндекс.Диск
def send_backup_to_yadisk(json_data, token):
    """Загрузка резервной копии данных на Яндекс.Диск"""
    try:
        # Обработка кириллических символов в данных участников
        processed_json_data = []
        for participant in json_data:
            processed_participant = participant.copy()
            
            # Проверяем кодировку ФИО
            full_name = participant.get('full_name', '')
            if isinstance(full_name, str):
                try:
                    # Проверяем корректность UTF-8
                    full_name.encode('utf-8').decode('utf-8')
                except UnicodeError:
                    # Если есть проблемы, пробуем исправить
                    try:
                        full_name = full_name.encode('latin1').decode('utf-8')
                    except:
                        full_name = full_name.encode('utf-8', errors='replace').decode('utf-8')
            processed_participant['full_name'] = full_name
            
            # Проверяем кодировку города
            if 'coordinates' in participant and 'city' in participant['coordinates'] and participant['coordinates']['city']:
                city = participant['coordinates']['city']
                if isinstance(city, str):
                    try:
                        city.encode('utf-8').decode('utf-8')
                    except UnicodeError:
                        try:
                            city = city.encode('latin1').decode('utf-8')
                        except:
                            city = city.encode('utf-8', errors='replace').decode('utf-8')
                    processed_participant['coordinates']['city'] = city
            
            if 'location' in participant and 'city' in participant['location'] and participant['location']['city']:
                city = participant['location']['city']
                if isinstance(city, str):
                    try:
                        city.encode('utf-8').decode('utf-8')
                    except UnicodeError:
                        try:
                            city = city.encode('latin1').decode('utf-8')
                        except:
                            city = city.encode('utf-8', errors='replace').decode('utf-8')
                    processed_participant['location']['city'] = city
            
            processed_json_data.append(processed_participant)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        print(f"[{datetime.now()}] Начинаем создание резервной копии и загрузку на Яндекс.Диск")
        
        # Создаем Excel-файл
        excel_data = create_excel_backup(processed_json_data)
        print(f"[{datetime.now()}] Excel файл создан в памяти")
        
        # Создаем JSON-файл, явно указываем, что не нужно экранировать не-ASCII символы
        json_str = json.dumps(processed_json_data, ensure_ascii=False, indent=4)
        json_bytes = json_str.encode('utf-8')
        print(f"[{datetime.now()}] JSON файл создан в памяти")
        
        # Путь на Яндекс.Диске, где будут храниться резервные копии
        folder_path = "/kvdarit_avto35_backup"
        
        # Создаем папку на Яндекс.Диске, если она не существует
        headers = {"Authorization": f"OAuth {token}"}
        create_folder_url = "https://cloud-api.yandex.net/v1/disk/resources"
        
        print(f"[{datetime.now()}] Проверяем/создаем папку {folder_path} на Яндекс.Диске")
        response = requests.put(
            create_folder_url,
            params={"path": folder_path, "overwrite": "true"},
            headers=headers
        )
        
        if response.status_code not in [200, 201, 409]:  # 409 - папка уже существует
            print(f"[{datetime.now()}] Ошибка при создании папки на Яндекс.Диске: {response.status_code}, {response.text}")
            return False
        
        # Загружаем Excel-файл
        excel_filename = f"participants_{timestamp}.xlsx"
        excel_upload_url = "https://cloud-api.yandex.net/v1/disk/resources/upload"
        excel_params = {
            "path": f"{folder_path}/{excel_filename}",
            "overwrite": "true"
        }
        
        # Получаем URL для загрузки Excel-файла
        print(f"[{datetime.now()}] Получаем URL для загрузки Excel файла")
        response = requests.get(excel_upload_url, params=excel_params, headers=headers)
        if response.status_code == 200:
            href = response.json().get("href", "")
            # Загружаем данные на полученный URL
            print(f"[{datetime.now()}] Загружаем Excel файл на Яндекс.Диск")
            upload_response = requests.put(href, data=excel_data.getvalue())
            if upload_response.status_code != 201:
                print(f"[{datetime.now()}] Ошибка при загрузке Excel-файла: {upload_response.status_code}, {upload_response.text}")
                return False
            print(f"[{datetime.now()}] Excel файл успешно загружен")
        else:
            print(f"[{datetime.now()}] Ошибка при получении URL для загрузки Excel-файла: {response.status_code}, {response.text}")
            return False
        
        # Загружаем JSON-файл
        json_filename = f"participants_{timestamp}.json"
        json_params = {
            "path": f"{folder_path}/{json_filename}",
            "overwrite": "true"
        }
        
        # Получаем URL для загрузки JSON-файла
        print(f"[{datetime.now()}] Получаем URL для загрузки JSON файла")
        # Исправляем переменную: указываем правильное название json_upload_url
        json_upload_url = "https://cloud-api.yandex.net/v1/disk/resources/upload"
        response = requests.get(json_upload_url, params=json_params, headers=headers)
        if response.status_code == 200:
            href = response.json().get("href", "")
            # Загружаем данные на полученный URL
            print(f"[{datetime.now()}] Загружаем JSON файл на Яндекс.Диск")
            upload_response = requests.put(href, data=json_bytes)
            if upload_response.status_code != 201:
                print(f"[{datetime.now()}] Ошибка при загрузке JSON-файла: {upload_response.status_code}, {upload_response.text}")
                return False
            print(f"[{datetime.now()}] JSON файл успешно загружен")
        else:
            print(f"[{datetime.now()}] Ошибка при получении URL для загрузки JSON-файла: {response.status_code}, {response.text}")
            return False
        
        print(f"[{datetime.now()}] Резервная копия успешно сохранена на Яндекс.Диске: {excel_filename}, {json_filename}")
        return True
    except Exception as e:
        import traceback
        print(f"[{datetime.now()}] Критическая ошибка при создании резервной копии на Яндекс.Диск: {e}")
        print(traceback.format_exc())  # Выводим полный стек вызовов для отладки
        return False

def create_excel_backup(json_data):
    """Создание Excel-файла с данными участников"""
    output = io.BytesIO()
    
    # Создаем Excel-файл с явной настройкой для поддержки Unicode
    workbook = xlsxwriter.Workbook(output, {'constant_memory': True, 'strings_to_unicode': True})
    worksheet = workbook.add_worksheet('Участники')
    
    # Форматирование
    header_format = workbook.add_format({
        'bold': True,
        'bg_color': '#007bff',
        'font_color': 'white',
        'border': 1
    })
    
    cell_format = workbook.add_format({
        'border': 1
    })
    
    # Заголовки
    headers = ['№', 'Номер участника', 'ФИО', 'Телефон', 'Возраст', 'Пол', 'Город', 'Дата регистрации', 'IP-адрес']
    for col, header in enumerate(headers):
        worksheet.write(0, col, header, header_format)
    
    # Данные участников
    for row, participant in enumerate(json_data, start=1):
        worksheet.write(row, 0, row, cell_format)
        worksheet.write(row, 1, participant.get('ticket_number', ''), cell_format)
        
        # Проверяем кодировку ФИО перед записью
        full_name = participant.get('full_name', '')
        if isinstance(full_name, str):
            try:
                # Проверяем корректность UTF-8
                full_name.encode('utf-8').decode('utf-8')
            except UnicodeError:
                # Если есть проблемы, пробуем исправить
                try:
                    full_name = full_name.encode('latin1').decode('utf-8')
                except:
                    full_name = full_name.encode('utf-8', errors='replace').decode('utf-8')
                    
        worksheet.write(row, 2, full_name, cell_format)
        worksheet.write(row, 3, participant.get('phone', ''), cell_format)
        worksheet.write(row, 4, participant.get('age', ''), cell_format)
        
        gender = 'Мужской' if participant.get('gender') == 'male' else 'Женский'
        worksheet.write(row, 5, gender, cell_format)
        
        # Определяем город из координат или IP и проверяем кодировку
        city = ''
        if participant.get('coordinates') and participant['coordinates'].get('city'):
            city = participant['coordinates']['city']
        elif participant.get('location') and participant['location'].get('city'):
            city = participant['location']['city']
            
        if isinstance(city, str):
            try:
                # Проверяем корректность UTF-8
                city.encode('utf-8').decode('utf-8')
            except UnicodeError:
                # Если есть проблемы, пробуем исправить
                try:
                    city = city.encode('latin1').decode('utf-8')
                except:
                    city = city.encode('utf-8', errors='replace').decode('utf-8')
                    
        worksheet.write(row, 6, city, cell_format)
        worksheet.write(row, 7, participant.get('registration_time', ''), cell_format)
        worksheet.write(row, 8, participant.get('ip_address', ''), cell_format)
    
    # Автонастройка ширины столбцов
    for i, width in enumerate([5, 15, 25, 15, 8, 10, 15, 20, 15]):
        worksheet.set_column(i, i, width)
        
    workbook.close()
    output.seek(0)
    return output

# Функция для создания и отправки резервной копии
def create_backup():
    """Создает резервную копию данных на Яндекс.Диске"""
    print(f"[{datetime.now()}] Запуск процесса создания резервной копии")
    try:
        # Загружаем данные участников напрямую из Яндекс.Диска
        participants = load_participants(force_reload=True)
        
        # Если нет участников, выходим
        if not participants:
            print(f"[{datetime.now()}] Нет данных участников для резервного копирования")
            return False
        
        # Загружаем настройки
        settings = load_settings()
        backup_settings = settings.get('backup_settings', {})
        
        # Получаем токен Яндекс.Диска
        yandex_token = backup_settings.get('yandex_token')
        
        if not yandex_token:
            print(f"[{datetime.now()}] Не найден токен Яндекс.Диска для создания резервной копии")
            return False
        
        # Обработка кириллических символов в данных участников
        processed_participants = []
        for participant in participants:
            processed_participant = participant.copy()
            
            # Проверяем кодировку ФИО
            full_name = participant.get('full_name', '')
            if isinstance(full_name, str):
                try:
                    # Проверяем корректность UTF-8
                    full_name.encode('utf-8').decode('utf-8')
                except UnicodeError:
                    # Если есть проблемы, пробуем исправить
                    try:
                        full_name = full_name.encode('latin1').decode('utf-8')
                    except:
                        full_name = full_name.encode('utf-8', errors='replace').decode('utf-8')
            processed_participant['full_name'] = full_name
            
            # Проверяем кодировку города
            if 'coordinates' in participant and 'city' in participant['coordinates'] and participant['coordinates']['city']:
                city = participant['coordinates']['city']
                if isinstance(city, str):
                    try:
                        city.encode('utf-8').decode('utf-8')
                    except UnicodeError:
                        try:
                            city = city.encode('latin1').decode('utf-8')
                        except:
                            city = city.encode('utf-8', errors='replace').decode('utf-8')
                    processed_participant['coordinates']['city'] = city
            
            if 'location' in participant and 'city' in participant['location'] and participant['location']['city']:
                city = participant['location']['city']
                if isinstance(city, str):
                    try:
                        city.encode('utf-8').decode('utf-8')
                    except UnicodeError:
                        try:
                            city = city.encode('latin1').decode('utf-8')
                        except:
                            city = city.encode('utf-8', errors='replace').decode('utf-8')
                    processed_participant['location']['city'] = city
            
            processed_participants.append(processed_participant)
        
        # Создаем папку с датой для хранения резервных копий
        current_date = datetime.now().strftime('%Y-%m-%d')
        headers = {"Authorization": f"OAuth {yandex_token}"}
        
        # Создаем папку для бэкапов, если ее нет
        backup_folder_url = "https://cloud-api.yandex.net/v1/disk/resources"
        backup_params = {"path": "app:/backups"}
        
        backup_folder_response = requests.get(backup_folder_url, headers=headers, params=backup_params)
        
        if backup_folder_response.status_code == 404:
            # Создаем папку для бэкапов
            requests.put(backup_folder_url, headers=headers, params=backup_params)
        
        # Создаем папку с текущей датой
        date_folder_params = {"path": f"app:/backups/{current_date}"}
        date_folder_response = requests.get(backup_folder_url, headers=headers, params=date_folder_params)
        
        if date_folder_response.status_code == 404:
            # Создаем папку с датой
            requests.put(backup_folder_url, headers=headers, params=date_folder_params)
        
        # Текущее время для имени файла
        current_time = datetime.now().strftime('%H-%M-%S')
        
        # Создаем копию JSON файла
        json_filename = f"participants_{current_date}_{current_time}.json"
        json_upload_url = "https://cloud-api.yandex.net/v1/disk/resources/upload"
        json_params = {"path": f"app:/backups/{current_date}/{json_filename}", "overwrite": "true"}
        
        json_upload_response = requests.get(json_upload_url, headers=headers, params=json_params)
        
        if json_upload_response.status_code == 200:
            json_upload_link = json_upload_response.json().get("href")
            json_data = json.dumps(processed_participants, ensure_ascii=False, indent=4)
            json_upload_result = requests.put(json_upload_link, data=json_data.encode('utf-8'))
            
            if not (json_upload_result.status_code == 201 or json_upload_result.status_code == 200):
                print(f"[{datetime.now()}] Ошибка при загрузке JSON файла: {json_upload_result.status_code}")
                return False
        else:
            print(f"[{datetime.now()}] Ошибка при получении ссылки для загрузки JSON: {json_upload_response.status_code}")
            return False
        
        # Создаем Excel файл
        excel_data = io.BytesIO()
        
        # Создаем Excel-документ с явной настройкой поддержки Unicode
        workbook = xlsxwriter.Workbook(excel_data, {'strings_to_unicode': True})
        worksheet = workbook.add_worksheet()
        
        # Добавляем заголовки
        excel_headers = [
            "№ участника", "ФИО", "Телефон", "Возраст", "Пол", "Город", 
            "Дата регистрации", "IP-адрес", "Координаты", "Источник города"
        ]
        
        for col, header in enumerate(excel_headers):
            worksheet.write(0, col, header)
        
        # Добавляем данные участников
        for row, participant in enumerate(processed_participants, start=1):
            worksheet.write(row, 0, participant.get('ticket_number', ''))
            worksheet.write(row, 1, participant.get('full_name', ''))
            worksheet.write(row, 2, participant.get('phone', ''))
            worksheet.write(row, 3, participant.get('age', ''))
            worksheet.write(row, 4, 'Мужской' if participant.get('gender') == 'male' else 'Женский')
            
            # Определяем город
            city = ''
            source = ''
            if participant.get('coordinates', {}).get('city'):
                city = participant.get('coordinates', {}).get('city', '')
                source = 'Браузер'
            elif participant.get('location', {}).get('city'):
                city = participant.get('location', {}).get('city', '')
                source = 'IP'
            
            worksheet.write(row, 5, city)
            worksheet.write(row, 6, participant.get('registration_time', ''))
            worksheet.write(row, 7, participant.get('ip_address', ''))
            
            # Координаты
            coords = ''
            if participant.get('coordinates'):
                lat = participant.get('coordinates', {}).get('latitude', '')
                lng = participant.get('coordinates', {}).get('longitude', '')
                coords = f"{lat}, {lng}"
            
            worksheet.write(row, 8, coords)
            worksheet.write(row, 9, source)
        
        # Закрываем книгу
        workbook.close()
        
        # Получаем данные из буфера
        excel_data.seek(0)
        
        # Загружаем Excel файл
        excel_filename = f"participants_{current_date}_{current_time}.xlsx"
        excel_params = {"path": f"app:/backups/{current_date}/{excel_filename}", "overwrite": "true"}
        
        # Исправляем переменную: используем excel_upload_url вместо json_upload_url
        excel_upload_url = "https://cloud-api.yandex.net/v1/disk/resources/upload"
        excel_upload_response = requests.get(excel_upload_url, headers=headers, params=excel_params)
        
        if excel_upload_response.status_code == 200:
            excel_upload_link = excel_upload_response.json().get("href")
            excel_upload_result = requests.put(excel_upload_link, data=excel_data.getvalue())
            
            if not (excel_upload_result.status_code == 201 or excel_upload_result.status_code == 200):
                print(f"[{datetime.now()}] Ошибка при загрузке Excel файла: {excel_upload_result.status_code}")
        else:
            print(f"[{datetime.now()}] Ошибка при получении ссылки для загрузки Excel: {excel_upload_response.status_code}")
        
        # Обновляем время последнего бэкапа
        backup_settings['last_backup'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        settings['backup_settings'] = backup_settings
        save_settings(settings)
        
        print(f"[{datetime.now()}] Резервная копия успешно создана")
        return True
    
    except Exception as e:
        print(f"[{datetime.now()}] Критическая ошибка при создании резервной копии: {str(e)}")
        traceback.print_exc()
        return False

def create_app_folder(token):
    """Создает папку приложения на Яндекс.Диске, если она не существует"""
    try:
        headers = {"Authorization": f"OAuth {token}"}
        
        # URL для работы с ресурсами
        url = "https://cloud-api.yandex.net/v1/disk/resources"
        
        # Проверяем существование папки приложения
        app_path = "app:/"
        try:
            app_response = requests.get(url, headers=headers, params={"path": app_path})
            
            # Если папка не существует (404), создаем ее
            if app_response.status_code == 404:
                app.logger.info("Папка приложения не найдена на Яндекс.Диске, создаем новую")
                create_response = requests.put(url, headers=headers, params={"path": app_path})
                
                if create_response.status_code not in [200, 201]:
                    app.logger.error(f"Ошибка при создании папки приложения: {create_response.status_code}")
                    return False
                else:
                    app.logger.info("Папка приложения успешно создана на Яндекс.Диске")
            elif app_response.status_code != 200:
                app.logger.error(f"Ошибка при проверке папки приложения: {app_response.status_code}")
                return False
            else:
                app.logger.info("Папка приложения уже существует на Яндекс.Диске")
                
        except Exception as e:
            app.logger.error(f"Ошибка при проверке/создании папки приложения: {str(e)}")
            return False
        
        # Проверяем наличие файла participants.json
        participants_path = "app:/participants.json"
        try:
            file_response = requests.get(url, headers=headers, params={"path": participants_path})
            
            # Если файл не существует (404), создаем его
            if file_response.status_code == 404:
                app.logger.info("Файл participants.json не найден на Яндекс.Диске, создаем новый")
                
                # Получаем ссылку для загрузки
                upload_url = "https://cloud-api.yandex.net/v1/disk/resources/upload"
                upload_params = {"path": participants_path, "overwrite": "true"}
                
                try:
                    upload_response = requests.get(upload_url, headers=headers, params=upload_params)
                    
                    if upload_response.status_code == 200:
                        upload_link = upload_response.json().get("href")
                        
                        # Загружаем пустой массив
                        put_response = requests.put(upload_link, data="[]".encode('utf-8'))
                        
                        if put_response.status_code in [200, 201]:
                            app.logger.info("Файл participants.json успешно создан на Яндекс.Диске")
                        else:
                            app.logger.error(f"Ошибка при создании файла participants.json: {put_response.status_code}")
                            return False
                    else:
                        app.logger.error(f"Ошибка при получении ссылки для загрузки файла: {upload_response.status_code}")
                        return False
                        
                except Exception as e:
                    app.logger.error(f"Ошибка при загрузке файла participants.json: {str(e)}")
                    return False
            elif file_response.status_code != 200:
                app.logger.error(f"Ошибка при проверке файла participants.json: {file_response.status_code}")
                return False
            else:
                app.logger.info("Файл participants.json уже существует на Яндекс.Диске")
                
        except Exception as e:
            app.logger.error(f"Ошибка при проверке/создании файла participants.json: {str(e)}")
            return False
        
        # Проверяем папку для резервных копий
        backups_path = "app:/backups"
        try:
            backups_response = requests.get(url, headers=headers, params={"path": backups_path})
            
            # Если папка не существует (404), создаем ее
            if backups_response.status_code == 404:
                app.logger.info("Папка backups не найдена на Яндекс.Диске, создаем новую")
                create_backups_response = requests.put(url, headers=headers, params={"path": backups_path})
                
                if create_backups_response.status_code not in [200, 201]:
                    app.logger.error(f"Ошибка при создании папки backups: {create_backups_response.status_code}")
                    return False
                else:
                    app.logger.info("Папка backups успешно создана на Яндекс.Диске")
            elif backups_response.status_code != 200:
                app.logger.error(f"Ошибка при проверке папки backups: {backups_response.status_code}")
                return False
            else:
                app.logger.info("Папка backups уже существует на Яндекс.Диске")
                
        except Exception as e:
            app.logger.error(f"Ошибка при проверке/создании папки backups: {str(e)}")
            return False
            
        return True
        
    except Exception as e:
        app.logger.error(f"Критическая ошибка при настройке папок на Яндекс.Диске: {str(e)}")
        return False

def init_backup_settings():
    """Инициализация настроек резервного копирования"""
    settings = load_settings()
    if 'backup_settings' not in settings:
        settings['backup_settings'] = {
            'enabled': False,
            'yandex_token': '',
            'interval': 'daily',
            'last_backup': None
        }
        save_settings(settings)
    
    # Проверяем наличие токена Яндекс.Диска
    yandex_token = settings.get('backup_settings', {}).get('yandex_token')
    if yandex_token:
        # Создаем папку приложения на Яндекс.Диске, если её нет
        create_app_folder(yandex_token)
        
        # Выводим сообщение об успешном обнаружении токена
        print(f"[{datetime.now()}] Токен Яндекс.Диска найден: {yandex_token[:5]}...{yandex_token[-4:]}")
        
        # Проверяем, если в приложении включено резервное копирование
        if settings.get('backup_settings', {}).get('enabled', False):
            # Создаем тестовую резервную копию при запуске
            print(f"[{datetime.now()}] Создание тестовой резервной копии при запуске планировщика...")
            try:
                create_backup()
            except Exception as e:
                print(f"[{datetime.now()}] ОШИБКА: Не удалось создать тестовую резервную копию")
    else:
        print(f"[{datetime.now()}] Токен Яндекс.Диска не найден. Резервное копирование не будет работать.")

def run_scheduler():
    """Запуск планировщика резервного копирования"""
    print(f"[{datetime.now()}] Запущен планировщик резервного копирования")
    
    # Инициализируем переменную next_time
    next_time = None
    
    # Выполняем инициализацию настроек резервного копирования
    settings = load_settings()
    
    # Получаем настройки резервного копирования
    backup_settings = settings.get('backup_settings', {})
    enabled = backup_settings.get('enabled', False)
    interval = backup_settings.get('interval', 'daily')
    yandex_token = backup_settings.get('yandex_token', '')
    
    if not yandex_token:
        print(f"[{datetime.now()}] ВНИМАНИЕ: Токен Яндекс.Диска не задан. Резервное копирование не будет работать!")
    else:
        print(f"[{datetime.now()}] Токен Яндекс.Диска найден: {yandex_token[:5]}...{yandex_token[-5:]}")
    
    # При запуске создаем тестовую резервную копию, чтобы проверить работоспособность
    if backup_settings.get('enabled', False):
        print(f"[{datetime.now()}] Создание тестовой резервной копии при запуске планировщика...")
        success = create_backup()
        if success:
            print(f"[{datetime.now()}] Тестовая резервная копия успешно создана")
        else:
            print(f"[{datetime.now()}] ОШИБКА: Не удалось создать тестовую резервную копию")
    
    while True:
        settings = load_settings()
        backup_settings = settings.get('backup_settings', {})
        
        if not backup_settings.get('enabled', False):
            # Если резервное копирование отключено, проверяем раз в минуту
            print(f"[{datetime.now()}] Резервное копирование отключено в настройках")
            # Проверяем на событие каждую секунду для более быстрого отклика
            for _ in range(60):
                if scheduler_event.is_set():
                    print(f"[{datetime.now()}] Получен сигнал об изменении настроек")
                    scheduler_event.clear()  # Сбрасываем флаг
                    break
                time.sleep(1)
            continue
        
        current_time = datetime.now()
        
        # Если было событие изменения настроек, сбрасываем расчет времени и проверяем сразу
        if scheduler_event.is_set():
            print(f"[{current_time}] Обрабатываем изменение настроек резервного копирования")
            scheduler_event.clear()  # Сбрасываем флаг
            next_time = None
            # Если включён пользовательский интервал и интервал короткий - создаем резервную копию немедленно
            interval = backup_settings.get('interval', 'daily')
            if interval == 'custom':
                value = int(backup_settings.get('custom_value', 24))
                unit = backup_settings.get('custom_unit', 'hours')
                print(f"[{current_time}] Новый интервал резервного копирования: {value} {unit}")
                if unit in ['seconds', 'minutes'] or (unit == 'hours' and value < 1):
                    print(f"[{current_time}] Создание резервной копии немедленно после изменения настроек")
                    if create_backup():
                        # Обновляем время последнего резервного копирования в файле настроек
                        settings = load_settings()
                        settings['backup_settings']['last_backup'] = current_time.strftime('%Y-%m-%d %H:%M:%S')
                        save_settings(settings)
                        print(f"[{current_time}] Обновлено время последнего резервного копирования: {settings['backup_settings']['last_backup']}")
        
        # Рассчитываем время следующего резервного копирования
        if next_time is None:
            # Первый запуск или настройки изменились
            interval = backup_settings.get('interval', 'daily')
            
            if interval == 'daily':
                # Ежедневное резервное копирование в 03:00
                next_time = current_time.replace(hour=3, minute=0, second=0, microsecond=0)
                if current_time >= next_time:
                    next_time += timedelta(days=1)
                print(f"[{current_time}] Следующее резервное копирование (daily): {next_time}")
            elif interval == 'hourly':
                # Ежечасное резервное копирование в начале часа
                next_time = current_time.replace(minute=0, second=0, microsecond=0)
                if current_time >= next_time:
                    next_time += timedelta(hours=1)
                print(f"[{current_time}] Следующее резервное копирование (hourly): {next_time}")
            elif interval == 'custom':
                # Произвольный интервал
                value = int(backup_settings.get('custom_value', 24))
                unit = backup_settings.get('custom_unit', 'hours')
                
                # Получаем последнее время резервного копирования
                last_backup = backup_settings.get('last_backup')
                
                if last_backup:
                    try:
                        last_backup_time = datetime.strptime(last_backup, '%Y-%m-%d %H:%M:%S')
                        print(f"[{current_time}] Последнее резервное копирование было в: {last_backup_time}")
                        
                        # Рассчитываем следующее время на основе последнего резервного копирования
                        if unit == 'seconds':
                            next_time = last_backup_time + timedelta(seconds=value)
                        elif unit == 'minutes':
                            next_time = last_backup_time + timedelta(minutes=value)
                        elif unit == 'hours':
                            next_time = last_backup_time + timedelta(hours=value)
                        elif unit == 'days':
                            next_time = last_backup_time + timedelta(days=value)
                        elif unit == 'weeks':
                            next_time = last_backup_time + timedelta(weeks=value)
                        else:
                            next_time = last_backup_time + timedelta(hours=24)
                        
                        print(f"[{current_time}] Следующее резервное копирование (custom {value} {unit}): {next_time}")
                            
                        # Если рассчитанное время уже прошло, делаем резервную копию сейчас
                        if next_time <= current_time:
                            print(f"[{current_time}] Рассчитанное время уже прошло, делаем копию сейчас")
                            next_time = current_time
                    except Exception as e:
                        print(f"[{current_time}] Ошибка при разборе даты последнего бэкапа: {e}")
                        next_time = current_time
                else:
                    # Если нет записи о последнем резервном копировании, делаем сейчас
                    print(f"[{current_time}] Нет данных о последнем резервном копировании, делаем копию сейчас")
                    next_time = current_time
        
        # Проверяем, наступило ли время для создания резервной копии
        if current_time >= next_time:
            print(f"[{current_time}] Время создания автоматической резервной копии")
            # Создаем резервную копию и обновляем метку времени только в случае успеха
            if create_backup():
                # Обновляем время последнего резервного копирования в файле настроек
                settings = load_settings()
                settings['backup_settings']['last_backup'] = current_time.strftime('%Y-%m-%d %H:%M:%S')
                save_settings(settings)
                print(f"[{current_time}] Время последнего резервного копирования обновлено: {settings['backup_settings']['last_backup']}")
                
                # Сбрасываем счетчик для следующего резервного копирования
                next_time = None
            else:
                # Если копирование не удалось, попробуем снова через минуту
                print(f"[{current_time}] Резервное копирование не удалось, следующая попытка через минуту")
                next_time = current_time + timedelta(minutes=1)
        else:
            # Для коротких интервалов используем более частые проверки
            interval = backup_settings.get('interval', 'daily')
            if interval == 'custom':
                unit = backup_settings.get('custom_unit', 'hours')
                if unit == 'seconds':
                    # Для секунд проверяем каждую секунду
                    wait_seconds = 1
                elif unit == 'minutes':
                    # Для минут проверяем каждые 5 секунд
                    wait_seconds = 5
                else:
                    # Для других интервалов проверяем не чаще раза в минуту
                    wait_seconds = min(60, (next_time - current_time).total_seconds())
            else:
                # Для стандартных интервалов проверяем не чаще раза в минуту
                wait_seconds = min(60, (next_time - current_time).total_seconds())
            
            if wait_seconds <= 0:
                wait_seconds = 1
                
            print(f"[{current_time}] Ожидание {wait_seconds} сек. до следующей проверки. Следующее резервное копирование в {next_time}")
            
            # Разбиваем ожидание на короткие интервалы для быстрого отклика на события
            for _ in range(int(wait_seconds)):
                if scheduler_event.is_set():
                    print(f"[{datetime.now()}] Получен сигнал об изменении настроек во время ожидания")
                    break
                time.sleep(1)

# Запуск фонового задания для резервного копирования
def start_backup_scheduler():
    # Запускаем планировщик в отдельном потоке вместо процесса
    scheduler_thread = threading.Thread(target=run_scheduler, daemon=True)
    scheduler_thread.start()
    print("Планировщик резервного копирования запущен в отдельном потоке")

# Функция инициализации приложения для запуска планировщика
def init_app(flask_app):
    # Инициализация настроек резервного копирования
    init_backup_settings()
    # Запуск планировщика резервного копирования
    # start_backup_scheduler()  # Закомментировали для предотвращения автозапуска при импорте

# Предотвращаем автоматический запуск при импорте
# init_app(app)

# Убедимся, что планировщик запускается только при непосредственном запуске приложения
if __name__ == '__main__':
    # Инициализация настроек резервного копирования
    init_backup_settings()
    # Запуск планировщика резервного копирования
    start_backup_scheduler()
    
    # Для продакшена используйте WSGI-сервер (gunicorn или uwsgi)
    # gunicorn -w 4 -b 0.0.0.0:5000 app:app
    app.run(debug=False, host='0.0.0.0') 

@app.route('/find-ticket', methods=['POST'])
def find_ticket():
    """Поиск номера участника по номеру телефона"""
    phone = request.form.get('phone', '')
    
    if not phone:
        return jsonify({'success': False, 'message': 'Пожалуйста, введите номер телефона.'})
    
    # Нормализуем телефон для поиска (удаляем все, кроме цифр)
    normalized_phone = ''.join(filter(str.isdigit, phone))
    
    # Проверяем, что номер телефона полный (не менее 11 цифр для российского номера)
    if len(normalized_phone) < 11:
        return jsonify({'success': False, 'message': 'Пожалуйста, введите полный номер телефона.'})
    
    # Если номер начинается с 8, заменяем на 7 для стандартизации
    if normalized_phone.startswith('8') and len(normalized_phone) == 11:
        normalized_phone = '7' + normalized_phone[1:]
    
    ticket_data = get_ticket_by_phone(normalized_phone)
    
    if ticket_data:
        return jsonify({
            'success': True, 
            'message': 'Номер участника найден!',
            'ticket_number': ticket_data['ticket_number'],
            'full_name': ticket_data['full_name']
        })
    else:
        return jsonify({
            'success': False, 
            'message': 'Этот номер телефона не зарегистрирован в розыгрыше.'
        }) 

@app.route('/check-data-updates')
def check_data_updates():
    """Проверяет наличие обновленных данных на Яндекс.Диске"""
    try:
        # Получаем токен Яндекс.Диска из настроек
        settings = load_settings()
        yandex_token = settings.get('backup_settings', {}).get('yandex_token')
        
        if not yandex_token:
            return jsonify({
                'success': False,
                'message': 'Не найден токен Яндекс.Диска',
                'has_updates': False
            })
        
        # Принудительно загружаем данные заново с Яндекс.Диска
        global PARTICIPANTS_CACHE
        temp_cache = PARTICIPANTS_CACHE
        PARTICIPANTS_CACHE = None  # Сбрасываем кэш для принудительной загрузки
        
        # Проверяем информацию о файле на Яндекс.Диске
        headers = {"Authorization": f"OAuth {yandex_token}"}
        url = "https://cloud-api.yandex.net/v1/disk/resources"
        params = {"path": "app:/participants.json"}
        
        response = requests.get(url, headers=headers, params=params)
        
        if response.status_code == 200:
            file_info = response.json()
            file_modified = file_info.get('modified', '')
            
            # Загружаем данные участников
            participants = load_participants()
            
            # Общее количество участников
            total = len(participants)
            
            # Информация о последних 5 участниках (для отображения в админке)
            latest_participants = []
            if participants:
                sorted_participants = sorted(participants, key=lambda x: x.get('registration_time', ''), reverse=True)
                latest_participants = sorted_participants[:5]
            
            has_updates = True  # Всегда считаем, что есть обновления (для упрощения)
            
            return jsonify({
                'success': True,
                'has_updates': has_updates,
                'file_modified': file_modified,
                'total_participants': total,
                'last_updated': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'latest_participants': [
                    {
                        'ticket_number': p.get('ticket_number'),
                        'full_name': p.get('full_name'),
                        'phone': p.get('phone'),
                        'registration_time': p.get('registration_time')
                    } for p in latest_participants
                ]
            })
        else:
            # Если файл не найден на Яндекс.Диске
            return jsonify({
                'success': False,
                'has_updates': False,
                'message': f'Файл с данными не найден на Яндекс.Диске: {response.status_code}',
                'total_participants': 0,
                'last_updated': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'latest_participants': []
            })
            
    except Exception as e:
        app.logger.error(f'Ошибка при проверке обновлений данных: {str(e)}')
        return jsonify({
            'success': False,
            'has_updates': False,
            'message': f'Ошибка при проверке обновлений: {str(e)}',
            'total_participants': 0,
            'last_updated': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'latest_participants': []
        })

@app.route('/get-backup-status', methods=['GET'])
def get_backup_status():
    """Получение текущего статуса резервного копирования"""
    # Проверка, что пользователь является администратором
    if not session.get('admin'):
        return jsonify({'success': False, 'message': 'Доступ запрещен'}), 403
    
    try:
        # Загрузка настроек
        settings = load_settings()
        
        # Получаем данные о резервном копировании
        backup_settings = settings.get('backup_settings', {})
        enabled = backup_settings.get('enabled', False)
        last_backup = backup_settings.get('last_backup', None)
        interval = backup_settings.get('interval', 'daily')
        
        # Расчет времени следующего бэкапа
        next_backup = None
        current_time = datetime.now()
        
        if enabled:
            if interval == 'daily':
                # Ежедневное резервное копирование в 03:00
                next_time = current_time.replace(hour=3, minute=0, second=0, microsecond=0)
                if current_time >= next_time:
                    next_time += timedelta(days=1)
                next_backup = f"В 03:00 ({next_time.strftime('%d.%m.%Y')})"
                
            elif interval == 'hourly':
                # Ежечасное резервное копирование в начале часа
                next_time = current_time.replace(minute=0, second=0, microsecond=0)
                if current_time >= next_time:
                    next_time += timedelta(hours=1)
                next_backup = f"В {next_time.hour}:00 ({next_time.strftime('%d.%m.%Y')})"
                
            elif interval == 'custom':
                # Произвольный интервал
                value = int(backup_settings.get('custom_value', 24))
                unit = backup_settings.get('custom_unit', 'hours')
                
                # Получаем время последнего резервного копирования
                if last_backup:
                    try:
                        last_backup_time = datetime.strptime(last_backup, '%Y-%m-%d %H:%M:%S')
                        
                        # Рассчитываем следующее время на основе последнего резервного копирования
                        if unit == 'seconds':
                            next_time = last_backup_time + timedelta(seconds=value)
                            unit_text = "секунд"
                        elif unit == 'minutes':
                            next_time = last_backup_time + timedelta(minutes=value)
                            unit_text = "минут"
                        elif unit == 'hours':
                            next_time = last_backup_time + timedelta(hours=value)
                            unit_text = "часов"
                        elif unit == 'days':
                            next_time = last_backup_time + timedelta(days=value)
                            unit_text = "дней"
                        elif unit == 'weeks':
                            next_time = last_backup_time + timedelta(weeks=value)
                            unit_text = "недель"
                        else:
                            next_time = last_backup_time + timedelta(hours=24)
                            unit_text = "часов"
                        
                        # Форматируем время для отображения
                        if next_time <= current_time:
                            next_backup = "В ближайшее время"
                        else:
                            time_diff = next_time - current_time
                            total_seconds = int(time_diff.total_seconds())
                            
                            # Форматируем время в удобочитаемом виде
                            if total_seconds < 60:
                                next_backup = f"Через {total_seconds} секунд"
                            elif total_seconds < 3600:
                                minutes = total_seconds // 60
                                next_backup = f"Через {minutes} минут"
                            elif total_seconds < 86400:
                                hours = total_seconds // 3600
                                next_backup = f"Через {hours} часов"
                            else:
                                days = total_seconds // 86400
                                next_backup = f"Через {days} дней"
                    except Exception as e:
                        print(f"Ошибка при расчете следующего времени резервного копирования: {e}")
                        next_backup = f"Через {value} {unit_text}"
                else:
                    next_backup = "После первого резервного копирования"
        else:
            next_backup = "Резервное копирование отключено"
        
        # Форматируем дату последнего бэкапа, если она есть
        formatted_last_backup = None
        if last_backup:
            try:
                last_backup_time = datetime.strptime(last_backup, '%Y-%m-%d %H:%M:%S')
                formatted_last_backup = last_backup_time.strftime('%d.%m.%Y %H:%M:%S')
            except:
                formatted_last_backup = last_backup
        
        return jsonify({
            'success': True,
            'enabled': enabled,
            'last_backup': formatted_last_backup,
            'next_backup': next_backup,
            'interval': interval,
            'custom_value': backup_settings.get('custom_value', 24),
            'custom_unit': backup_settings.get('custom_unit', 'hours')
        })
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500

@app.route('/check-backup-status')
def check_backup_status():
    """Получение текущего статуса резервного копирования"""
    if not session.get('admin'):
        return jsonify({'success': False, 'message': 'Доступ запрещен'}), 403
    
    try:
        settings = load_settings()
        backup_settings = settings.get('backup_settings', {})
        
        # Получаем информацию о последнем резервном копировании
        last_backup = backup_settings.get('last_backup', None)
        
        # Рассчитываем предполагаемое время следующего резервного копирования
        next_backup = get_next_backup_info()
        
        return jsonify({
            'success': True,
            'enabled': backup_settings.get('enabled', False),
            'last_backup': last_backup,
            'next_backup': next_backup,
            'interval': backup_settings.get('interval', 'daily'),
            'custom_value': backup_settings.get('custom_value', 24),
            'custom_unit': backup_settings.get('custom_unit', 'hours')
        })
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500

def get_next_backup_info():
    """Расчет времени следующего резервного копирования"""
    try:
        settings = load_settings()
        backup_settings = settings.get('backup_settings', {})
        
        if not backup_settings.get('enabled', False):
            return "Резервное копирование отключено"
        
        interval = backup_settings.get('interval', 'daily')
        current_time = datetime.now()
        
        if interval == 'daily':
            # Ежедневное резервное копирование в 03:00
            next_time = current_time.replace(hour=3, minute=0, second=0, microsecond=0)
            if current_time >= next_time:
                next_time += timedelta(days=1)
            return f"В 03:00 {next_time.strftime('%d.%m.%Y')}"
            
        elif interval == 'hourly':
            # Ежечасное резервное копирование в начале часа
            next_time = current_time.replace(minute=0, second=0, microsecond=0)
            if current_time >= next_time:
                next_time += timedelta(hours=1)
            return f"В {next_time.strftime('%H:%M')} {next_time.strftime('%d.%m.%Y')}"
            
        elif interval == 'custom':
            # Произвольный интервал
            value = int(backup_settings.get('custom_value', 24))
            unit = backup_settings.get('custom_unit', 'hours')
            
            # Получаем последнее время резервного копирования
            last_backup = backup_settings.get('last_backup')
            
            if last_backup:
                try:
                    last_backup_time = datetime.strptime(last_backup, '%Y-%m-%d %H:%M:%S')
                    
                    # Рассчитываем следующее время на основе последнего резервного копирования
                    if unit == 'seconds':
                        next_time = last_backup_time + timedelta(seconds=value)
                    elif unit == 'minutes':
                        next_time = last_backup_time + timedelta(minutes=value)
                    elif unit == 'hours':
                        next_time = last_backup_time + timedelta(hours=value)
                    elif unit == 'days':
                        next_time = last_backup_time + timedelta(days=value)
                    elif unit == 'weeks':
                        next_time = last_backup_time + timedelta(weeks=value)
                    else:
                        next_time = last_backup_time + timedelta(hours=24)
                    
                    # Форматируем время для отображения
                    time_format = "%d.%m.%Y %H:%M"
                    
                    if next_time <= current_time:
                        return "В ближайшее время"
                    else:
                        # Получаем разницу времени
                        time_diff = next_time - current_time
                        hours, remainder = divmod(time_diff.seconds, 3600)
                        minutes, _ = divmod(remainder, 60)
                        
                        if time_diff.days > 0:
                            return f"Через {time_diff.days} д. {hours} ч. ({next_time.strftime(time_format)})"
                        elif hours > 0:
                            return f"Через {hours} ч. {minutes} мин. ({next_time.strftime(time_format)})"
                        else:
                            return f"Через {minutes} мин. ({next_time.strftime(time_format)})"
                except Exception as e:
                    print(f"Ошибка при расчете времени следующего бэкапа: {e}")
                    return "Не удалось определить"
            else:
                return "При следующем запуске планировщика"
        
        return "Не запланировано"
    except Exception as e:
        print(f"Ошибка при получении информации о следующем бэкапе: {e}")
        return "Не удалось определить"

@app.route('/admin-login', methods=['GET'])
def admin_login_page():
    """Страница входа для администратора"""
    if session.get('admin'):
        return redirect(url_for('admin_panel'))
    return render_template('admin_login.html')

@app.route('/admin-data')
@login_required
def admin_data():
    """Возвращает данные участников в формате JSON для AJAX-обновления админки"""
    try:
        # Получаем токен Яндекс.Диска из настроек
        settings = load_settings()
        yandex_token = settings.get('backup_settings', {}).get('yandex_token')
        
        if not yandex_token:
            return jsonify({
                'success': False,
                'message': 'Не найден токен Яндекс.Диска. Настройте токен в параметрах резервного копирования.'
            }), 500
        
        # Проверяем доступность папки приложения на Яндекс.Диске
        headers = {"Authorization": f"OAuth {yandex_token}"}
        url = "https://cloud-api.yandex.net/v1/disk/resources"
        params = {"path": "app:/"}
        
        response = requests.get(url, headers=headers, params=params)
        
        if response.status_code != 200:
            # Если папка не существует, создаем её
            create_app_folder(yandex_token)
        
        # Загружаем список участников
        all_participants = load_participants()
        
        # Если есть GET-параметр page, используем пагинацию
        page = request.args.get('page', 1, type=int)
        ajax_request = request.args.get('ajax', 'false') == 'true'
        
        # Настройка пагинации
        per_page = 50  # Количество участников на странице
        total_participants = len(all_participants)
        total_pages = (total_participants + per_page - 1) // per_page if total_participants > 0 else 1
        
        # Проверка валидности номера страницы
        if page < 1:
            page = 1
        elif page > total_pages:
            page = total_pages
        
        # Вычисляем индексы для выборки участников на текущей странице
        start_idx = (page - 1) * per_page
        end_idx = min(start_idx + per_page, total_participants)
        
        # Получаем участников для текущей страницы
        participants = all_participants[start_idx:end_idx] if total_participants > 0 else []
        
        # Подготовка данных о пагинации
        pagination = {
            'page': page,
            'per_page': per_page,
            'total_pages': total_pages,
            'total_participants': total_participants
        }
        
        # Подготовка данных о статистике
        statistics = {
            'total': total_participants,
            'male': len([p for p in participants if p.get('gender') == 'male']),
            'female': len([p for p in participants if p.get('gender') == 'female'])
        }
        
        return jsonify({
            'success': True,
            'participants': participants,
            'pagination': pagination,
            'statistics': statistics
        })
    except Exception as e:
        app.logger.error(f'Ошибка при загрузке данных для админки: {str(e)}')
        return jsonify({
            'success': False,
            'message': f'Ошибка при загрузке данных: {str(e)}'
        }), 500

# Добавляем в начало файла после импортов
def fix_cyrillic(text):
    """Специальная функция для исправления кодировки кириллицы"""
    if not text or not isinstance(text, str):
        return text
        
    # Первый метод: прямая попытка декодирования из cp1251
    try:
        # Пробуем сначала распознать как UTF-8
        text.encode('utf-8').decode('utf-8')
    except UnicodeError:
        # Пробуем разные кодировки для русского языка
        for encoding in ['cp1251', 'koi8-r', 'iso-8859-5', 'latin1']:
            try:
                decoded = text.encode('latin1').decode(encoding)
                # Проверяем, что результат содержит кириллицу
                if any(ord('А') <= ord(c) <= ord('я') for c in decoded):
                    return decoded
            except (UnicodeError, LookupError):
                continue
                
    # Второй метод: если строка выглядит как кракозябры типа Ð¡ÑÐ»ÐµÐ¹Ð¼Ð°Ð½Ð¾Ð²
    if 'Ð' in text or 'Ñ' in text:
        try:
            # Это может быть UTF-8, закодированный как Latin-1
            bytes_data = text.encode('latin1')
            return bytes_data.decode('utf-8')
        except UnicodeError:
            pass
    
    # Третий метод: для особо сложных случаев
    if 'Ð' in text:
        # Ручное исправление типичных замен для русских букв
        replacements = {
            'Ð': 'А', 'Ñ': 'с', 'Ð°': 'а', 'Ðµ': 'е', 'Ð¸': 'и',
            'Ð¾': 'о', 'Ñ': 'у', 'Ð¼': 'м', 'Ð½': 'н', 'Ð²': 'в'
        }
        for bad, good in replacements.items():
            text = text.replace(bad, good)
            
    return text