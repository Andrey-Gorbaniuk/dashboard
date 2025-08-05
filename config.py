# config.py
import os
from dotenv import load_dotenv

# Загружаем переменные окружения из файла .env
load_dotenv()

# Yandex Metrika
METRIKA_TOKEN = os.getenv("METRIKA_TOKEN")
METRIKA_COUNTER_ID = os.getenv("METRIKA_COUNTER_ID")
METRIKA_API_URL = os.getenv("METRIKA_API_URL", "https://api-metrika.yandex.net/stat/v1/data")

# Topvisor
TOPVISOR_API_KEY = os.getenv("TOPVISOR_API_KEY")
TOPVISOR_USER_ID = os.getenv("TOPVISOR_USER_ID")
TOPVISOR_PROJECT_ID = os.getenv("TOPVISOR_PROJECT_ID")
_region_indexes_str = os.getenv("TOPVISOR_REGION_INDEXES", "")
# ИСПРАВЛЕНИЕ: Добавлен .strip() для удаления случайных пробелов
TOPVISOR_REGION_INDEXES = [int(r.strip()) for r in _region_indexes_str.split(',') if r.strip()] if _region_indexes_str else []
_searchers_str = os.getenv("TOPVISOR_SEARCHERS", "")
# ИСПРАВЛЕНИЕ: Добавлен .strip() для удаления случайных пробелов
TOPVISOR_SEARCHERS = [int(s.strip()) for s in _searchers_str.split(',') if s.strip()] if _searchers_str else []
TOPVISOR_API_URL = os.getenv("TOPVISOR_API_URL", "https://api.topvisor.com/v2/json/get")

# PostgreSQL Database
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

# Logging
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
LOG_FILE = os.getenv("LOG_FILE", "logs/data_loader.log")

# Goals
METRIKA_GOALS_MAP = {
    "117840214": "Обратный звонок",
    "117840244": "Заказ звонка футер",
    "117840256": "Бесплатная консультация",
    "117840301": "Запись в 1 клик",
    "138470269": "Запись под таблицей цен",
    '133377763': "Клиент заполнил форму контактов в окне чата",
    '133377664': "Начат чат с клиентоми",
    "133377721": "Клиент нажал на кнопку “Перезвоните мне” в чате",
    '168219178': "Запись на услугу к специалисту внизу страницы",
    '172154146': "WhatsApp",
    '172158958': "WhatsApp шапка",
    '172158967': "WhatsApp страница услуг",
    '172158976': "WhatsApp плавающее боковое",
    '191685478': "Клик на Instagram",
    "191910028": "Автоцель: клик по номеру телефона",
    "191910031": "Автоцель: скачивание файла",
    "192015796": "Автоцель: поиск по сайту",
    "192893782": "Автоцель: клик по email",
    "196456420": "Автоцель: переход в соц.сеть",
    "249697844": "Автоцель: заполнил контактные данные",
    "305034088": "Переход в Telegram",
    "322936485": "Автоцель: переход в мессенджер"
}
METRIKA_GOAL_IDS_FOR_REQUEST = list(METRIKA_GOALS_MAP.keys())


def check_config():
    required_vars = {
        "METRIKA_TOKEN": METRIKA_TOKEN,
        "METRIKA_COUNTER_ID": METRIKA_COUNTER_ID,
        "DB_HOST": DB_HOST,
        "DB_NAME": DB_NAME,
        "DB_USER": DB_USER,
        "DB_PASSWORD": DB_PASSWORD,
    }
    if TOPVISOR_API_KEY:
        required_vars.update({
            "TOPVISOR_USER_ID": TOPVISOR_USER_ID,
            "TOPVISOR_PROJECT_ID": TOPVISOR_PROJECT_ID
        })
    missing_vars = [key for key, value in required_vars.items() if value is None]
    if missing_vars:
        raise EnvironmentError(f"Missing required environment variables: {', '.join(missing_vars)}")
    print("Configuration loaded successfully.")


if __name__ == '__main__':
    try:
        check_config()
        print(f"Metrika Token: {'*' * 5 if METRIKA_TOKEN else None}")  # Не выводим сам токен в лог
        print(f"Metrika Counter ID: {METRIKA_COUNTER_ID}")
        print(f"DB Host: {DB_HOST}")
        print(f"DB Name: {DB_NAME}")
        # ... и так далее для других переменных, если нужно проверить
        print(f"Metrika Goals for request: {METRIKA_GOAL_IDS_FOR_REQUEST}")
        print(f"Topvisor Region Indexes: {TOPVISOR_REGION_INDEXES}")
        print(f"Topvisor Searchers: {TOPVISOR_SEARCHERS}")

    except EnvironmentError as e:
        print(f"Error loading configuration: {e}")
