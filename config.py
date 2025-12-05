# Конфигурационные параметры
BASE_URL = "https://spb.cian.ru/cat.php"
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
}

# Параметры для поиска
SEARCH_PARAMS = {
    'deal_type': 'sale',
    'engine_version': 2,
    'offer_type': 'flat',  # квартиры
    'region': 2,  # СПб
    'sort': 'creation_date_desc'
}

# Настройки БД
DATABASE_URL = 'sqlite:///data/cian_database.db'
"""
Конфигурационный файл для парсера CIAN
"""

# Настройки парсера
PARSER_CONFIG = {
    'delay': 2.0,  # Задержка между запросами в секундах
    'max_retries': 3,  # Максимальное количество попыток
    'timeout': 30,  # Таймаут запроса
    'user_agents': [
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
    ]
}

# CSS-селекторы для CIAN (могут меняться!)
SELECTORS = {
    'listing_card': 'article[data-name="CardComponent"]',
    'title': 'h1[data-name="OfferTitle"]',
    'price': 'span[data-mark="MainPrice"]',
    'address': 'a[data-name="GeoLabel"]',
    'features': 'div[data-name="Features"]',
    'description': 'div[data-name="Description"]',
}

# Регулярные выражения для поиска
PATTERNS = {
    'price': r'(\d[\d\s]+)₽',
    'phone': r'[\+]?[7-8][\s\(]?\d{3}[\)\s]?\d{3}[\-\s]?\d{2}[\-\s]?\d{2}',
}
# config.py
DB_CONFIG = {
    'host': 'localhost',
    'database': 'cian_parser',
    'user': 'parser_user',
    'password': 'Mamba123',  # Замените на ваш
    'port': '5432'
}

CIAN_URLS = {
    'secondary': [
        "https://spb.cian.ru/cat.php?deal_type=sale&engine_version=2&offer_type=flat&region=2&room1=1",
        "https://spb.cian.ru/cat.php?deal_type=sale&engine_version=2&offer_type=flat&region=2&room2=1",
        "https://spb.cian.ru/cat.php?deal_type=sale&engine_version=2&offer_type=flat&region=2&room3=1"
    ],
    'newbuilding': [
        "https://spb.cian.ru/cat.php?deal_type=sale&engine_version=2&offer_type=newbuilding&region=2"
    ]
}

PARSING_SETTINGS = {
    'max_pages': 3,
    'offers_per_page': 5,
    'delay_between_requests': 3,
    'headless': False,
    'save_screenshots': True
}