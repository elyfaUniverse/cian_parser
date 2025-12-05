import sqlite3
import os

def create_database():
    """Создание базы данных и таблицы"""
    
    # Удаляем старую базу если есть
    if os.path.exists('cian_data.db'):
        os.remove('cian_data.db')
        print("✅ Удалена старая база данных")
    
    # Создаем новое подключение
    conn = sqlite3.connect('cian_data.db')
    cursor = conn.cursor()
    
    # Создаем таблицу
    cursor.execute('''
        CREATE TABLE properties (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            external_id TEXT UNIQUE,
            url TEXT,
            title TEXT,
            address TEXT,
            price REAL,
            price_per_m2 REAL,
            rooms INTEGER,
            total_area REAL,
            floor INTEGER,
            total_floors INTEGER,
            building_type TEXT,
            year_built INTEGER,
            district TEXT,
            metro TEXT,
            description TEXT,
            publication_date TEXT,
            update_date TEXT,
            is_active INTEGER,
            previous_price REAL,
            created_at TEXT
        )
    ''')
    
    # Создаем индекс для быстрого поиска
    cursor.execute('CREATE INDEX idx_external_id ON properties(external_id)')
    cursor.execute('CREATE INDEX idx_price ON properties(price)')
    
    conn.commit()
    conn.close()
    
    print("✅ База данных создана: cian_data.db")
    print("✅ Таблица 'properties' создана")
    
    # Добавим тестовую запись
    add_test_record()

def add_test_record():
    """Добавление тестовой записи"""
    import datetime
    
    conn = sqlite3.connect('cian_data.db')
    cursor = conn.cursor()
    
    test_data = {
        'external_id': 'test_001',
        'url': 'https://spb.cian.ru/sale/flat/test/',
        'title': 'Тестовая квартира',
        'address': 'Санкт-Петербург, тестовый адрес',
        'price': 10000000.0,
        'price_per_m2': 200000.0,
        'rooms': 2,
        'total_area': 50.0,
        'floor': 5,
        'total_floors': 10,
        'building_type': 'панельный',
        'year_built': 2010,
        'district': 'тестовый район',
        'metro': 'тестовое метро',
        'description': 'Тестовое описание квартиры',
        'publication_date': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'update_date': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'is_active': 1,
        'previous_price': None,
        'created_at': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    }
    
    cursor.execute('''
        INSERT INTO properties (
            external_id, url, title, address, price, price_per_m2,
            rooms, total_area, floor, total_floors, building_type,
            year_built, district, metro, description, publication_date,
            update_date, is_active, previous_price, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', tuple(test_data.values()))
    
    conn.commit()
    conn.close()
    
    print("✅ Добавлена тестовая запись")
    print("   ID: test_001")
    print("   Цена: 10,000,000 ₽")

if __name__ == "__main__":
    print("=" * 60)
    print("СОЗДАНИЕ БАЗЫ ДАННЫХ ДЛЯ ПАРСЕРА ЦИАН")
    print("=" * 60)
    
    create_database()
    
    print("\n" + "=" * 60)
    print("Теперь можно:")
    print("1. Запустить парсер: python simple_parser.py")
    print("2. Посмотреть данные: python view_data.py")
    print("=" * 60)