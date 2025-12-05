# test_db.py
import psycopg2

def test_connection():
    print("🔍 Тест подключения к PostgreSQL")
    
    # Пробуем разные варианты подключения
    test_configs = [
        {
            'host': 'localhost',
            'port': '5432',
            'database': 'cian_parser_2',
            'user': 'postgres',
            'password': 'Mamba123'  # Самый простой пароль
        },
        {
            'host': 'localhost',
            'port': '5432',
            'database': 'postgres',
            'user': 'postgres',
            'password': ''  # Пустой пароль
        },
        {
            'host': 'localhost',
            'port': '5432',
            'database': 'postgres',
            'user': 'postgres',
            'password': 'password'
        }
    ]
    
    for i, config in enumerate(test_configs):
        print(f"\nПопытка {i+1}: Пароль = '{config['password']}'")
        
        try:
            conn = psycopg2.connect(**config)
            cursor = conn.cursor()
            
            # Выполняем простой запрос
            cursor.execute("SELECT version()")
            version = cursor.fetchone()[0]
            
            print(f"✅ Успешно!")
            print(f"   PostgreSQL: {version.split(',')[0]}")
            
            # Проверяем доступные базы
            cursor.execute("SELECT datname FROM pg_database WHERE datistemplate = false;")
            databases = cursor.fetchall()
            
            print(f"   Доступные базы: {[db[0] for db in databases]}")
            
            cursor.close()
            conn.close()
            return config
            
        except Exception as e:
            print(f"❌ Ошибка: {e}")
    
    return None

if __name__ == "__main__":
    working_config = test_connection()
    
    if working_config:
        print(f"\n✅ Рабочий конфиг найден!")
        print(f"   Используйте его в парсере")
    else:
        print("\n❌ Не удалось подключиться к PostgreSQL")
        print("\nПроверьте:")
        print("1. Запущен ли PostgreSQL сервер")
        print("2. Правильность пароля")
        print("3. Можно попробовать пароль без русских букв и спецсимволов")
    
    input("\nНажмите Enter для выхода...")