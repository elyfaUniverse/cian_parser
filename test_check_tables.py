# check_tables.py
import psycopg2

def check_tables():
    """Проверка таблиц в базе"""
    conn = psycopg2.connect(
        host='localhost',
        port='5432',
        database='cian_parser_2',
        user='postgres',
        password='Mamba123'
    )
    
    cursor = conn.cursor()
    
    # Получаем все таблицы
    cursor.execute("""
        SELECT table_name, table_type 
        FROM information_schema.tables 
        WHERE table_schema = 'public'
        ORDER BY table_name
    """)
    
    tables = cursor.fetchall()
    
    print("📊 ТАБЛИЦЫ В БАЗЕ ДАННЫХ:")
    print("="*50)
    
    for table in tables:
        print(f"📋 {table[0]} ({table[1]})")
        
        # Получаем структуру таблицы
        cursor.execute(f"""
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_name = '{table[0]}'
            ORDER BY ordinal_position
        """)
        
        columns = cursor.fetchall()
        print(f"   Столбцы ({len(columns)}):")
        for col in columns:
            print(f"     - {col[0]}: {col[1]} ({'NULL' if col[2] == 'YES' else 'NOT NULL'})")
        
        # Получаем количество записей
        cursor.execute(f"SELECT COUNT(*) FROM {table[0]}")
        count = cursor.fetchone()[0]
        print(f"   Записей: {count}")
        print()
    
    cursor.close()
    conn.close()

if __name__ == "__main__":
    check_tables()
    input("\nНажмите Enter для выхода...")