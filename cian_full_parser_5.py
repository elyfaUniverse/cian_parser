# cian_parser_final.py
import time
import re
import json
import sys
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options

class CianParser:
    def __init__(self):
        """Парсер ЦИАН для таблицы cian_offers"""
        self.driver = None
        self.conn = None
        self.cursor = None
        self.table_name = "cian_offers"  # Ваша таблица!
    
    def setup_browser(self):
        """Настройка браузера"""
        print("\n🌐 Настройка браузера...")
        
        options = Options()
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
        
        try:
            self.driver = webdriver.Chrome(options=options)
            print("✅ Браузер запущен")
            return True
        except Exception as e:
            print(f"❌ Ошибка запуска браузера: {e}")
            return False
    
    def setup_database(self):
        """Подключение к базе"""
        print("\n🔌 Подключение к базе данных...")
        
        # Ваши параметры подключения
        db_config = {
            'host': 'localhost',
            'port': '5432',
            'database': 'cian_parser_2',
            'user': 'postgres',
            'password': 'Mamba123'
        }
        
        try:
            import psycopg2
            
            conn = psycopg2.connect(
                host=db_config['host'],
                port=db_config['port'],
                database=db_config['database'],
                user=db_config['user'],
                password=db_config['password'],
                client_encoding='UTF8'
            )
            
            self.conn = conn
            self.cursor = self.conn.cursor()
            
            print(f"✅ Подключено к базе: {db_config['database']}")
            print(f"📋 Используем таблицу: {self.table_name}")
            
            # Проверяем таблицу
            self.check_table_structure()
            
            return True
            
        except ImportError:
            print("❌ Установите: pip install psycopg2-binary")
            return False
        except Exception as e:
            print(f"❌ Ошибка подключения: {str(e)}")
            return False
    
    def check_table_structure(self):
        """Проверка структуры таблицы"""
        try:
            # Проверяем, существует ли таблица
            self.cursor.execute(f"""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_name = '{self.table_name}'
                )
            """)
            
            table_exists = self.cursor.fetchone()[0]
            
            if not table_exists:
                print(f"❌ Таблица '{self.table_name}' не найдена!")
                print("Создайте таблицу в DBeaver:")
                print("""
CREATE TABLE cian_offers (
    id SERIAL PRIMARY KEY,
    cian_id VARCHAR(50) UNIQUE,
    url TEXT,
    title TEXT,
    address TEXT,
    price NUMERIC(12,2),
    price_per_m2 NUMERIC(10,2),
    area NUMERIC(6,2),
    rooms INTEGER,
    floor INTEGER,
    total_floors INTEGER,
    property_type VARCHAR(50),
    publication_date DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
                """)
                return False
            
            # Получаем структуру таблицы
            self.cursor.execute(f"""
                SELECT column_name, data_type, is_nullable
                FROM information_schema.columns
                WHERE table_name = '{self.table_name}'
                ORDER BY ordinal_position
            """)
            
            columns = self.cursor.fetchall()
            print(f"\n📋 Структура таблицы '{self.table_name}':")
            for col in columns:
                print(f"  {col[0]}: {col[1]}")
            
            # Проверяем наличие необходимых столбцов
            required_columns = ['cian_id', 'url', 'price', 'title']
            actual_columns = [col[0] for col in columns]
            
            missing = [col for col in required_columns if col not in actual_columns]
            if missing:
                print(f"⚠️ Отсутствуют столбцы: {missing}")
                return False
            
            print("✅ Структура таблицы подходит")
            return True
            
        except Exception as e:
            print(f"❌ Ошибка проверки структуры: {e}")
            return False
    
    def parse_search_page(self, url):
        """Парсинг поисковой страницы"""
        print(f"\n🔍 Поиск объявлений: {url}")
        
        try:
            self.driver.get(url)
            time.sleep(3)
            
            # Прокрутка
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(2)
            
            # Ищем все ссылки
            links = self.driver.find_elements(By.TAG_NAME, 'a')
            offers = []
            
            for link in links:
                try:
                    href = link.get_attribute('href')
                    if href and 'cian.ru/sale/flat' in href:
                        match = re.search(r'/(\d+)/?$', href)
                        if match:
                            offer_id = match.group(1)
                            # Проверяем, нет ли уже такого ID
                            if not any(o['id'] == offer_id for o in offers):
                                offers.append({
                                    'id': offer_id,
                                    'url': href
                                })
                except:
                    continue
            
            print(f"✅ Найдено объявлений: {len(offers)}")
            return offers[:10]  # Ограничиваем 10
            
        except Exception as e:
            print(f"❌ Ошибка при поиске: {e}")
            return []
    
    def parse_offer(self, offer):
        """Парсинг одного объявления"""
        try:
            print(f"\n📄 Парсим ID: {offer['id']}")
            
            self.driver.get(offer['url'])
            time.sleep(2)
            
            # Парсим данные
            data = {
                'cian_id': offer['id'],
                'url': offer['url'],
                'title': self.get_element_text('h1') or self.get_element_text('[data-name="OfferTitle"]'),
                'address': self.get_element_text('[data-name="AddressContainer"]') or self.get_element_text('.address'),
                'price': self.extract_price(),
                'price_per_m2': self.extract_price_per_m2(),
                'area': self.extract_area(),
                'rooms': self.extract_rooms(),
                'floor': self.extract_floor('current'),
                'total_floors': self.extract_floor('total'),
                'property_type': 'новостройка' if 'newbuilding' in offer['url'] else 'вторичка',
                'publication_date': datetime.now().strftime('%Y-%m-%d')
            }
            
            # Выводим результат
            if data['title']:
                print(f"   📝 {data['title'][:50]}...")
            if data['price']:
                print(f"   💰 {data['price']:,} ₽")
            if data['address']:
                print(f"   📍 {data['address'][:40]}...")
            
            return data
            
        except Exception as e:
            print(f"   ❌ Ошибка парсинга: {e}")
            return None
    
    def get_element_text(self, selector):
        """Получить текст элемента"""
        try:
            element = self.driver.find_element(By.CSS_SELECTOR, selector)
            return element.text.strip()
        except:
            return None
    
    def extract_price(self):
        """Извлечь цену"""
        try:
            # Ищем в различных элементах
            price_selectors = [
                '[data-mark="MainPrice"]',
                'span[class*="price"]',
                'div[class*="price"]',
                '[data-name="PriceInfo"]'
            ]
            
            for selector in price_selectors:
                try:
                    elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                    for elem in elements:
                        text = elem.text
                        # Ищем числа
                        numbers = re.findall(r'[\d\s]+', text.replace(',', '.'))
                        if numbers:
                            price_str = numbers[0].replace(' ', '').replace('\xa0', '')
                            if price_str.isdigit() and len(price_str) > 3:
                                return int(price_str)
                except:
                    continue
        except:
            pass
        return None
    
    def extract_price_per_m2(self):
        """Извлечь цену за м2"""
        try:
            elements = self.driver.find_elements(By.CSS_SELECTOR, '[data-mark="PricePerMeter"]')
            for elem in elements:
                text = elem.text
                numbers = re.findall(r'[\d\s]+', text)
                if numbers:
                    price_str = numbers[0].replace(' ', '').replace('\xa0', '')
                    if price_str.isdigit():
                        return int(price_str)
        except:
            pass
        return None
    
    def extract_area(self):
        """Извлечь площадь"""
        try:
            # Ищем в тексте страницы
            page_text = self.driver.page_source
            match = re.search(r'(\d+[,.]?\d*)\s*м²', page_text)
            if match:
                return float(match.group(1).replace(',', '.'))
        except:
            pass
        return None
    
    def extract_rooms(self):
        """Извлечь количество комнат"""
        try:
            # Проверяем студию
            page_text = self.driver.page_source.lower()
            if 'студия' in page_text:
                return 0
            
            # Ищем в заголовке
            title = self.get_element_text('h1') or ''
            title_lower = title.lower()
            
            if '1-комн' in title_lower or '1 комн' in title_lower:
                return 1
            elif '2-комн' in title_lower or '2 комн' in title_lower:
                return 2
            elif '3-комн' in title_lower or '3 комн' in title_lower:
                return 3
            elif '4-комн' in title_lower or '4 комн' in title_lower:
                return 4
        except:
            pass
        return None
    
    def extract_floor(self, floor_type):
        """Извлечь этаж"""
        try:
            # Ищем в тексте страницы
            page_text = self.driver.page_source
            
            if floor_type == 'current':
                match = re.search(r'(\d+)\s*этаж', page_text)
                if match:
                    return int(match.group(1))
            
            elif floor_type == 'total':
                match = re.search(r'из\s*(\d+)', page_text)
                if match:
                    return int(match.group(1))
                    
        except:
            pass
        return None
    
    def save_to_database(self, data):
        """Сохранить в базу cian_offers"""
        if not data or not self.conn:
            return False
        
        try:
            # Проверяем существующую запись
            self.cursor.execute(
                f"SELECT price FROM {self.table_name} WHERE cian_id = %s",
                (data['cian_id'],)
            )
            
            existing = self.cursor.fetchone()
            
            if existing:
                # Обновляем существующую запись
                old_price = existing[0]
                new_price = data.get('price')
                
                # Если цена изменилась
                if new_price and new_price != old_price:
                    print(f"   💱 Цена изменилась: {old_price:,} → {new_price:,} ₽")
                
                # SQL для обновления
                update_sql = f"""
                    UPDATE {self.table_name} SET
                    url = %s, title = %s, address = %s, price = %s, price_per_m2 = %s,
                    area = %s, rooms = %s, floor = %s, total_floors = %s,
                    property_type = %s, publication_date = %s, updated_at = %s
                    WHERE cian_id = %s
                """
                
                self.cursor.execute(update_sql, (
                    data['url'], data['title'], data['address'],
                    data['price'], data['price_per_m2'],
                    data['area'], data['rooms'], data['floor'], data['total_floors'],
                    data['property_type'], data['publication_date'],
                    datetime.now(), data['cian_id']
                ))
                
                print(f"   📝 Обновлено в базе")
                
            else:
                # Новая запись
                insert_sql = f"""
                    INSERT INTO {self.table_name} 
                    (cian_id, url, title, address, price, price_per_m2, 
                     area, rooms, floor, total_floors, property_type, 
                     publication_date, created_at, updated_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                
                self.cursor.execute(insert_sql, (
                    data['cian_id'], data['url'], data['title'], data['address'],
                    data['price'], data['price_per_m2'],
                    data['area'], data['rooms'], data['floor'], data['total_floors'],
                    data['property_type'], data['publication_date'],
                    datetime.now(), datetime.now()
                ))
                
                print(f"   ✅ Сохранено в базу")
            
            self.conn.commit()
            return True
            
        except Exception as e:
            print(f"   ❌ Ошибка базы: {e}")
            self.conn.rollback()
            return False
    
    def save_to_file(self, data):
        """Сохранить в файл"""
        try:
            filename = f"cian_offers_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2, default=str)
            
            print(f"\n💾 Данные сохранены в файл: {filename}")
            return True
        except Exception as e:
            print(f"❌ Ошибка сохранения в файл: {e}")
            return False
        
    
    
    def show_stats(self):
        """Показать статистику"""
        if not self.conn:
            print("\n❌ Нет подключения к базе")
            return
        
        try:
            print("\n" + "="*50)
            print(f"📊 СТАТИСТИКА ТАБЛИЦЫ '{self.table_name}'")
            print("="*50)
            
            # Общее количество
            self.cursor.execute(f"SELECT COUNT(*) FROM {self.table_name}")
            total = self.cursor.fetchone()[0]
            print(f"Всего объявлений: {total}")
            
            # По типам недвижимости
            self.cursor.execute(f"""
                SELECT property_type, COUNT(*) 
                FROM {self.table_name} 
                WHERE property_type IS NOT NULL
                GROUP BY property_type
            """)
            types = self.cursor.fetchall()
            for t in types:
                print(f"  {t[0]}: {t[1]}")
            
            # Средняя цена
            self.cursor.execute(f"SELECT AVG(price) FROM {self.table_name} WHERE price > 0")
            avg_price = self.cursor.fetchone()[0]
            if avg_price:
                print(f"Средняя цена: {avg_price:,.0f} ₽")
            
            # Последние добавленные
            self.cursor.execute(f"""
                SELECT cian_id, title, price, created_at 
                FROM {self.table_name} 
                ORDER BY created_at DESC 
                LIMIT 3
            """)
            recent = self.cursor.fetchall()
            print("\n📅 Последние добавленные:")
            for r in recent:
                print(f"  ID: {r[0]}, Цена: {r[2]:,} ₽")
            
            print("="*50)
            
        except Exception as e:
            print(f"Ошибка статистики: {e}")
    
    def run_parsing(self):
        """Запуск парсинга"""
        # URL для парсинга
        urls = [
            "https://spb.cian.ru/cat.php?deal_type=sale&engine_version=2&offer_type=flat&region=2&room1=1",
            "https://spb.cian.ru/cat.php?deal_type=sale&engine_version=2&offer_type=flat&region=2&room2=1"
        ]
        
        all_data = []
        parsed_count = 0
        
        for url in urls[:1]:  # Берем только первую для теста
            print(f"\n🌐 Парсим: {url}")
            
            # Ищем объявления
            offers = self.parse_search_page(url)
            
            if not offers:
                print("❌ Не найдено объявлений")
                continue
            
            # Парсим каждое объявление
            for i, offer in enumerate(offers[:5]):  # Ограничиваем 5
                print(f"\n[{i+1}/{min(5, len(offers))}]")
                
                data = self.parse_offer(offer)
                
                if data:
                    # Сохраняем в базу
                    if self.conn:
                        self.save_to_database(data)
                    
                    # Сохраняем для файла
                    all_data.append(data)
                    parsed_count += 1
                
                # Пауза между запросами
                time.sleep(2)
        
        return all_data, parsed_count
    
    def run(self):
        """Запуск парсера"""
        print("="*60)
        print("ПАРСЕР ЦИАН ДЛЯ ТАБЛИЦЫ cian_offers")
        print("="*60)
        
        # 1. Настройка браузера
        if not self.setup_browser():
            return
        
        # 2. Настройка базы данных
        use_db = input("\nСохранять в базу данных? (y/n): ").lower() == 'y'
        
        if use_db:
            if not self.setup_database():
                print("\n⚠️ Продолжаем без базы данных")
                use_db = False
        
        # 3. Запуск парсинга
        all_data, parsed_count = self.run_parsing()
        
        # 4. Сохраняем в файл
        if all_data:
            self.save_to_file(all_data)
        
        # 5. Статистика
        if use_db:
            self.show_stats()
        
        print(f"\n✅ Парсинг завершен!")
        print(f"📈 Обработано: {parsed_count} объявлений")
        
        if use_db:
            print("\n💡 Откройте DBeaver и выполните:")
            print(f"   SELECT * FROM {self.table_name} ORDER BY created_at DESC;")
            print(f"   SELECT COUNT(*) FROM {self.table_name};")
        
        print("\n" + "="*60)
    
    def close(self):
        """Закрытие"""
        try:
            if self.driver:
                self.driver.quit()
                print("✅ Браузер закрыт")
        except:
            pass
        
        try:
            if self.cursor:
                self.cursor.close()
            if self.conn:
                self.conn.close()
                print("✅ Соединение с базой закрыто")
        except:
            pass

def main():
    """Главная функция"""
    parser = CianParser()
    
    try:
        parser.run()
    except KeyboardInterrupt:
        print("\n\n⚠️ Прервано пользователем")
    except Exception as e:
        print(f"\n❌ Ошибка: {str(e)}")
        import traceback
        traceback.print_exc()
    finally:
        parser.close()
    
    input("\nНажмите Enter для выхода...")

if __name__ == "__main__":
    # Проверяем зависимости
    try:
        import selenium
    except ImportError:
        print("Установите: pip install selenium")
        sys.exit(1)
    
    main()