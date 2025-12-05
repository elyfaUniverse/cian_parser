import time
import random
import re
import sqlite3
import os
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException

class AggressiveCianParser:
    def __init__(self):
        self.setup_database()
        self.setup_driver()
        
    def setup_database(self):
        """Создание базы данных"""
        # Убедимся что файл существует
        db_file = 'cian_complete.db'
        if os.path.exists(db_file):
            os.remove(db_file)  # Удаляем старый файл для чистого запуска
        
        self.conn = sqlite3.connect(db_file)
        self.cursor = self.conn.cursor()
        
        self.cursor.execute('''
            CREATE TABLE properties (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                cian_id TEXT UNIQUE,
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
                building_series TEXT,
                year_built INTEGER,
                district TEXT,
                metro TEXT,
                metro_distance_walk INTEGER,
                metro_distance_transport INTEGER,
                coordinates TEXT,
                publication_date TEXT,
                update_date TEXT,
                is_active INTEGER,
                created_at TEXT
            )
        ''')
        
        self.conn.commit()
        print("✅ База данных создана: cian_complete.db")
    
    def setup_driver(self):
        """Настройка драйвера"""
        print("🔄 Настраиваю драйвер Chrome...")
        
        options = Options()
        options.add_argument('--disable-blink-features=AutomationControlled')
        options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36')
        options.add_argument('--start-maximized')
        options.add_argument('--disable-notifications')
        options.add_argument('--disable-gpu')
        options.add_argument('--no-sandbox')
        
        self.driver = webdriver.Chrome(options=options)
        print("✅ Драйвер готов")
    
    def find_element_text(self, selectors, timeout=3):
        """Поиск элемента по нескольким селекторам"""
        for selector in selectors:
            try:
                element = WebDriverWait(self.driver, timeout).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, selector))
                )
                if element and element.text.strip():
                    return element.text.strip()
            except:
                continue
        return ""
    
    def extract_number(self, text):
        """Извлечение числа из текста"""
        if not text:
            return 0
        numbers = re.findall(r'[\d,\.]+', text.replace(' ', ''))
        if numbers:
            try:
                return float(numbers[0].replace(',', '.'))
            except:
                return 0
        return 0
    
    def get_listing_urls(self, url):
        """Получение ссылок на объявления"""
        print(f"🔍 Получаю объявления с: {url}")
        
        self.driver.get(url)
        time.sleep(5)
        
        # Прокручиваем для загрузки
        for i in range(3):
            self.driver.execute_script(f"window.scrollTo(0, document.body.scrollHeight * {0.3 * (i+1)});")
            time.sleep(2)
        
        urls = []
        
        # Ищем все возможные ссылки на объявления
        selectors = [
            'a[href*="/sale/flat/"]',
            'article a[href*="/flat/"]',
            '[data-name="LinkArea"] a',
            '[data-testid="offer-card"] a'
        ]
        
        for selector in selectors:
            try:
                links = self.driver.find_elements(By.CSS_SELECTOR, selector)
                for link in links:
                    try:
                        href = link.get_attribute('href')
                        if href and '/sale/flat/' in href and '/cat.php' not in href:
                            if href not in urls:
                                urls.append(href)
                    except:
                        continue
            except:
                continue
        
        print(f"✅ Найдено {len(urls)} объявлений")
        return urls
    
    def parse_floor_info(self, text):
        """Парсинг информации об этажах"""
        floor = 0
        total_floors = 0
        
        if not text:
            return floor, total_floors
        
        # Паттерны для поиска этажей
        patterns = [
            r'(\d+)\s*/\s*(\d+)\s*эт',
            r'этаж\s*(\d+)\s*из\s*(\d+)',
            r'(\d+)\s*из\s*(\d+)\s*этаж',
            r'(\d+)\s*этаж\s*из\s*(\d+)'
        ]
        
        for pattern in patterns:
            match = re.search(pattern, text.lower())
            if match:
                floor = int(match.group(1))
                total_floors = int(match.group(2))
                break
        
        # Если нашли только этаж
        if floor == 0 and total_floors == 0:
            floor_match = re.search(r'этаж\s*(\d+)', text.lower())
            if floor_match:
                floor = int(floor_match.group(1))
        
        return floor, total_floors
    
    def parse_metro_info(self, text):
        """Парсинг информации о метро"""
        metro = ""
        walk_minutes = 0
        transport_minutes = 0
        
        if not text:
            return metro, walk_minutes, transport_minutes
        
        # Ищем название метро
        metro_patterns = [
            r'м\.\s*([^,\n]+)',
            r'метро\s*"([^"]+)"',
            r'метро\s*([^,\n]+)'
        ]
        
        for pattern in metro_patterns:
            match = re.search(pattern, text)
            if match:
                metro = match.group(1).strip()
                break
        
        # Ищем время до метро
        time_patterns = [
            r'(\d+)\s*мин\s*пешком',
            r'пешком\s*(\d+)\s*мин',
            r'(\d+)\s*мин\s*на\s*транспорте',
            r'транспортом\s*(\d+)\s*мин',
            r'(\d+)\s*мин\s*\(пешком\)',
            r'(\d+)\s*мин\s*\(транспорт\)'
        ]
        
        for pattern in time_patterns:
            matches = re.findall(pattern, text.lower())
            for match in matches:
                minutes = int(match)
                if 'пеш' in pattern or 'пешком' in text.lower():
                    walk_minutes = minutes
                elif 'транспорт' in pattern or 'транспортом' in text.lower():
                    transport_minutes = minutes
        
        return metro, walk_minutes, transport_minutes
    
    def parse_listing(self, url):
        """Парсинг одного объявления"""
        print(f"\n📄 Парсю: {url[:70]}...")
        
        try:
            self.driver.get(url)
            time.sleep(4)
            
            data = {
                'cian_id': '',
                'url': url,
                'title': '',
                'address': '',
                'price': 0,
                'price_per_m2': 0,
                'rooms': 0,
                'total_area': 0,
                'floor': 0,
                'total_floors': 0,
                'building_type': '',
                'building_series': '',
                'year_built': 0,
                'district': '',
                'metro': '',
                'metro_distance_walk': 0,
                'metro_distance_transport': 0,
                'coordinates': '',
                'publication_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'update_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'is_active': 1,
                'created_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
            
            # 1. ID объявления из URL
            match = re.search(r'/(\d+)/', url)
            if match:
                data['cian_id'] = match.group(1)
                print(f"   🆔 ID: {data['cian_id']}")
            
            # 2. Заголовок
            title = self.find_element_text(['h1', '[data-name="OfferTitle"]', '.offer-card__title'], 5)
            if title:
                data['title'] = title[:200]
                print(f"   📝 {data['title'][:60]}...")
            
            # 3. Цена
            price_text = self.find_element_text([
                '[data-testid="price-amount"]',
                '[data-mark="MainPrice"]',
                '.offer-card__price'
            ], 3)
            
            if price_text:
                data['price'] = self.extract_number(price_text)
                print(f"   💰 Цена: {data['price']:,.0f} ₽")
            
            # Цена за м²
            price_m2_text = self.find_element_text([
                '[data-testid="price-per-square"]',
                '.price-per-meter',
                '[data-mark="PricePerMeter"]'
            ], 2)
            
            if price_m2_text:
                data['price_per_m2'] = self.extract_number(price_m2_text)
                print(f"   📊 Цена за м²: {data['price_per_m2']:,.0f} ₽")
            
            # 4. Адрес
            address = self.find_element_text([
                '[data-name="AddressContainer"]',
                '[data-testid="address-line"]',
                'address'
            ], 3)
            
            if address:
                data['address'] = address[:300]
                print(f"   📍 Адрес: {data['address'][:60]}...")
                
                # Пробуем извлечь район
                if 'р-н' in data['address']:
                    parts = data['address'].split(',')
                    for part in parts:
                        if 'р-н' in part:
                            data['district'] = part.strip()
                            break
                
                # Пробуем найти метро в адресе
                metro, walk, transport = self.parse_metro_info(data['address'])
                if metro:
                    data['metro'] = metro
                    data['metro_distance_walk'] = walk
                    data['metro_distance_transport'] = transport
            
            # 5. Ищем блок с характеристиками - более агрессивный поиск
            print("   🔍 Ищу характеристики...")
            
            # Пробуем найти все возможные блоки с данными
            try:
                # Прокручиваем страницу для загрузки
                for i in range(2):
                    self.driver.execute_script(f"window.scrollTo(0, {500 * (i+1)});")
                    time.sleep(1)
                
                # Ищем все div, li, span с текстом
                all_elements = self.driver.find_elements(By.XPATH, "//div | //li | //span | //p")
                all_text = ""
                
                for element in all_elements[:100]:  # Берем первые 100 элементов
                    try:
                        text = element.text.strip()
                        if text and len(text) < 200:  # Не слишком длинные тексты
                            all_text += text + "\n"
                    except:
                        continue
                
                if all_text:
                    # Парсим все что нашли
                    text_lower = all_text.lower()
                    
                    # Комнаты
                    if data['rooms'] == 0:
                        room_match = re.search(r'(\d+)\s*-?\s*комн', text_lower)
                        if not room_match:
                            room_match = re.search(r'(\d+)\s*к\.', text_lower)
                        if room_match:
                            data['rooms'] = int(room_match.group(1))
                    
                    # Площадь
                    if data['total_area'] == 0:
                        area_match = re.search(r'(\d+[.,]?\d*)\s*м²', text_lower)
                        if area_match:
                            data['total_area'] = float(area_match.group(1).replace(',', '.'))
                    
                    # Этажи
                    if data['floor'] == 0:
                        floor, total = self.parse_floor_info(text_lower)
                        data['floor'] = floor
                        data['total_floors'] = total
                    
                    # Год постройки
                    if data['year_built'] == 0:
                        year_match = re.search(r'(\d{4})\s*г', text_lower)
                        if year_match:
                            data['year_built'] = int(year_match.group(1))
                    
                    # Тип дома
                    if not data['building_type']:
                        if 'кирпич' in text_lower:
                            data['building_type'] = 'кирпичный'
                        elif 'панель' in text_lower:
                            data['building_type'] = 'панельный'
                            # Пробуем найти серию
                            series_match = re.search(r'серия\s*([а-яa-z\d-]+)', text_lower, re.IGNORECASE)
                            if series_match:
                                data['building_series'] = series_match.group(1).strip()
                        elif 'монолит' in text_lower:
                            data['building_type'] = 'монолитный'
                        elif 'блоч' in text_lower:
                            data['building_type'] = 'блочный'
                    
                    # Если метро не нашли в адресе, ищем в тексте
                    if not data['metro']:
                        metro, walk, transport = self.parse_metro_info(all_text)
                        if metro:
                            data['metro'] = metro
                            data['metro_distance_walk'] = walk
                            data['metro_distance_transport'] = transport
                    
                    print(f"   ✅ Характеристики найдены")
                    
            except Exception as e:
                print(f"   ⚠️  Ошибка поиска характеристик: {e}")
            
            # 6. Координаты
            try:
                # Ищем карту
                map_elements = self.driver.find_elements(By.CSS_SELECTOR, '[data-name="Map"], .map, iframe[src*="map"]')
                for map_elem in map_elements:
                    # Пробуем получить координаты из data-атрибутов
                    lat = map_elem.get_attribute('data-lat') or map_elem.get_attribute('lat')
                    lon = map_elem.get_attribute('data-lon') or map_elem.get_attribute('lon')
                    
                    if lat and lon:
                        data['coordinates'] = f"{lat},{lon}"
                        print(f"   🗺️  Координаты: {data['coordinates']}")
                        break
            except:
                pass
            
            # 7. Дата публикации
            try:
                date_elem = self.driver.find_element(By.XPATH, "//*[contains(text(), 'Обновлено') or contains(text(), 'Размещено') or contains(text(), 'сегодня') or contains(text(), 'вчера')]")
                if date_elem:
                    date_text = date_elem.text.lower()
                    if 'сегодня' in date_text:
                        data['publication_date'] = datetime.now().strftime('%Y-%m-%d')
                    elif 'вчера' in date_text:
                        yesterday = datetime.now().replace(day=datetime.now().day-1)
                        data['publication_date'] = yesterday.strftime('%Y-%m-%d')
            except:
                pass
            
            # 8. Выводим результат
            print(f"   🏠 Результат: {data['rooms']}к, {data['total_area']}м², {data['floor']}/{data['total_floors']}эт")
            if data['metro']:
                print(f"   🚇 Метро: {data['metro']} ({data['metro_distance_walk']} мин пешком, {data['metro_distance_transport']} мин транспорт)")
            if data['building_type']:
                print(f"   🏗️  Тип дома: {data['building_type']}", end="")
                if data['building_series']:
                    print(f" (серия {data['building_series']})", end="")
                print()
            
            return data
            
        except Exception as e:
            print(f"   ❌ Ошибка парсинга: {e}")
            return None
    
    def save_to_db(self, data):
        """Сохранение в базу данных"""
        if not data or not data['cian_id']:
            return False
        
        try:
            self.cursor.execute('''
                INSERT OR REPLACE INTO properties 
                (cian_id, url, title, address, price, price_per_m2, rooms, total_area,
                 floor, total_floors, building_type, building_series, year_built,
                 district, metro, metro_distance_walk, metro_distance_transport,
                 coordinates, publication_date, update_date, is_active, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                data['cian_id'], data['url'], data['title'], data['address'],
                data['price'], data['price_per_m2'], data['rooms'], data['total_area'],
                data['floor'], data['total_floors'], data['building_type'], data['building_series'],
                data['year_built'], data['district'], data['metro'], data['metro_distance_walk'],
                data['metro_distance_transport'], data['coordinates'], data['publication_date'],
                data['update_date'], data['is_active'], data['created_at']
            ))
            
            self.conn.commit()
            print(f"   💾 Сохранено в базу")
            return True
            
        except Exception as e:
            print(f"   ❌ Ошибка сохранения: {e}")
            return False
    
    def export_to_csv(self):
        """Экспорт в CSV файлы"""
        try:
            # Получаем все данные
            self.cursor.execute("SELECT * FROM properties")
            rows = self.cursor.fetchall()
            columns = [description[0] for description in self.cursor.description]
            
            if not rows:
                print("❌ Нет данных для экспорта!")
                return
            
            # Полный файл
            import csv
            with open('cian_full_data.csv', 'w', newline='', encoding='utf-8-sig') as f:
                writer = csv.writer(f)
                writer.writerow(columns)
                writer.writerows(rows)
            
            print(f"✅ Полные данные сохранены в cian_full_data.csv ({len(rows)} записей)")
            
            # Упрощенный файл
            simple_columns = [
                'cian_id', 'title', 'address', 'price', 'price_per_m2', 
                'rooms', 'total_area', 'floor', 'total_floors',
                'building_type', 'building_series', 'year_built',
                'district', 'metro', 'metro_distance_walk', 'metro_distance_transport',
                'coordinates', 'publication_date', 'is_active'
            ]
            
            # Создаем индексное отображение
            col_indices = {}
            for i, col in enumerate(columns):
                col_indices[col] = i
            
            with open('cian_simple_data.csv', 'w', newline='', encoding='utf-8-sig') as f:
                writer = csv.writer(f)
                writer.writerow(simple_columns)
                
                for row in rows:
                    simple_row = []
                    for col in simple_columns:
                        if col in col_indices:
                            simple_row.append(row[col_indices[col]])
                        else:
                            simple_row.append('')
                    writer.writerow(simple_row)
            
            print(f"✅ Упрощенные данные сохранены в cian_simple_data.csv")
            
            # Проверяем что файлы созданы
            if os.path.exists('cian_full_data.csv'):
                print(f"📁 Размер cian_full_data.csv: {os.path.getsize('cian_full_data.csv')} байт")
            if os.path.exists('cian_simple_data.csv'):
                print(f"📁 Размер cian_simple_data.csv: {os.path.getsize('cian_simple_data.csv')} байт")
            
        except Exception as e:
            print(f"❌ Ошибка экспорта: {e}")
            import traceback
            traceback.print_exc()
    
    def run(self):
        """Запуск парсера"""
        print("\n" + "="*60)
        print("🚀 АГРЕССИВНЫЙ ПАРСЕР ЦИАН")
        print("="*60)
        
        all_urls = []
        
        # Собираем объявления с 1 страницы вторички
        print("\n🔍 Собираю объявления...")
        
        url = "https://spb.cian.ru/cat.php?deal_type=sale&engine_version=2&offer_type=flat&region=2&sort=creation_date_desc"
        urls = self.get_listing_urls(url)
        all_urls.extend(urls)
        
        print(f"\n📊 Всего найдено {len(all_urls)} объявлений")
        
        if not all_urls:
            print("❌ Нет объявлений для парсинга!")
            return
        
        # Парсим каждое объявление
        print("\n" + "="*60)
        print("🔄 Начинаю парсинг объявлений...")
        print("="*60)
        
        successful = 0
        failed = 0
        
        for i, url in enumerate(all_urls):
            print(f"\n[{i+1}/{len(all_urls)}]")
            
            data = self.parse_listing(url)
            
            if data:
                if self.save_to_db(data):
                    successful += 1
                else:
                    failed += 1
            else:
                failed += 1
            
            # Пауза
            pause = random.uniform(3, 6)
            print(f"   ⏸️  Пауза {pause:.1f} сек...")
            time.sleep(pause)
        
        # Статистика
        print("\n" + "="*60)
        print("📊 РЕЗУЛЬТАТЫ:")
        print("="*60)
        print(f"✅ Успешно: {successful}")
        print(f"❌ Неудачно: {failed}")
        
        # Экспорт
        print("\n💾 Экспортирую данные...")
        self.export_to_csv()
        
        print("\n" + "="*60)
        print("🏁 ЗАВЕРШЕНО!")
        print("="*60)
    
    def close(self):
        """Закрытие"""
        try:
            self.conn.close()
            self.driver.quit()
            print("\n✅ Ресурсы освобождены")
        except:
            pass

def main():
    """Главная функция"""
    print("""
    ╔══════════════════════════════════════════════════╗
    ║     АГРЕССИВНЫЙ ПАРСЕР ЦИАН - ВСЕ ДАННЫЕ         ║
    ║           Этажи, метро, координаты               ║
    ╚══════════════════════════════════════════════════╝
    """)
    
    parser = AggressiveCianParser()
    
    try:
        parser.run()
    except KeyboardInterrupt:
        print("\n\n⏹️  Прервано пользователем")
    except Exception as e:
        print(f"\n\n❌ Ошибка: {e}")
        import traceback
        traceback.print_exc()
    finally:
        parser.close()
        
        # Показываем где файлы
        print("\n📁 Проверьте файлы в папке проекта:")
        print("   cian_full_data.csv - все данные")
        print("   cian_simple_data.csv - основные поля")
        print("   cian_complete.db - база данных SQLite")
        
        if os.path.exists('cian_simple_data.csv'):
            print("\n✅ Файлы созданы успешно!")
        else:
            print("\n⚠️  Файлы не созданы. Проверьте ошибки выше.")

if __name__ == "__main__":
    main()