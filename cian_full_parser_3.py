import time
import random
import re
import sqlite3
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

class CompleteCianParser:
    def __init__(self):
        self.setup_database()
        self.setup_driver()
        
    def setup_database(self):
        """Создание базы данных с полным набором полей"""
        self.conn = sqlite3.connect('cian_complete.db')
        self.cursor = self.conn.cursor()
        
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS properties (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                cian_id TEXT UNIQUE,               -- ID объявления на ЦИАН
                external_id TEXT,                  -- Внешний ID (из URL)
                url TEXT,
                title TEXT,
                address TEXT,
                full_address TEXT,                 -- Полный адрес
                coordinates TEXT,                  -- Координаты (широта,долгота)
                price REAL,
                price_per_m2 REAL,
                rooms INTEGER,
                total_area REAL,
                living_area REAL,                  -- Жилая площадь
                kitchen_area REAL,                 -- Площадь кухни
                floor INTEGER,
                total_floors INTEGER,
                building_type TEXT,                -- Тип дома
                building_series TEXT,             -- Серия дома
                year_built INTEGER,
                district TEXT,                     -- Район
                metro TEXT,                        -- Станция метро
                metro_distance_walk INTEGER,      -- До метро пешком (минут)
                metro_distance_transport INTEGER, -- До метро на транспорте (минут)
                ceiling_height REAL,              -- Высота потолков
                balcony TEXT,                     -- Балкон/лоджия
                bathroom TEXT,                    -- Санузел
                renovation TEXT,                  -- Ремонт
                elevator TEXT,                    -- Лифт
                parking TEXT,                     -- Парковка
                publication_date TEXT,
                update_date TEXT,
                is_active INTEGER,
                previous_price REAL,
                created_at TEXT,
                last_parsed TEXT
            )
        ''')
        
        self.conn.commit()
        print("✅ База данных готова")
    
    def setup_driver(self):
        """Настройка драйвера"""
        print("🔄 Настраиваю драйвер...")
        
        options = Options()
        options.add_argument('--disable-blink-features=AutomationControlled')
        options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36')
        options.add_argument('--start-maximized')
        options.add_argument('--disable-notifications')
        
        self.driver = webdriver.Chrome(options=options)
        print("✅ Драйвер готов")
    
    def wait_element(self, selector, by=By.CSS_SELECTOR, timeout=10):
        """Ожидание элемента"""
        try:
            element = WebDriverWait(self.driver, timeout).until(
                EC.presence_of_element_located((by, selector))
            )
            return element
        except:
            return None
    
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
    
    def get_search_urls(self, pages=1):
        """Получение URL для поиска"""
        base_urls = []
        
        # Вторичка
        for page in range(1, pages + 1):
            url = f"https://spb.cian.ru/cat.php?deal_type=sale&engine_version=2&offer_type=flat&region=2&p={page}"
            base_urls.append(url)
        
        # Новостройки
        for page in range(1, pages + 1):
            url = f"https://spb.cian.ru/cat.php?deal_type=sale&engine_version=2&offer_type=newbuilding&region=2&p={page}"
            base_urls.append(url)
            
        return base_urls
    
    def collect_listing_urls(self, url):
        """Сбор ссылок на объявления с одной страницы"""
        self.driver.get(url)
        time.sleep(random.uniform(4, 6))
        
        urls = []
        
        # Прокрутка для загрузки
        for _ in range(2):
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight * 0.7);")
            time.sleep(2)
        
        # Ищем все ссылки на объявления
        links = self.driver.find_elements(By.CSS_SELECTOR, 'a[href*="/sale/flat/"], a[href*="/flat/"]')
        
        for link in links:
            try:
                href = link.get_attribute('href')
                if href and '/cat.php' not in href:
                    urls.append(href)
            except:
                continue
        
        return list(set(urls))
    
    def parse_metro_distance(self, text):
        """Парсинг времени до метро"""
        walk_minutes = 0
        transport_minutes = 0
        
        if not text:
            return walk_minutes, transport_minutes
        
        # Пешком
        walk_match = re.search(r'пешком\s*(\d+)\s*мин', text)
        if walk_match:
            walk_minutes = int(walk_match.group(1))
        
        # На транспорте
        transport_match = re.search(r'транспортом\s*(\d+)\s*мин', text)
        if transport_match:
            transport_minutes = int(transport_match.group(1))
        
        # Если просто число без указания
        simple_match = re.search(r'(\d+)\s*мин', text)
        if simple_match and walk_minutes == 0 and transport_minutes == 0:
            if 'транспорт' in text.lower():
                transport_minutes = int(simple_match.group(1))
            else:
                walk_minutes = int(simple_match.group(1))
        
        return walk_minutes, transport_minutes
    
    def parse_listing(self, url):
        """Парсинг одного объявления со всеми данными"""
        print(f"   📄 Парсю: {url[:70]}...")
        
        try:
            self.driver.get(url)
            time.sleep(random.uniform(3, 5))
            
            data = {
                'url': url,
                'cian_id': '',                     # ID объявления
                'external_id': '',                 # ID из URL
                'title': '',
                'address': '',
                'full_address': '',               # Полный адрес
                'coordinates': '',                # Координаты
                'price': 0,
                'price_per_m2': 0,
                'rooms': 0,
                'total_area': 0,
                'living_area': 0,                 # Жилая площадь
                'kitchen_area': 0,                # Площадь кухни
                'floor': 0,
                'total_floors': 0,
                'building_type': '',              # Тип дома
                'building_series': '',           # Серия дома
                'year_built': 0,
                'district': '',                   # Район
                'metro': '',                      # Станция метро
                'metro_distance_walk': 0,        # До метро пешком
                'metro_distance_transport': 0,   # До метро на транспорте
                'ceiling_height': 0,             # Высота потолков
                'balcony': '',                   # Балкон/лоджия
                'bathroom': '',                  # Санузел
                'renovation': '',                # Ремонт
                'elevator': '',                  # Лифт
                'parking': '',                   # Парковка
                'publication_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'update_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'is_active': 1,
                'previous_price': None,
                'created_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'last_parsed': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
            
            # 1. Извлекаем ID объявления
            cian_id_match = re.search(r'/(\d+)/', url)
            if cian_id_match:
                data['cian_id'] = cian_id_match.group(1)
                data['external_id'] = cian_id_match.group(1)
            else:
                data['external_id'] = str(abs(hash(url)))[:10]
            
            print(f"      🆔 ID: {data['cian_id']}")
            
            # 2. Заголовок
            try:
                title_elem = self.wait_element('h1', By.TAG_NAME, 5)
                if title_elem:
                    data['title'] = title_elem.text.strip()[:200]
                    print(f"      📝 {data['title'][:50]}...")
            except:
                pass
            
            # 3. Цена
            try:
                # Основная цена
                price_elem = self.wait_element('[data-testid="price-amount"]', timeout=5)
                if price_elem:
                    data['price'] = self.extract_number(price_elem.text)
                    print(f"      💰 Цена: {data['price']:,.0f} ₽")
                
                # Цена за м²
                price_m2_elem = self.wait_element('[data-testid="price-per-square"]', timeout=3)
                if price_m2_elem:
                    data['price_per_m2'] = self.extract_number(price_m2_elem.text)
                    print(f"      📊 Цена за м²: {data['price_per_m2']:,.0f} ₽")
                    
            except Exception as e:
                print(f"      Ошибка цены: {e}")
            
            # 4. Адрес и координаты
            try:
                # Полный адрес
                address_elem = self.wait_element('[data-name="AddressContainer"]', timeout=5)
                if address_elem:
                    data['address'] = address_elem.text.strip()[:200]
                    data['full_address'] = address_elem.text.strip()[:500]
                    print(f"      📍 Адрес: {data['address'][:60]}...")
                    
                    # Извлекаем район
                    if 'р-н' in data['address']:
                        parts = data['address'].split(',')
                        for part in parts:
                            if 'р-н' in part:
                                data['district'] = part.strip()
                                break
                    
                    # Извлекаем метро
                    metro_match = re.search(r'м\.\s*([^,]+)', data['address'])
                    if metro_match:
                        data['metro'] = metro_match.group(1).strip()
                    
                    # Координаты (пробуем найти на карте)
                    try:
                        map_element = self.driver.find_element(By.CSS_SELECTOR, '[data-name="Map"]')
                        if map_element:
                            # Получаем data-атрибуты с координатами
                            lat = map_element.get_attribute('data-lat')
                            lon = map_element.get_attribute('data-lon')
                            if lat and lon:
                                data['coordinates'] = f"{lat},{lon}"
                                print(f"      🗺️  Координаты: {data['coordinates']}")
                    except:
                        pass
                    
            except Exception as e:
                print(f"      Ошибка адреса: {e}")
            
            # 5. Основные характеристики (блок с детальной информацией)
            try:
                # Ищем все блоки с характеристиками
                features_sections = self.driver.find_elements(By.CSS_SELECTOR, '[data-name="FeaturesGroup"], [data-name="ObjectFactoidsGroup"]')
                
                all_features_text = ""
                for section in features_sections:
                    all_features_text += section.text + "\n"
                
                if all_features_text:
                    features_lower = all_features_text.lower()
                    
                    # Этаж и общее количество этажей
                    floor_match = re.search(r'этаж\s*(\d+)\s*из\s*(\d+)', features_lower)
                    if floor_match:
                        data['floor'] = int(floor_match.group(1))
                        data['total_floors'] = int(floor_match.group(2))
                    else:
                        # Альтернативный поиск
                        floor_match = re.search(r'(\d+)\s*/\s*(\d+)\s*эт', features_lower)
                        if floor_match:
                            data['floor'] = int(floor_match.group(1))
                            data['total_floors'] = int(floor_match.group(2))
                    
                    # Общая площадь
                    total_area_match = re.search(r'общая\s*площадь[^\d]*(\d+[.,]?\d*)', features_lower)
                    if total_area_match:
                        data['total_area'] = float(total_area_match.group(1).replace(',', '.'))
                    
                    # Жилая площадь
                    living_area_match = re.search(r'жилая\s*площадь[^\d]*(\d+[.,]?\d*)', features_lower)
                    if living_area_match:
                        data['living_area'] = float(living_area_match.group(1).replace(',', '.'))
                    
                    # Площадь кухни
                    kitchen_area_match = re.search(r'кухня[^\d]*(\d+[.,]?\d*)', features_lower)
                    if kitchen_area_match:
                        data['kitchen_area'] = float(kitchen_area_match.group(1).replace(',', '.'))
                    
                    # Комнаты
                    rooms_match = re.search(r'(\d+)\s*-?\s*комн', features_lower)
                    if not rooms_match:
                        rooms_match = re.search(r'(\d+)\s*к\.', features_lower)
                    if rooms_match:
                        data['rooms'] = int(rooms_match.group(1))
                    
                    # Год постройки
                    year_match = re.search(r'год\s*постройки[^\d]*(\d{4})', features_lower)
                    if year_match:
                        data['year_built'] = int(year_match.group(1))
                    
                    # Тип дома
                    if 'кирпич' in features_lower:
                        data['building_type'] = 'кирпичный'
                    elif 'панель' in features_lower:
                        data['building_type'] = 'панельный'
                        # Пробуем определить серию
                        series_match = re.search(r'серия[^\d]*([а-яa-z\d-]+)', features_lower, re.IGNORECASE)
                        if series_match:
                            data['building_series'] = series_match.group(1).strip()
                    elif 'монолит' in features_lower:
                        data['building_type'] = 'монолитный'
                    elif 'блоч' in features_lower:
                        data['building_type'] = 'блочный'
                    
                    # Высота потолков
                    height_match = re.search(r'потолки[^\d]*(\d+[.,]?\d*)', features_lower)
                    if height_match:
                        data['ceiling_height'] = float(height_match.group(1).replace(',', '.'))
                    
                    # Балкон/лоджия
                    if 'балкон' in features_lower:
                        data['balcony'] = 'балкон'
                    if 'лоджия' in features_lower:
                        if data['balcony']:
                            data['balcony'] += ', лоджия'
                        else:
                            data['balcony'] = 'лоджия'
                    
                    # Санузел
                    if 'раздел' in features_lower and 'санузел' in features_lower:
                        data['bathroom'] = 'раздельный'
                    elif 'совмещ' in features_lower and 'санузел' in features_lower:
                        data['bathroom'] = 'совмещенный'
                    
                    # Ремонт
                    if 'евроремонт' in features_lower:
                        data['renovation'] = 'евроремонт'
                    elif 'дизайнерск' in features_lower:
                        data['renovation'] = 'дизайнерский'
                    elif 'косметическ' in features_lower:
                        data['renovation'] = 'косметический'
                    
                    # Лифт
                    if 'лифт' in features_lower:
                        data['elevator'] = 'есть'
                    
                    # Парковка
                    if 'паркинг' in features_lower:
                        data['parking'] = 'паркинг'
                    elif 'парковка' in features_lower:
                        data['parking'] = 'парковка'
                    
                    print(f"      🏠 {data['rooms']}к, {data['total_area']}м², {data['floor']}/{data['total_floors']}эт")
                    if data['building_type']:
                        print(f"      🏗️  {data['building_type']}", end='')
                        if data['building_series']:
                            print(f" (серия {data['building_series']})", end='')
                        print()
                    
            except Exception as e:
                print(f"      Ошибка характеристик: {e}")
            
            # 6. Информация о метро (время пути)
            try:
                # Ищем блок с информацией о метро
                metro_section = self.wait_element('[data-name="UndergroundAndTransport"]', timeout=3)
                if metro_section:
                    metro_text = metro_section.text
                    
                    # Парсим время до метро
                    walk_min, transport_min = self.parse_metro_distance(metro_text.lower())
                    data['metro_distance_walk'] = walk_min
                    data['metro_distance_transport'] = transport_min
                    
                    if walk_min > 0 or transport_min > 0:
                        print(f"      🚇 До метро: пешком {walk_min} мин, транспорт {transport_min} мин")
                    
            except Exception as e:
                print(f"      Ошибка метро: {e}")
            
            # 7. Дата публикации
            try:
                date_elem = self.wait_element('[data-name="TimeLabel"]', timeout=3)
                if date_elem:
                    date_text = date_elem.text.lower()
                    
                    if 'сегодня' in date_text:
                        data['publication_date'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    elif 'вчера' in date_text:
                        yesterday = datetime.now().replace(day=datetime.now().day-1)
                        data['publication_date'] = yesterday.strftime('%Y-%m-%d %H:%M:%S')
                    else:
                        # Ищем дату в формате ДД.ММ.ГГГГ
                        date_match = re.search(r'(\d{2}[./]\d{2}[./]\d{4})', date_text)
                        if date_match:
                            date_str = date_match.group(1).replace('/', '.')
                            try:
                                pub_date = datetime.strptime(date_str, '%d.%m.%Y')
                                data['publication_date'] = pub_date.strftime('%Y-%m-%d %H:%M:%S')
                            except:
                                pass
            except:
                pass
            
            # 8. Проверка активности
            try:
                inactive_selectors = [
                    "//*[contains(text(), 'снято')]",
                    "//*[contains(text(), 'неактивно')]",
                    "//*[contains(text(), 'удалено')]"
                ]
                
                for xpath in inactive_selectors:
                    elements = self.driver.find_elements(By.XPATH, xpath)
                    if elements:
                        data['is_active'] = 0
                        print(f"      ⚠️  Объявление неактивно")
                        break
            except:
                pass
            
            return data
            
        except Exception as e:
            print(f"      ❌ Ошибка парсинга: {e}")
            return None
    
    def save_property(self, data):
        """Сохранение объявления в базу"""
        if not data or data['price'] <= 0:
            return False
        
        try:
            # Проверяем существующую запись
            self.cursor.execute(
                "SELECT cian_id, price FROM properties WHERE cian_id = ?",
                (data['cian_id'],)
            )
            existing = self.cursor.fetchone()
            
            if existing:
                old_price = existing[1]
                new_price = data['price']
                
                # Если цена изменилась
                if old_price != new_price:
                    print(f"      📈 Цена изменилась: {old_price:,.0f} → {new_price:,.0f} ₽")
                    data['previous_price'] = old_price
                
                # Обновляем запись
                update_sql = '''
                    UPDATE properties SET
                        url=?, external_id=?, title=?, address=?, full_address=?,
                        coordinates=?, price=?, price_per_m2=?, rooms=?, total_area=?,
                        living_area=?, kitchen_area=?, floor=?, total_floors=?,
                        building_type=?, building_series=?, year_built=?, district=?,
                        metro=?, metro_distance_walk=?, metro_distance_transport=?,
                        ceiling_height=?, balcony=?, bathroom=?, renovation=?,
                        elevator=?, parking=?, publication_date=?, update_date=?,
                        is_active=?, previous_price=?, last_parsed=?
                    WHERE cian_id=?
                '''
                
                self.cursor.execute(update_sql, (
                    data['url'], data['external_id'], data['title'], data['address'],
                    data['full_address'], data['coordinates'], data['price'],
                    data['price_per_m2'], data['rooms'], data['total_area'],
                    data['living_area'], data['kitchen_area'], data['floor'],
                    data['total_floors'], data['building_type'], data['building_series'],
                    data['year_built'], data['district'], data['metro'],
                    data['metro_distance_walk'], data['metro_distance_transport'],
                    data['ceiling_height'], data['balcony'], data['bathroom'],
                    data['renovation'], data['elevator'], data['parking'],
                    data['publication_date'], data['update_date'], data['is_active'],
                    data['previous_price'], data['last_parsed'], data['cian_id']
                ))
                
                print(f"      🔄 Обновлено: {data['cian_id']}")
                
            else:
                # Добавляем новую запись
                insert_sql = '''
                    INSERT INTO properties (
                        cian_id, external_id, url, title, address, full_address,
                        coordinates, price, price_per_m2, rooms, total_area,
                        living_area, kitchen_area, floor, total_floors,
                        building_type, building_series, year_built, district,
                        metro, metro_distance_walk, metro_distance_transport,
                        ceiling_height, balcony, bathroom, renovation,
                        elevator, parking, publication_date, update_date,
                        is_active, previous_price, created_at, last_parsed
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                '''
                
                self.cursor.execute(insert_sql, (
                    data['cian_id'], data['external_id'], data['url'], data['title'],
                    data['address'], data['full_address'], data['coordinates'],
                    data['price'], data['price_per_m2'], data['rooms'], data['total_area'],
                    data['living_area'], data['kitchen_area'], data['floor'],
                    data['total_floors'], data['building_type'], data['building_series'],
                    data['year_built'], data['district'], data['metro'],
                    data['metro_distance_walk'], data['metro_distance_transport'],
                    data['ceiling_height'], data['balcony'], data['bathroom'],
                    data['renovation'], data['elevator'], data['parking'],
                    data['publication_date'], data['update_date'], data['is_active'],
                    data['previous_price'], data['created_at'], data['last_parsed']
                ))
                
                print(f"      ➕ Добавлено: {data['cian_id']}")
            
            self.conn.commit()
            return True
            
        except Exception as e:
            print(f"      ❌ Ошибка сохранения: {e}")
            self.conn.rollback()
            return False
    
    def run(self, total_pages=1):
        """Запуск парсера"""
        print("\n" + "="*70)
        print("🚀 ПОЛНЫЙ ПАРСЕР ЦИАН СО ВСЕМИ ДАННЫМИ")
        print("="*70)
        
        all_urls = []
        
        # Собираем URL со всех страниц
        print(f"\n🔍 Собираю объявления с {total_pages} страниц...")
        
        for page in range(1, total_pages + 1):
            print(f"\n📄 Страница {page}")
            
            # Вторичка
            url = f"https://spb.cian.ru/cat.php?deal_type=sale&engine_version=2&offer_type=flat&region=2&p={page}"
            urls = self.collect_listing_urls(url)
            all_urls.extend(urls)
            print(f"   Вторичка: {len(urls)} объявлений")
            
            # Новостройки
            url = f"https://spb.cian.ru/cat.php?deal_type=sale&engine_version=2&offer_type=newbuilding&region=2&p={page}"
            urls = self.collect_listing_urls(url)
            all_urls.extend(urls)
            print(f"   Новостройки: {len(urls)} объявлений")
            
            time.sleep(2)
        
        # Убираем дубликаты
        unique_urls = list(set(all_urls))
        
        if not unique_urls:
            print("❌ Не найдено объявлений!")
            return
        
        print(f"\n📊 Всего уникальных объявлений для парсинга: {len(unique_urls)}")
        print("\n" + "="*70)
        
        # Парсим каждое объявление
        successful = 0
        failed = 0
        
        for i, url in enumerate(unique_urls):
            print(f"\n[{i+1}/{len(unique_urls)}]")
            
            data = self.parse_listing(url)
            
            if data:
                if self.save_property(data):
                    successful += 1
                else:
                    failed += 1
            else:
                failed += 1
            
            # Пауза между запросами
            pause = random.uniform(4, 8)
            print(f"      ⏸️  Пауза {pause:.1f} секунд...")
            time.sleep(pause)
        
        # Статистика
        self.show_statistics(successful, failed)
    
    def show_statistics(self, successful, failed):
        """Показать статистику"""
        print("\n" + "="*70)
        print("📊 РЕЗУЛЬТАТЫ ПАРСИНГА")
        print("="*70)
        
        print(f"\n✅ Успешно обработано: {successful}")
        print(f"❌ Не удалось: {failed}")
        
        # Общая статистика из базы
        self.cursor.execute("SELECT COUNT(*) FROM properties")
        total = self.cursor.fetchone()[0]
        
        print(f"\n📈 Всего в базе: {total} записей")
        
        if total > 0:
            # Статистика по ценам
            self.cursor.execute("SELECT MIN(price), MAX(price), AVG(price) FROM properties WHERE price > 0")
            min_price, max_price, avg_price = self.cursor.fetchone()
            
            print(f"\n💰 Цены:")
            print(f"   Мин: {min_price:,.0f} ₽")
            print(f"   Макс: {max_price:,.0f} ₽")
            print(f"   Сред: {avg_price:,.0f} ₽")
            
            # Статистика по метро
            self.cursor.execute("SELECT AVG(metro_distance_walk), AVG(metro_distance_transport) FROM properties WHERE metro_distance_walk > 0")
            avg_walk, avg_transport = self.cursor.fetchone()
            
            print(f"\n🚇 До метро:")
            print(f"   Среднее пешком: {avg_walk:.1f} мин")
            print(f"   Среднее на транспорте: {avg_transport:.1f} мин")
            
            # Типы домов
            self.cursor.execute("SELECT building_type, COUNT(*) FROM properties WHERE building_type != '' GROUP BY building_type")
            building_types = self.cursor.fetchall()
            
            if building_types:
                print(f"\n🏗️  Типы домов:")
                for btype, count in building_types:
                    print(f"   {btype}: {count}")
        
        print("\n💾 Экспортирую данные...")
        self.export_to_csv()
        
        print("="*70)
        print("🏁 ПАРСИНГ ЗАВЕРШЕН!")
        print("="*70)
    
    def export_to_csv(self):
        """Экспорт данных в CSV"""
        try:
            import csv
            
            # Получаем все данные
            self.cursor.execute("SELECT * FROM properties")
            columns = [description[0] for description in self.cursor.description]
            rows = self.cursor.fetchall()
            
            # Сохраняем в CSV
            with open('cian_complete_data.csv', 'w', newline='', encoding='utf-8-sig') as f:
                writer = csv.writer(f)
                writer.writerow(columns)
                writer.writerows(rows)
            
            print(f"✅ Данные сохранены в cian_complete_data.csv ({len(rows)} записей)")
            
            # Также создаем упрощенный файл
            self.export_simple_csv()
            
        except Exception as e:
            print(f"❌ Ошибка экспорта: {e}")
    
    def export_simple_csv(self):
        """Экспорт упрощенного CSV с основными полями"""
        try:
            import csv
            
            self.cursor.execute("""
                SELECT cian_id, title, address, price, price_per_m2, rooms, total_area,
                       floor, total_floors, building_type, building_series, year_built,
                       district, metro, metro_distance_walk, metro_distance_transport,
                       publication_date, is_active
                FROM properties
            """)
            
            simple_columns = [
                'ID', 'Заголовок', 'Адрес', 'Цена', 'Цена_м2', 'Комнат', 'Площадь',
                'Этаж', 'Этажей_всего', 'Тип_дома', 'Серия_дома', 'Год_постройки',
                'Район', 'Метро', 'До_метро_пешком', 'До_метро_транспорт',
                'Дата_публикации', 'Активно'
            ]
            
            rows = self.cursor.fetchall()
            
            with open('cian_simple_data.csv', 'w', newline='', encoding='utf-8-sig') as f:
                writer = csv.writer(f)
                writer.writerow(simple_columns)
                writer.writerows(rows)
            
            print(f"✅ Упрощенные данные в cian_simple_data.csv")
            
        except Exception as e:
            print(f"Ошибка упрощенного экспорта: {e}")
    
    def close(self):
        """Закрытие ресурсов"""
        try:
            self.conn.close()
        except:
            pass
        
        try:
            self.driver.quit()
        except:
            pass

def main():
    """Главная функция"""
    print("""
    ╔══════════════════════════════════════════════════════════╗
    ║           ПОЛНЫЙ ПАРСЕР ЦИАН - Санкт-Петербург           ║
    ║        Со сбором ВСЕХ данных о недвижимости              ║
    ╚══════════════════════════════════════════════════════════╝
    """)
    
    parser = None
    
    try:
        # Создаем парсер
        parser = CompleteCianParser()
        
        # Начинаем с 1 страницы
        print("\n⚡ Начинаю парсинг 1 страницы (вторичка + новостройки)")
        print("   Это займет несколько минут...")
        
        # Запускаем парсер
        parser.run(total_pages=1)
        
    except KeyboardInterrupt:
        print("\n\n⏹️  Прервано пользователем")
    except Exception as e:
        print(f"\n\n❌ Критическая ошибка: {e}")
        import traceback
        traceback.print_exc()
    finally:
        if parser:
            parser.close()
        
        print("\n" + "="*70)
        print("📁 Файлы с данными:")
        print("   cian_complete_data.csv - все данные")
        print("   cian_simple_data.csv - основные поля")
        print("="*70)

if __name__ == "__main__":
    main()