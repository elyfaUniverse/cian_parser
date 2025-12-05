import time
import random
import re
import sqlite3
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException

class CianRealParser:
    def __init__(self):
        self.setup_database()
        self.setup_driver()
        
    def setup_database(self):
        """Создание базы данных"""
        self.conn = sqlite3.connect('cian_real.db')
        self.cursor = self.conn.cursor()
        
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS properties (
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
                created_at TEXT,
                last_parsed TEXT
            )
        ''')
        
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS price_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                property_id TEXT,
                price REAL,
                date TEXT
            )
        ''')
        
        self.conn.commit()
        print("✅ База данных готова")
    
    def setup_driver(self):
        """Настройка драйвера"""
        print("🔄 Настраиваю драйвер...")
        
        options = Options()
        options.add_argument('--disable-blink-features=AutomationControlled')
        options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
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
    
    def get_search_urls(self):
        """URL для поиска"""
        base_urls = [
            "https://spb.cian.ru/cat.php?deal_type=sale&engine_version=2&offer_type=flat&region=2&sort=creation_date_desc",
            "https://spb.cian.ru/cat.php?deal_type=sale&engine_version=2&offer_type=newbuilding&region=2&sort=creation_date_desc"
        ]
        return base_urls
    
    def collect_listing_urls(self, url, pages=2):
        """Сбор ссылок на объявления"""
        print(f"\n🔍 Собираю объявления с: {url}")
        
        all_urls = []
        
        for page in range(1, pages + 1):
            print(f"   Страница {page}")
            
            page_url = f"{url}&p={page}" if page > 1 else url
            self.driver.get(page_url)
            time.sleep(random.uniform(4, 6))
            
            # Прокрутка для загрузки
            for _ in range(3):
                self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight * 0.7);")
                time.sleep(1)
            
            # Ищем карточки объявлений
            cards = self.driver.find_elements(By.CSS_SELECTOR, '[data-name="CardComponent"], article')
            
            for card in cards:
                try:
                    link = card.find_element(By.CSS_SELECTOR, 'a[href*="/sale/"], a[href*="/flat/"]')
                    href = link.get_attribute('href')
                    if href and '/cat.php' not in href:
                        all_urls.append(href)
                except:
                    continue
            
            print(f"   Найдено: {len(cards)} карточек")
            
            if page < pages:
                time.sleep(random.uniform(3, 5))
        
        # Убираем дубликаты
        unique_urls = list(set(all_urls))
        print(f"✅ Уникальных объявлений: {len(unique_urls)}")
        return unique_urls
    
    def parse_listing(self, url):
        """Парсинг одного объявления"""
        print(f"   📄 Парсю: {url[:80]}...")
        
        try:
            self.driver.get(url)
            time.sleep(random.uniform(3, 5))
            
            data = {
                'url': url,
                'external_id': self.extract_id(url),
                'title': '',
                'address': '',
                'price': 0,
                'price_per_m2': 0,
                'rooms': 0,
                'total_area': 0,
                'floor': 0,
                'total_floors': 0,
                'building_type': '',
                'year_built': 0,
                'district': '',
                'metro': '',
                'description': '',
                'publication_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'update_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'is_active': 1,
                'previous_price': None,
                'created_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'last_parsed': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
            
            # 1. Заголовок
            try:
                title_elem = self.wait_element('h1', By.TAG_NAME, 5)
                if title_elem:
                    data['title'] = title_elem.text.strip()[:200]
            except:
                pass
            
            # 2. Цена
            try:
                # Основная цена
                price_selectors = [
                    '[data-testid="price-amount"]',
                    '[data-mark="MainPrice"]',
                    '.price'
                ]
                
                for selector in price_selectors:
                    price_elem = self.wait_element(selector, timeout=3)
                    if price_elem and price_elem.text:
                        data['price'] = self.extract_number(price_elem.text)
                        break
                
                # Цена за м²
                price_m2_selectors = [
                    '[data-testid="price-per-square"]',
                    '.price-per-meter'
                ]
                
                for selector in price_m2_selectors:
                    price_m2_elem = self.wait_element(selector, timeout=2)
                    if price_m2_elem and '₽/м²' in price_m2_elem.text:
                        data['price_per_m2'] = self.extract_number(price_m2_elem.text)
                        break
                        
            except Exception as e:
                print(f"      Ошибка цены: {e}")
            
            # 3. Адрес
            try:
                address_elem = self.wait_element('[data-name="AddressContainer"]', timeout=5)
                if address_elem:
                    data['address'] = address_elem.text.strip()[:300]
                    
                    # Извлекаем район и метро
                    address_lower = data['address'].lower()
                    
                    # Район
                    district_match = re.search(r'([^,]+район)', address_lower)
                    if district_match:
                        data['district'] = district_match.group(1).strip().title()
                    
                    # Метро
                    metro_match = re.search(r'метро\s+"?([^",]+)', address_lower)
                    if metro_match:
                        data['metro'] = metro_match.group(1).strip().title()
                        
            except Exception as e:
                print(f"      Ошибка адреса: {e}")
            
            # 4. Основные характеристики
            try:
                # Ищем блок с характеристиками
                features_container = None
                containers = [
                    '[data-name="FeaturesList"]',
                    '[data-name="ObjectSummaryDescription"]',
                    '.offer-card__features'
                ]
                
                for container in containers:
                    elem = self.wait_element(container, timeout=3)
                    if elem:
                        features_container = elem
                        break
                
                if features_container:
                    features_text = features_container.text.lower()
                    
                    # Комнаты
                    room_match = re.search(r'(\d+)\s*-?\s*комн', features_text)
                    if room_match:
                        data['rooms'] = int(room_match.group(1))
                    
                    # Площадь
                    area_match = re.search(r'(\d+[.,]?\d*)\s*м²', features_text)
                    if area_match:
                        data['total_area'] = float(area_match.group(1).replace(',', '.'))
                    
                    # Этаж
                    floor_match = re.search(r'(\d+)\s*/\s*(\d+)\s*эт', features_text)
                    if floor_match:
                        data['floor'] = int(floor_match.group(1))
                        data['total_floors'] = int(floor_match.group(2))
                    
                    # Год постройки
                    year_match = re.search(r'(\d{4})\s*г[^а-я]', features_text)
                    if year_match:
                        data['year_built'] = int(year_match.group(1))
                    
                    # Тип дома
                    if 'кирпич' in features_text:
                        data['building_type'] = 'кирпичный'
                    elif 'панель' in features_text:
                        data['building_type'] = 'панельный'
                    elif 'монолит' in features_text:
                        data['building_type'] = 'монолитный'
                        
            except Exception as e:
                print(f"      Ошибка характеристик: {e}")
            
            # 5. Описание
            try:
                desc_elem = self.wait_element('[data-name="Description"]', timeout=3)
                if desc_elem:
                    data['description'] = desc_elem.text.strip()[:1000]
            except:
                pass
            
            # 6. Дата публикации
            try:
                date_elem = self.wait_element('[data-name="TimeLabel"]', timeout=3)
                if date_elem:
                    date_text = date_elem.text.lower()
                    
                    if 'сегодня' in date_text:
                        data['publication_date'] = datetime.now().strftime('%Y-%m-%d')
                    elif 'вчера' in date_text:
                        yesterday = datetime.now().replace(day=datetime.now().day-1)
                        data['publication_date'] = yesterday.strftime('%Y-%m-%d')
                    else:
                        # Ищем дату в формате ДД.ММ.ГГГГ
                        date_match = re.search(r'(\d{2}[./]\d{2}[./]\d{4})', date_text)
                        if date_match:
                            date_str = date_match.group(1).replace('/', '.')
                            try:
                                pub_date = datetime.strptime(date_str, '%d.%m.%Y')
                                data['publication_date'] = pub_date.strftime('%Y-%m-%d')
                            except:
                                pass
            except:
                pass
            
            # Проверяем активность
            try:
                inactive_elem = self.driver.find_elements(By.XPATH, "//*[contains(text(), 'снято') or contains(text(), 'неактивно')]")
                if inactive_elem:
                    data['is_active'] = 0
            except:
                pass
            
            return data
            
        except Exception as e:
            print(f"      ❌ Ошибка парсинга: {e}")
            return None
    
    def extract_id(self, url):
        """Извлечение ID из URL"""
        try:
            match = re.search(r'/(\d+)/', url)
            if match:
                return match.group(1)
            
            match = re.search(r'-(\d+)$', url)
            if match:
                return match.group(1)
            
            # Если не нашли, создаем из хэша
            return str(abs(hash(url)))[:10]
            
        except:
            return str(abs(hash(url)))[:10]
    
    def save_property(self, data):
        """Сохранение объявления в базу"""
        if not data or data['price'] <= 0:
            return False
        
        try:
            # Проверяем существующую запись
            self.cursor.execute(
                "SELECT external_id, price FROM properties WHERE external_id = ?",
                (data['external_id'],)
            )
            existing = self.cursor.fetchone()
            
            if existing:
                old_price = existing[1]
                new_price = data['price']
                
                # Если цена изменилась
                if old_price != new_price:
                    print(f"      📈 Цена изменилась: {old_price:,.0f} → {new_price:,.0f} ₽")
                    data['previous_price'] = old_price
                    
                    # Сохраняем в историю цен
                    self.cursor.execute(
                        "INSERT INTO price_history (property_id, price, date) VALUES (?, ?, ?)",
                        (data['external_id'], new_price, datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
                    )
                
                # Обновляем запись
                update_sql = '''
                    UPDATE properties SET
                        url=?, title=?, address=?, price=?, price_per_m2=?,
                        rooms=?, total_area=?, floor=?, total_floors=?,
                        building_type=?, year_built=?, district=?, metro=?,
                        description=?, update_date=?, is_active=?,
                        previous_price=?, last_parsed=?
                    WHERE external_id=?
                '''
                
                self.cursor.execute(update_sql, (
                    data['url'], data['title'], data['address'], data['price'],
                    data['price_per_m2'], data['rooms'], data['total_area'],
                    data['floor'], data['total_floors'], data['building_type'],
                    data['year_built'], data['district'], data['metro'],
                    data['description'], data['update_date'], data['is_active'],
                    data['previous_price'], data['last_parsed'],
                    data['external_id']
                ))
                
                print(f"      🔄 Обновлено: {data['external_id']}")
                
            else:
                # Добавляем новую запись
                insert_sql = '''
                    INSERT INTO properties (
                        external_id, url, title, address, price, price_per_m2,
                        rooms, total_area, floor, total_floors, building_type,
                        year_built, district, metro, description, publication_date,
                        update_date, is_active, previous_price, created_at, last_parsed
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                '''
                
                self.cursor.execute(insert_sql, (
                    data['external_id'], data['url'], data['title'], data['address'],
                    data['price'], data['price_per_m2'], data['rooms'], data['total_area'],
                    data['floor'], data['total_floors'], data['building_type'],
                    data['year_built'], data['district'], data['metro'], data['description'],
                    data['publication_date'], data['update_date'], data['is_active'],
                    data['previous_price'], data['created_at'], data['last_parsed']
                ))
                
                # Добавляем в историю цен
                self.cursor.execute(
                    "INSERT INTO price_history (property_id, price, date) VALUES (?, ?, ?)",
                    (data['external_id'], data['price'], datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
                )
                
                print(f"      ➕ Добавлено: {data['external_id']}")
            
            self.conn.commit()
            return True
            
        except Exception as e:
            print(f"      ❌ Ошибка сохранения: {e}")
            self.conn.rollback()
            return False
    
    def run(self, pages=2):
        """Запуск парсера"""
        print("\n" + "="*60)
        print("🚀 ЗАПУСК ПАРСЕРА ЦИАН")
        print("="*60)
        
        all_urls = []
        
        # Собираем URL со всех страниц поиска
        search_urls = self.get_search_urls()
        for search_url in search_urls:
            urls = self.collect_listing_urls(search_url, pages=pages)
            all_urls.extend(urls)
        
        # Убираем дубликаты
        unique_urls = list(set(all_urls))
        
        if not unique_urls:
            print("❌ Не найдено объявлений!")
            return
        
        print(f"\n📊 Всего объявлений для парсинга: {len(unique_urls)}")
        
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
            pause = random.uniform(3, 7)
            time.sleep(pause)
        
        # Статистика
        self.show_statistics(successful, failed)
    
    def show_statistics(self, successful, failed):
        """Показать статистику"""
        print("\n" + "="*60)
        print("📊 РЕЗУЛЬТАТЫ ПАРСИНГА")
        print("="*60)
        
        print(f"\n✅ Успешно обработано: {successful}")
        print(f"❌ Не удалось: {failed}")
        
        # Общая статистика из базы
        self.cursor.execute("SELECT COUNT(*) FROM properties")
        total = self.cursor.fetchone()[0]
        
        self.cursor.execute("SELECT COUNT(*) FROM properties WHERE is_active = 1")
        active = self.cursor.fetchone()[0]
        
        print(f"\n📈 Всего в базе: {total} записей")
        print(f"📈 Активных: {active}")
        print(f"📈 Неактивных: {total - active}")
        
        if total > 0:
            self.cursor.execute("SELECT MIN(price), MAX(price), AVG(price) FROM properties WHERE price > 0")
            min_price, max_price, avg_price = self.cursor.fetchone()
            
            print(f"\n💰 Статистика цен:")
            print(f"   Мин: {min_price:,.0f} ₽")
            print(f"   Макс: {max_price:,.0f} ₽")
            print(f"   Сред: {avg_price:,.0f} ₽")
            
            self.cursor.execute("SELECT COUNT(DISTINCT district) FROM properties WHERE district != ''")
            districts = self.cursor.fetchone()[0]
            print(f"\n📍 Уникальных районов: {districts}")
        
        print("\n💾 Экспортирую данные...")
        self.export_to_csv()
        
        print("="*60)
        print("🏁 ПАРСИНГ ЗАВЕРШЕН!")
        print("="*60)
    
    def export_to_csv(self):
        """Экспорт данных в CSV"""
        try:
            import csv
            
            # Получаем все данные
            self.cursor.execute("SELECT * FROM properties")
            columns = [description[0] for description in self.cursor.description]
            rows = self.cursor.fetchall()
            
            # Сохраняем в CSV
            with open('cian_data.csv', 'w', newline='', encoding='utf-8-sig') as f:
                writer = csv.writer(f)
                writer.writerow(columns)
                writer.writerows(rows)
            
            print(f"✅ Данные сохранены в cian_data.csv ({len(rows)} записей)")
            
        except Exception as e:
            print(f"❌ Ошибка экспорта: {e}")
    
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
    ╔══════════════════════════════════════════════════╗
    ║           ПАРСЕР ЦИАН - Санкт-Петербург          ║
    ║         Вторичка и новостройки для продажи       ║
    ╚══════════════════════════════════════════════════╝
    """)
    
    parser = None
    
    try:
        # Создаем парсер
        parser = CianRealParser()
        
        # Спрашиваем количество страниц
        pages = 1  # Начни с 1 страницы для теста
        
        # Запускаем парсер
        parser.run(pages=pages)
        
    except KeyboardInterrupt:
        print("\n\n⏹️  Прервано пользователем")
    except Exception as e:
        print(f"\n\n❌ Критическая ошибка: {e}")
    finally:
        if parser:
            parser.close()
        
        print("\nДля просмотра данных откройте файл cian_data.csv")
        print("Или запустите: python view_data_simple.py")

if __name__ == "__main__":
    main()