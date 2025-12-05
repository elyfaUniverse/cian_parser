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
        # Ищем числа с десятичными разделителями
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
            
            # Ищем карточки объявлений - более точные селекторы
            cards = self.driver.find_elements(By.CSS_SELECTOR, '[data-name="CardComponent"], article[data-name="CardComponent"], ._93444fe79c--container--2l4V_')
            
            for card in cards:
                try:
                    # Ищем ссылку внутри карточки
                    link = card.find_element(By.CSS_SELECTOR, 'a[href*="/sale/flat/"], a[href*="/flat/"]')
                    href = link.get_attribute('href')
                    if href and '/cat.php' not in href:
                        all_urls.append(href)
                except:
                    continue
            
            print(f"   Найдено карточек: {len(cards)}")
            
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
                    print(f"      📝 Заголовок: {data['title'][:50]}...")
            except:
                pass
            
            # 2. Цена
            try:
                # Основная цена
                price_elem = self.wait_element('[data-testid="price-amount"]', timeout=5)
                if price_elem and price_elem.text:
                    data['price'] = self.extract_number(price_elem.text)
                    print(f"      💰 Цена: {data['price']:,.0f} ₽")
                
                # Цена за м²
                price_m2_elem = self.wait_element('[data-testid="price-per-square"]', timeout=3)
                if price_m2_elem and '₽/м²' in price_m2_elem.text:
                    data['price_per_m2'] = self.extract_number(price_m2_elem.text)
                    print(f"      📊 Цена за м²: {data['price_per_m2']:,.0f} ₽")
                    
            except Exception as e:
                print(f"      Ошибка цены: {e}")
            
            # 3. Адрес
            try:
                address_elem = self.wait_element('[data-name="AddressContainer"]', timeout=5)
                if address_elem:
                    data['address'] = address_elem.text.strip()[:300]
                    print(f"      📍 Адрес: {data['address'][:60]}...")
                    
                    # Извлекаем район и метро из адреса
                    address_lower = data['address'].lower()
                    
                    # Район
                    if 'р-н' in address_lower:
                        parts = data['address'].split(',')
                        for part in parts:
                            if 'р-н' in part:
                                data['district'] = part.strip()
                                break
                    elif 'район' in address_lower:
                        match = re.search(r'([^,]+район)', address_lower)
                        if match:
                            data['district'] = match.group(1).strip().title()
                    
                    # Метро
                    metro_match = re.search(r'м\.\s*([^,]+)', data['address'])
                    if not metro_match:
                        metro_match = re.search(r'метро\s+"?([^",]+)', address_lower)
                    if metro_match:
                        data['metro'] = metro_match.group(1).strip().title()
                        
                    if data['district']:
                        print(f"      🗺️  Район: {data['district']}")
                    if data['metro']:
                        print(f"      🚇 Метро: {data['metro']}")
                        
            except Exception as e:
                print(f"      Ошибка адреса: {e}")
            
            # 4. Основные характеристики
            try:
                # Пробуем найти блок с характеристиками
                features_elem = None
                
                # Пробуем разные селекторы
                selectors = [
                    '[data-name="ObjectSummaryDescription"]',
                    '.a10a38f197--info--1FoHI',
                    '[data-name="FeaturesList"]'
                ]
                
                for selector in selectors:
                    elem = self.wait_element(selector, timeout=3)
                    if elem:
                        features_elem = elem
                        break
                
                if not features_elem:
                    # Альтернативный поиск
                    features_containers = self.driver.find_elements(By.CSS_SELECTOR, 'div, ul, li')
                    for container in features_containers:
                        text = container.text.lower()
                        if 'комнат' in text or 'м²' in text or 'этаж' in text:
                            if len(text) < 500:  # Не слишком длинный текст
                                features_elem = container
                                break
                
                if features_elem:
                    features_text = features_elem.text.lower()
                    print(f"      📊 Характеристики найдены")
                    
                    # Комнаты из заголовка
                    if data['rooms'] == 0:
                        title_lower = data['title'].lower()
                        room_patterns = [
                            r'(\d+)[-\s]*комн',
                            r'(\d+)[-\s]*к\.',
                            r'(\d+)[-\s]*room'
                        ]
                        for pattern in room_patterns:
                            match = re.search(pattern, title_lower)
                            if match:
                                data['rooms'] = int(match.group(1))
                                break
                    
                    # Площадь из заголовка
                    if data['total_area'] == 0:
                        title_lower = data['title'].lower()
                        area_match = re.search(r'(\d+[.,]?\d*)\s*м²', title_lower)
                        if area_match:
                            data['total_area'] = float(area_match.group(1).replace(',', '.'))
                    
                    # Поиск в тексте характеристик
                    if 'комнат' in features_text or 'к.' in features_text:
                        room_match = re.search(r'(\d+)\s*-?\s*комн', features_text)
                        if not room_match:
                            room_match = re.search(r'(\d+)\s*к\.', features_text)
                        if room_match:
                            data['rooms'] = int(room_match.group(1))
                    
                    if 'м²' in features_text:
                        area_match = re.search(r'(\d+[.,]?\d*)\s*м²', features_text)
                        if area_match:
                            data['total_area'] = float(area_match.group(1).replace(',', '.'))
                    
                    if 'этаж' in features_text:
                        floor_match = re.search(r'(\d+)\s*/\s*(\d+)', features_text)
                        if floor_match:
                            data['floor'] = int(floor_match.group(1))
                            data['total_floors'] = int(floor_match.group(2))
                        else:
                            # Ищем просто этаж
                            floor_match = re.search(r'этаж\s*(\d+)', features_text)
                            if floor_match:
                                data['floor'] = int(floor_match.group(1))
                    
                    if 'кирпич' in features_text:
                        data['building_type'] = 'кирпичный'
                    elif 'панель' in features_text:
                        data['building_type'] = 'панельный'
                    elif 'монолит' in features_text:
                        data['building_type'] = 'монолитный'
                    
                    if 'год' in features_text:
                        year_match = re.search(r'(\d{4})\s*г', features_text)
                        if year_match:
                            data['year_built'] = int(year_match.group(1))
                    
                    print(f"      🏠 Комнат: {data['rooms']}, Площадь: {data['total_area']} м²")
                    print(f"      🏢 Этаж: {data['floor']}/{data['total_floors']}")
                    if data['building_type']:
                        print(f"      🏗️  Тип: {data['building_type']}")
                    if data['year_built']:
                        print(f"      📅 Год: {data['year_built']}")
                        
            except Exception as e:
                print(f"      Ошибка характеристик: {e}")
            
            # 5. Дата публикации
            try:
                # Ищем элемент с датой
                date_selectors = [
                    '[data-name="TimeLabel"]',
                    'time',
                    '.a10a38f197--absolute--2RejM'
                ]
                
                for selector in date_selectors:
                    date_elem = self.wait_element(selector, timeout=2)
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
                        break
            except:
                pass
            
            # Проверяем активность объявления
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
    
    def extract_id(self, url):
        """Извлечение ID из URL"""
        try:
            # Пробуем разные паттерны для ID
            patterns = [
                r'/(\d+)/',
                r'-(\d+)$',
                r'flat-(\d+)'
            ]
            
            for pattern in patterns:
                match = re.search(pattern, url)
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
                
                # Обновляем запись
                update_sql = '''
                    UPDATE properties SET
                        url=?, title=?, address=?, price=?, price_per_m2=?,
                        rooms=?, total_area=?, floor=?, total_floors=?,
                        building_type=?, year_built=?, district=?, metro=?,
                        publication_date=?, update_date=?, is_active=?,
                        previous_price=?, last_parsed=?
                    WHERE external_id=?
                '''
                
                self.cursor.execute(update_sql, (
                    data['url'], data['title'], data['address'], data['price'],
                    data['price_per_m2'], data['rooms'], data['total_area'],
                    data['floor'], data['total_floors'], data['building_type'],
                    data['year_built'], data['district'], data['metro'],
                    data['publication_date'], data['update_date'], data['is_active'],
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
                        year_built, district, metro, publication_date,
                        update_date, is_active, previous_price, created_at, last_parsed
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                '''
                
                self.cursor.execute(insert_sql, (
                    data['external_id'], data['url'], data['title'], data['address'],
                    data['price'], data['price_per_m2'], data['rooms'], data['total_area'],
                    data['floor'], data['total_floors'], data['building_type'],
                    data['year_built'], data['district'], data['metro'],
                    data['publication_date'], data['update_date'], data['is_active'],
                    data['previous_price'], data['created_at'], data['last_parsed']
                ))
                
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
            print(f"      ⏸️  Пауза {pause:.1f} секунд...")
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
            
            # Статистика по комнатам
            self.cursor.execute("SELECT AVG(rooms), AVG(total_area) FROM properties WHERE rooms > 0")
            avg_rooms, avg_area = self.cursor.fetchone()
            
            print(f"\n🏠 Статистика по квартирам:")
            print(f"   Среднее комнат: {avg_rooms:.1f}")
            print(f"   Средняя площадь: {avg_area:.1f} м²")
            
            # Районы
            self.cursor.execute("SELECT district, COUNT(*) FROM properties WHERE district != '' GROUP BY district ORDER BY COUNT(*) DESC LIMIT 5")
            districts = self.cursor.fetchall()
            
            if districts:
                print(f"\n📍 Топ районов:")
                for district, count in districts:
                    print(f"   {district}: {count}")
        
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
            self.cursor.execute("""
                SELECT id, external_id, url, title, address, price, price_per_m2,
                       rooms, total_area, floor, total_floors, building_type,
                       year_built, district, metro, publication_date,
                       update_date, is_active, previous_price, created_at, last_parsed
                FROM properties
            """)
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
        try:
            pages = int(input("\nСколько страниц парсить? (рекомендуется 1-2): ") or "1")
        except:
            pages = 1
        
        # Запускаем парсер
        parser.run(pages=pages)
        
    except KeyboardInterrupt:
        print("\n\n⏹️  Прервано пользователем")
    except Exception as e:
        print(f"\n\n❌ Критическая ошибка: {e}")
        import traceback
        traceback.print_exc()
    finally:
        if parser:
            parser.close()
        
        print("\nДля просмотра данных откройте файл cian_data.csv")

if __name__ == "__main__":
    main()