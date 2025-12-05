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
        self.table_name = "cian_offers"
    
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
            self.cursor.execute(f"""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_name = '{self.table_name}'
                )
            """)
            
            table_exists = self.cursor.fetchone()[0]
            
            if not table_exists:
                print(f"❌ Таблица '{self.table_name}' не найдена!")
                return False
            
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
            
            required_columns = ['cian_id', 'url', 'price', 'title', 'area_total']
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
            
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(2)
            
            links = self.driver.find_elements(By.TAG_NAME, 'a')
            offers = []
            
            for link in links:
                try:
                    href = link.get_attribute('href')
                    if href and 'cian.ru/sale/flat' in href:
                        match = re.search(r'/(\d+)/?$', href)
                        if match:
                            offer_id = match.group(1)
                            if not any(o['id'] == offer_id for o in offers):
                                offers.append({
                                    'id': offer_id,
                                    'url': href
                                })
                except:
                    continue
            
            print(f"✅ Найдено объявлений: {len(offers)}")
            return offers[:10]
            
        except Exception as e:
            print(f"❌ Ошибка при поиске: {e}")
            return []
    
    def parse_offer(self, offer):
        """Парсинг одного объявления"""
        try:
            print(f"\n📄 Парсим ID: {offer['id']}")
            
            self.driver.get(offer['url'])
            time.sleep(2)
            
            data = {
                'cian_id': offer['id'],
                'url': offer['url'],
                'title': self.get_element_text('h1') or self.get_element_text('[data-name="OfferTitle"]'),
                'address': self.get_element_text('[data-name="GeoLabel"]') or self.get_element_text('[data-name="AddressContainer"]'),
                'price': self.extract_price(),
                'price_per_m2': self.extract_price_per_m2(),
                'old_price': self.extract_old_price(),
                'area_total': self.extract_area_from_title() or self.extract_area_from_description(),
                'area_living': self.extract_living_area(),
                'area_kitchen': self.extract_kitchen_area(),
                'floor_current': self.extract_current_floor(),
                'floor_total': self.extract_total_floors(),
                'rooms': self.extract_rooms(),
                'year_built': self.extract_year_built(),
                'building_type': self.extract_building_type(),
                'property_type': self.extract_property_type(),
                'description': self.get_description(),
                'seller_type': self.extract_seller_type(),
                'publication_date': datetime.now().strftime('%Y-%m-%d'),
                'is_active': True,
                'district': self.extract_district(),
                'metro_station': self.extract_metro_station(),
                'metro_time': self.extract_metro_time(),
                'last_checked': datetime.now()
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
            import traceback
            traceback.print_exc()
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
            # Основная цена
            price_selectors = [
                '[data-mark="MainPrice"]',
                '[data-name="PriceInfo"] span',
                'span[class*="price"]',
                'div[class*="price"]'
            ]
            
            for selector in price_selectors:
                try:
                    elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                    for elem in elements:
                        text = elem.text
                        # Убираем валюту и лишние символы
                        price_text = re.sub(r'[^\d\s]', '', text)
                        numbers = re.findall(r'[\d\s]+', price_text)
                        if numbers:
                            price_str = numbers[0].replace(' ', '').replace('\xa0', '')
                            if price_str.isdigit() and len(price_str) > 3:
                                return int(price_str)
                except:
                    continue
            
            # Альтернативный метод: поиск по всему тексту
            page_text = self.driver.page_source
            match = re.search(r'"price":\s*(\d+)', page_text)
            if match:
                return int(match.group(1))
                
        except:
            pass
        return None
    
    def extract_old_price(self):
        """Извлечь старую цену"""
        try:
            elements = self.driver.find_elements(By.CSS_SELECTOR, '[data-mark="OldPrice"]')
            for elem in elements:
                text = elem.text
                numbers = re.findall(r'[\d\s]+', text)
                if numbers:
                    price_str = numbers[0].replace(' ', '').replace('\xa0', '')
                    if price_str.isdigit() and len(price_str) > 3:
                        return int(price_str)
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
    
    def extract_area_from_title(self):
        """Извлечь площадь из заголовка"""
        try:
            title = self.get_element_text('h1') or ''
            match = re.search(r'(\d+[,.]?\d*)\s*м²', title)
            if match:
                return float(match.group(1).replace(',', '.'))
        except:
            pass
        return None
    
    def extract_area_from_description(self):
        """Извлечь площадь из описания"""
        try:
            # Ищем в блоке характеристик
            elements = self.driver.find_elements(By.CSS_SELECTOR, '[data-name="Features"]')
            for elem in elements:
                text = elem.text
                match = re.search(r'(\d+[,.]?\d*)\s*м²', text)
                if match:
                    return float(match.group(1).replace(',', '.'))
            
            # Ищем в общем описании
            description = self.get_element_text('[data-name="Description"]')
            if description:
                match = re.search(r'(\d+[,.]?\d*)\s*кв\.?м', description)
                if match:
                    return float(match.group(1).replace(',', '.'))
        except:
            pass
        return None
    
    def extract_living_area(self):
        """Извлечь жилую площадь"""
        try:
            # Ищем в характеристиках
            elements = self.driver.find_elements(By.CSS_SELECTOR, '[data-name="Features"]')
            for elem in elements:
                text = elem.text
                # Ищем "Жилая площадь"
                match = re.search(r'Жилая площадь[:\s]*(\d+[,.]?\d*)\s*м²', text, re.IGNORECASE)
                if match:
                    return float(match.group(1).replace(',', '.'))
            
            # Ищем в описании
            description = self.get_element_text('[data-name="Description"]')
            if description:
                match = re.search(r'комнаты?\s*(\d+[,.]?\d*)\s*м²', description, re.IGNORECASE)
                if match:
                    return float(match.group(1).replace(',', '.'))
        except:
            pass
        return None
    
    def extract_kitchen_area(self):
        """Извлечь площадь кухни"""
        try:
            elements = self.driver.find_elements(By.CSS_SELECTOR, '[data-name="Features"]')
            for elem in elements:
                text = elem.text
                match = re.search(r'Площадь кухни[:\s]*(\d+[,.]?\d*)\s*м²', text, re.IGNORECASE)
                if match:
                    return float(match.group(1).replace(',', '.'))
            
            description = self.get_element_text('[data-name="Description"]')
            if description:
                match = re.search(r'кухня\s*(\d+[,.]?\d*)\s*м²', description, re.IGNORECASE)
                if match:
                    return float(match.group(1).replace(',', '.'))
        except:
            pass
        return None
    
    def extract_rooms(self):
        """Извлечь количество комнат"""
        try:
            title = self.get_element_text('h1') or ''
            title_lower = title.lower()
            
            # Проверяем студию
            if 'студия' in title_lower:
                return 0
            
            # Ищем шаблоны типа "1-комн", "2 комн", "3 комнатная"
            match = re.search(r'(\d+)[-\s]*(?:комн|комнат)', title_lower)
            if match:
                return int(match.group(1))
            
            # Альтернативный поиск
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
    
    def extract_current_floor(self):
        """Извлечь текущий этаж"""
        try:
            # Ищем в характеристиках
            elements = self.driver.find_elements(By.CSS_SELECTOR, '[data-name="Features"]')
            for elem in elements:
                text = elem.text
                # Ищем "этаж"
                match = re.search(r'(\d+)\s*этаж', text)
                if match:
                    return int(match.group(1))
            
            # Ищем в общем тексте страницы
            page_text = self.driver.page_source
            match = re.search(r'"floor":\s*"(\d+)"', page_text)
            if match:
                return int(match.group(1))
        except:
            pass
        return None
    
    def extract_total_floors(self):
        """Извлечь общее количество этажей"""
        try:
            elements = self.driver.find_elements(By.CSS_SELECTOR, '[data-name="Features"]')
            for elem in elements:
                text = elem.text
                # Ищем паттерн "X из Y"
                match = re.search(r'из\s*(\d+)', text)
                if match:
                    return int(match.group(1))
        except:
            pass
        return None
    
    def extract_year_built(self):
        """Извлечь год постройки"""
        try:
            # Ищем в характеристиках
            elements = self.driver.find_elements(By.CSS_SELECTOR, '[data-name="Features"]')
            for elem in elements:
                text = elem.text
                match = re.search(r'Год постройки[:\s]*(\d{4})', text, re.IGNORECASE)
                if match:
                    return int(match.group(1))
            
            # Ищем в JSON данных на странице
            page_text = self.driver.page_source
            match = re.search(r'"year":\s*(\d{4})', page_text)
            if match:
                return int(match.group(1))
        except:
            pass
        return None
    
    def extract_building_type(self):
        """Извлечь тип дома"""
        try:
            elements = self.driver.find_elements(By.CSS_SELECTOR, '[data-name="Features"]')
            for elem in elements:
                text = elem.text.lower()
                if 'панельный' in text:
                    return 'панельный'
                elif 'кирпичный' in text:
                    return 'кирпичный'
                elif 'монолитный' in text:
                    return 'монолитный'
                elif 'блочный' in text:
                    return 'блочный'
                elif 'деревянный' in text:
                    return 'деревянный'
        except:
            pass
        return None
    
    def extract_property_type(self):
        """Извлечь тип недвижимости"""
        try:
            # Определяем по URL
            current_url = self.driver.current_url
            if 'newbuilding' in current_url:
                return 'новостройка'
            else:
                return 'вторичка'
        except:
            return 'вторичка'
    
    def get_description(self):
        """Получить описание"""
        try:
            # Получаем основное описание
            description = self.get_element_text('[data-name="Description"]')
            if description:
                # Ограничиваем длину и чистим текст
                clean_desc = description.strip()
                if len(clean_desc) > 2000:
                    clean_desc = clean_desc[:2000] + "..."
                return clean_desc
        except:
            pass
        return None
    
    def extract_seller_type(self):
        """Извлечь тип продавца"""
        try:
            # Ищем в информации о продавце
            elements = self.driver.find_elements(By.CSS_SELECTOR, '[data-name="Owner"]')
            for elem in elements:
                text = elem.text.lower()
                if 'собственник' in text or 'владелец' in text:
                    return 'собственник'
                elif 'агентство' in text or 'риелтор' in text:
                    return 'агентство'
                elif 'застройщик' in text:
                    return 'застройщик'
            
            # Ищем в описании
            description = self.get_description()
            if description:
                desc_lower = description.lower()
                if 'собственник' in desc_lower or 'владелец' in desc_lower:
                    return 'собственник'
                elif 'агентство' in desc_lower or 'риелтор' in desc_lower:
                    return 'агентство'
        except:
            pass
        return None
    
    def extract_district(self):
        """Извлечь район"""
        try:
            address = self.get_element_text('[data-name="GeoLabel"]')
            if address:
                # Пытаемся вытащить район
                parts = address.split(',')
                for part in parts:
                    if 'р-н' in part:
                        return part.strip().replace('р-н', '').strip()
                    elif 'район' in part:
                        return part.strip()
        except:
            pass
        return None
    
    def extract_metro_station(self):
        """Извлечь станцию метро"""
        try:
            # Ищем элемент метро
            elements = self.driver.find_elements(By.CSS_SELECTOR, '[data-name="UndergroundStation"]')
            if elements:
                return elements[0].text.strip()
            
            # Альтернативный поиск
            elements = self.driver.find_elements(By.CSS_SELECTOR, '[data-name="GeoLabel"]')
            for elem in elements:
                text = elem.text
                # Ищем названия станций метро
                stations = ['Академическая', 'Политехническая', 'Лесная', 'Выборгская', 'Площадь Ленина']
                for station in stations:
                    if station in text:
                        return station
        except:
            pass
        return None
    
    def extract_metro_time(self):
        """Извлечь время до метро"""
        try:
            elements = self.driver.find_elements(By.CSS_SELECTOR, '[data-name="UndergroundStation"]')
            for elem in elements:
                text = elem.text
                # Ищем время в минутах
                match = re.search(r'(\d+)\s*мин', text)
                if match:
                    return f"{match.group(1)} мин"
        except:
            pass
        return None
    
    def save_to_database(self, data):
        """Сохранить в базу"""
        if not data or not self.conn:
            return False
        
        try:
            self.cursor.execute(
                f"SELECT price FROM {self.table_name} WHERE cian_id = %s",
                (data['cian_id'],)
            )
            
            existing = self.cursor.fetchone()
            now = datetime.now()
            
            if existing:
                old_price = existing[0]
                new_price = data.get('price')
                
                if new_price and new_price != old_price:
                    print(f"   💱 Цена изменилась: {old_price:,} → {new_price:,} ₽")
                    self.save_to_price_history(data['cian_id'], new_price, now)
                
                update_sql = f"""
                    UPDATE {self.table_name} SET
                    url = %s, title = %s, address = %s, price = %s, price_per_m2 = %s,
                    old_price = %s, area_total = %s, area_living = %s, area_kitchen = %s,
                    floor_current = %s, floor_total = %s, rooms = %s, year_built = %s,
                    building_type = %s, property_type = %s, description = %s,
                    seller_type = %s, publication_date = %s, is_active = %s,
                    district = %s, metro_station = %s, metro_time = %s,
                    updated_at = %s, last_checked = %s
                    WHERE cian_id = %s
                """
                
                self.cursor.execute(update_sql, (
                    data['url'], data['title'], data['address'],
                    data['price'], data['price_per_m2'],
                    data['old_price'], data['area_total'], data['area_living'], data['area_kitchen'],
                    data['floor_current'], data['floor_total'], data['rooms'], data['year_built'],
                    data['building_type'], data['property_type'], data['description'],
                    data['seller_type'], data['publication_date'], data['is_active'],
                    data['district'], data['metro_station'], data['metro_time'],
                    now, now, data['cian_id']
                ))
                
                print(f"   📝 Обновлено в базе")
                
            else:
                insert_sql = f"""
                    INSERT INTO {self.table_name} 
                    (cian_id, url, title, address, price, price_per_m2, old_price,
                     area_total, area_living, area_kitchen, floor_current, floor_total, 
                     rooms, year_built, building_type, property_type, description,
                     seller_type, publication_date, is_active, district, metro_station,
                     metro_time, created_at, updated_at, last_checked)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                
                self.cursor.execute(insert_sql, (
                    data['cian_id'], data['url'], data['title'], data['address'],
                    data['price'], data['price_per_m2'], data['old_price'],
                    data['area_total'], data['area_living'], data['area_kitchen'],
                    data['floor_current'], data['floor_total'], data['rooms'], data['year_built'],
                    data['building_type'], data['property_type'], data['description'],
                    data['seller_type'], data['publication_date'], data['is_active'],
                    data['district'], data['metro_station'], data['metro_time'],
                    now, now, now
                ))
                
                print(f"   ✅ Сохранено в базу")
            
            self.conn.commit()
            return True
            
        except Exception as e:
            print(f"   ❌ Ошибка базы: {e}")
            import traceback
            traceback.print_exc()
            self.conn.rollback()
            return False
    
    def save_to_price_history(self, cian_id, price, timestamp):
        """Сохранить историю цен"""
        try:
            insert_sql = """
                INSERT INTO price_history (cian_id, price, date_recorded, change_type)
                VALUES (%s, %s, %s, %s)
            """
            
            self.cursor.execute(insert_sql, (cian_id, price, timestamp, 'update'))
        except:
            pass
    
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
            
            self.cursor.execute(f"SELECT COUNT(*) FROM {self.table_name}")
            total = self.cursor.fetchone()[0]
            print(f"Всего объявлений: {total}")
            
            self.cursor.execute(f"""
                SELECT property_type, COUNT(*) 
                FROM {self.table_name} 
                WHERE property_type IS NOT NULL
                GROUP BY property_type
            """)
            types = self.cursor.fetchall()
            print("\n🏠 По типам недвижимости:")
            for t in types:
                print(f"  {t[0]}: {t[1]}")
            
            self.cursor.execute(f"SELECT AVG(price) FROM {self.table_name} WHERE price > 0")
            avg_price = self.cursor.fetchone()[0]
            if avg_price:
                print(f"\n💰 Средняя цена: {avg_price:,.0f} ₽")
            
            self.cursor.execute(f"""
                SELECT cian_id, title, price, created_at 
                FROM {self.table_name} 
                ORDER BY created_at DESC 
                LIMIT 3
            """)
            recent = self.cursor.fetchall()
            print("\n📅 Последние добавленные:")
            for r in recent:
                title_short = r[1][:30] + "..." if r[1] and len(r[1]) > 30 else r[1]
                print(f"  ID: {r[0]}, {title_short}, Цена: {r[2]:,} ₽")
            
            print("="*50)
            
        except Exception as e:
            print(f"Ошибка статистики: {e}")
    
    def run_parsing(self):
        """Запуск парсинга"""
        urls = [
            "https://spb.cian.ru/cat.php?deal_type=sale&engine_version=2&offer_type=flat&region=2&room1=1",
            "https://spb.cian.ru/cat.php?deal_type=sale&engine_version=2&offer_type=flat&region=2&room2=1"
        ]
        
        all_data = []
        parsed_count = 0
        
        for url in urls[:1]:
            print(f"\n🌐 Парсим: {url}")
            
            offers = self.parse_search_page(url)
            
            if not offers:
                print("❌ Не найдено объявлений")
                continue
            
            for i, offer in enumerate(offers[:5]):
                print(f"\n[{i+1}/{min(5, len(offers))}]")
                
                data = self.parse_offer(offer)
                
                if data:
                    if self.conn:
                        self.save_to_database(data)
                    
                    all_data.append(data)
                    parsed_count += 1
                
                time.sleep(2)
        
        return all_data, parsed_count
    
    def run(self):
        """Запуск парсера"""
        print("="*60)
        print("ПАРСЕР ЦИАН ДЛЯ ТАБЛИЦЫ cian_offers")
        print("="*60)
        
        if not self.setup_browser():
            return
        
        use_db = input("\nСохранять в базу данных? (y/n): ").lower() == 'y'
        
        if use_db:
            if not self.setup_database():
                print("\n⚠️ Продолжаем без базы данных")
                use_db = False
        
        all_data, parsed_count = self.run_parsing()
        
        if all_data:
            self.save_to_file(all_data)
        
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
    try:
        import selenium
    except ImportError:
        print("Установите: pip install selenium")
        sys.exit(1)
    
    main()