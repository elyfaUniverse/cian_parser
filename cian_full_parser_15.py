# cian_parser_final.py
import time
import re
import json
import sys
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup

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
            
            # Создаем BeautifulSoup объект для продвинутого парсинга
            soup = BeautifulSoup(self.driver.page_source, 'html.parser')
            
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
                'year_built': self.extract_year_built_improved(soup),
                'building_type': self.extract_building_type_improved(soup),
                'property_type': self.extract_property_type(),
                'description': None,
                'seller_type': self.extract_seller_type(),
                'publication_date': datetime.now().strftime('%Y-%m-%d'),
                'is_active': True,
                'district': self.extract_district(),
                'metro_station': self.extract_metro_station_improved(soup),
                'metro_time': self.extract_metro_time_improved(soup),
                'last_checked': datetime.now()
            }
            
            # Выводим результат
            if data['title']:
                print(f"   📝 {data['title'][:50]}...")
            if data['price']:
                print(f"   💰 {data['price']:,} ₽")
            if data['address']:
                print(f"   📍 {data['address'][:40]}...")
            if data['building_type']:
                print(f"   🏠 Тип дома: {data['building_type']}")
            if data['property_type']:
                print(f"   🏢 Тип недвижимости: {data['property_type']}")
            if data['year_built']:
                print(f"   📅 Год постройки: {data['year_built']}")
            if data['metro_station']:
                metro_info = f"🚇 Метро: {data['metro_station']}"
                if data['metro_time']:
                    metro_info += f" ({data['metro_time']})"
                print(f"   {metro_info}")
            else:
                print(f"   🚇 Метро: не найдено (время: {data['metro_time'] if data['metro_time'] else 'нет'})")
            if data['floor_current']:
                print(f"   🏢 Этаж: {data['floor_current']}/{data['floor_total'] if data['floor_total'] else '?'}")
            
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
            
            # Альтернативный метод
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
            
            # Ищем в общем тексте
            page_text = self.driver.page_source
            match = re.search(r'"totalArea":\s*(\d+\.?\d*)', page_text)
            if match:
                return float(match.group(1))
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
                match = re.search(r'Жилая площадь[:\s]*(\d+[,.]?\d*)\s*м²', text, re.IGNORECASE)
                if match:
                    return float(match.group(1).replace(',', '.'))
            
            page_text = self.driver.page_source
            match = re.search(r'"livingArea":\s*(\d+\.?\d*)', page_text)
            if match:
                return float(match.group(1))
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
            
            page_text = self.driver.page_source
            match = re.search(r'"kitchenArea":\s*(\d+\.?\d*)', page_text)
            if match:
                return float(match.group(1))
        except:
            pass
        return None
    
    def extract_rooms(self):
        """Извлечь количество комнат"""
        try:
            title = self.get_element_text('h1') or ''
            title_lower = title.lower()
            
            if 'студия' in title_lower or 'апартамент' in title_lower:
                return 0
            
            match = re.search(r'(\d+)[-\s]*(?:комн|комнат)', title_lower)
            if match:
                return int(match.group(1))
            
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
            elements = self.driver.find_elements(By.CSS_SELECTOR, '[data-name="Features"]')
            for elem in elements:
                text = elem.text
                match = re.search(r'(\d+)\s*этаж', text)
                if match:
                    return int(match.group(1))
                
                match = re.search(r'(\d+)\s*/\s*(\d+)', text)
                if match and 'этаж' in text.lower():
                    return int(match.group(1))
            
            page_text = self.driver.page_source
            match = re.search(r'"floor":\s*"(\d+)"', page_text)
            if match:
                return int(match.group(1))
            
            match = re.search(r'"floorNumber":\s*(\d+)', page_text)
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
                match = re.search(r'из\s*(\d+)', text)
                if match:
                    return int(match.group(1))
                
                match = re.search(r'(\d+)\s*/\s*(\d+)', text)
                if match and 'этаж' in text.lower():
                    return int(match.group(2))
            
            page_text = self.driver.page_source
            match = re.search(r'"floorsCount":\s*(\d+)', page_text)
            if match:
                return int(match.group(1))
            
            match = re.search(r'"totalFloors":\s*(\d+)', page_text)
            if match:
                return int(match.group(1))
                
        except:
            pass
        return None
    
    def extract_year_built_improved(self, soup):
        """Извлечь год постройки - УЛУЧШЕННЫЙ МЕТОД"""
        try:
            # 1. Поиск в характеристиках (самый надежный способ)
            page_text = soup.get_text()
            
            # Паттерны для поиска года
            patterns = [
                r'Год постройки[:\s]*(\d{4})',
                r'Построен в[:\s]*(\d{4})',
                r'Сдан в[:\s]*(\d{4})',
                r'Дом\s+(\d{4})\s+года',
                r'(\d{4})\s+год\s+постройки',
                r'год[:\s]*(\d{4})',
                r'built.*?(\d{4})',
                r'construction.*?(\d{4})',
            ]
            
            for pattern in patterns:
                match = re.search(pattern, page_text, re.IGNORECASE)
                if match:
                    year = int(match.group(1))
                    if 1800 <= year <= datetime.now().year:
                        return year
            
            # 2. Поиск в JSON-LD данных
            json_ld = soup.find('script', type='application/ld+json')
            if json_ld and json_ld.string:
                try:
                    data = json.loads(json_ld.string)
                    # Пробуем разные возможные поля
                    for field in ['yearBuilt', 'dateBuilt', 'constructionDate', 'buildDate']:
                        if field in data:
                            year_str = str(data[field])
                            match = re.search(r'(\d{4})', year_str)
                            if match:
                                year = int(match.group(1))
                                if 1800 <= year <= datetime.now().year:
                                    return year
                except:
                    pass
            
            # 3. Поиск в скриптах с данными
            scripts = soup.find_all('script')
            for script in scripts:
                if script.string:
                    # Ищем год в различных форматах
                    script_patterns = [
                        r'"year":\s*"(\d{4})"',
                        r'"year":\s*(\d{4})',
                        r'"buildYear":\s*"(\d{4})"',
                        r'"buildYear":\s*(\d{4})',
                        r'"constructionYear":\s*"(\d{4})"',
                        r'"constructionYear":\s*(\d{4})',
                        r'"yearBuilt":\s*"(\d{4})"',
                        r'"yearBuilt":\s*(\d{4})',
                    ]
                    
                    for pattern in script_patterns:
                        match = re.search(pattern, script.string)
                        if match:
                            year = int(match.group(1))
                            if 1800 <= year <= datetime.now().year:
                                return year
            
            # 4. Поиск по серии дома (косвенный способ)
            # Определяем тип дома и примерный год по серии
            building_type = self.extract_building_type_improved(soup)
            if building_type:
                # Приблизительные годы для разных типов домов в СПб
                if building_type == 'хрущевский':
                    return 1960  # Примерно 1950-1970
                elif building_type == 'брежневский':
                    return 1975  # Примерно 1960-1980
                elif building_type == 'сталинский':
                    return 1950  # Примерно 1930-1955
                elif building_type == 'панельный':
                    # Для панельных домов в Академическом районе
                    if 'гражданский' in page_text.lower() or 'академическ' in page_text.lower():
                        return 1970  # Типичные годы для этого района
            
            # 5. Поиск в описании
            description = soup.find('div', {'data-name': 'Description'})
            if description:
                desc_text = description.get_text()
                match = re.search(r'(\d{4})\s+год', desc_text)
                if match:
                    year = int(match.group(1))
                    if 1800 <= year <= datetime.now().year:
                        return year
            
        except Exception as e:
            print(f"   ⚠️ Ошибка при парсинге года постройки: {e}")
        return None
    
    def extract_building_type_improved(self, soup):
        """Извлечь тип дома - ПРОСТОЙ И ЭФФЕКТИВНЫЙ МЕТОД"""
        try:
            # 1. Ищем в блоках характеристик (самый надежный способ)
            # На ЦИАН тип дома обычно в блоках с характеристиками
            feature_blocks = soup.find_all(['div', 'li'], class_=lambda x: x and any(
                word in str(x).lower() for word in ['feature', 'param', 'item', 'characteristic']
            ))
            
            for block in feature_blocks:
                text = block.get_text().lower()
                # Ищем фразу "Тип дома" или "Материал стен"
                if 'тип дома' in text or 'материал стен' in text or 'тип здания' in text:
                    # Извлекаем значение после двоеточия или тире
                    value = text.split(':')[-1].split('-')[-1].strip()
                    
                    # Определяем тип по ключевым словам
                    if any(word in value for word in ['панель', 'панельный']):
                        return 'панельный'
                    elif any(word in value for word in ['кирпич', 'кирпичный']):
                        return 'кирпичный'
                    elif any(word in value for word in ['монолит', 'монолитный']):
                        return 'монолитный'
                    elif any(word in value for word in ['блок', 'блочный']):
                        return 'блочный'
                    elif any(word in value for word in ['дерево', 'деревянный']):
                        return 'деревянный'
                    elif any(word in value for word in ['хрущ', 'хрущев']):
                        return 'хрущевский'
                    elif any(word in value for word in ['сталин']):
                        return 'сталинский'
                    elif any(word in value for word in ['брежнев']):
                        return 'брежневский'
            
            # 2. Ищем в структурированных данных (JSON-LD или скриптах)
            # Проверяем JSON-LD
            json_ld = soup.find('script', type='application/ld+json')
            if json_ld and json_ld.string:
                try:
                    data = json.loads(json_ld.string)
                    # Ищем информацию о здании
                    for key, value in data.items():
                        if isinstance(value, str) and 'дом' in value.lower():
                            value_lower = value.lower()
                            if 'панель' in value_lower:
                                return 'панельный'
                            elif 'кирпич' in value_lower:
                                return 'кирпичный'
                except:
                    pass
            
            # 3. Ищем по всему тексту ключевые слова
            page_text_lower = soup.get_text().lower()
            
            # Определяем по ключевым словам
            if 'панельный дом' in page_text_lower or 'панель' in page_text_lower:
                return 'панельный'
            elif 'кирпичный дом' in page_text_lower or 'кирпич' in page_text_lower:
                return 'кирпичный'
            elif 'монолитный дом' in page_text_lower or 'монолит' in page_text_lower:
                return 'монолитный'
            elif 'блочный дом' in page_text_lower or 'блочный' in page_text_lower:
                return 'блочный'
            elif 'хрущевка' in page_text_lower or 'хрущ' in page_text_lower:
                return 'хрущевский'
            elif 'сталинка' in page_text_lower or 'сталин' in page_text_lower:
                return 'сталинский'
            
            # 4. Если не нашли, проверяем год постройки
            year = self.extract_year_built_improved(soup)
            if year:
                if 1930 <= year <= 1955:
                    return 'сталинский'
                elif 1956 <= year <= 1970:
                    return 'хрущевский'
                elif 1971 <= year <= 1990:
                    return 'панельный'
                elif year > 1990:
                    return 'панельный'  # По умолчанию для современных домов
            
            return None  # Не нашли
            
        except Exception as e:
            print(f"   ⚠️ Ошибка при парсинге типа дома: {e}")
            return None

    def extract_property_type(self):
        """Извлечь тип недвижимости"""
        try:
            current_url = self.driver.current_url
            if 'newbuilding' in current_url:
                return 'новостройка'
            else:
                return 'вторичка'
        except:
            return 'вторичка'
    
    def extract_seller_type(self):
        """Извлечь тип продавца"""
        try:
            elements = self.driver.find_elements(By.CSS_SELECTOR, '[data-name="Owner"]')
            for elem in elements:
                text = elem.text.lower()
                if 'собственник' in text or 'владелец' in text:
                    return 'собственник'
                elif 'агентство' in text or 'риелтор' in text:
                    return 'агентство'
        except:
            pass
        return None
    
    def extract_district(self):
        """Извлечь район"""
        try:
            address = self.get_element_text('[data-name="GeoLabel"]') or self.get_element_text('[data-name="AddressContainer"]')
            if address:
                parts = address.split(',')
                for part in parts:
                    part = part.strip()
                    if 'р-н' in part:
                        return part.replace('р-н', '').strip()
                    elif 'район' in part:
                        return part.replace('район', '').strip()
                    
                if len(parts) > 1:
                    return parts[1].strip()
        except:
            pass
        return None
    
    def extract_metro_station_improved(self, soup):
        """Извлечь станцию метро - УПРОЩЕННЫЙ МЕТОД"""
        try:
            # 1. Ищем в специальных блоках ЦИАН
            metro_selectors = [
                '[data-name="UndergroundStation"]',
                '[data-name="GeoUnderground"]',
                '[class*="underground"]',
                '[class*="metro"]',
                'a[href*="underground"]',
                'a[href*="metro"]',
            ]
            
            for selector in metro_selectors:
                try:
                    elements = soup.select(selector)
                    for elem in elements:
                        text = elem.get_text(strip=True)
                        if text and 'метро' in text.lower():
                            # Очищаем от лишнего
                            station = re.sub(r'\([^)]+\)', '', text)  # Убираем скобки с временем
                            station = station.replace('метро', '').replace('м.', '').strip()
                            station = re.sub(r'\d+\s*мин', '', station)  # Убираем время
                            station = station.strip()
                            
                            if station and 2 < len(station) < 30:
                                return station
                except:
                    continue
            
            # 2. Ищем по списку станций СПб
            stations_spb = [
                'Академическая', 'Гражданский проспект', 'Девяткино',
                'Политехническая', 'Площадь Мужества', 'Лесная',
                'Выборгская', 'Площадь Ленина', 'Чернышевская',
                'Площадь Восстания', 'Владимирская', 'Пушкинская',
                'Технологический институт', 'Балтийская', 'Нарвская',
                'Кировский завод', 'Автово', 'Ленинский проспект',
                'Проспект Ветеранов', 'Парк Победы', 'Электросила',
                'Московская', 'Звёздная', 'Купчино',
            ]
            
            page_text = soup.get_text()
            for station in stations_spb:
                if station in page_text:
                    # Проверяем контекст
                    start = max(0, page_text.find(station) - 50)
                    end = min(len(page_text), page_text.find(station) + 50)
                    context = page_text[start:end].lower()
                    
                    if any(word in context for word in ['метро', 'станция', 'м.', 'ст.']):
                        return station
            
            return None
            
        except Exception as e:
            print(f"   ⚠️ Ошибка при парсинге метро: {e}")
            return None

    def extract_metro_time_improved(self, soup):
        """Извлечь время до метро - УЛУЧШЕННЫЙ МЕТОД"""
        try:
            page_text = soup.get_text()
            
            # 1. Ищем время рядом с упоминанием метро
            metro_positions = []
            for match in re.finditer(r'метро|станция', page_text, re.IGNORECASE):
                metro_positions.append(match.start())
            
            for pos in metro_positions:
                start = max(0, pos - 100)
                end = min(len(page_text), pos + 100)
                context = page_text[start:end]
                
                time_patterns = [
                    r'(\d+)\s*мин(?:ут)?\.?',
                    r'\((\d+)\s*мин(?:ут)?\.?\)',
                    r'(\d+)\s*минут',
                ]
                
                for pattern in time_patterns:
                    match = re.search(pattern, context, re.IGNORECASE)
                    if match:
                        time_str = match.group(1)
                        if time_str.isdigit():
                            time_val = int(time_str)
                            if 1 <= time_val <= 120:
                                return f"{time_val} мин"
            
            # 2. Ищем в специальных элементах ЦИАН
            time_selectors = [
                '[data-name="UndergroundTime"]',
                '[class*="underground-time"]',
                '[class*="metro-time"]',
                '[class*="walk-time"]',
                '[data-name="TransportTime"]',
            ]
            
            for selector in time_selectors:
                try:
                    elements = soup.select(selector)
                    for elem in elements:
                        text = elem.get_text(strip=True)
                        # Извлекаем только цифры
                        match = re.search(r'(\d+)', text)
                        if match:
                            time_val = int(match.group(1))
                            if 1 <= time_val <= 120:
                                return f"{time_val} мин"
                except:
                    continue
            
            # 3. Ищем рядом с названием станции метро
            if self.extract_metro_station_improved(soup):
                # Ищем в контексте станции метро
                station_pos = page_text.find(self.extract_metro_station_improved(soup))
                if station_pos != -1:
                    # Смотрим текст вокруг станции (100 символов в обе стороны)
                    start = max(0, station_pos - 100)
                    end = min(len(page_text), station_pos + 100)
                    context = page_text[start:end]
                    
                    # Ищем время в этом контексте
                    for pattern in time_patterns:
                        match = re.search(pattern, context, re.IGNORECASE)
                        if match:
                            time_str = match.group(1)
                            if time_str.isdigit():
                                time_val = int(time_str)
                                if 1 <= time_val <= 120:
                                    return f"{time_val} мин"
            
            return None
            
        except Exception as e:
            print(f"   ⚠️ Ошибка при парсинге времени до метро: {e}")
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
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
            
            # Статистика по типам домов
            self.cursor.execute(f"""
                SELECT building_type, COUNT(*) 
                FROM {self.table_name} 
                WHERE building_type IS NOT NULL AND building_type != ''
                GROUP BY building_type
                ORDER BY COUNT(*) DESC
            """)
            building_types = self.cursor.fetchall()
            print("\n🏠 Типы домов (найдено/всего):")
            if building_types:
                found_count = sum(bt[1] for bt in building_types)
                print(f"  С типом дома: {found_count} из {total}")
                for bt in building_types:
                    print(f"    {bt[0]}: {bt[1]}")
            else:
                print("  Типы домов не найдены ни в одном объявлении")
            
            # Статистика по категориям недвижимости
            self.cursor.execute(f"""
                SELECT property_type, COUNT(*) 
                FROM {self.table_name} 
                WHERE property_type IS NOT NULL AND property_type != ''
                GROUP BY property_type
                ORDER BY COUNT(*) DESC
            """)
            categories = self.cursor.fetchall()
            print("\n🏢 Категории недвижимости:")
            if categories:
                for cat in categories:
                    print(f"    {cat[0]}: {cat[1]}")
            
            # Статистика по годам постройки
            self.cursor.execute(f"""
                SELECT COUNT(*) FROM {self.table_name} 
                WHERE year_built IS NOT NULL
            """)
            with_year = self.cursor.fetchone()[0]
            print(f"\n📅 С годом постройки: {with_year} из {total}")
            
            # Статистика по метро
            print("\n🚇 Информация о метро:")
            self.cursor.execute(f"""
                SELECT COUNT(*) FROM {self.table_name} 
                WHERE metro_station IS NOT NULL AND metro_station != ''
            """)
            with_metro = self.cursor.fetchone()[0]
            print(f"  Со станцией метро: {with_metro} из {total}")
            
            self.cursor.execute(f"""
                SELECT COUNT(*) FROM {self.table_name} 
                WHERE metro_time IS NOT NULL AND metro_time != ''
            """)
            with_metro_time = self.cursor.fetchone()[0]
            print(f"  С временем до метро: {with_metro_time} из {total}")
            
            # Примеры с метро
            print("\n📋 Примеры объявлений с метро:")
            self.cursor.execute(f"""
                SELECT cian_id, metro_station, metro_time, building_type, property_type, year_built
                FROM {self.table_name} 
                WHERE metro_station IS NOT NULL AND metro_station != ''
                ORDER BY created_at DESC 
                LIMIT 5
            """)
            examples = self.cursor.fetchall()
            for ex in examples:
                metro_info = f"Метро: {ex[1]}"
                if ex[2]:
                    metro_info += f" ({ex[2]})"
                building_info = f", Тип дома: {ex[3]}" if ex[3] else ""
                category_info = f", Категория: {ex[4]}" if ex[4] else ""
                year_info = f", Год: {ex[5]}" if ex[5] else ""
                print(f"  ID: {ex[0]}, {metro_info}{building_info}{category_info}{year_info}")
            
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
            
            for i, offer in enumerate(offers[:3]):  # Парсим только 3 для теста
                print(f"\n[{i+1}/{min(3, len(offers))}]")
                
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
            print(f"   SELECT cian_id, building_type, property_type, year_built, metro_station, metro_time FROM {self.table_name} ORDER BY created_at DESC;")
        
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
        import bs4
    except ImportError:
        print("Установите зависимости: pip install selenium beautifulsoup4")
        sys.exit(1)
    
    main()