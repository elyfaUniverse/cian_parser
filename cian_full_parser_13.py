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
                'property_category': self.extract_property_category_improved(soup),
                'property_type': self.extract_property_type(),
                'description': None,
                'seller_type': self.extract_seller_type(),
                'publication_date': datetime.now().strftime('%Y-%m-%d'),
                'is_active': True,
                'district': self.extract_district(),
                'metro_station': self.extract_metro_station_improved(soup),  # Используем улучшенный метод
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
            if data['property_category']:
                print(f"   🏢 Категория: {data['property_category']}")
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
        """Извлечь тип дома - СУПЕР УЛУЧШЕННЫЙ МЕТОД"""
        try:
            page_text = soup.get_text()
            page_text_lower = page_text.lower()
            
            # 1. Сначала проверяем серии домов - это самый надежный способ
            series_patterns = {
                'панельный': [
                    r'серия\s*[:\s]*(П|П-|ПА|ПАНЕЛЬ|ПАН)',
                    r'(П-?\d+)',  # П-44, П-3, П-55, П-46, П-30, П-43
                    r'(1-?[ЛГ]-?606)', r'(1-?511)', r'(1-?515)', r'(1-?528)',
                    r'(И-?209А)', r'(И-?155)', r'(И-?1723)',
                    r'(К-?7)', r'(КТ)',  # Коттеджные серии
                    r'(464)', r'(467)', r'(600.11)',
                    r'(ЛОД)', r'(ДОК)',  # Ленинградские
                    r'(ГОС)', r'(ДОК)',  # Государственные
                    r'(МОП)', r'(ОП)',  # Московские
                    r'(хрущевк|хрущ)',  # Хрущевки (обычно панельные)
                    r'1ЛГ-?606', r'1-?ЛГ-?606',  # Конкретная серия для Академического
                ],
                'кирпичный': [
                    r'серия\s*[:\s]*(К|КИРП|КИРПИЧ|КИР)',
                    r'(К-?\d+)',
                    r'(1-?510)', r'(1-?335)', r'(1-?447)',
                    r'(Тишинск)', r'(Смирновск)', r'(Сталинск)',  # Сталинки обычно кирпичные
                    r'(Царскосельск)',
                ],
                'монолитный': [
                    r'серия\s*[:\s]*(М|МОНОЛИТ|МОН)',
                    r'(М-?\d+)',
                    r'(И-?155)', r'(И-?1723)',  # Некоторые И-серии
                    r'(П-?44Т)', r'(П-?3М)',  # Модернизированные панельные
                    r'(индивидуальн)',  # Индивидуальные проекты
                ],
                'блочный': [
                    r'серия\s*[:\s]*(Б|БЛОК|БЛ)',
                    r'(Б-?\d+)',
                    r'(1-?480)', r'(1-?600)', r'(1-?606)',
                    r'(ЛенЗНИИЭП)', r'(СПб)',  # Ленинградские блоки
                ],
                'хрущевский': [
                    r'хрущевк', r'хрущ', r'хрущёв',
                    r'1-?511', r'1-?515', r'К-?7',
                ],
                'сталинский': [
                    r'сталинк', r'сталин', r'сталинск',
                ],
                'брежневский': [
                    r'брежневк', r'брежнев', r'брежневск',
                ]
            }
            
            # Проверяем серии (регистронезависимый поиск)
            for building_type, patterns in series_patterns.items():
                for pattern in patterns:
                    if re.search(pattern, page_text, re.IGNORECASE):
                        return building_type
            
            # 2. Ищем прямые упоминания в тексте
            direct_keywords = {
                'панельный': ['панельный', 'панельн', 'панель'],
                'кирпичный': ['кирпичный', 'кирпичн', 'кирпич'],
                'монолитный': ['монолитный', 'монолитн', 'монолит'],
                'блочный': ['блочный', 'блочн', 'блок'],
                'деревянный': ['деревянный', 'деревянн', 'дерево'],
                'хрущевский': ['хрущевск', 'хрущ', 'хрущёв'],
                'сталинский': ['сталинск', 'сталин'],
                'брежневский': ['брежневск', 'брежнев'],
            }
            
            for building_type, keywords in direct_keywords.items():
                for keyword in keywords:
                    if keyword in page_text_lower:
                        return building_type
            
            # 3. Ищем в характеристиках
            features_selectors = [
                '[data-name="Features"]',
                '[data-name="ObjectSummary"]',
                '.a10a3f92e9--container--2F3KV',
                '.a10a3f92e9--item--3dG_0',
            ]
            
            for selector in features_selectors:
                elements = soup.select(selector)
                for elem in elements:
                    text = elem.get_text().lower()
                    lines = text.split('\n')
                    for line in lines:
                        line = line.strip()
                        if 'тип дома' in line or 'тип здания' in line or 'материал стен' in line:
                            if 'панель' in line:
                                return 'панельный'
                            elif 'кирпич' in line:
                                return 'кирпичный'
                            elif 'монолит' in line:
                                return 'монолитный'
                            elif 'блок' in line:
                                return 'блочный'
                            elif 'дерево' in line:
                                return 'деревянный'
                            elif 'сталин' in line:
                                return 'сталинский'
                            elif 'хрущ' in line:
                                return 'хрущевский'
                            elif 'брежнев' in line:
                                return 'брежневский'
            
            # 4. Анализ адреса и района
            address = self.get_element_text('[data-name="GeoLabel"]') or ''
            address_lower = address.lower()
            
            # Для известных районов определяем типичный тип домов
            if 'гражданский' in address_lower or 'академическ' in address_lower:
                # Академический район - в основном панельные дома серии 1ЛГ-606
                return 'панельный'
            elif any(word in address_lower for word in ['невский', 'литейный', 'садовая', 'васильевский']):
                # Исторический центр - чаще кирпичные
                return 'кирпичный'
            elif any(word in address_lower for word in ['купчино', 'проспект славы', 'московская']):
                # Спальные районы - много панельных
                return 'панельный'
            
            # 5. Анализ по году постройки (если удалось найти)
            year = self.extract_year_built_improved(soup)
            if year:
                if 1956 <= year <= 1985:
                    # Хрущевки и брежневки - в основном панельные
                    if 1956 <= year <= 1965:
                        return 'хрущевский'
                    else:
                        return 'панельный'
                elif 1930 <= year <= 1955:
                    # Сталинки - обычно кирпичные
                    return 'сталинский'
                elif year >= 1990:
                    # Современные дома - чаще панельные или монолитные
                    # Для СПб после 2000 много панельных
                    return 'панельный'
            
            # 6. По умолчанию для СПб - панельный (самый распространенный)
            return 'панельный'
            
        except Exception as e:
            print(f"   ⚠️ Ошибка при парсинге типа дома: {e}")
            return None
    
    def extract_property_category_improved(self, soup):
        """Определить категорию недвижимости: вторичка или новостройка - УЛУЧШЕННЫЙ МЕТОД"""
        try:
            page_text = soup.get_text()
            page_text_lower = page_text.lower()
            
            # 1. Проверяем год сдачи (самый надежный признак)
            year_patterns = [
                r'сдача.*?(\d{4})',
                r'год.*?сдачи.*?(\d{4})',
                r'(\d{4})\s*год.*?сдачи',
                r'готовность.*?(\d{4})',
                r'заселение.*?(\d{4})',
                r'квартира.*?сдается.*?(\d{4})',
                r'построен.*?(\d{4})',
                r'дом.*?(\d{4}).*?года',
            ]
            
            current_year = datetime.now().year
            
            for pattern in year_patterns:
                match = re.search(pattern, page_text, re.IGNORECASE)
                if match:
                    year = int(match.group(1))
                    # Если год сдачи в будущем - это новостройка
                    if year > current_year:
                        return 'новостройка'
                    # Если год сдачи очень недавний (последние 2 года) - тоже новостройка
                    elif year >= current_year - 1:
                        # Но проверяем другие признаки
                        if 'вторич' not in page_text_lower:
                            return 'новостройка'
                    # Если год сдачи в прошлом - скорее всего вторичка
                    else:
                        return 'вторичка'
            
            # 2. Считаем ключевые слова
            new_building_keywords = [
                'новостройка', 'новый дом', 'новый комплекс', 'стройка',
                'сдается в', 'готовность', 'заселение с', 'срок сдачи',
                'первичка', 'первичный рынок', 'от застройщика',
                'квартира с отделкой', 'с чистовой отделкой',
                'в только что построенном доме', 'в новом доме',
                'отделка от застройщика', 'с ремонтом от застройщика',
                'первичная продажа', 'прямая продажа от застройщика'
            ]
            
            old_building_keywords = [
                'вторичка', 'вторичный рынок', 'вторичное жилье',
                'хрущевка', 'сталинка', 'брежневка',
                'во вторичном фонде', 'ранее не проживал',
                'ранее проживали', 'бывшее жилье',
                'в доме советской постройки', 'дом старого фонда'
            ]
            
            new_count = sum(1 for keyword in new_building_keywords if keyword in page_text_lower)
            old_count = sum(1 for keyword in old_building_keywords if keyword in page_text_lower)
            
            if new_count > old_count:
                return 'новостройка'
            elif old_count > new_count:
                return 'вторичка'
            
            # 3. Проверяем год постройки дома
            year_built = self.extract_year_built_improved(soup)
            if year_built:
                if year_built >= current_year - 3:  # Дома последних 3 лет
                    return 'новостройка'
                else:
                    return 'вторичка'
            
            # 4. Анализ URL
            current_url = self.driver.current_url if self.driver else ""
            if 'newbuilding' in current_url.lower():
                return 'новостройка'
            
            # 5. Проверяем наличие информации о застройщике
            if any(word in page_text_lower for word in ['застройщик', 'стройкомпания', 'девелопер']):
                return 'новостройка'
            
            # 6. Проверяем тип дома (косвенный признак)
            building_type = self.extract_building_type_improved(soup)
            if building_type in ['хрущевский', 'сталинский', 'брежневский']:
                return 'вторичка'
            
            # 7. По умолчанию для объявлений о продаже - вторичка
            return 'вторичка'
            
        except Exception as e:
            print(f"   ⚠️ Ошибка определения категории: {e}")
            return 'вторичка'
    
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
        """Извлечь станцию метро - УЛУЧШЕННЫЙ МЕТОД"""
        try:
            # 1. Ищем в элементах ЦИАН с данными о метро
            metro_selectors = [
                'a[href*="underground"]',
                '[data-name="UndergroundStation"]',
                '[data-name="GeoUnderground"]',
                '[class*="underground"]',
                '[class*="metro"]',
                '.underground-item',
                '.metro-item',
            ]
            
            for selector in metro_selectors:
                elements = soup.select(selector)
                for elem in elements:
                    text = elem.get_text(strip=True)
                    if text and len(text) < 50 and 'метро' in text.lower():
                        # Извлекаем название станции
                        station = text.replace('метро', '').replace('м.', '').strip()
                        # Убираем время в скобках
                        station = re.sub(r'\s*\([^)]+\)', '', station).strip()
                        if station:
                            return station
            
            # 2. Ищем в JSON-LD
            json_ld = soup.find('script', type='application/ld+json')
            if json_ld and json_ld.string:
                try:
                    data = json.loads(json_ld.string)
                    if 'description' in data:
                        desc = data['description']
                        # Список станций метро СПб
                        stations_spb = [
                            'Академическая', 'Гражданский проспект', 'Девяткино',
                            'Политехническая', 'Площадь Мужества', 'Лесная',
                            'Выборгская', 'Площадь Ленина', 'Чернышевская',
                            'Площадь Восстания', 'Владимирская', 'Пушкинская',
                            'Технологический институт', 'Балтийская', 'Нарвская',
                            'Кировский завод', 'Автово', 'Ленинский проспект',
                            'Проспект Ветеранов', 'Парк Победы', 'Электросила',
                            'Московская', 'Звёздная', 'Купчино', 'Ладожская',
                            'Проспект Большевиков', 'Улица Дыбенко'
                        ]
                        
                        for station in stations_spb:
                            if station in desc:
                                return station
                except:
                    pass
            
            # 3. Ищем по тексту страницы
            page_text = soup.get_text()
            
            # Список станций для поиска
            common_stations = [
                'Академическая', 'Гражданский проспект', 'Политехническая',
                'Лесная', 'Выборгская', 'Площадь Ленина', 'Чернышевская',
                'Площадь Восстания', 'Владимирская', 'Пушкинская',
                'Технологический институт', 'Балтийская', 'Нарвская',
                'Кировский завод', 'Автово'
            ]
            
            for station in common_stations:
                if station in page_text:
                    # Проверяем, что это именно станция метро, а не часть адреса
                    context = page_text[page_text.find(station)-50:page_text.find(station)+50]
                    if 'метро' in context.lower() or 'станция' in context.lower():
                        return station
            
            # 4. Ищем в блоке "Ближайшее метро"
            for elem in soup.find_all(['div', 'p', 'span']):
                text = elem.get_text(strip=True)
                if 'ближайшее метро' in text.lower() or 'станция метро' in text.lower():
                    # Извлекаем название из следующего элемента
                    next_elem = elem.find_next()
                    if next_elem:
                        station_text = next_elem.get_text(strip=True)
                        # Убираем время и лишние слова
                        station = re.sub(r'\s*\([^)]+\)', '', station_text)
                        station = station.replace('метро', '').strip()
                        if station and len(station) < 30:
                            return station
            
        except Exception as e:
            print(f"   ⚠️ Ошибка при парсинге метро: {e}")
        return None
    
    def extract_metro_station_improved(self, soup):
        """Извлечь станцию метро - УЛУЧШЕННЫЙ МЕТОД"""
        try:
            page_text = soup.get_text()
            
            # 1. Сначала ищем в специальных элементах ЦИАН
            metro_selectors = [
                'a[href*="underground"]',
                '[data-name="UndergroundStation"]',
                '[data-name="GeoUnderground"]',
                '[class*="underground"]',
                '[class*="metro"]',
                '.underground-item',
                '.metro-item',
                '[data-name="UndergroundGeneralInfo"]',
                '[data-name="TransportInfo"]',
            ]
            
            for selector in metro_selectors:
                try:
                    elements = soup.select(selector)
                    for elem in elements:
                        text = elem.get_text(strip=True)
                        if text and len(text) < 50:
                            # Убираем время до метро
                            station = re.sub(r'\(\d+\s*мин\)', '', text)
                            station = re.sub(r'\d+\s*мин', '', station)
                            station = re.sub(r'\d+', '', station)  # Убираем цифры
                            station = station.replace('метро', '').replace('м.', '').replace('ст.', '').strip()
                            # Убираем лишние символы
                            station = re.sub(r'[^\w\s\-]', '', station).strip()
                            
                            if station and len(station) > 2 and len(station) < 30:
                                # Проверяем, что это не пустая строка и не только время
                                if not re.match(r'^\d+.*мин', station):
                                    return station
                except:
                    continue
            
            # 2. Ищем станции метро в тексте по известным названиям
            # Список станций метро СПб (все линии)
            stations_spb = [
                # Линия 1 (Кировско-Выборгская)
                'Девяткино', 'Гражданский проспект', 'Академическая',
                'Политехническая', 'Площадь Мужества', 'Лесная',
                'Выборгская', 'Площадь Ленина', 'Чернышевская',
                'Площадь Восстания', 'Владимирская', 'Пушкинская',
                'Технологический институт', 'Балтийская', 'Нарвская',
                'Кировский завод', 'Автово', 'Ленинский проспект',
                'Проспект Ветеранов', 'Парк Победы', 'Электросила',
                'Московская', 'Звёздная', 'Купчино',
                
                # Линия 2 (Московско-Петроградская)
                'Парнас', 'Проспект Просвещения', 'Озерки',
                'Удельная', 'Пионерская', 'Чёрная речка',
                'Петроградская', 'Горьковская', 'Невский проспект',
                'Сенная площадь', 'Технологический институт',
                'Фрунзенская', 'Московские ворота', 'Электросила',
                'Парк Победы', 'Московская', 'Звёздная', 'Купчино',
                
                # Линия 3 (Невско-Василеостровская)
                'Беговая', 'Зенит', 'Приморская', 'Василеостровская',
                'Гостиный двор', 'Маяковская', 'Площадь Александра Невского',
                'Елизаровская', 'Ломоносовская', 'Пролетарская',
                'Обухово', 'Рыбацкое',
                
                # Линия 4 (Правобережная)
                'Спасская', 'Достоевская', 'Лиговский проспект',
                'Площадь Александра Невского', 'Новочеркасская',
                'Ладожская', 'Проспект Большевиков', 'Улица Дыбенко',
                
                # Линия 5 (Фрунзенско-Приморская)
                'Комендантский проспект', 'Старая Деревня', 'Крестовский остров',
                'Чкаловская', 'Спортивная', 'Адмиралтейская',
                'Садовая', 'Звенигородская', 'Обводный канал',
                'Волковская', 'Бухарестская', 'Международная',
                
                # Новые станции и планируемые
                'Горный институт', 'Театральная', 'Шушары'
            ]
            
            # Ищем названия станций в тексте
            for station in stations_spb:
                if station in page_text:
                    # Проверяем контекст, чтобы убедиться что это именно станция метро
                    context_start = page_text.find(station) - 50
                    context_end = page_text.find(station) + 50
                    if context_start < 0:
                        context_start = 0
                    if context_end > len(page_text):
                        context_end = len(page_text)
                    
                    context = page_text[context_start:context_end].lower()
                    # Если в контексте есть слова связанные с метро
                    if any(word in context for word in ['метро', 'станция', 'ближайшее', 'м.', 'ст.']):
                        return station
            
            # 3. Ищем в блоке с информацией о транспорте
            transport_blocks = soup.find_all(['div', 'section'], class_=lambda x: x and any(
                word in str(x).lower() for word in ['transport', 'commute', 'infrastructure', 'location']
            ))
            
            for block in transport_blocks:
                text = block.get_text()
                # Ищем упоминания метро
                if 'метро' in text.lower():
                    lines = text.split('\n')
                    for line in lines:
                        line = line.strip()
                        # Ищем название станции в строке
                        for station in stations_spb:
                            if station in line:
                                # Очищаем от времени и лишнего
                                clean_line = re.sub(r'\(\d+\s*мин\)', '', line)
                                clean_line = re.sub(r'\d+\s*мин', '', clean_line)
                                if station in clean_line:
                                    return station
            
            # 4. Ищем в JSON-LD данных (если есть)
            json_ld = soup.find('script', type='application/ld+json')
            if json_ld and json_ld.string:
                try:
                    data = json.loads(json_ld.string)
                    # Проверяем разные поля
                    for field in ['description', 'name', 'address']:
                        if field in data:
                            field_text = str(data[field])
                            for station in stations_spb:
                                if station in field_text:
                                    return station
                except:
                    pass
            
            # 5. Ищем по адресу (косвенный метод)
            address = self.extract_district() or ''
            address_lower = str(address).lower()
            
            # Маппинг районов на ближайшие станции
            district_station_map = {
                'фрунзенский': ['Купчино', 'Звёздная', 'Московская', 'Электросила', 'Парк Победы', 'Фрунзенская'],
                'калининский': ['Академическая', 'Гражданский проспект', 'Политехническая'],
                'выборгский': ['Озерки', 'Проспект Просвещения', 'Парнас', 'Удельная'],
                'приморский': ['Комендантский проспект', 'Старая Деревня', 'Чёрная речка'],
                'петроградский': ['Петроградская', 'Горьковская', 'Чкаловская'],
                'василеостровский': ['Василеостровская', 'Приморская'],
                'центральный': ['Невский проспект', 'Гостиный двор', 'Площадь Восстания'],
                'адмиралтейский': ['Адмиралтейская', 'Сенная площадь', 'Садовая'],
                'московский': ['Московская', 'Звёздная', 'Купчино'],
                'невский': ['Пролетарская', 'Обухово', 'Рыбацкое', 'Улица Дыбенко'],
            }
            
            # Определяем район и ищем станции для него
            for district, stations in district_station_map.items():
                if district in address_lower:
                    # Проверяем, есть ли какая-то из этих станций на странице
                    for station in stations:
                        if station in page_text:
                            return station
            
            # 6. Ищем рядом с упоминанием времени до метро
            if self.extract_metro_time_improved(soup):
                # Если нашли время до метро, ищем название рядом
                time_patterns = [
                    r'(\d+)\s*мин',
                    r'\((\d+)\s*мин\)',
                ]
                
                for pattern in time_patterns:
                    matches = list(re.finditer(pattern, page_text))
                    for match in matches:
                        # Ищем текст перед временем (50 символов)
                        start = max(0, match.start() - 100)
                        context = page_text[start:match.start()]
                        
                        # Ищем названия станций в этом контексте
                        for station in stations_spb:
                            if station in context:
                                return station
            
        except Exception as e:
            print(f"   ⚠️ Ошибка при парсинге метро: {e}")
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
                    building_type = %s, property_category = %s, property_type = %s, description = %s,
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
                    data['building_type'], data['property_category'], data['property_type'], data['description'],
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
                     rooms, year_built, building_type, property_category, property_type, description,
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
                    data['building_type'], data['property_category'], data['property_type'], data['description'],
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
                SELECT property_category, COUNT(*) 
                FROM {self.table_name} 
                WHERE property_category IS NOT NULL AND property_category != ''
                GROUP BY property_category
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
                SELECT cian_id, metro_station, metro_time, building_type, property_category, year_built
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
            print(f"   SELECT cian_id, building_type, property_category, year_built, metro_station, metro_time FROM {self.table_name} ORDER BY created_at DESC;")
        
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