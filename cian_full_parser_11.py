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
            
            # ДЕБАГ: Сохраняем HTML страницы для анализа
            with open(f"debug_{offer['id']}.html", "w", encoding="utf-8") as f:
                f.write(self.driver.page_source)
            print(f"   💾 Сохранен HTML для отладки: debug_{offer['id']}.html")
            
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
                'year_built': self.extract_year_built(),
                'building_type': self.extract_building_type(soup),  # Используем улучшенный метод с BeautifulSoup
                'property_category': self.extract_property_category(soup),  # Новый метод для категории
                'property_type': self.extract_property_type(),
                'description': None,
                'seller_type': self.extract_seller_type(),
                'publication_date': datetime.now().strftime('%Y-%m-%d'),
                'is_active': True,
                'district': self.extract_district(),
                'metro_station': self.extract_metro_station(soup),  # Улучшенный метод
                'metro_time': self.extract_metro_time(soup),  # Улучшенный метод
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
            else:
                print(f"   🏠 Тип дома: не найден")
            if data['property_category']:
                print(f"   🏢 Категория: {data['property_category']}")
            if data['metro_station']:
                metro_info = f"🚇 Метро: {data['metro_station']}"
                if data['metro_time']:
                    metro_info += f" ({data['metro_time']})"
                else:
                    metro_info += f" (время не найдено)"
                print(f"   {metro_info}")
            else:
                print(f"   🚇 Метро: не найдено")
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
    
    def extract_year_built(self):
        """Извлечь год постройки"""
        try:
            elements = self.driver.find_elements(By.CSS_SELECTOR, '[data-name="Features"]')
            for elem in elements:
                text = elem.text
                match = re.search(r'Год постройки[:\s]*(\d{4})', text, re.IGNORECASE)
                if match:
                    return int(match.group(1))
            
            page_text = self.driver.page_source
            match = re.search(r'"year":\s*(\d{4})', page_text)
            if match:
                return int(match.group(1))
        except:
            pass
        return None
    
    def extract_building_type(self, soup):
        """Извлечь тип дома - УЛУЧШЕННЫЙ МЕТОД с BeautifulSoup"""
        try:
            # 1. Поиск в JSON-LD данных
            json_ld = soup.find('script', type='application/ld+json')
            if json_ld and json_ld.string:
                try:
                    data = json.loads(json_ld.string)
                    if 'description' in data:
                        desc = data['description'].lower()
                        if 'панельный' in desc or 'панель' in desc:
                            return 'панельный'
                        elif 'кирпичный' in desc or 'кирпич' in desc:
                            return 'кирпичный'
                        elif 'монолитный' in desc or 'монолит' in desc:
                            return 'монолитный'
                        elif 'блочный' in desc or 'блок' in desc:
                            return 'блочный'
                except:
                    pass
            
            # 2. Поиск в скриптах с JSON
            scripts = soup.find_all('script')
            for script in scripts:
                if script.string:
                    # Ищем houseType в JSON
                    match = re.search(r'"houseType":\s*"([^"]+)"', script.string, re.IGNORECASE)
                    if match:
                        house_type = match.group(1).lower()
                        if 'panel' in house_type or 'панель' in house_type:
                            return 'панельный'
                        elif 'brick' in house_type or 'кирпич' in house_type:
                            return 'кирпичный'
                        elif 'monolithic' in house_type or 'монолит' in house_type:
                            return 'монолитный'
                        elif 'block' in house_type or 'блок' in house_type:
                            return 'блочный'
                        elif 'wood' in house_type or 'дерево' in house_type:
                            return 'деревянный'
            
            # 3. Поиск в тексте страницы
            page_text = soup.get_text().lower()
            
            # Сначала ищем в характеристиках
            features_sections = soup.find_all(['div', 'section'], class_=lambda x: x and any(keyword in str(x) for keyword in ['features', 'characteristics', 'specs']))
            
            for section in features_sections:
                section_text = section.get_text().lower()
                lines = section_text.split('\n')
                for line in lines:
                    if 'тип дома' in line or 'тип здания' in line:
                        if 'панельный' in line:
                            return 'панельный'
                        elif 'кирпичный' in line:
                            return 'кирпичный'
                        elif 'монолитный' in line:
                            return 'монолитный'
                        elif 'блочный' in line:
                            return 'блочный'
                        elif 'деревянный' in line:
                            return 'деревянный'
            
            # 4. Поиск по серии дома
            series_patterns = [
                r'серия\s*[:\s]*([^\s,]+)',
                r'([А-Яа-я]\s*[-—]?\s*\d+\s*[А-Яа-я]?)',
                r'(П\s*[-—]?\s*\d+)',
                r'(И\s*[-—]?\s*\d+)',
                r'(1\s*[-—]?\s*[А-Яа-я])'
            ]
            
            for pattern in series_patterns:
                match = re.search(pattern, page_text, re.IGNORECASE)
                if match:
                    series = match.group(1).upper()
                    if any(s in series for s in ['П', 'ПАНЕЛЬ', 'П-', 'ПА']):
                        return 'панельный'
                    elif any(s in series for s in ['К', 'КИРП', 'К-']):
                        return 'кирпичный'
                    elif any(s in series for s in ['М', 'МОНОЛИТ', 'М-']):
                        return 'монолитный'
            
            # 5. Прямой поиск ключевых слов
            keywords = {
                'панельный': ['панельный', 'панель', 'панельное'],
                'кирпичный': ['кирпичный', 'кирпич', 'кирпичное'],
                'монолитный': ['монолитный', 'монолит', 'монолитное'],
                'блочный': ['блочный', 'блок', 'блочное'],
                'деревянный': ['деревянный', 'дерево', 'деревянное'],
                'сталинский': ['сталинский', 'сталинка'],
                'хрущевский': ['хрущевский', 'хрущевка'],
                'брежневский': ['брежневский', 'брежневка']
            }
            
            for house_type, word_list in keywords.items():
                for word in word_list:
                    if word in page_text:
                        return house_type
            
            # 6. Определение по году постройки (косвенный признак)
            year_match = re.search(r'год[:\s]*(\d{4})', page_text)
            if year_match:
                year = int(year_match.group(1))
                if year >= 1990:
                    # Большинство современных домов - панельные или монолитные
                    return 'панельный'  # По умолчанию для современных домов
            
        except Exception as e:
            print(f"   ⚠️ Ошибка при парсинге типа дома: {e}")
        return None
    
    def extract_property_category(self, soup):
        """Определить категорию недвижимости: вторичка или новостройка"""
        try:
            page_text = soup.get_text().lower()
            
            # 1. Проверяем год сдачи
            year_patterns = [
                r'сдача.*?(\d{4})',
                r'год.*?сдачи.*?(\d{4})',
                r'(\d{4})\s*год.*?сдачи',
                r'квартира.*?сдается.*?(\d{4})',
                r'готовность.*?(\d{4})'
            ]
            
            for pattern in year_patterns:
                match = re.search(pattern, page_text, re.IGNORECASE)
                if match:
                    year = int(match.group(1))
                    if year >= datetime.now().year or year >= 2023:  # Будущий год или недавний
                        return 'новостройка'
            
            # 2. Ищем ключевые слова новостроек
            new_building_keywords = [
                'новостройка', 'новый дом', 'новый комплекс', 'стройка',
                'сдается в', 'готовность', 'заселение с', 'срок сдачи',
                'первичка', 'первичный рынок', 'от застройщика'
            ]
            
            for keyword in new_building_keywords:
                if keyword in page_text:
                    return 'новостройка'
            
            # 3. Ищем в JSON-LD
            json_ld = soup.find('script', type='application/ld+json')
            if json_ld and json_ld.string:
                try:
                    data = json.loads(json_ld.string)
                    if 'description' in data:
                        desc = data['description'].lower()
                        if any(keyword in desc for keyword in new_building_keywords):
                            return 'новостройка'
                except:
                    pass
            
            # 4. Ищем в скриптах
            scripts = soup.find_all('script')
            for script in scripts:
                if script.string:
                    # Ищем realEstateType
                    match = re.search(r'"realEstateType":\s*"([^"]+)"', script.string, re.IGNORECASE)
                    if match:
                        estate_type = match.group(1).lower()
                        if 'new' in estate_type or 'новострой' in estate_type:
                            return 'новостройка'
                        elif 'secondary' in estate_type or 'вторич' in estate_type:
                            return 'вторичка'
            
            # 5. Проверяем год постройки
            year_match = re.search(r'год.*?постройки[:\s]*(\d{4})', page_text)
            if year_match:
                year = int(year_match.group(1))
                if year >= datetime.now().year - 5:  # Постройки последних 5 лет
                    return 'новостройка'
            
            # По умолчанию считаем вторичкой
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
    
    def extract_metro_station(self, soup):
        """Извлечь станцию метро - УЛУЧШЕННЫЙ МЕТОД с BeautifulSoup"""
        try:
            # 1. Ищем в специальных элементах ЦИАН
            metro_selectors = [
                {'data-name': 'UndergroundStation'},
                {'class': lambda x: x and ('underground' in x.lower() or 'metro' in x.lower())},
                {'href': lambda x: x and 'underground' in x}
            ]
            
            for selector in metro_selectors:
                elements = soup.find_all('a', selector)
                for elem in elements:
                    text = elem.get_text(strip=True)
                    if text and len(text) < 50:
                        # Убираем время до метро
                        station = re.sub(r'\(\d+\s*мин\)', '', text)
                        station = re.sub(r'\d+\s*мин', '', station)
                        station = station.replace('метро', '').strip()
                        if station and station not in ['', 'м']:
                            return station
            
            # 2. Ищем в JSON-LD
            json_ld = soup.find('script', type='application/ld+json')
            if json_ld and json_ld.string:
                try:
                    data = json.loads(json_ld.string)
                    if 'description' in data:
                        desc = data['description']
                        # Ищем станции метро в описании
                        stations_spb = [
                            'Академическая', 'Политехническая', 'Лесная', 'Выборгская',
                            'Площадь Ленина', 'Чернышевская', 'Площадь Восстания',
                            'Владимирская', 'Пушкинская', 'Технологический институт',
                            'Балтийская', 'Нарвская', 'Кировский завод', 'Автово',
                            'Ленинский проспект', 'Проспект Ветеранов', 'Парк Победы',
                            'Электросила', 'Московская', 'Звёздная', 'Купчино',
                            'Девяткино', 'Гражданский проспект', 'Академическая',
                            'Политехническая', 'Площадь Мужества', 'Петроградская',
                            'Горьковская', 'Невский проспект', 'Сенная площадь',
                            'Технологический институт', 'Фрунзенская', 'Московские ворота',
                            'Электросила', 'Парк Победы', 'Московская', 'Звёздная',
                            'Купчино', 'Ладожская', 'Проспект Большевиков', 'Улица Дыбенко'
                        ]
                        
                        for station in stations_spb:
                            if station in desc:
                                return station
                except:
                    pass
            
            # 3. Ищем по тексту страницы
            page_text = soup.get_text()
            stations_spb = [
                'Академическая', 'Политехническая', 'Лесная', 'Выборгская',
                'Площадь Ленина', 'Чернышевская', 'Площадь Восстания'
            ]
            
            for station in stations_spb:
                if station in page_text:
                    return station
            
            # 4. Ищем в блоке "Ближайшее метро"
            for elem in soup.find_all(['div', 'p', 'span']):
                text = elem.get_text(strip=True)
                if 'ближайшее метро' in text.lower() or 'станция метро' in text.lower():
                    # Извлекаем название станции
                    lines = text.split('\n')
                    for line in lines:
                        line = line.strip()
                        if line and 'метро' not in line.lower() and 'ближайшее' not in line.lower():
                            station = re.sub(r'\(\d+\s*мин\)', '', line)
                            station = re.sub(r'\d+\s*мин', '', station)
                            station = station.replace('метро', '').strip()
                            if station:
                                return station
            
        except Exception as e:
            print(f"   ⚠️ Ошибка при парсинге метро: {e}")
        return None
    
    def extract_metro_time(self, soup):
        """Извлечь время до метро - УЛУЧШЕННЫЙ МЕТОД"""
        try:
            page_text = soup.get_text()
            
            # 1. Ищем паттерны времени в тексте
            patterns = [
                r'\((\d+)\s*мин\.?\)',  # (10 мин)
                r'(\d+)\s*мин\.?\s*(?:до метро|пешком|ходьбы)',  # 10 мин до метро
                r'до метро\s*(\d+)\s*мин\.?',  # до метро 10 мин
                r'пешком\s*(\d+)\s*мин\.?',  # пешком 10 мин
                r'(\d+)\s*минут\s*(?:до метро|пешком)',  # 10 минут до метро
                r'(\d+)-минутная\s+ходьба',  # 10-минутная ходьба
                r'(\d+)\s*мин\.?\s*от\s*метро',  # 10 мин от метро
            ]
            
            for pattern in patterns:
                matches = re.findall(pattern, page_text, re.IGNORECASE)
                for match in matches:
                    if str(match).isdigit() and 1 <= int(match) <= 120:
                        return f"{match} мин"
            
            # 2. Ищем в JSON данных
            scripts = soup.find_all('script')
            for script in scripts:
                if script.string:
                    # Ищем timeToMetro
                    time_match = re.search(r'"timeToMetro":\s*(\d+)', script.string)
                    if time_match:
                        minutes = time_match.group(1)
                        if 1 <= int(minutes) <= 120:
                            return f"{minutes} мин"
            
            # 3. Ищем в видимых элементах около упоминаний метро
            metro_elements = soup.find_all(['div', 'span', 'p'], 
                                          class_=lambda x: x and ('metro' in str(x).lower() or 'underground' in str(x).lower()))
            
            for elem in metro_elements:
                text = elem.get_text()
                time_match = re.search(r'(\d+)\s*мин', text)
                if time_match:
                    return f"{time_match.group(1)} мин"
            
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
                SELECT cian_id, metro_station, metro_time, building_type, property_category
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
                print(f"  ID: {ex[0]}, {metro_info}{building_info}{category_info}")
            
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
            print(f"   SELECT cian_id, building_type, property_category, metro_station, metro_time FROM {self.table_name} ORDER BY created_at DESC;")
        
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