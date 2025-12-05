import time
import random
import re
import os
from datetime import datetime, timedelta
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from database import get_session, Property

class SimpleCianParser:
    def __init__(self):
        self.setup_driver()
        self.session = get_session()
        print("Парсер инициализирован!")
    
    def setup_driver(self):
        """Простая настройка драйвера"""
        print("Настраиваем драйвер...")
        
        options = Options()
        
       
        yandex_path = r"C:\Users\Elvira\AppData\Local\Yandex\YandexBrowser\Application\browser.exe"
        if os.path.exists(yandex_path):
            options.binary_location = yandex_path
        
        options.add_argument('--disable-blink-features=AutomationControlled')
        options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36')
        options.add_argument('--start-maximized')
        options.add_argument('--disable-notifications')
        
        # Используем ChromeDriver
        self.driver = webdriver.Chrome(options=options)
        print("Драйвер готов!")
    
    def get_listing_urls(self, url, max_pages=1):
        """Получение URL объявлений"""
        print(f"Получаем объявления с: {url}")
        
        self.driver.get(url)
        time.sleep(5)
        
        urls = []
        page = 1
        
        while page <= max_pages:
            print(f"Страница {page}")
            
            # Прокручиваем страницу
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(3)
            
            # Ищем ссылки на объявления
            try:
                links = self.driver.find_elements(By.CSS_SELECTOR, 'a[href*="/flat/"]')
                for link in links:
                    href = link.get_attribute('href')
                    if href and '/sale/' in href and href not in urls:
                        urls.append(href)
                
                print(f"Найдено ссылок: {len(links)}")
                
            except Exception as e:
                print(f"Ошибка при поиске ссылок: {e}")
            
            # Пытаемся перейти на следующую страницу
            if page < max_pages:
                try:
                    next_button = self.driver.find_element(By.CSS_SELECTOR, '[data-name="PaginationButtonNext"]')
                    if next_button:
                        next_button.click()
                        time.sleep(4)
                        page += 1
                    else:
                        break
                except:
                    print("Нет кнопки 'Далее'")
                    break
        
        unique_urls = list(set(urls))
        print(f"Всего уникальных объявлений: {len(unique_urls)}")
        return unique_urls
    
    def parse_listing(self, url):
        """Парсинг одного объявления"""
        print(f"Парсим: {url[:80]}...")
        
        try:
            self.driver.get(url)
            time.sleep(4)
            
            data = {
                'url': url,
                'external_id': self.get_id_from_url(url),
                'title': '',
                'address': '',
                'price': 0,
                'rooms': 0,
                'total_area': 0,
                'floor': 0,
                'total_floors': 0,
                'publication_date': datetime.now(),
                'update_date': datetime.now(),
                'is_active': True
            }
            
            # Заголовок
            try:
                title = self.driver.find_element(By.TAG_NAME, 'h1')
                data['title'] = title.text[:200]
            except:
                pass
            
            # Цена
            try:
                price_elem = self.driver.find_element(By.CSS_SELECTOR, '[data-testid="price-amount"]')
                price_text = price_elem.text.replace(' ', '').replace('₽', '')
                data['price'] = float(price_text) if price_text else 0
            except:
                pass
            
            # Адрес
            try:
                address_elem = self.driver.find_element(By.CSS_SELECTOR, '[data-name="AddressContainer"]')
                data['address'] = address_elem.text[:300]
            except:
                pass
            
            # Характеристики
            try:
                features = self.driver.find_elements(By.CSS_SELECTOR, '[data-name="FeaturesList"] li')
                for feature in features:
                    text = feature.text.lower()
                    
                    if 'комнат' in text:
                        try:
                            match = re.search(r'(\d+)', text)
                            if match:
                                data['rooms'] = int(match.group(1))
                        except:
                            pass
                    
                    if 'общая' in text:
                        try:
                            match = re.search(r'(\d+\.?\d*)', text)
                            if match:
                                data['total_area'] = float(match.group(1))
                        except:
                            pass
                    
                    if 'этаж' in text:
                        try:
                            numbers = re.findall(r'(\d+)', text)
                            if len(numbers) >= 2:
                                data['floor'] = int(numbers[0])
                                data['total_floors'] = int(numbers[1])
                        except:
                            pass
            except:
                pass
            
            return data
            
        except Exception as e:
            print(f"Ошибка при парсинге: {e}")
            return None
    
    def get_id_from_url(self, url):
        """Извлечение ID из URL"""
        try:
            match = re.search(r'/(\d+)/', url)
            return match.group(1) if match else str(hash(url))[:10]
        except:
            return str(hash(url))[:10]
    
    def save_data(self, data):
        """Сохранение данных в БД"""
        if not data:
            return False
        
        try:
            # Проверяем существующую запись
            existing = self.session.query(Property).filter(
                Property.external_id == data['external_id']
            ).first()
            
            if existing:
                # Обновляем цену если изменилась
                if existing.price != data['price'] and data['price'] > 0:
                    print(f"Цена изменилась: {existing.price} -> {data['price']}")
                    existing.previous_price = existing.price
                    existing.price = data['price']
                
                # Обновляем другие поля
                for key in ['title', 'address', 'rooms', 'total_area', 'floor', 'total_floors']:
                    if key in data and data[key]:
                        setattr(existing, key, data[key])
                
                existing.last_parsed = datetime.now()
                existing.update_date = datetime.now()
                print(f"Обновлено: {data['external_id']}")
                
            else:
                # Создаем новую запись
                property_obj = Property(**data)
                self.session.add(property_obj)
                print(f"Добавлено: {data['external_id']}")
            
            self.session.commit()
            return True
            
        except Exception as e:
            print(f"Ошибка при сохранении: {e}")
            self.session.rollback()
            return False
    
    def run(self):
        """Запуск парсера"""
        print("=" * 50)
        print("ЗАПУСК ПАРСЕРА ЦИАН")
        print("=" * 50)
        
        # URL для поиска (вторичка СПб)
        search_url = "https://spb.cian.ru/cat.php?deal_type=sale&engine_version=2&offer_type=flat&region=2&sort=creation_date_desc"
        
        # Получаем URL объявлений
        urls = self.get_listing_urls(search_url, max_pages=1)
        
        if not urls:
            print("Не найдено объявлений!")
            return
        
        print(f"\nНачинаем парсинг {len(urls)} объявлений...")
        
        successful = 0
        for i, url in enumerate(urls):
            print(f"\n[{i+1}/{len(urls)}]")
            
            data = self.parse_listing(url)
            
            if data and self.save_data(data):
                successful += 1
            
            # Пауза между запросами
            time.sleep(random.uniform(3, 6))
        
        print(f"\n" + "=" * 50)
        print(f"ГОТОВО! Успешно обработано: {successful}/{len(urls)}")
        print("=" * 50)
    
    def close(self):
        """Закрытие"""
        self.session.close()
        self.driver.quit()
        print("Ресурсы освобождены")

def main():
    parser = SimpleCianParser()
    
    try:
        parser.run()
    except KeyboardInterrupt:
        print("\nПрервано пользователем")
    except Exception as e:
        print(f"\nОшибка: {e}")
    finally:
        parser.close()

if __name__ == "__main__":
    main()