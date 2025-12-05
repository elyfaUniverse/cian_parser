# test_parser.py
import time
import re
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options

def quick_test():
    """Быстрый тест парсера"""
    print("🚀 Быстрый тест парсера ЦИАН")
    
    options = Options()
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
    
    driver = webdriver.Chrome(options=options)
    
    try:
        # Открываем страницу
        url = "https://spb.cian.ru/cat.php?deal_type=sale&engine_version=2&offer_type=flat&region=2&room1=1"
        print(f"🌐 Открываем: {url}")
        
        driver.get(url)
        time.sleep(3)
        
        # Ищем заголовки
        titles = driver.find_elements(By.CSS_SELECTOR, '[data-name="TitleComponent"]')
        print(f"\n📰 Найдено заголовков: {len(titles)}")
        
        for i, title in enumerate(titles[:3]):
            print(f"  {i+1}. {title.text[:80]}...")
        
        # Ищем цены
        prices = driver.find_elements(By.CSS_SELECTOR, '[data-mark="MainPrice"]')
        print(f"\n💰 Найдено цен: {len(prices)}")
        
        for i, price in enumerate(prices[:3]):
            print(f"  {i+1}. {price.text}")
        
        # Ищем адреса
        addresses = driver.find_elements(By.CSS_SELECTOR, '[data-name="AddressContainer"]')
        print(f"\n📍 Найдено адресов: {len(addresses)}")
        
        for i, addr in enumerate(addresses[:3]):
            print(f"  {i+1}. {addr.text[:60]}...")
        
        print("\n✅ Тест завершен успешно!")
        
    except Exception as e:
        print(f"❌ Ошибка: {e}")
    finally:
        driver.quit()

if __name__ == "__main__":
    quick_test()
    input("\nНажмите Enter для выхода...")