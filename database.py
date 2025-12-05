import os
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, Boolean, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import datetime

# Создаем папку data если ее нет
os.makedirs('data', exist_ok=True)

# Указываем абсолютный путь к базе данных
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATABASE_PATH = os.path.join(BASE_DIR, 'data', 'cian_database.db')
DATABASE_URL = f'sqlite:///{DATABASE_PATH}'

Base = declarative_base()

class Property(Base):
    __tablename__ = 'properties'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    external_id = Column(String(50), unique=True, nullable=False)
    url = Column(String(500))
    
    # Основная информация
    title = Column(String(500))
    address = Column(String(500))
    district = Column(String(100))
    metro = Column(String(200))
    
    # Характеристики
    rooms = Column(Integer)
    total_area = Column(Float)
    living_area = Column(Float)
    kitchen_area = Column(Float)
    floor = Column(Integer)
    total_floors = Column(Integer)
    building_type = Column(String(100))
    year_built = Column(Integer)
    
    # Цены
    price = Column(Float)
    price_per_m2 = Column(Float)
    previous_price = Column(Float, nullable=True)
    
    # Информация об объявлении
    publication_date = Column(DateTime)
    update_date = Column(DateTime)
    is_active = Column(Boolean, default=True)
    
    # Дополнительно
    description = Column(Text)
    seller_type = Column(String(50))
    
    # Системные поля
    created_at = Column(DateTime, default=datetime.now)
    last_parsed = Column(DateTime, default=datetime.now)
    
    def __repr__(self):
        return f"<Property(id={self.external_id}, price={self.price})>"

def init_database():
    """Инициализация базы данных"""
    try:
        engine = create_engine(DATABASE_URL)
        
        # Проверяем соединение
        with engine.connect() as conn:
            conn.execute("SELECT 1")
        
        # Создаем таблицы
        Base.metadata.create_all(engine)
        print(f"База данных создана: {DATABASE_PATH}")
        return engine
        
    except Exception as e:
        print(f"Ошибка при создании базы данных: {e}")
        
        # Пробуем альтернативный путь
        alt_path = os.path.join(BASE_DIR, 'cian_database.db')
        alt_url = f'sqlite:///{alt_path}'
        print(f"Пробуем альтернативный путь: {alt_path}")
        
        engine = create_engine(alt_url)
        Base.metadata.create_all(engine)
        print(f"База данных создана по альтернативному пути: {alt_path}")
        return engine

def get_session():
    """Получение сессии базы данных"""
    engine = init_database()
    Session = sessionmaker(bind=engine)
    return Session()