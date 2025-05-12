import psycopg2
from datetime import datetime, timedelta
import random
from faker import Faker


fake = Faker('ru_RU')

# параметры подключения к PostgreSQL в Docker
conn = psycopg2.connect(
    dbname="source_database",       
    user="source",         
    password="source",     
    host="localhost",      
    port=6543,
    sslmode='disable'
)

cur = conn.cursor()

# Создание таблиц
cur.execute("""
CREATE TABLE IF NOT EXISTS customers (
    customer_id INT PRIMARY KEY,
    name VARCHAR(100),
    region_id INT,
    age INT
);
""")

cur.execute("""
CREATE TABLE IF NOT EXISTS orders (
    order_id INT PRIMARY KEY,
    product_id INT,
    timestamp TIMESTAMP,
    customer_id INT REFERENCES customers(customer_id),
    amount FLOAT
);
""")

print("Таблицы orders и customers созданы")

# покупатели
customers = []
for i in range(1, 101):
    name = fake.name()
    region_id = random.randint(1, 10)
    age = random.randint(18, 70)
    customers.append((i, name, region_id, age))

cur.executemany(
    "INSERT INTO customers (customer_id, name, region_id, age) VALUES (%s, %s, %s, %s) " \
    "ON CONFLICT (customer_id) DO NOTHING;",
    customers
)

print("Вставлены данные в таблицу customers")

# заказы
orders = []
start_date = datetime(2023, 10, 1)
for i in range(1, 1001):
    order_id = i
    product_id = random.randint(100, 200)
    
    # равномерно по 4 дням
    day_offset = (i - 1) // 250
    order_date = start_date + timedelta(days=day_offset)
    
    # случайное время в течение дня
    order_time = order_date + timedelta(
        hours=random.randint(0, 23),
        minutes=random.randint(0, 59),
        seconds=random.randint(0, 59)
    )
    customer_id = random.randint(1, 100)
    amount = round(random.uniform(100, 10000), 2)
    orders.append((order_id, product_id, order_time, customer_id, amount))

cur.executemany(
    "INSERT INTO orders (order_id, product_id, timestamp, customer_id, amount) VALUES (%s, %s, %s, %s, %s) ON CONFLICT (order_id) DO NOTHING;",
    orders
)

print("Вставлены данные в таблицу orders")

conn.commit()
cur.close()
conn.close()