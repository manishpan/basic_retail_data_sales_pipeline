from faker import Faker
import pandas as pd
import random
import uuid
from datetime import datetime, timedelta


fake = Faker()

# Parameters
NUM_PRODUCTS = 10
NUM_STORES = 5
NUM_SALES = 10000

# ------------ Products category ---------------
product_categories = ['Electronics', 'Furniture', 'Stationery', 'Home', 'Accessories']

products = []

for i in range(NUM_PRODUCTS):
    product_id = 1001 + i
    name = fake.word().capitalize() + " " + random.choice(['Set', 'Device', 'Kit', 'Pack'])
    category = random.choice(product_categories)
    cost_price = round(random.uniform(2.0, 5.0), 2)
    unit_price = round(cost_price * random.uniform(1.2, 1.8), 2)
    products.append({
        "product_id": product_id,
        "name": name,
        "category": category,
        "cost_price": cost_price,
        "unit_price": unit_price
    })

df_products = pd.DataFrame(products)
df_products.to_csv('data/raw/sales.csv', index=False)

# ---------- STORES ----------------
store_names = ["Midtown", 'Uptown', 'Bay Area', 'Lakeside', 'Downtown']
cities = ['New York', 'San Francisco', 'Chicago', 'Boston', 'Los Angeles']

stores = []

for i in range(NUM_STORES):
    stores.append({
        'store_id': i + 1,
        'store_name': store_names[i] + " Store",
        "location": cities[i]
    })

df_stores = pd.DataFrame(stores)
df_stores.to_csv('data/raw/stores.csv', index=False)


# ------------- Sales -------------------
sales = []
for _ in range(NUM_SALES):
    product = random.choice(products)
    store = random.choice(stores)
    quantity = random.randint(1, 5)
    sale_date = fake.date_between(start_date='-12M', end_date='today')

    sales.append({
        'transaction_id': str(uuid.uuid4()),
        'date': sale_date.strftime("%Y-%m-%d"),
        'store_id': store['store_id'],
        'product_id': product['product_id'],
        'quantity': quantity,
        'unit_price': product['unit_price'],
        'total_price': round(quantity * product['unit_price'], 2)
    })

df_sales = pd.DataFrame(sales)
df_sales.to_csv('data/raw/sales.csv', index=False)

print("✅ Generated products.csv, stores.csv, and sales.csv in data/raw/")