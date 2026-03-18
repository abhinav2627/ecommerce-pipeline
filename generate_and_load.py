import psycopg2
import pandas as pd
from faker import Faker
import random
from datetime import datetime, timedelta

fake = Faker()
random.seed(42)

# -- Config ----------------------------------------------------------------
DB = dict(host='localhost', dbname='ecommerce_dw', user='postgres',
          password='47f5188fa31a470886534a2faa05473d', port=5432)

NUM_CUSTOMERS = 1000
NUM_PRODUCTS  = 200
NUM_ORDERS    = 50000

CATEGORIES = ['Electronics', 'Clothing', 'Home & Garden', 'Sports', 'Books', 'Toys', 'Beauty', 'Food']
STATUSES    = ['completed', 'cancelled', 'refunded', 'pending']
STATUS_W    = [0.75, 0.10, 0.08, 0.07]

def get_conn():
    return psycopg2.connect(**DB)

# -- Create RAW tables -----------------------------------------------------
def create_tables():
    conn = get_conn()
    cur  = conn.cursor()
    cur.execute('''
        CREATE TABLE IF NOT EXISTS raw.customers (
            customer_id     VARCHAR PRIMARY KEY,
            first_name      VARCHAR,
            last_name       VARCHAR,
            email           VARCHAR,
            country         VARCHAR,
            city            VARCHAR,
            signup_date     DATE,
            age             INT,
            gender          VARCHAR
        );

        CREATE TABLE IF NOT EXISTS raw.products (
            product_id      VARCHAR PRIMARY KEY,
            product_name    VARCHAR,
            category        VARCHAR,
            sub_category    VARCHAR,
            price           NUMERIC(10,2),
            cost            NUMERIC(10,2),
            brand           VARCHAR,
            is_active       BOOLEAN
        );

        CREATE TABLE IF NOT EXISTS raw.orders (
            order_id        VARCHAR PRIMARY KEY,
            customer_id     VARCHAR,
            order_date      TIMESTAMP,
            status          VARCHAR,
            shipping_country VARCHAR,
            payment_method  VARCHAR
        );

        CREATE TABLE IF NOT EXISTS raw.order_items (
            item_id         SERIAL PRIMARY KEY,
            order_id        VARCHAR,
            product_id      VARCHAR,
            quantity        INT,
            unit_price      NUMERIC(10,2),
            discount_pct    NUMERIC(5,2)
        );
    ''')
    conn.commit()
    cur.close()
    conn.close()
    print('RAW tables created')

# -- Generate customers ----------------------------------------------------
def generate_customers():
    countries = ['USA', 'UK', 'Canada', 'Australia', 'Germany', 'France', 'UAE', 'Singapore']
    rows = []
    for i in range(NUM_CUSTOMERS):
        rows.append((
            f'CUST_{i+1:05d}',
            fake.first_name(),
            fake.last_name(),
            fake.email(),
            random.choice(countries),
            fake.city(),
            fake.date_between(start_date='-3y', end_date='-1m'),
            random.randint(18, 70),
            random.choice(['M', 'F', 'Other'])
        ))
    conn = get_conn()
    cur  = conn.cursor()
    cur.execute('TRUNCATE raw.customers')
    cur.executemany('''
        INSERT INTO raw.customers VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
    ''', rows)
    conn.commit()
    cur.close()
    conn.close()
    print(f'Customers loaded: {len(rows):,}')
    return [r[0] for r in rows]

# -- Generate products -----------------------------------------------------
def generate_products():
    rows = []
    for i in range(NUM_PRODUCTS):
        cat   = random.choice(CATEGORIES)
        price = round(random.uniform(5, 500), 2)
        cost  = round(price * random.uniform(0.3, 0.7), 2)
        rows.append((
            f'PROD_{i+1:05d}',
            fake.catch_phrase(),
            cat,
            f'{cat} - Type {random.randint(1,5)}',
            price,
            cost,
            fake.company(),
            random.random() > 0.1
        ))
    conn = get_conn()
    cur  = conn.cursor()
    cur.execute('TRUNCATE raw.products')
    cur.executemany('''
        INSERT INTO raw.products VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
    ''', rows)
    conn.commit()
    cur.close()
    conn.close()
    print(f'Products loaded: {len(rows):,}')
    return [r[0] for r in rows]

# -- Generate orders + items -----------------------------------------------
def generate_orders(customer_ids, product_ids):
    payments = ['credit_card', 'debit_card', 'paypal', 'bank_transfer']
    countries = ['USA', 'UK', 'Canada', 'Australia', 'Germany', 'France', 'UAE', 'Singapore']
    start_date = datetime.now() - timedelta(days=365)

    orders = []
    items  = []
    for i in range(NUM_ORDERS):
        order_id   = f'ORD_{i+1:07d}'
        order_date = start_date + timedelta(
            days=random.randint(0, 365),
            hours=random.randint(0, 23),
            minutes=random.randint(0, 59)
        )
        orders.append((
            order_id,
            random.choice(customer_ids),
            order_date,
            random.choices(STATUSES, STATUS_W)[0],
            random.choice(countries),
            random.choice(payments)
        ))
        # 1-5 items per order
        for _ in range(random.randint(1, 5)):
            prod   = random.choice(product_ids)
            qty    = random.randint(1, 10)
            price  = round(random.uniform(5, 500), 2)
            disc   = round(random.choice([0, 0, 0, 5, 10, 15, 20]), 2)
            items.append((order_id, prod, qty, price, disc))

    conn = get_conn()
    cur  = conn.cursor()
    cur.execute('TRUNCATE raw.order_items')
    cur.execute('TRUNCATE raw.orders')

    # Load orders in chunks
    chunk = 5000
    for i in range(0, len(orders), chunk):
        cur.executemany('''
            INSERT INTO raw.orders VALUES (%s,%s,%s,%s,%s,%s)
        ''', orders[i:i+chunk])
        print(f'  Orders: {min(i+chunk, len(orders)):,} / {len(orders):,}')

    # Load items in chunks
    for i in range(0, len(items), chunk):
        cur.executemany('''
            INSERT INTO raw.order_items (order_id,product_id,quantity,unit_price,discount_pct)
            VALUES (%s,%s,%s,%s,%s)
        ''', items[i:i+chunk])
        print(f'  Items : {min(i+chunk, len(items)):,} / {len(items):,}')

    conn.commit()
    cur.close()
    conn.close()
    print(f'Orders loaded : {len(orders):,}')
    print(f'Items loaded  : {len(items):,}')

# -- Main ------------------------------------------------------------------
if __name__ == '__main__':
    print('=' * 50)
    print('  E-COMMERCE DATA GENERATOR')
    print('=' * 50)
    create_tables()
    customer_ids = generate_customers()
    product_ids  = generate_products()
    generate_orders(customer_ids, product_ids)
    print()
    print('=' * 50)
    print('  DONE! All RAW data loaded.')
    print('=' * 50)
