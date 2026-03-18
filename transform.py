import psycopg2

DB = dict(host='localhost', dbname='ecommerce_dw', user='postgres',
          password='47f5188fa31a470886534a2faa05473d', port=5432)

def get_conn():
    return psycopg2.connect(**DB)

def run_sql(cur, sql, label):
    cur.execute(sql)
    print(f'  ? {label}')

# -- Staging Layer ---------------------------------------------------------
def build_staging():
    conn = get_conn()
    cur  = conn.cursor()
    print('Building Staging layer...')

    run_sql(cur, '''
        CREATE OR REPLACE VIEW staging.stg_customers AS
        SELECT
            customer_id,
            TRIM(first_name)            AS first_name,
            TRIM(last_name)             AS last_name,
            LOWER(TRIM(email))          AS email,
            country,
            city,
            signup_date,
            age,
            gender,
            CASE
                WHEN age < 25 THEN '18-24'
                WHEN age < 35 THEN '25-34'
                WHEN age < 45 THEN '35-44'
                WHEN age < 55 THEN '45-54'
                ELSE '55+'
            END                         AS age_group
        FROM raw.customers
        WHERE customer_id IS NOT NULL
          AND email IS NOT NULL
    ''', 'stg_customers')

    run_sql(cur, '''
        CREATE OR REPLACE VIEW staging.stg_products AS
        SELECT
            product_id,
            TRIM(product_name)          AS product_name,
            category,
            sub_category,
            price,
            cost,
            ROUND(price - cost, 2)      AS gross_margin,
            CASE WHEN price > 0
                THEN ROUND((price - cost) / price * 100, 2)
                ELSE 0
            END                         AS margin_pct,
            brand,
            is_active
        FROM raw.products
        WHERE product_id IS NOT NULL
          AND price > 0
    ''', 'stg_products')

    run_sql(cur, '''
        CREATE OR REPLACE VIEW staging.stg_orders AS
        SELECT
            o.order_id,
            o.customer_id,
            o.order_date,
            DATE(o.order_date)          AS order_date_only,
            EXTRACT(YEAR  FROM o.order_date)::INT   AS order_year,
            EXTRACT(MONTH FROM o.order_date)::INT   AS order_month,
            EXTRACT(DOW   FROM o.order_date)::INT   AS order_dow,
            EXTRACT(HOUR  FROM o.order_date)::INT   AS order_hour,
            o.status,
            o.shipping_country,
            o.payment_method,
            CASE WHEN o.status = 'completed' THEN TRUE ELSE FALSE END AS is_completed
        FROM raw.orders o
        WHERE o.order_id IS NOT NULL
          AND o.customer_id IS NOT NULL
    ''', 'stg_orders')

    run_sql(cur, '''
        CREATE OR REPLACE VIEW staging.stg_order_items AS
        SELECT
            oi.item_id,
            oi.order_id,
            oi.product_id,
            oi.quantity,
            oi.unit_price,
            oi.discount_pct,
            ROUND(oi.unit_price * (1 - oi.discount_pct / 100), 2)          AS net_price,
            ROUND(oi.quantity * oi.unit_price * (1 - oi.discount_pct/100), 2) AS line_total,
            ROUND(oi.quantity * p.cost, 2)                                  AS line_cost,
            ROUND((oi.unit_price - p.cost) * oi.quantity, 2)               AS line_profit
        FROM raw.order_items oi
        LEFT JOIN raw.products p ON oi.product_id = p.product_id
        WHERE oi.order_id IS NOT NULL
    ''', 'stg_order_items')

    conn.commit()
    cur.close()
    conn.close()
    print('Staging layer complete!')

# -- Mart Layer ------------------------------------------------------------
def build_marts():
    conn = get_conn()
    cur  = conn.cursor()
    print('Building Marts layer...')

    run_sql(cur, '''
        CREATE TABLE IF NOT EXISTS marts.mart_customer_kpis AS
        SELECT
            c.customer_id,
            c.first_name || ' ' || c.last_name   AS full_name,
            c.country,
            c.age_group,
            c.gender,
            COUNT(DISTINCT o.order_id)            AS total_orders,
            COUNT(DISTINCT CASE WHEN o.is_completed THEN o.order_id END) AS completed_orders,
            ROUND(SUM(oi.line_total), 2)          AS total_revenue,
            ROUND(AVG(oi.line_total), 2)          AS avg_order_value,
            ROUND(SUM(oi.line_profit), 2)         AS total_profit,
            MIN(o.order_date_only)                AS first_order_date,
            MAX(o.order_date_only)                AS last_order_date,
            COUNT(DISTINCT o.order_date_only)     AS active_days,
            RANK() OVER (ORDER BY SUM(oi.line_total) DESC) AS revenue_rank
        FROM staging.stg_customers c
        LEFT JOIN staging.stg_orders o      ON c.customer_id = o.customer_id
        LEFT JOIN staging.stg_order_items oi ON o.order_id   = oi.order_id
        GROUP BY c.customer_id, c.first_name, c.last_name,
                 c.country, c.age_group, c.gender
    ''', 'mart_customer_kpis')

    run_sql(cur, '''
        CREATE TABLE IF NOT EXISTS marts.mart_product_performance AS
        SELECT
            p.product_id,
            p.product_name,
            p.category,
            p.sub_category,
            p.price,
            p.margin_pct,
            COUNT(DISTINCT oi.order_id)           AS total_orders,
            SUM(oi.quantity)                      AS units_sold,
            ROUND(SUM(oi.line_total), 2)          AS total_revenue,
            ROUND(SUM(oi.line_profit), 2)         AS total_profit,
            ROUND(AVG(oi.discount_pct), 1)        AS avg_discount_pct,
            RANK() OVER (ORDER BY SUM(oi.line_total) DESC)  AS revenue_rank,
            RANK() OVER (ORDER BY SUM(oi.quantity) DESC)    AS units_rank
        FROM staging.stg_products p
        LEFT JOIN staging.stg_order_items oi ON p.product_id = oi.product_id
        LEFT JOIN staging.stg_orders o        ON oi.order_id = o.order_id
            AND o.is_completed = TRUE
        GROUP BY p.product_id, p.product_name, p.category,
                 p.sub_category, p.price, p.margin_pct
    ''', 'mart_product_performance')

    run_sql(cur, '''
        CREATE TABLE IF NOT EXISTS marts.mart_monthly_revenue AS
        SELECT
            o.order_year,
            o.order_month,
            TO_CHAR(DATE_TRUNC('month', o.order_date), 'YYYY-MM') AS month_label,
            o.shipping_country                    AS country,
            COUNT(DISTINCT o.order_id)            AS total_orders,
            COUNT(DISTINCT o.customer_id)         AS unique_customers,
            ROUND(SUM(oi.line_total), 2)          AS total_revenue,
            ROUND(SUM(oi.line_profit), 2)         AS total_profit,
            ROUND(AVG(oi.line_total), 2)          AS avg_order_value,
            SUM(oi.quantity)                      AS units_sold
        FROM staging.stg_orders o
        JOIN staging.stg_order_items oi ON o.order_id = oi.order_id
        WHERE o.is_completed = TRUE
        GROUP BY o.order_year, o.order_month,
                 DATE_TRUNC('month', o.order_date),
                 o.shipping_country
        ORDER BY o.order_year, o.order_month
    ''', 'mart_monthly_revenue')

    conn.commit()
    cur.close()
    conn.close()
    print('Marts layer complete!')

# -- Validate --------------------------------------------------------------
def validate():
    conn = get_conn()
    cur  = conn.cursor()
    print()
    print('=' * 55)
    print('  E-COMMERCE PIPELINE FINAL REPORT')
    print('=' * 55)

    tables = [
        ('raw.customers',               'Customers'),
        ('raw.products',                'Products'),
        ('raw.orders',                  'Orders'),
        ('raw.order_items',             'Order Items'),
        ('marts.mart_customer_kpis',    'Customer KPIs'),
        ('marts.mart_product_performance', 'Product Performance'),
        ('marts.mart_monthly_revenue',  'Monthly Revenue'),
    ]
    for table, label in tables:
        cur.execute(f'SELECT COUNT(*) FROM {table}')
        count = cur.fetchone()[0]
        print(f'  ? {label:<25} {count:>8,} rows')

    # Top customer
    cur.execute('''
        SELECT full_name, country, total_revenue, total_orders
        FROM marts.mart_customer_kpis
        ORDER BY revenue_rank LIMIT 1
    ''')
    top = cur.fetchone()
    print()
    print(f'  ?? Top Customer   : {top[0]} ({top[1]})')
    print(f'     Revenue        :   |  Orders: {top[3]}')

    # Top product
    cur.execute('''
        SELECT product_name, category, total_revenue, units_sold
        FROM marts.mart_product_performance
        ORDER BY revenue_rank LIMIT 1
    ''')
    prod = cur.fetchone()
    print(f'  ?? Top Product    : {prod[0]}')
    print(f'     Category       : {prod[1]}  |  Revenue:   |  Units: {prod[3]}')

    print('=' * 55)
    cur.close()
    conn.close()

# -- Main ------------------------------------------------------------------
if __name__ == '__main__':
    build_staging()
    build_marts()
    validate()
