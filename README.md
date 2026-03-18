# 🛒 E-Commerce ETL Pipeline
### End-to-End Data Engineering Project — Python + PostgreSQL + SQL

![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-4169E1?style=for-the-badge&logo=postgresql&logoColor=white)
![SQL](https://img.shields.io/badge/SQL-4479A1?style=for-the-badge&logo=mysql&logoColor=white)
![Pandas](https://img.shields.io/badge/Pandas-150458?style=for-the-badge&logo=pandas&logoColor=white)

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│              E-COMMERCE ETL PIPELINE                                │
│         3-Layer Medallion Architecture on PostgreSQL                │
└─────────────────────────────────────────────────────────────────────┘

  [Synthetic E-Commerce Data]
  1,000 customers · 200 products · 50,000 orders · 150,231 items
        │
        ▼  Python + Faker (data generation) + psycopg2 (ETL)
  ┌─────────────────────────────────────────────────────┐
  │                  RAW LAYER                          │
  │  raw.customers    raw.products                      │
  │  raw.orders       raw.order_items                   │
  │  • Direct load, no transforms                       │
  │  • 201,431 total rows across 4 tables               │
  └──────────────────────┬──────────────────────────────┘
                         │
                         ▼  SQL Views — cleaning + derived metrics
  ┌─────────────────────────────────────────────────────┐
  │                STAGING LAYER                        │
  │  stg_customers   stg_products                       │
  │  stg_orders      stg_order_items                    │
  │  • Trimmed + normalised columns                     │
  │  • Age group bucketing                              │
  │  • Margin % calculation                             │
  │  • Line total + line profit per item                │
  └──────────────────────┬──────────────────────────────┘
                         │
                         ▼  SQL Aggregations + Window Functions
  ┌──────────────────────┐ ┌─────────────────────────┐ ┌──────────────────────┐
  │  mart_customer_kpis  │ │ mart_product_performance│ │ mart_monthly_revenue │
  │  • 1,000 customers   │ │  • 200 products         │ │  • 104 months        │
  │  • Revenue rank      │ │  • Revenue + units rank │ │  • By country        │
  │  • LTV metrics       │ │  • Margin analysis      │ │  • Profit trends     │
  └──────────────────────┘ └─────────────────────────┘ └──────────────────────┘
```

---

## 📊 Results

| Metric | Value |
|---|---|
| Total Customers | 1,000 |
| Total Products | 200 |
| Total Orders | 50,000 |
| Total Order Items | 150,231 |
| Mart Tables Built | 3 |
| Top Customer Revenue | $314,594 (Brandon Davis — Germany) |
| Top Product Category | Sports |
| Monthly Revenue Rows | 104 |

---

## 🛠️ Tech Stack

| Tool | Purpose |
|---|---|
| **Python 3.11** | Data generation + ETL orchestration |
| **PostgreSQL 18** | Cloud data warehouse (3-schema architecture) |
| **psycopg2** | Python → PostgreSQL connector |
| **Faker** | Realistic synthetic data generation |
| **pandas** | DataFrame transformations |
| **SQL** | Staging views + mart aggregations |

---

## 📁 Project Structure

```
ecommerce-pipeline/
│
├── generate_and_load.py     # Generate synthetic data + load to RAW layer
├── transform.py             # Build staging views + mart tables
└── README.md
```

---

## 🚀 How to Run

### Prerequisites
- Python 3.11+
- PostgreSQL 18
- pip packages: `pip install psycopg2-binary pandas faker`

### Steps

**1. Clone the repo**
```bash
git clone https://github.com/abhinav2627/ecommerce-pipeline.git
cd ecommerce-pipeline
```

**2. Create the database and schemas**
```sql
CREATE DATABASE ecommerce_dw;
\c ecommerce_dw
CREATE SCHEMA raw;
CREATE SCHEMA staging;
CREATE SCHEMA marts;
```

**3. Update credentials in both scripts**
```python
DB = dict(
    host='localhost',
    dbname='ecommerce_dw',
    user='YOUR_USER',
    password='YOUR_PASSWORD',
    port=5432
)
```

**4. Generate and load raw data**
```bash
python generate_and_load.py
```

**5. Build staging + marts**
```bash
python transform.py
```

---

## 🔍 Data Model

### RAW Layer (4 tables)
| Table | Rows | Description |
|---|---|---|
| `raw.customers` | 1,000 | Customer demographics |
| `raw.products` | 200 | Product catalogue with cost + price |
| `raw.orders` | 50,000 | Order headers with status + payment |
| `raw.order_items` | 150,231 | Line items with quantity + discount |

### Staging Layer (4 views)
| View | Description |
|---|---|
| `stg_customers` | Cleaned + age group bucketing |
| `stg_products` | Margin % + gross margin calculated |
| `stg_orders` | Date dimensions extracted (year, month, DOW, hour) |
| `stg_order_items` | Net price + line total + line profit calculated |

### Marts Layer (3 tables)
| Table | Grain | Key Metrics |
|---|---|---|
| `mart_customer_kpis` | 1 row per customer | LTV, orders, profit, revenue rank |
| `mart_product_performance` | 1 row per product | Units sold, revenue, margin, rank |
| `mart_monthly_revenue` | 1 row per month per country | Revenue, profit, AOV trends |

---

## 💡 Key Learnings

- **Star schema** design separates facts (orders, items) from dimensions (customers, products) for clean analytics queries
- **SQL views** for staging = zero storage cost, always fresh, automatically reflect upstream changes
- **psycopg2 executemany** with chunking is the most efficient way to bulk-load data into PostgreSQL
- **Window functions** (RANK() OVER) enable global rankings without subqueries
- **Margin analysis** at the product level reveals which categories drive profit vs just revenue

---

## 🗺️ Portfolio Roadmap

This is **Project 4** of a 6-month data engineering portfolio series:

- ✅ Project 1 — [Financial Transactions Lakehouse](https://github.com/abhinav2627/financial-transactions-lakehouse)
- ✅ Project 2 — [NYC Taxi Real-Time Pipeline](https://github.com/abhinav2627/nyc-taxi-realtime-pipeline)
- ✅ Project 3 — [COVID Data Warehouse](https://github.com/abhinav2627/covid-data-warehouse)
- ✅ Project 4 — E-Commerce ETL Pipeline (Python + PostgreSQL)
- 🔜 Project 5 — Weather API Data Lake (AWS S3 + Lambda + Glue)

---

## 👤 Author

**Abhinav Mandal**
- LinkedIn: [linkedin.com/in/abhinavmandal27](https://linkedin.com/in/abhinavmandal27)
- GitHub: [github.com/abhinav2627](https://github.com/abhinav2627)

---

## 📄 License

MIT License — feel free to use this as a template for your own portfolio.
