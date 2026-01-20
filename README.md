# 📊 Retail Analytics Dashboard

An end-to-end data analytics project featuring ETL pipeline with data validation, star schema database design, and interactive Streamlit dashboard.

![Python](https://img.shields.io/badge/Python-3.9+-blue?style=for-the-badge&logo=python)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-Database-blue?style=for-the-badge&logo=postgresql)
![Streamlit](https://img.shields.io/badge/Streamlit-Dashboard-red?style=for-the-badge&logo=streamlit)
## 🌐 Live Demo

👉 **[View Live Dashboard](https://retail-analytics-dashboard-xd7epno6dywzpzn4lezqjv.streamlit.app/)**
---

## 🎯 Business Questions Answered

- How do sales and profit trend month-over-month?
- Which regions and categories drive profitability?
- Which products generate high revenue but low margins?
- How do customer segments compare in purchasing behavior?
- What are the top-performing products and customers?

---

## 📈 Key Metrics

| Metric | Value |
|--------|-------|
| Total Sales | $2.29M |
| Total Profit | $286K |
| Profit Margin | 12.47% |
| Total Orders | 9,994 |
| Unique Customers | 793 |
| Unique Products | 1,862 |

---

## 🏗️ Architecture

```
┌─────────────────┐
│  Superstore CSV │
└────────┬────────┘
         ▼
┌─────────────────────────────────┐
│  ETL Pipeline (Python)          │
│  • Pandera schema validation    │
│  • Data quality checks          │
│  • Reconciliation verification  │
└────────┬────────────────────────┘
         ▼
┌─────────────────────────────────┐
│  PostgreSQL Star Schema         │
│  • 1 Fact table (9,994 records) │
│  • 4 Dimension tables           │
│  • 8 KPI Views                  │
└────────┬────────────────────────┘
         ▼
┌─────────────────────────────────┐
│  Streamlit Dashboard            │
│  • Interactive visualizations   │
│  • Real-time filtering          │
│  • Executive-style KPIs         │
└─────────────────────────────────┘
```

---

## ✅ Data Validation & Reconciliation

The ETL pipeline includes comprehensive data quality checks:

| Check Type | Description | Status |
|------------|-------------|--------|
| Schema | Column types, nullability | ✅ Passed |
| Range | Quantity > 0, Discount ∈ [0,1], Sales ≥ 0 | ✅ Passed |
| Uniqueness | Order ID + Product ID combinations | ✅ Verified |
| Business Rule | Ship Date ≥ Order Date | ✅ 0 violations |
| Deduplication | Exact line-item duplicates removed | ✅ 8 handled |

### Reconciliation Results

```
Metric              Source          Database        Match
─────────────────────────────────────────────────────────
Total Sales         $2,297,200.86   $2,297,201.07   ✅
Total Profit        $286,397.02     $286,397.79     ✅
Total Orders        9,994           9,994           ✅
Total Quantity      37,873          37,873          ✅
─────────────────────────────────────────────────────────
Overall Reconciliation: PASSED ✅
```

---

## ⚡ Performance

| Metric | Value |
|--------|-------|
| ETL Runtime | ~2.2 seconds for 9,994 records |
| Dashboard Queries | Pre-aggregated KPI views for sub-second response |
| Data Freshness | Batch refresh via ETL pipeline |

---

## 🗄️ Database Schema

### Star Schema Design

**Fact Table:**
- `fact_orders` — Transaction facts with foreign keys (9,994 records)

**Dimension Tables:**
- `dim_customer` — Customer details (793 records)
- `dim_product` — Product/category info (1,862 records)
- `dim_location` — City/Region/State (632 records)
- `dim_date` — Date breakdowns (1,434 records)

### SQL Views for KPIs

| View | Purpose |
|------|---------|
| `vw_overall_kpis` | Summary metrics |
| `vw_daily_sales` | Daily sales trends |
| `vw_monthly_trend` | Monthly performance |
| `vw_sales_by_region` | Regional breakdown |
| `vw_sales_by_category` | Category analysis |
| `vw_sales_by_segment` | Customer segments |
| `vw_top_products` | Best selling products |
| `vw_top_customers` | Top customers |

### Why This Design?

- **Star Schema:** Optimized for analytical queries, reduces joins
- **Pre-aggregated Views:** Dashboard queries run in milliseconds
- **Dimension Tables:** Enable drill-down analysis without redundant data

---

## 🛠️ Tech Stack

| Component | Technology |
|-----------|------------|
| Language | Python 3.9+ |
| Database | PostgreSQL |
| Data Validation | Pandera |
| Dashboard | Streamlit |
| Visualization | Plotly |
| ORM | SQLAlchemy |

---

## 📂 Project Structure

```
retail-analytics-dashboard/
├── Data/
│   └── Superstore_data.csv      # Source data
├── Source/
│   ├── etl_pipeline.py          # ETL with validation & reconciliation
│   └── dashboard.py             # Streamlit dashboard
├── .env.example                 # Environment template
├── requirements.txt             # Dependencies
└── README.md
```

---

## 🚀 Quick Start

### 1. Clone the repository
```bash
git clone https://github.com/kizamehoshigaki/retail-analytics-dashboard.git
cd retail-analytics-dashboard
```

### 2. Install dependencies
```bash
pip install -r requirements.txt
```

### 3. Set up environment
```bash
cp .env.example .env
# Edit .env with your database credentials
```

### 4. Set up PostgreSQL
- Create database named `retaildb`
- Run the star schema SQL (see `schema.sql`)
- Run the views SQL

### 5. Run ETL Pipeline
```bash
python Source/etl_pipeline.py
```

Expected output:
```
✅ Schema validation: PASSED
✅ Quality checks: PASSED
✅ Reconciliation: PASSED
🎉 ETL COMPLETE!
```

### 6. Launch Dashboard
```bash
streamlit run Source/dashboard.py
```

---

## 📊 Dashboard Features

- **KPI Cards** — Total sales, profit, orders, margin at a glance
- **Monthly Trend** — Sales over time with interactive line chart
- **Regional Analysis** — Sales distribution by region (pie chart)
- **Category Breakdown** — Performance by product category
- **Segment Analysis** — Customer segment comparison
- **Top Products** — Best selling products table
- **Top Customers** — Highest value customers
- **Treemap** — Sub-category performance with profit margins

---

## 🔧 ETL Pipeline Features

- **Schema Validation** — Pandera enforces data types and constraints
- **Quality Checks** — Nulls, duplicates, range violations, business rules
- **Reconciliation** — Source-to-database verification
- **Logging** — Clear console output with status indicators
- **Idempotent** — Safe to re-run without duplicating data

---

## 📝 License

This project is for educational and portfolio purposes.

---

## 👤 Author

**Aaditya Krishna**  
Graduate Student, Data Analytics Engineering  
Northeastern University

- GitHub: [@kizamehoshigaki](https://github.com/kizamehoshigaki)
- LinkedIn: [Aaditya Krishna](https://www.linkedin.com/in/aaditya-krishna/)

---
