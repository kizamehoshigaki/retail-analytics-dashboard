# 📊 Retail Analytics Dashboard

An end-to-end data analytics project featuring ETL pipeline, star schema database design, and interactive Streamlit dashboard for retail sales analysis.

![Dashboard Preview](https://img.shields.io/badge/Streamlit-Dashboard-red?style=for-the-badge&logo=streamlit)
![Python](https://img.shields.io/badge/Python-3.9+-blue?style=for-the-badge&logo=python)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-Database-blue?style=for-the-badge&logo=postgresql)

## 🎯 Project Overview

This project demonstrates a complete data analytics workflow:
- **ETL Pipeline** with data validation using Pandera
- **Star Schema** database design in PostgreSQL
- **Interactive Dashboard** built with Streamlit and Plotly

## 📈 Key Metrics

| Metric | Value |
|--------|-------|
| Total Sales | $2.29M |
| Total Profit | $286K |
| Profit Margin | 12.47% |
| Total Orders | 9,994 |
| Unique Customers | 793 |
| Unique Products | 1,862 |

## 🏗️ Architecture

```
Superstore CSV
      ↓
ETL Pipeline (Python + Pandera Validation)
      ↓
PostgreSQL Star Schema
      ↓
SQL Views (Pre-aggregated KPIs)
      ↓
Streamlit Dashboard
```

## 🗄️ Database Schema

**Star Schema Design:**
- `fact_orders` - Transaction facts (9,994 records)
- `dim_customer` - Customer dimension (793 records)
- `dim_product` - Product dimension (1,862 records)
- `dim_location` - Location dimension (632 records)
- `dim_date` - Date dimension (1,434 records)

**SQL Views for KPIs:**
- `vw_overall_kpis` - Summary metrics
- `vw_daily_sales` - Daily sales trends
- `vw_monthly_trend` - Monthly performance
- `vw_sales_by_region` - Regional breakdown
- `vw_sales_by_category` - Category analysis
- `vw_sales_by_segment` - Customer segments
- `vw_top_products` - Best selling products
- `vw_top_customers` - Top customers

## 🛠️ Tech Stack

| Component | Technology |
|-----------|------------|
| Language | Python 3.9+ |
| Database | PostgreSQL |
| ETL Validation | Pandera |
| Dashboard | Streamlit |
| Visualization | Plotly |
| ORM | SQLAlchemy |

## 📂 Project Structure

```
retail-analytics-dashboard/
├── Data/
│   └── Superstore_data.csv
├── Source/
│   ├── etl_pipeline.py      # ETL with Pandera validation
│   └── dashboard.py         # Streamlit dashboard
├── requirements.txt
└── README.md
```

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

### 3. Set up PostgreSQL
- Create database named `retaildb`
- Update password in `etl_pipeline.py` (line 22)

### 4. Run ETL Pipeline
```bash
python Source/etl_pipeline.py
```

### 5. Launch Dashboard
```bash
streamlit run Source/dashboard.py
```

## 📊 Dashboard Features

- **KPI Cards** - Total sales, profit, orders, margin
- **Monthly Trend** - Sales over time with line chart
- **Regional Analysis** - Sales distribution by region (pie chart)
- **Category Breakdown** - Performance by product category
- **Segment Analysis** - Customer segment comparison
- **Top Products** - Best selling products table
- **Top Customers** - Highest value customers
- **Treemap** - Sub-category performance with profit margins

## 🔧 ETL Pipeline Features

- **Data Validation** using Pandera schemas
- **Automated dimension loading** with deduplication
- **Foreign key mapping** for fact table
- **Error handling** and logging
- **Fast bulk loading** (~1 second for 10K records)

## 📝 License

This project is for educational and portfolio purposes.

## 👤 Author

**Aaditya Krishna**
- GitHub: [@kizamehoshigaki](https://github.com/kizamehoshigaki)
- LinkedIn: [Aaditya Krishna](https://www.linkedin.com/in/aaditya-krishna/)
