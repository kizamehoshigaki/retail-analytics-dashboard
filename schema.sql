-- =============================================
-- RETAIL ANALYTICS - STAR SCHEMA + VIEWS
-- Run this SQL to set up the database
-- =============================================

-- =============================================
-- DIMENSION TABLES
-- =============================================

DROP TABLE IF EXISTS fact_orders CASCADE;
DROP TABLE IF EXISTS dim_customer CASCADE;
DROP TABLE IF EXISTS dim_product CASCADE;
DROP TABLE IF EXISTS dim_location CASCADE;
DROP TABLE IF EXISTS dim_date CASCADE;

CREATE TABLE dim_customer (
    customer_key SERIAL PRIMARY KEY,
    customer_id VARCHAR(20) UNIQUE NOT NULL,
    customer_name VARCHAR(100),
    segment VARCHAR(50)
);

CREATE TABLE dim_product (
    product_key SERIAL PRIMARY KEY,
    product_id VARCHAR(20) UNIQUE NOT NULL,
    product_name VARCHAR(255),
    category VARCHAR(50),
    sub_category VARCHAR(50)
);

CREATE TABLE dim_location (
    location_key SERIAL PRIMARY KEY,
    postal_code VARCHAR(20),
    city VARCHAR(100),
    state VARCHAR(100),
    region VARCHAR(50),
    country VARCHAR(100),
    UNIQUE(postal_code, city)
);

CREATE TABLE dim_date (
    date_key SERIAL PRIMARY KEY,
    full_date DATE UNIQUE NOT NULL,
    year INT,
    quarter INT,
    month INT,
    month_name VARCHAR(20),
    week INT,
    day_of_week INT,
    day_name VARCHAR(20),
    is_weekend BOOLEAN
);

-- =============================================
-- FACT TABLE
-- =============================================

CREATE TABLE fact_orders (
    order_key SERIAL PRIMARY KEY,
    order_id VARCHAR(20) NOT NULL,
    order_date_key INT REFERENCES dim_date(date_key),
    ship_date_key INT REFERENCES dim_date(date_key),
    customer_key INT REFERENCES dim_customer(customer_key),
    product_key INT REFERENCES dim_product(product_key),
    location_key INT REFERENCES dim_location(location_key),
    sales DECIMAL(12,2),
    quantity INT,
    discount DECIMAL(5,2),
    profit DECIMAL(12,2),
    ship_mode VARCHAR(50)
);

-- Indexes for performance
CREATE INDEX idx_fact_order_date ON fact_orders(order_date_key);
CREATE INDEX idx_fact_customer ON fact_orders(customer_key);
CREATE INDEX idx_fact_product ON fact_orders(product_key);
CREATE INDEX idx_fact_location ON fact_orders(location_key);

-- =============================================
-- KPI VIEWS
-- =============================================

-- Daily Sales
CREATE OR REPLACE VIEW vw_daily_sales AS
SELECT 
    d.full_date, d.year, d.month, d.month_name, d.day_name, d.is_weekend,
    COUNT(f.order_key) AS total_orders,
    SUM(f.sales) AS total_sales,
    SUM(f.profit) AS total_profit,
    SUM(f.quantity) AS total_quantity,
    ROUND(AVG(f.discount)::numeric, 2) AS avg_discount
FROM fact_orders f
JOIN dim_date d ON f.order_date_key = d.date_key
GROUP BY d.full_date, d.year, d.month, d.month_name, d.day_name, d.is_weekend;

-- Sales by Region
CREATE OR REPLACE VIEW vw_sales_by_region AS
SELECT 
    l.region,
    COUNT(f.order_key) AS total_orders,
    SUM(f.sales) AS total_sales,
    SUM(f.profit) AS total_profit,
    ROUND(SUM(f.profit) / NULLIF(SUM(f.sales), 0) * 100, 2) AS profit_margin_pct
FROM fact_orders f
JOIN dim_location l ON f.location_key = l.location_key
GROUP BY l.region;

-- Sales by Category
CREATE OR REPLACE VIEW vw_sales_by_category AS
SELECT 
    p.category, p.sub_category,
    COUNT(f.order_key) AS total_orders,
    SUM(f.sales) AS total_sales,
    SUM(f.profit) AS total_profit,
    SUM(f.quantity) AS total_quantity,
    ROUND(SUM(f.profit) / NULLIF(SUM(f.sales), 0) * 100, 2) AS profit_margin_pct
FROM fact_orders f
JOIN dim_product p ON f.product_key = p.product_key
GROUP BY p.category, p.sub_category;

-- Sales by Segment
CREATE OR REPLACE VIEW vw_sales_by_segment AS
SELECT 
    c.segment,
    COUNT(DISTINCT c.customer_key) AS total_customers,
    COUNT(f.order_key) AS total_orders,
    SUM(f.sales) AS total_sales,
    SUM(f.profit) AS total_profit,
    ROUND(SUM(f.sales) / COUNT(DISTINCT c.customer_key), 2) AS avg_sales_per_customer
FROM fact_orders f
JOIN dim_customer c ON f.customer_key = c.customer_key
GROUP BY c.segment;

-- Monthly Trend
CREATE OR REPLACE VIEW vw_monthly_trend AS
SELECT 
    d.year, d.month, d.month_name,
    COUNT(f.order_key) AS total_orders,
    SUM(f.sales) AS total_sales,
    SUM(f.profit) AS total_profit,
    COUNT(DISTINCT f.customer_key) AS unique_customers
FROM fact_orders f
JOIN dim_date d ON f.order_date_key = d.date_key
GROUP BY d.year, d.month, d.month_name;

-- Top 10 Products
CREATE OR REPLACE VIEW vw_top_products AS
SELECT 
    p.product_name, p.category, p.sub_category,
    SUM(f.sales) AS total_sales,
    SUM(f.profit) AS total_profit,
    SUM(f.quantity) AS total_quantity
FROM fact_orders f
JOIN dim_product p ON f.product_key = p.product_key
GROUP BY p.product_name, p.category, p.sub_category
ORDER BY total_sales DESC
LIMIT 10;

-- Top 10 Customers
CREATE OR REPLACE VIEW vw_top_customers AS
SELECT 
    c.customer_name, c.segment,
    COUNT(f.order_key) AS total_orders,
    SUM(f.sales) AS total_sales,
    SUM(f.profit) AS total_profit
FROM fact_orders f
JOIN dim_customer c ON f.customer_key = c.customer_key
GROUP BY c.customer_name, c.segment
ORDER BY total_sales DESC
LIMIT 10;

-- Overall KPIs
CREATE OR REPLACE VIEW vw_overall_kpis AS
SELECT 
    COUNT(*) AS total_orders,
    SUM(sales) AS total_sales,
    SUM(profit) AS total_profit,
    SUM(quantity) AS total_quantity,
    ROUND(AVG(sales)::numeric, 2) AS avg_order_value,
    ROUND(SUM(profit) / NULLIF(SUM(sales), 0) * 100, 2) AS profit_margin_pct,
    COUNT(DISTINCT customer_key) AS unique_customers,
    COUNT(DISTINCT product_key) AS unique_products
FROM fact_orders;

-- Discount Impact Analysis (A/B Style)
CREATE OR REPLACE VIEW vw_discount_impact AS
SELECT 
    CASE 
        WHEN discount = 0 THEN 'No Discount'
        WHEN discount <= 0.1 THEN 'Low (1-10%)'
        WHEN discount <= 0.2 THEN 'Medium (11-20%)'
        ELSE 'High (>20%)'
    END AS discount_bucket,
    COUNT(*) AS total_orders,
    ROUND(SUM(sales)::numeric, 2) AS total_sales,
    ROUND(SUM(profit)::numeric, 2) AS total_profit,
    ROUND(AVG(profit)::numeric, 2) AS avg_profit_per_order,
    ROUND(SUM(profit) / NULLIF(SUM(sales), 0) * 100, 2) AS profit_margin_pct
FROM fact_orders
GROUP BY 1
ORDER BY 1;
