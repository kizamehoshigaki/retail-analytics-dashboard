"""
Retail Analytics ETL Pipeline v2
Features: Data validation, reconciliation checks, quality reporting
"""

import pandas as pd
import numpy as np
import pandera as pa
from pandera import Column, Check, DataFrameSchema
from sqlalchemy import create_engine, text
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

# =============================================
# CONFIGURATION
# =============================================

# Database connection from environment variable
DATABASE_URL = os.getenv('DATABASE_URL', 'postgresql://postgres:postgres123@localhost:5432/retaildb')

# File path
DATA_DIR = Path(__file__).parent.parent / "Data"
CSV_FILE = DATA_DIR / "Superstore_data.csv"

# =============================================
# PANDERA VALIDATION SCHEMA
# =============================================

superstore_schema = DataFrameSchema({
    "Order ID": Column(str, nullable=False),
    "Order Date": Column(pa.DateTime, nullable=False),
    "Ship Date": Column(pa.DateTime, nullable=False),
    "Customer ID": Column(str, nullable=False),
    "Customer Name": Column(str, nullable=False),
    "Segment": Column(str, Check.isin(["Consumer", "Corporate", "Home Office"])),
    "City": Column(str, nullable=False),
    "State": Column(str, nullable=False),
    "Postal Code": Column(nullable=True),
    "Region": Column(str, Check.isin(["East", "West", "Central", "South"])),
    "Product ID": Column(str, nullable=False),
    "Category": Column(str, Check.isin(["Furniture", "Office Supplies", "Technology"])),
    "Sub-Category": Column(str, nullable=False),
    "Product Name": Column(str, nullable=False),
    "Sales": Column(float, Check.ge(0)),
    "Quantity": Column(int, Check.gt(0)),
    "Discount": Column(float, Check.in_range(0, 1)),
    "Profit": Column(float),
})

# =============================================
# DATA QUALITY CHECKS
# =============================================

def run_quality_checks(df):
    """Run comprehensive data quality checks"""
    print("\n🔍 DATA QUALITY CHECKS")
    print("=" * 50)
    
    quality_report = {}
    
    # 1. Schema validation
    schema_valid = True
    try:
        superstore_schema.validate(df, lazy=True)
        print("✅ Schema validation: PASSED")
    except pa.errors.SchemaErrors as e:
        schema_valid = False
        print(f"⚠️ Schema validation: {len(e.failure_cases)} issues found")
    quality_report['schema_valid'] = schema_valid
    
    # 2. Null checks
    null_counts = df.isnull().sum()
    critical_nulls = null_counts[['Order ID', 'Customer ID', 'Product ID', 'Sales']].sum()
    print(f"✅ Critical null check: {critical_nulls} nulls in key columns")
    quality_report['critical_nulls'] = int(critical_nulls)
    
    # 3. Duplicate check
    duplicate_rows = df.duplicated(subset=['Order ID', 'Product ID', 'Customer ID', 'Order Date']).sum()
    print(f"✅ Duplicate rows: {duplicate_rows} found")
    quality_report['duplicate_rows'] = int(duplicate_rows)
    
    # 4. Business rule: Ship Date >= Order Date
    ship_before_order = (df['Ship Date'] < df['Order Date']).sum()
    status = "PASSED" if ship_before_order == 0 else f"FAILED ({ship_before_order} violations)"
    print(f"✅ Ship date >= Order date: {status}")
    quality_report['ship_date_violations'] = int(ship_before_order)
    
    # 5. Range checks
    negative_sales = (df['Sales'] < 0).sum()
    invalid_discount = ((df['Discount'] < 0) | (df['Discount'] > 1)).sum()
    invalid_quantity = (df['Quantity'] <= 0).sum()
    print(f"✅ Range checks: Sales<0={negative_sales}, InvalidDiscount={invalid_discount}, Qty<=0={invalid_quantity}")
    quality_report['range_violations'] = int(negative_sales + invalid_discount + invalid_quantity)
    
    # 6. Uniqueness check for Order ID + Row ID combination
    unique_orders = df['Order ID'].nunique()
    total_rows = len(df)
    print(f"✅ Uniqueness: {unique_orders} unique orders, {total_rows} line items")
    quality_report['unique_orders'] = unique_orders
    quality_report['total_rows'] = total_rows
   
    print("=" * 50)
    return quality_report

# =============================================
# RECONCILIATION CHECKS
# =============================================

def run_reconciliation(engine, df):
    """Reconcile source data against loaded database"""
    print("\n📊 RECONCILIATION CHECKS")
    print("=" * 50)
    
    recon_report = {}
    
    # Source metrics
    source_sales = df['Sales'].sum()
    source_profit = df['Profit'].sum()
    source_orders = len(df)
    source_quantity = df['Quantity'].sum()
    
    # Database metrics
    with engine.connect() as conn:
        db_metrics = pd.read_sql("SELECT * FROM vw_overall_kpis", conn).iloc[0]
        
    db_sales = float(db_metrics['total_sales'])
    db_profit = float(db_metrics['total_profit'])
    db_orders = int(db_metrics['total_orders'])
    db_quantity = int(db_metrics['total_quantity'])
    
    # Compare (tolerance of $1 for floating-point precision)
    sales_match = abs(source_sales - db_sales) < 1.00
    profit_match = abs(source_profit - db_profit) < 1.00
    orders_match = source_orders == db_orders
    quantity_match = source_quantity == db_quantity
    
    print(f"{'Metric':<20} {'Source':>15} {'Database':>15} {'Match':>10}")
    print("-" * 60)
    print(f"{'Total Sales':<20} ${source_sales:>14,.2f} ${db_sales:>14,.2f} {'✅' if sales_match else '❌':>10}")
    print(f"{'Total Profit':<20} ${source_profit:>14,.2f} ${db_profit:>14,.2f} {'✅' if profit_match else '❌':>10}")
    print(f"{'Total Orders':<20} {source_orders:>15,} {db_orders:>15,} {'✅' if orders_match else '❌':>10}")
    print(f"{'Total Quantity':<20} {source_quantity:>15,} {db_quantity:>15,} {'✅' if quantity_match else '❌':>10}")
    print("=" * 50)
    
    all_match = all([sales_match, profit_match, orders_match, quantity_match])
    print(f"🎯 Overall Reconciliation: {'PASSED ✅' if all_match else 'FAILED ❌'}")
    
    recon_report = {
        'sales_match': sales_match,
        'profit_match': profit_match,
        'orders_match': orders_match,
        'quantity_match': quantity_match,
        'all_passed': all_match
    }
    
    return recon_report

# =============================================
# EXTRACT
# =============================================

def extract_data(file_path):
    """Extract data from CSV file"""
    print(f"\n📂 EXTRACT")
    print("=" * 50)
    print(f"Source: {file_path}")
    
    df = pd.read_csv(
        file_path,
        parse_dates=["Order Date", "Ship Date"],
        encoding='latin-1'
    )
    
    print(f"✅ Loaded {len(df):,} rows, {len(df.columns)} columns")
    print(f"   Date range: {df['Order Date'].min().date()} to {df['Order Date'].max().date()}")
    print(f"   Regions: {df['Region'].nunique()} | Categories: {df['Category'].nunique()}")
    return df

# =============================================
# TRANSFORM
# =============================================

def transform_dim_customer(df):
    """Create customer dimension"""
    dim_customer = df[['Customer ID', 'Customer Name', 'Segment']].drop_duplicates(subset=['Customer ID'], keep='first')
    dim_customer.columns = ['customer_id', 'customer_name', 'segment']
    return dim_customer

def transform_dim_product(df):
    """Create product dimension - keep first occurrence of each product_id"""
    dim_product = df[['Product ID', 'Product Name', 'Category', 'Sub-Category']].drop_duplicates(subset=['Product ID'], keep='first')
    dim_product.columns = ['product_id', 'product_name', 'category', 'sub_category']
    return dim_product

def transform_dim_location(df):
    """Create location dimension"""
    dim_location = df[['Postal Code', 'City', 'State', 'Region', 'Country']].drop_duplicates()
    dim_location.columns = ['postal_code', 'city', 'state', 'region', 'country']
    dim_location['postal_code'] = dim_location['postal_code'].astype(str).replace('nan', None)
    return dim_location

def transform_dim_date(df):
    """Create date dimension from all dates in dataset"""
    all_dates = pd.concat([df['Order Date'], df['Ship Date']]).drop_duplicates()
    
    dim_date = pd.DataFrame({'full_date': all_dates})
    dim_date['year'] = dim_date['full_date'].dt.year
    dim_date['quarter'] = dim_date['full_date'].dt.quarter
    dim_date['month'] = dim_date['full_date'].dt.month
    dim_date['month_name'] = dim_date['full_date'].dt.month_name()
    dim_date['week'] = dim_date['full_date'].dt.isocalendar().week.astype(int)
    dim_date['day_of_week'] = dim_date['full_date'].dt.dayofweek
    dim_date['day_name'] = dim_date['full_date'].dt.day_name()
    dim_date['is_weekend'] = dim_date['day_of_week'].isin([5, 6])
    
    dim_date = dim_date.sort_values('full_date').reset_index(drop=True)
    return dim_date

def transform_all(df):
    """Run all transformations"""
    print(f"\n🔄 TRANSFORM")
    print("=" * 50)
    
    dim_customer = transform_dim_customer(df)
    dim_product = transform_dim_product(df)
    dim_location = transform_dim_location(df)
    dim_date = transform_dim_date(df)
    
    print(f"   👤 dim_customer: {len(dim_customer):,} unique customers")
    print(f"   📦 dim_product: {len(dim_product):,} unique products")
    print(f"   📍 dim_location: {len(dim_location):,} unique locations")
    print(f"   📅 dim_date: {len(dim_date):,} unique dates")
    
    return dim_customer, dim_product, dim_location, dim_date

# =============================================
# LOAD
# =============================================

def get_engine():
    """Create database connection"""
    return create_engine(DATABASE_URL)

def load_dimensions(engine, dim_customer, dim_product, dim_location, dim_date):
    """Load dimension tables"""
    print(f"\n📤 LOAD DIMENSIONS")
    print("=" * 50)
    
    # Clear existing data
    with engine.connect() as conn:
        conn.execute(text("DELETE FROM fact_orders"))
        conn.execute(text("DELETE FROM dim_customer"))
        conn.execute(text("DELETE FROM dim_product"))
        conn.execute(text("DELETE FROM dim_location"))
        conn.execute(text("DELETE FROM dim_date"))
        conn.execute(text("ALTER SEQUENCE dim_customer_customer_key_seq RESTART WITH 1"))
        conn.execute(text("ALTER SEQUENCE dim_product_product_key_seq RESTART WITH 1"))
        conn.execute(text("ALTER SEQUENCE dim_location_location_key_seq RESTART WITH 1"))
        conn.execute(text("ALTER SEQUENCE dim_date_date_key_seq RESTART WITH 1"))
        conn.commit()
    
    dim_customer.to_sql('dim_customer', engine, if_exists='append', index=False)
    dim_product.to_sql('dim_product', engine, if_exists='append', index=False)
    dim_location.to_sql('dim_location', engine, if_exists='append', index=False)
    dim_date.to_sql('dim_date', engine, if_exists='append', index=False)
    
    print("✅ All dimensions loaded!")

def load_fact_orders(engine, df):
    """Load fact table with foreign keys"""
    print(f"\n📤 LOAD FACTS")
    print("=" * 50)
    
    # Get key mappings from database
    with engine.connect() as conn:
        customer_keys = pd.read_sql("SELECT customer_key, customer_id FROM dim_customer", conn)
        product_keys = pd.read_sql("SELECT product_key, product_id FROM dim_product", conn)
        location_keys = pd.read_sql("SELECT location_key, postal_code, city FROM dim_location", conn)
        date_keys = pd.read_sql("SELECT date_key, full_date FROM dim_date", conn)
    
    # Prepare fact table
    fact_orders = df[['Order ID', 'Order Date', 'Ship Date', 'Customer ID', 'Product ID', 
                      'Postal Code', 'City', 'Sales', 'Quantity', 'Discount', 'Profit', 'Ship Mode']].copy()
    
    fact_orders.columns = ['order_id', 'order_date', 'ship_date', 'customer_id', 'product_id',
                           'postal_code', 'city', 'sales', 'quantity', 'discount', 'profit', 'ship_mode']
    
    # Convert postal_code to string for matching
    fact_orders['postal_code'] = fact_orders['postal_code'].astype(str).replace('nan', None)
    location_keys['postal_code'] = location_keys['postal_code'].astype(str).replace('nan', None)
    
    # Map foreign keys
    fact_orders = fact_orders.merge(customer_keys, on='customer_id', how='left')
    fact_orders = fact_orders.merge(product_keys, on='product_id', how='left')
    fact_orders = fact_orders.merge(location_keys, on=['postal_code', 'city'], how='left')
    
    # Map date keys
    date_keys['full_date'] = pd.to_datetime(date_keys['full_date'])
    order_date_keys = date_keys.rename(columns={'date_key': 'order_date_key', 'full_date': 'order_date'})
    ship_date_keys = date_keys.rename(columns={'date_key': 'ship_date_key', 'full_date': 'ship_date'})
    
    fact_orders = fact_orders.merge(order_date_keys, on='order_date', how='left')
    fact_orders = fact_orders.merge(ship_date_keys, on='ship_date', how='left')
    
    # Select final columns
    fact_final = fact_orders[['order_id', 'order_date_key', 'ship_date_key', 'customer_key', 
                              'product_key', 'location_key', 'sales', 'quantity', 'discount', 
                              'profit', 'ship_mode']]
    
    fact_final.to_sql('fact_orders', engine, if_exists='append', index=False)
    print(f"✅ Loaded {len(fact_final):,} orders into fact_orders")

# =============================================
# MAIN ETL PIPELINE
# =============================================

def run_etl():
    """Run the complete ETL pipeline with quality checks"""
    print("\n" + "=" * 60)
    print("🚀 RETAIL ANALYTICS ETL PIPELINE v2")
    print("=" * 60)
    start_time = datetime.now()
    
    # Extract
    df = extract_data(CSV_FILE)
    
    # Quality Checks
    quality_report = run_quality_checks(df)
    
    # Transform
    dim_customer, dim_product, dim_location, dim_date = transform_all(df)
    
    # Load
    engine = get_engine()
    load_dimensions(engine, dim_customer, dim_product, dim_location, dim_date)
    load_fact_orders(engine, df)
    
    # Reconciliation
    recon_report = run_reconciliation(engine, df)
    
    # Summary
    elapsed = (datetime.now() - start_time).total_seconds()
    
    print("\n" + "=" * 60)
    print("📋 ETL SUMMARY")
    print("=" * 60)
    print(f"   ⏱️  Runtime: {elapsed:.2f} seconds")
    print(f"   📊 Records processed: {len(df):,}")
    print(f"   ✅ Quality checks: {'PASSED' if quality_report['range_violations'] == 0 else 'ISSUES FOUND'}")
    print(f"   ✅ Reconciliation: {'PASSED' if recon_report['all_passed'] else 'FAILED'}")
    print("=" * 60)
    print("🎉 ETL COMPLETE!")
    print("=" * 60)

if __name__ == "__main__":
    run_etl()
