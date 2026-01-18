"""
Retail Analytics ETL Pipeline
Extracts data from Superstore CSV, validates with Pandera, loads into PostgreSQL star schema
"""

import pandas as pd
import numpy as np
import pandera as pa
from pandera import Column, Check, DataFrameSchema
from sqlalchemy import create_engine, text
from pathlib import Path
from datetime import datetime

# =============================================
# CONFIGURATION
# =============================================

# Database connection - UPDATE PASSWORD if different
DB_CONFIG = {
    'host': 'localhost',
    'port': '5432',
    'database': 'retaildb',
    'user': 'postgres',
    'password': 'postgres123'  # <-- Change this to your password
}

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
    "Postal Code": Column(nullable=True),  # Some postal codes might be missing
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
# EXTRACT
# =============================================

def extract_data(file_path):
    """Extract data from CSV file"""
    print(f"📂 Extracting data from {file_path}")
    
    df = pd.read_csv(
        file_path,
        parse_dates=["Order Date", "Ship Date"],
        encoding='latin-1'
    )
    
    print(f"   ✅ Loaded {len(df):,} rows, {len(df.columns)} columns")
    return df

# =============================================
# VALIDATE
# =============================================

def validate_data(df):
    """Validate data using Pandera schema"""
    print("🔍 Validating data...")
    
    try:
        validated_df = superstore_schema.validate(df, lazy=True)
        print("   ✅ All validations passed!")
        return validated_df
    except pa.errors.SchemaErrors as e:
        print(f"   ⚠️ Validation errors found: {len(e.failure_cases)} issues")
        print(e.failure_cases.head(10))
        # Continue with cleaning
        return df

# =============================================
# TRANSFORM
# =============================================

def transform_dim_customer(df):
    """Create customer dimension"""
    dim_customer = df[['Customer ID', 'Customer Name', 'Segment']].drop_duplicates()
    dim_customer.columns = ['customer_id', 'customer_name', 'segment']
    print(f"   👤 dim_customer: {len(dim_customer):,} unique customers")
    return dim_customer

def transform_dim_product(df):
    """Create product dimension - keep first occurrence of each product_id"""
    dim_product = df[['Product ID', 'Product Name', 'Category', 'Sub-Category']].drop_duplicates(subset=['Product ID'], keep='first')
    dim_product.columns = ['product_id', 'product_name', 'category', 'sub_category']
    print(f"   📦 dim_product: {len(dim_product):,} unique products")
    return dim_product

def transform_dim_location(df):
    """Create location dimension"""
    dim_location = df[['Postal Code', 'City', 'State', 'Region', 'Country']].drop_duplicates()
    dim_location.columns = ['postal_code', 'city', 'state', 'region', 'country']
    dim_location['postal_code'] = dim_location['postal_code'].astype(str).replace('nan', None)
    print(f"   📍 dim_location: {len(dim_location):,} unique locations")
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
    print(f"   📅 dim_date: {len(dim_date):,} unique dates")
    return dim_date

# =============================================
# LOAD
# =============================================

def get_engine():
    """Create database connection"""
    conn_string = f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
    return create_engine(conn_string)

def load_dimensions(engine, dim_customer, dim_product, dim_location, dim_date):
    """Load dimension tables"""
    print("📤 Loading dimension tables...")
    
    # Clear existing data
    with engine.connect() as conn:
        conn.execute(text("TRUNCATE fact_orders, dim_customer, dim_product, dim_location, dim_date RESTART IDENTITY CASCADE"))
        conn.commit()
    
    dim_customer.to_sql('dim_customer', engine, if_exists='append', index=False)
    dim_product.to_sql('dim_product', engine, if_exists='append', index=False)
    dim_location.to_sql('dim_location', engine, if_exists='append', index=False)
    dim_date.to_sql('dim_date', engine, if_exists='append', index=False)
    
    print("   ✅ Dimensions loaded!")

def load_fact_orders(engine, df, dim_customer, dim_product, dim_location, dim_date):
    """Load fact table with foreign keys"""
    print("📤 Loading fact_orders...")
    
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
    print(f"   ✅ Loaded {len(fact_final):,} orders into fact_orders")

# =============================================
# MAIN ETL PIPELINE
# =============================================

def run_etl():
    """Run the complete ETL pipeline"""
    print("=" * 50)
    print("🚀 RETAIL ANALYTICS ETL PIPELINE")
    print("=" * 50)
    start_time = datetime.now()
    
    # Extract
    df = extract_data(CSV_FILE)
    
    # Validate
    df = validate_data(df)
    
    # Transform
    print("🔄 Transforming data...")
    dim_customer = transform_dim_customer(df)
    dim_product = transform_dim_product(df)
    dim_location = transform_dim_location(df)
    dim_date = transform_dim_date(df)
    
    # Load
    engine = get_engine()
    load_dimensions(engine, dim_customer, dim_product, dim_location, dim_date)
    load_fact_orders(engine, df, dim_customer, dim_product, dim_location, dim_date)
    
    # Summary
    elapsed = (datetime.now() - start_time).total_seconds()
    print("=" * 50)
    print(f"✅ ETL COMPLETE in {elapsed:.2f} seconds")
    print("=" * 50)

if __name__ == "__main__":
    run_etl()