"""
Retail Analytics Dashboard
Interactive Streamlit dashboard for sales analysis
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from sqlalchemy import create_engine

# =============================================
# PAGE CONFIG
# =============================================

st.set_page_config(
    page_title="Retail Analytics Dashboard",
    page_icon="📊",
    layout="wide"
)

# =============================================
# DATABASE CONNECTION
# =============================================

import os
from dotenv import load_dotenv

load_dotenv()

@st.cache_resource
def get_engine():
    DATABASE_URL = os.getenv('DATABASE_URL', 'postgresql://postgres:postgres123@localhost:5432/retaildb')
    return create_engine(DATABASE_URL)

@st.cache_data(ttl=600)
def load_data(query):
    engine = get_engine()
    return pd.read_sql(query, engine)

# =============================================
# LOAD DATA FROM VIEWS
# =============================================

@st.cache_data(ttl=600)
def load_all_data():
    data = {
        'kpis': load_data("SELECT * FROM vw_overall_kpis"),
        'daily_sales': load_data("SELECT * FROM vw_daily_sales"),
        'monthly_trend': load_data("SELECT * FROM vw_monthly_trend"),
        'by_region': load_data("SELECT * FROM vw_sales_by_region"),
        'by_category': load_data("SELECT * FROM vw_sales_by_category"),
        'by_segment': load_data("SELECT * FROM vw_sales_by_segment"),
        'top_products': load_data("SELECT * FROM vw_top_products"),
        'top_customers': load_data("SELECT * FROM vw_top_customers")
    }
    return data

# =============================================
# DASHBOARD
# =============================================

def main():
    # Header
    st.title("📊 Retail Analytics Dashboard")
    st.markdown("---")
    
    # Load data
    data = load_all_data()
    kpis = data['kpis'].iloc[0]
    
    # =========================================
    # KPI CARDS
    # =========================================
    
    st.subheader("Key Performance Indicators")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(
            label="Total Sales",
            value=f"${kpis['total_sales']:,.0f}"
        )
    
    with col2:
        st.metric(
            label="Total Profit",
            value=f"${kpis['total_profit']:,.0f}"
        )
    
    with col3:
        st.metric(
            label="Total Orders",
            value=f"{kpis['total_orders']:,}"
        )
    
    with col4:
        st.metric(
            label="Profit Margin",
            value=f"{kpis['profit_margin_pct']}%"
        )
    
    col5, col6, col7, col8 = st.columns(4)
    
    with col5:
        st.metric(
            label="Avg Order Value",
            value=f"${kpis['avg_order_value']:,.2f}"
        )
    
    with col6:
        st.metric(
            label="Total Quantity",
            value=f"{kpis['total_quantity']:,}"
        )
    
    with col7:
        st.metric(
            label="Unique Customers",
            value=f"{kpis['unique_customers']:,}"
        )
    
    with col8:
        st.metric(
            label="Unique Products",
            value=f"{kpis['unique_products']:,}"
        )
    
    st.markdown("---")
    
    # =========================================
    # CHARTS ROW 1
    # =========================================
    
    col_left, col_right = st.columns(2)
    
    with col_left:
        st.subheader("📈 Monthly Sales Trend")
        monthly = data['monthly_trend'].copy()
        monthly['period'] = monthly['year'].astype(str) + '-' + monthly['month'].astype(str).str.zfill(2)
        
        fig = px.line(
            monthly, 
            x='period', 
            y='total_sales',
            markers=True
        )
        fig.update_layout(
            xaxis_title="Month",
            yaxis_title="Sales ($)",
            hovermode='x unified'
        )
        st.plotly_chart(fig, use_container_width=True)
    
    with col_right:
        st.subheader("🌍 Sales by Region")
        fig = px.pie(
            data['by_region'], 
            values='total_sales', 
            names='region',
            hole=0.4
        )
        fig.update_traces(textposition='inside', textinfo='percent+label')
        st.plotly_chart(fig, use_container_width=True)
    
    # =========================================
    # CHARTS ROW 2
    # =========================================
    
    col_left2, col_right2 = st.columns(2)
    
    with col_left2:
        st.subheader("📦 Sales by Category")
        fig = px.bar(
            data['by_category'].groupby('category').agg({'total_sales': 'sum'}).reset_index(),
            x='category',
            y='total_sales',
            color='category'
        )
        fig.update_layout(
            xaxis_title="Category",
            yaxis_title="Sales ($)",
            showlegend=False
        )
        st.plotly_chart(fig, use_container_width=True)
    
    with col_right2:
        st.subheader("👥 Sales by Customer Segment")
        fig = px.bar(
            data['by_segment'],
            x='segment',
            y='total_sales',
            color='segment'
        )
        fig.update_layout(
            xaxis_title="Segment",
            yaxis_title="Sales ($)",
            showlegend=False
        )
        st.plotly_chart(fig, use_container_width=True)
    
    st.markdown("---")
    
    # =========================================
    # TABLES
    # =========================================
    
    col_t1, col_t2 = st.columns(2)
    
    with col_t1:
        st.subheader("🏆 Top 10 Products")
        top_products = data['top_products'][['product_name', 'category', 'total_sales', 'total_profit']].copy()
        top_products['total_sales'] = top_products['total_sales'].apply(lambda x: f"${x:,.0f}")
        top_products['total_profit'] = top_products['total_profit'].apply(lambda x: f"${x:,.0f}")
        top_products.columns = ['Product', 'Category', 'Sales', 'Profit']
        st.dataframe(top_products, use_container_width=True, hide_index=True)
    
    with col_t2:
        st.subheader("🌟 Top 10 Customers")
        top_customers = data['top_customers'][['customer_name', 'segment', 'total_orders', 'total_sales']].copy()
        top_customers['total_sales'] = top_customers['total_sales'].apply(lambda x: f"${x:,.0f}")
        top_customers.columns = ['Customer', 'Segment', 'Orders', 'Sales']
        st.dataframe(top_customers, use_container_width=True, hide_index=True)
    
    st.markdown("---")
    
    # =========================================
    # SUB-CATEGORY ANALYSIS
    # =========================================
    
    st.subheader("📊 Sub-Category Performance")
    
    fig = px.treemap(
        data['by_category'],
        path=['category', 'sub_category'],
        values='total_sales',
        color='profit_margin_pct',
        color_continuous_scale='RdYlGn',
        color_continuous_midpoint=10
    )
    fig.update_layout(height=500)
    st.plotly_chart(fig, use_container_width=True)
    
    # =========================================
    # FOOTER
    # =========================================
    
    st.markdown("---")
    st.markdown(
        """
        <div style='text-align: center; color: gray;'>
            Built by Aaditya Krishna | MS in Data Analytics Engineering
        </div>
        """,
        unsafe_allow_html=True
    )

if __name__ == "__main__":
    main()
