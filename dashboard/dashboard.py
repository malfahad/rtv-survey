import streamlit as st
import plotly.express as px
import pandas as pd
import psycopg2
from sqlalchemy import create_engine
import streamlit as st
import os

# Initialize connection
@st.cache_resource
def init_connection():
    return create_engine(os.getenv('DATABASE_URL'))

# Load data
@st.cache_data(ttl=600)
def load_data():
    conn = init_connection()
    query = """
    SELECT * FROM metrics.survey_summary
    """
    return pd.read_sql_query(query, conn)


#
# Create dashboard
st.title("RTV Household Survey Dashboard")

# Sidebar filters
st.sidebar.header("Filters")
region = st.sidebar.selectbox("Select Region", ["All", "Region1", "Region2"])

# Load data
survey_data = load_data()


# Metrics overview
st.header("Key Metrics")
col1, col2, col3 = st.columns(3)
with col1:
    st.metric("Total Households", survey_data['hhid_2_nunique'].nunique())
with col2:
    mean_poverty_rate = survey_data['hhh_read_write_mean'].mean()
    if pd.isna(mean_poverty_rate):  
        st.metric("Poverty Rate", "N/A")
    else:
        st.metric("Poverty Rate", f"{mean_poverty_rate:.2%}")
with col3:
    mean_average_income = survey_data['assets_reported_total_mean'].mean()
    if pd.isna(mean_average_income):
        st.metric("Average Income", "N/A")
    else:
        st.metric("Average Income", f"UGX {mean_average_income:,.2f}")

# Visualizations
st.header("Trend Analysis")

# Income distribution
fig_income = px.histogram(
    survey_data,
    x="assets_reported_total_mean",
    nbins=50,
    title="Household Income Distribution"
)
st.plotly_chart(fig_income)

# Poverty status by region
fig_poverty = px.bar(
    survey_data.groupby(['district_', 'hhh_read_write_mean']).size().reset_index(name='count'),
    x='district_',
    y='count',
    color='hhh_read_write_mean',
    title="Poverty Status by Region"
)
st.plotly_chart(fig_poverty)

# Download data
@st.cache_data
def convert_df(df):
    return df.to_csv().encode('utf-8')

st.download_button(
    label="Download data as CSV",
    data=convert_df(survey_data),
    file_name='survey_data.csv',
    mime='text/csv',
)
