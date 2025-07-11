import streamlit as st
import pandas as pd
import psycopg2
from sqlalchemy import create_engine, text
import plotly.express as px
from datetime import datetime, timedelta
import os
import urllib.parse
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Database connection function
def get_db_connection():
    user = os.getenv("DB_USER")
    password = urllib.parse.quote_plus(os.getenv("DB_PASSWORD"))
    host = os.getenv("DB_HOST")
    port = os.getenv("DB_PORT")
    db = os.getenv("DB_NAME")
    return create_engine(f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}")

# Fetch cities from DB
def fetch_cities_from_db():
    engine = get_db_connection()
    
    # Wrap the query in 'text()' to make it executable
    query = text("SELECT city_id, city_name FROM cities")
    
    # Fetch results using the execute() method
    with engine.connect() as conn:
        result = conn.execute(query).fetchall()
    
    return result

# Fetch all available dates for a city
def fetch_all_dates_for_city(city_id):
    engine = get_db_connection()
    query = f"""
    SELECT DISTINCT date
    FROM weather_data
    WHERE city_id = {city_id}
    ORDER BY date DESC
    """
    df = pd.read_sql(query, engine)
    return df['date'].tolist()

# Fetch weather data for a city and a specific date
def fetch_weather_data_for_city_and_date(city_id, selected_date):
    engine = get_db_connection()
    query = f"""
    SELECT date, time, temperature_c, humidity_percent, wind_speed_mps 
    FROM weather_data
    WHERE city_id = {city_id} AND date = '{selected_date}'
    ORDER BY time
    """
    df = pd.read_sql(query, engine)
    return df

# Display the dashboard
def display_dashboard():
    st.title("Weather Dashboard")
    
    # Fetch cities from the database
    cities = fetch_cities_from_db()
    
    # Dropdown to select city
    selected_city = st.selectbox("Select a city", [city[1] for city in cities])
    
    # Get the selected city's ID
    city_id = next(city[0] for city in cities if city[1] == selected_city)
    
    # Fetch all available dates for the selected city
    available_dates = fetch_all_dates_for_city(city_id)
    
    # Dropdown to select the date
    selected_date = st.selectbox("Select a date", available_dates)
    
    # Fetch weather data for the selected city and date
    weather_data = fetch_weather_data_for_city_and_date(city_id, selected_date)
    
    if weather_data.empty:
        st.write(f"No data available for {selected_city} on {selected_date}.")
    else:
        # Display Weather Data for the selected day
        st.write(f"Weather data for {selected_city} on {selected_date}:")
        st.dataframe(weather_data)
        
        # Plot Hourly Temperature Trend (Line Chart)
        st.subheader(f"Hourly Temperature Trend for {selected_city} on {selected_date}")
        hourly_temp_data = weather_data[['time', 'temperature_c']]
        fig = px.line(hourly_temp_data, x='time', y='temperature_c', title="Hourly Temperature")
        st.plotly_chart(fig)
        
        # Plot Hourly Humidity Data
        st.subheader(f"Hourly Humidity for {selected_city} on {selected_date}")
        hourly_humidity_data = weather_data[['time', 'humidity_percent']]
        fig2 = px.line(hourly_humidity_data, x='time', y='humidity_percent', title="Hourly Humidity")
        st.plotly_chart(fig2)
        
        # Plot Hourly Wind Speed Data
        st.subheader(f"Hourly Wind Speed for {selected_city} on {selected_date}")
        hourly_wind_speed_data = weather_data[['time', 'wind_speed_mps']]
        fig3 = px.line(hourly_wind_speed_data, x='time', y='wind_speed_mps', title="Hourly Wind Speed")
        st.plotly_chart(fig3)

if __name__ == "__main__":
    display_dashboard()
