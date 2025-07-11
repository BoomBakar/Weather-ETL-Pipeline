import streamlit as st
import pandas as pd
import psycopg2
from sqlalchemy import create_engine, text
import plotly.express as px
from datetime import datetime, timedelta
import os
import urllib.parse
from dotenv import load_dotenv
# Database connection function
load_dotenv()

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

# Fetch weather data from DB for a city
def fetch_weather_data_for_city(city_id):
    engine = get_db_connection()
    query = f"SELECT date, time, temperature_c, humidity_percent, wind_speed_mps FROM weather_data WHERE city_id = {city_id} ORDER BY date DESC"
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
    
    # Fetch weather data for the selected city
    weather_data = fetch_weather_data_for_city(city_id)
    
    if weather_data.empty:
        st.write(f"No data available for {selected_city}.")
    else:
        # Display Current Data
        st.write(f"Weather data for {selected_city}:")
        st.dataframe(weather_data)
        
        # Plot Daily Temperature Trend (Bar Chart)
        st.subheader(f"Temperature Trend for {selected_city} over the Last Week")
        daily_temp_data = weather_data.groupby('date')['temperature_c'].mean().reset_index()
        fig = px.bar(daily_temp_data, x='date', y='temperature_c', title="Daily Average Temperature")
        st.plotly_chart(fig)
        
        # Plot Hourly Temperature Trend (Line Chart)
        st.subheader(f"Hourly Temperature Trend for {selected_city}")
        hourly_temp_data = weather_data.groupby('time')['temperature_c'].mean().reset_index()
        fig2 = px.line(hourly_temp_data, x='time', y='temperature_c', title="Hourly Temperature")
        st.plotly_chart(fig2)
        
        # Plot Humidity Data
        st.subheader(f"Humidity Levels in {selected_city}")
        humidity_data = weather_data.groupby('time')['humidity_percent'].mean().reset_index()
        fig3 = px.line(humidity_data, x='time', y='humidity_percent', title="Hourly Humidity")
        st.plotly_chart(fig3)

        # Plot Wind Speed Data
        st.subheader(f"Wind Speed in {selected_city}")
        wind_speed_data = weather_data.groupby('time')['wind_speed_mps'].mean().reset_index()
        fig4 = px.line(wind_speed_data, x='time', y='wind_speed_mps', title="Hourly Wind Speed")
        st.plotly_chart(fig4)

if __name__ == "__main__":
    display_dashboard()
