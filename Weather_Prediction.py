# forecast_weather.py

import os
import pandas as pd
import psycopg2
from prophet import Prophet
from dotenv import load_dotenv
from datetime import datetime, timedelta

# Load environment variables
load_dotenv()

DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_PORT = os.getenv("DB_PORT")

# Connect to PostgreSQL
def get_db_connection():
    return psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT
    )

# Fetch historical weather data for a given city_id
def fetch_city_weather_data(city_id):
    conn = get_db_connection()
    query = """
    SELECT (date + time)::timestamp AS datetime, temperature_c
    FROM weather_data
    WHERE city_id = %s
    ORDER BY datetime;
    """
    cursor = conn.cursor()
    cursor.execute(query, (city_id,))
    records = cursor.fetchall()
    df = pd.DataFrame(records, columns=["ds", "y"])  
    conn.close()
    return df

# Forecast temperature for next day using Prophet
def forecast_next_day_temperature(df):
    df = df.rename(columns={"date": "ds", "temp": "y"})
    model = Prophet()
    model.fit(df)
    future = model.make_future_dataframe(periods=1)
    forecast = model.predict(future)
    next_day_forecast = forecast.iloc[-1]
    return round(next_day_forecast['yhat'], 2), next_day_forecast['ds'].date()

def save_forecast_to_db(city_id, forecast_date, forecast_temp):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO weather_forecast (city_id, forecast_date, predicted_temperature_c)
        VALUES (%s, %s, %s)
        ON CONFLICT (city_id, forecast_date) DO UPDATE
        SET predicted_temperature_c = EXCLUDED.predicted_temperature_c;
    """, (city_id, forecast_date, float(forecast_temp)))  # Ensure float is native
    conn.commit()
    conn.close()

def fetch_all_city_ids():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT city_id, city_name FROM cities;")
    cities = cursor.fetchall()
    conn.close()
    return cities

# Main forecast pipeline
def main():
    cities = fetch_all_city_ids()
    for city_id, city_name in cities:
        print(f"Forecasting for {city_name} ({city_id})")
        df = fetch_city_weather_data(city_id)
        if len(df) < 10:
            print(f"Skipping {city_name} due to insufficient data")
            continue
        try:
            forecast_temp, forecast_date = forecast_next_day_temperature(df)
            save_forecast_to_db(city_id, forecast_date, forecast_temp)
        except Exception as e:
            print(f"Failed forecasting {city_name}: {e}")

if __name__ == "__main__":
    main()
