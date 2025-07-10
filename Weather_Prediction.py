import os
import pandas as pd
from prophet import Prophet
from dotenv import load_dotenv
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
import urllib.parse
import numpy as np

# Load environment variables
load_dotenv()

# Setup database engine
def get_db_engine():
    user = os.getenv("DB_USER")
    password = urllib.parse.quote_plus(os.getenv("DB_PASSWORD"))
    host = os.getenv("DB_HOST")
    port = os.getenv("DB_PORT")
    db = os.getenv("DB_NAME")
    return create_engine(f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}")

def fetch_cities(engine):
    query = "SELECT city_id, city_name FROM cities;"
    with engine.connect() as conn:
        df = pd.read_sql(query, conn)
    return list(df.itertuples(index=False, name=None))


# Fetch city temperature data at 3 PM for past 7 days
def fetch_city_temperature_data(city_id, engine):
    query = text("""
        SELECT date, time, temperature_c
        FROM weather_data
        WHERE city_id = :city_id
          AND date >= CURRENT_DATE - INTERVAL '7 days'
        ORDER BY date, time;
    """)
    
    df = pd.read_sql(query, engine, params={"city_id": city_id})

    if df.empty:
        return pd.DataFrame()

    # Combine date and time
    df['datetime'] = pd.to_datetime(df['date'].astype(str) + ' ' + df['time'].astype(str))

    # Keep only rows within 1-hour window around 3 PM
    df = df[
        (df['datetime'].dt.time >= datetime.strptime("15:00", "%H:%M").time()) &
        (df['datetime'].dt.time <= datetime.strptime("18:30", "%H:%M").time())
    ]

    # Rename for Prophet
    df = df[['datetime', 'temperature_c']].rename(columns={"datetime": "ds", "temperature_c": "y"})


    return df


def save_forecast_to_db(engine, city_id, forecast_date, forecast_time, forecast_temp):
    # Ensure forecast_temp is a native Python float (not np.float64)
    if isinstance(forecast_temp, (np.floating, np.float64)):
        forecast_temp = float(forecast_temp)

    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO weather_forecast (city_id, forecast_date, forecast_time, predicted_temperature_c)
            VALUES (:city_id, :forecast_date, :forecast_time, :predicted_temperature_c)
            ON CONFLICT (city_id, forecast_date, forecast_time) DO UPDATE
            SET predicted_temperature_c = EXCLUDED.predicted_temperature_c;
        """), {
            "city_id": city_id,
            "forecast_date": forecast_date,
            "forecast_time": forecast_time,
            "predicted_temperature_c": forecast_temp
        })

# Forecast function
def forecast_temperature_at_3pm(city_id, city_name, engine):
    df = fetch_city_temperature_data(city_id, engine)

    if len(df) < 5:
        print(f"Not enough data to forecast for {city_name}")
        return

    model = Prophet()
    model.fit(df)

    # Generate forecast for 3 PM tomorrow
    next_day_5pm = datetime.now().replace(hour=17, minute=0, second=0, microsecond=0) + timedelta(days=1)
    future = pd.DataFrame({"ds": [next_day_5pm]})
    forecast = model.predict(future)

    predicted_temp = round(forecast.iloc[0]['yhat'], 2)

    save_forecast_to_db(engine, city_id, next_day_5pm.date(), next_day_5pm.time(), predicted_temp)

# Main execution
if __name__ == "__main__":
    engine = get_db_engine()

    cities = fetch_cities(engine)

    for city_id, city_name in cities:
        try:
            forecast_temperature_at_3pm(city_id, city_name, engine)
        except Exception as e:
            print(f"Failed forecasting {city_name}: {e}")
