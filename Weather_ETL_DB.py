import os
import requests
import json
import pandas as pd
import psycopg2
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

# Load the API key from .env
load_dotenv()
API_KEY = os.getenv("WEATHER_API_KEY")

DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_PORT = os.getenv("DB_PORT")


# Connect to PostgreSQL
def get_db_connection():
    conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT
        )
    return conn

# Fetch cities from DB
def fetch_cities_from_db():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT city_id, city_name, latitude, longitude FROM cities;")
    cities = cursor.fetchall()
    conn.close()
    return cities

# Fetch weather data for cities
def fetch_weather_data(city):
    params = {
        "lat": city[2],
        "lon": city[3],
        "appid": API_KEY,
        "units": "metric"
    }
    response = requests.get("https://api.openweathermap.org/data/2.5/weather", params=params)
    
    if response.status_code == 200:
        city_weather = response.json()
        return city_weather
    else:
        print(f"Failed to fetch weather for {city[1]}")
        return None

# Insert weather data into the DB
def insert_weather_data_into_db(weather_data, city_id, weather_type_id, weather_description):
    conn = get_db_connection()
    cursor = conn.cursor()
    query = """
        INSERT INTO weather_data (city_id, temperature_c, humidity_percent, wind_speed_mps, wind_direction_deg, weather_type_id, weather_description, date, time)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
    """
    timestamp = weather_data.get("dt")
    dt_pkt = datetime.fromtimestamp(timestamp, timezone.utc).astimezone(timezone(timedelta(hours=5)))  # Pakistan Standard Time (PKT)
    date = dt_pkt.strftime("%Y-%m-%d")
    time = dt_pkt.strftime("%H:%M:%S")
    
    cursor.execute(query, (
        city_id,
        weather_data.get("main", {}).get("temp"),
        weather_data.get("main", {}).get("humidity"),
        weather_data.get("wind", {}).get("speed"),
        weather_data.get("wind", {}).get("deg"),
        weather_type_id,
        weather_description,
        date,
        time
    ))
    conn.commit()
    conn.close()

# Insert weather type into DB (if it doesn't exist)
def insert_weather_type(weather_main):
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute("SELECT weather_type_id FROM weather_types WHERE weather_name = %s;", (weather_main,))
    existing_type = cursor.fetchone()
    
    if existing_type:
        weather_type_id = existing_type[0]
    else:
        cursor.execute("""
            INSERT INTO weather_types (weather_name) 
            VALUES (%s) 
            RETURNING weather_type_id;
        """, (weather_main,))
        weather_type_id = cursor.fetchone()[0]
    
    conn.commit()
    conn.close()
    return weather_type_id

# Save data to CSV and JSON
def save_weather_data_to_files(structured_data):
    # Save to JSON
    with open("pakistani_cities_weather.json", "w") as f:
        json.dump(structured_data, f, indent=2)
    print("Weather data saved to pakistani_cities_weather.json")

    # Save to CSV
    df = pd.DataFrame(structured_data)
    df.to_csv("cleaned_weather_data.csv", index=False)
    print("Cleaned weather data saved to cleaned_weather_data.csv")

# Main function to handle everything
def main():
    cities = fetch_cities_from_db()
    structured_data = fetch_weather_data_parallel(cities)
    print("Weather data fetched and stored in the database.")
    save_weather_data_to_files(structured_data)
    


log_lock = threading.Lock()
log_file_path = "weather_etl_log.txt"

def log_to_file(message):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{timestamp}] {message}\n"
    with log_lock:
        with open(log_file_path, "a", encoding="utf-8") as log_file:
            log_file.write(line)

def fetch_weather_data_sequential(cities):
    structured_data = []

    for idx, city in enumerate(cities, start=1):
        log_to_file(f"Processing ({idx}/{len(cities)}): {city[1]}")
        try:
            weather_data = fetch_weather_data(city)
            if weather_data:
                weather_main = weather_data["weather"][0]["main"]
                weather_description = weather_data["weather"][0]["description"]
                weather_type_id = insert_weather_type(weather_main)

                # Avoid duplicate inserts
                timestamp = weather_data.get("dt")
                date = datetime.fromtimestamp(timestamp, timezone.utc).astimezone(
                    timezone(timedelta(hours=5))
                ).strftime("%Y-%m-%d")

                # if weather_entry_exists(city[0], date):
                #     log_to_file(f"Skipping duplicate for {city[1]} on {date}")
                #     continue

                insert_weather_data_into_db(weather_data, city[0], weather_type_id, weather_description)

                structured_data.append({
                    "city_name": city[1],
                    "temperature_c": weather_data["main"]["temp"],
                    "humidity_percent": weather_data["main"]["humidity"],
                    "wind_speed_mps": weather_data["wind"]["speed"],
                    "weather_main": weather_main,
                    "weather_description": weather_description,
                    "date_pkt": date,
                    "time_pkt": datetime.fromtimestamp(weather_data["dt"]).strftime("%H:%M:%S")
                })
        except Exception as e:
            log_to_file(f"Error processing {city[1]}: {str(e)}")

    return structured_data

def fetch_weather_data_parallel(cities):
    structured_data = []

    log_to_file("==== New ETL Run Started ====\n")
    
    def process_city(index, city):
        city_name = city[1]
        log_to_file(f"Processing ({index + 1}/{len(cities)}): {city_name}")
        try:
            weather_data = fetch_weather_data(city)
            if weather_data:
                weather_main = weather_data["weather"][0]["main"]
                weather_description = weather_data["weather"][0]["description"]
                weather_type_id = insert_weather_type(weather_main)
                insert_weather_data_into_db(weather_data, city[0], weather_type_id, weather_description)

                return {
                    "city_name": city_name,
                    "temperature_c": weather_data["main"]["temp"],
                    "humidity_percent": weather_data["main"]["humidity"],
                    "wind_speed_mps": weather_data["wind"]["speed"],
                    "weather_main": weather_main,
                    "weather_description": weather_description,
                    "date_pkt": datetime.fromtimestamp(weather_data["dt"]).strftime("%Y-%m-%d"),
                    "time_pkt": datetime.fromtimestamp(weather_data["dt"]).strftime("%H:%M:%S")
                }
        except Exception as e:
            log_to_file(f"Error processing {city_name}: {e}")
        return None


    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(process_city, i, city) for i, city in enumerate(cities)]
        for future in as_completed(futures):
            result = future.result()
            if result:
                structured_data.append(result)

    return structured_data

if __name__ == "__main__":
    main()
