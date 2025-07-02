import os
import requests
import json
import pandas as pd
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv

# Load the API key from .env
load_dotenv()
API_KEY = os.getenv("WEATHER_API_KEY")

# List of major Pakistani cities with lat/lon
cities = [
    {"name": "Karachi", "lat": 24.8607, "lon": 67.0011},
    {"name": "Lahore", "lat": 31.5497, "lon": 74.3436},
    {"name": "Islamabad", "lat": 33.6844, "lon": 73.0479},
    {"name": "Rawalpindi", "lat": 33.6007, "lon": 73.0679},
    {"name": "Faisalabad", "lat": 31.4504, "lon": 73.1350},
    {"name": "Multan", "lat": 30.1575, "lon": 71.5249},
    {"name": "Peshawar", "lat": 34.0151, "lon": 71.5805},
    {"name": "Quetta", "lat": 30.1798, "lon": 66.9750},
    {"name": "Sialkot", "lat": 32.4945, "lon": 74.5229},
    {"name": "Gujranwala", "lat": 32.1877, "lon": 74.1945},
    {"name": "Hyderabad", "lat": 25.3960, "lon": 68.3578},
    {"name": "Bahawalpur", "lat": 29.3956, "lon": 71.6836},
    {"name": "Sargodha", "lat": 32.0836, "lon": 72.6711},
    {"name": "Abbottabad", "lat": 34.1688, "lon": 73.2215},
    {"name": "Mirpur", "lat": 33.1483, "lon": 73.7518},
    {"name": "Muzaffarabad", "lat": 34.3700, "lon": 73.4711},
    {"name": "Sukkur", "lat": 27.7052, "lon": 68.8574}
]

BASE_URL = "https://api.openweathermap.org/data/2.5/weather"
weather_data = []
structured_data = []

for city in cities:
    params = {
        "lat": city["lat"],
        "lon": city["lon"],
        "appid": API_KEY,
        "units": "metric"
    }
    response = requests.get(BASE_URL, params=params)

    if response.status_code == 200:
        city_weather = response.json()
        city_weather["city"] = city["name"]
        weather_data.append(city_weather)

        # Standardize and clean with null-safe defaults
        main = city_weather.get("main", {})
        wind = city_weather.get("wind", {})
        weather = city_weather.get("weather", [{}])[0]
        clouds = city_weather.get("clouds", {})
        timestamp = city_weather.get("dt", 0)

        pkt = timezone(timedelta(hours=5))
        dt_pkt = datetime.fromtimestamp(timestamp, timezone.utc).astimezone(pkt)

        structured_data.append({
            "city_name": city.get("name", "Unknown"),
            "temperature_c": main.get("temp", 0.0),
            "humidity_percent": main.get("humidity", 0),
            "wind_speed_mps": wind.get("speed", 0.0),
            "weather_main": weather.get("main", "Unknown"),
            "weather_description": weather.get("description", "Unknown"),
            "date_pkt": dt_pkt.strftime("%Y-%m-%d"),
            "time_pkt": dt_pkt.strftime("%H:%M:%S")
        })
    else:
        print(f"Failed to fetch weather for {city['name']}")

# Save raw data as JSON
with open("pakistani_cities_weather.json", "w") as f:
    json.dump(weather_data, f, indent=2)
print("Raw weather data saved to pakistani_cities_weather.json")

# Save cleaned structured data as CSV
df = pd.DataFrame(structured_data)
df.to_csv("cleaned_weather_data.csv", index=False)
print("Cleaned data saved to cleaned_weather_data.csv")
