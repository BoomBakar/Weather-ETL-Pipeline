import os
import requests
from dotenv import load_dotenv

# Load API key from .env file
load_dotenv()
API_KEY = os.getenv("WEATHER_API_KEY")

# Function to fetch weather data
def get_weather(lat, lon):
    if not API_KEY:
        raise ValueError("API key not found. Please set WEATHER_API_KEY in the .env file.")
    
    url = f"https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={API_KEY}"
    response = requests.get(url)
    
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error fetching data: {response.status_code}")
        return None

# Example usage
if __name__ == "__main__":
    latitude = 40.7128   # Example: New York City
    longitude = -74.0060
    weather_data = get_weather(latitude, longitude)
    
    if weather_data:
        print("Weather Data:")
        print(weather_data)
