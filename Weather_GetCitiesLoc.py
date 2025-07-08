import requests
import psycopg2
from dotenv import load_dotenv
import os

load_dotenv()

# DB connection
conn = psycopg2.connect(
    dbname=os.getenv("DB_NAME"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD"),
    host=os.getenv("DB_HOST"),
    port=os.getenv("DB_PORT")
)

# GeoNames API configuration
GEONAMES_USERNAME = os.getenv("GEONAMES_USER")  # Set this in your .env
url = f"http://api.geonames.org/searchJSON?country=PK&maxRows=500&featureClass=P&username={GEONAMES_USERNAME}"

response = requests.get(url)
data = response.json()
cities = data.get("geonames", [])

# Insert into DB
with conn.cursor() as cur:
    cur.execute("DELETE FROM cities;")  # Optional: clears old records
    insert_sql = """
    INSERT INTO cities (city_name, latitude, longitude, country, region)
    VALUES (%s, %s, %s, %s, %s);
    """
    for city in cities:
        cur.execute(insert_sql, (
            city.get("name"),
            float(city.get("lat")),
            float(city.get("lng")),
            city.get("countryName"),
            city.get("adminName1")
        ))

conn.commit()
conn.close()
print(f"Inserted {len(cities)} cities into the database.")
