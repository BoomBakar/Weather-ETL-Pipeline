import psycopg2
from dotenv import load_dotenv
import os
import requests

load_dotenv()

# DB connection
conn = psycopg2.connect(
    dbname=os.getenv("DB_NAME"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD"),
    host=os.getenv("DB_HOST"),
    port=os.getenv("DB_PORT")
)
cur = conn.cursor()

# Fetch from API
url = "http://api.geonames.org/searchJSON"
params = {
    "country": "PK",                   # Restrict to Pakistan
    "maxRows": 700,
    "orderby": "population",
    "featureClass": "P",              # 'P' = Populated place
    "username": os.getenv("GEONAMES_USERNAME")
}
response = requests.get(url, params=params)
data = response.json()

cities = data.get("geonames", [])
insert_count = 0
update_count = 0

for city in cities:
    city_name = city.get("name")
    lat = round(float(city.get("lat")), 5)
    lon = round(float(city.get("lng")), 5)
    country = city.get("countryName")
    region = city.get("adminName1")

    # Match by city_name + lat + lon
    cur.execute("""
        SELECT city_id FROM cities
        WHERE city_name = %s AND latitude = %s AND longitude = %s;
    """, (city_name, lat, lon))
    
    result = cur.fetchone()

    if result:
        cur.execute("""
            UPDATE cities
            SET region = %s, country = %s
            WHERE city_name = %s AND latitude = %s AND longitude = %s;
        """, (region, country, city_name, lat, lon))
        update_count += 1
    else:
        cur.execute("""
            INSERT INTO cities (city_name, latitude, longitude, country, region)
            VALUES (%s, %s, %s, %s, %s);
        """, (city_name, lat, lon, country, region))
        insert_count += 1

conn.commit()
conn.close()

print(f"Updated {update_count} cities and inserted {insert_count} new ones.")
