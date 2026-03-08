import requests
import pandas as pd
from datetime import datetime
import pytz

print("\nFetching arrivals for TLV (JSON)...\n")

url = "https://data-live.flightradar24.com/zones/fcgi/feed.js"

params = {
    "bounds": "32.3,31.7,34.9,34.5",   # אזור נתב״ג
    "faa": "1",
    "satellite": "1",
    "mlat": "1",
    "flarm": "1",
    "adsb": "1",
    "gnd": "1",
    "air": "1",
    "vehicles": "0",
    "estimated": "1",
    "maxage": "14400",
    "gliders": "0",
    "stats": "1"
}

headers = {
    "User-Agent": "Mozilla/5.0"
}

response = requests.get(url, params=params, headers=headers)

data = response.json()

flights = []

for key, value in data.items():

    if not isinstance(value, list):
        continue

    try:

        latitude = value[1]
        longitude = value[2]
        heading = value[3]
        altitude = value[4]
        speed = value[5]
        callsign = value[16]

        flights.append({
            "callsign": callsign,
            "latitude": latitude,
            "longitude": longitude,
            "altitude": altitude,
            "speed": speed
        })

    except:
        continue


df = pd.DataFrame(flights)

print(df.head(20))

print("\nTotal aircraft:", len(df))
