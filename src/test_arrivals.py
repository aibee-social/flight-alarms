from pyflightdata import FlightData
import pandas as pd
from datetime import datetime
import pytz

fd = FlightData()

print("\nFetching arrivals for TLV...\n")

arrivals = fd.get_airport_arrivals("TLV")

flights = []

for f in arrivals:

    try:

        flight = f["flight"]

        flight_number = flight["identification"]["number"]["default"]

        airline = flight["airline"]["name"]

        origin_airport = flight["airport"]["origin"]["code"]["iata"]

        origin_city = flight["airport"]["origin"]["position"]["region"]["city"]

        status = flight["status"]["generic"]["status"]["text"]

        scheduled_arrival = flight["time"]["scheduled"]["arrival"]

        real_arrival = flight["time"]["real"]["arrival"]

        if scheduled_arrival != "None":
            scheduled_arrival = datetime.utcfromtimestamp(scheduled_arrival)

        if real_arrival != "None":
            real_arrival = datetime.utcfromtimestamp(real_arrival)

        flights.append({
            "flight_number": flight_number,
            "airline": airline,
            "origin_airport": origin_airport,
            "origin_city": origin_city,
            "scheduled_arrival_utc": scheduled_arrival,
            "real_arrival_utc": real_arrival,
            "status": status
        })

    except Exception:
        continue

df = pd.DataFrame(flights)

print(df.head(20))

print("\nTotal flights:", len(df))



