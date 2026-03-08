import json
from pyflightdata import FlightData

fd = FlightData()
arrivals = fd.get_airport_arrivals("TLV")

print(f"Total raw arrivals: {len(arrivals)}")

# שומר את כל ה-raw לקובץ
with open("debug/raw_arrivals_full.json", "w", encoding="utf-8") as f:
    json.dump(arrivals, f, ensure_ascii=False, indent=2)

print("Saved full raw feed to debug/raw_arrivals_full.json")

# טיסות מעניינות לבדיקה
watch = {"LY118", "QS1284", "LY420", "ET414", "5C606", "IZ224", "LY1014", "LY324"}

matched = []
for item in arrivals:
    try:
        num = item["flight"]["identification"]["number"]["default"]
        if num in watch:
            matched.append(item)
    except Exception:
        pass

with open("debug/raw_arrivals_watch.json", "w", encoding="utf-8") as f:
    json.dump(matched, f, ensure_ascii=False, indent=2)

print(f"Saved {len(matched)} watched flights to debug/raw_arrivals_watch.json")

# גם מדפיס למסך תקציר שימושי
for item in matched:
    flight = item["flight"]
    ident = flight.get("identification", {})
    airport = flight.get("airport", {})
    status = flight.get("status", {})
    time = flight.get("time", {})
    aircraft = flight.get("aircraft", {})
    airline = flight.get("airline", {})

    print("\n" + "=" * 80)
    print("flight_number:", ident.get("number", {}).get("default"))
    print("callsign:", ident.get("callsign"))
    print("airline:", airline.get("name"))
    print("origin:", airport.get("origin", {}).get("code", {}).get("iata"))
    print("status_text:", status.get("text"))
    print("generic_status:", status.get("generic", {}).get("status", {}).get("text"))
    print("scheduled_arrival:", time.get("scheduled", {}).get("arrival"))
    print("estimated_arrival:", time.get("estimated", {}).get("arrival"))
    print("real_arrival:", time.get("real", {}).get("arrival"))
    print("eta:", time.get("other", {}).get("eta"))
    print("registration:", aircraft.get("registration"))
    print("model_code:", aircraft.get("model", {}).get("code"))
    print("owner:", flight.get("owner"))
