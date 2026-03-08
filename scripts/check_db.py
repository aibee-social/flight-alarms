import sqlite3

conn = sqlite3.connect("data/flights.db")
c = conn.cursor()

queries = {
    "current_count": "SELECT COUNT(*) FROM flights_current",
    "snapshots_count": "SELECT COUNT(*) FROM flight_snapshots",
    "updates_count": "SELECT COUNT(*) FROM flight_updates",
    "landed_count": "SELECT COUNT(*) FROM flights_landed",
}

for name, q in queries.items():
    value = c.execute(q).fetchone()[0]
    print(f"{name}: {value}")

print("\nSample current flights:")
for row in c.execute("""
SELECT flight_number, status, scheduled_arrival_israel, last_collected_israel
FROM flights_current
ORDER BY scheduled_arrival_utc
LIMIT 10
"""):
    print(row)

conn.close()
