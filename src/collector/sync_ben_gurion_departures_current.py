import sqlite3
from pathlib import Path

DB_PATH = Path("data/flights.db")


def ensure_table(conn):
    conn.execute("""
    CREATE TABLE IF NOT EXISTS departures_current (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        flight_number TEXT NOT NULL,
        airline TEXT,
        destination_city TEXT,
        terminal TEXT,
        scheduled_date TEXT,
        scheduled_departure_israel TEXT,
        estimated_departure_israel TEXT,
        gate_info TEXT,
        scraped_at TEXT,
        UNIQUE(flight_number, scheduled_date)
    )
    """)
    conn.commit()


def main():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    ensure_table(conn)
    c.execute("DELETE FROM departures_current")

    c.execute("""
    INSERT OR REPLACE INTO departures_current (
        flight_number,
        airline,
        destination_city,
        terminal,
        scheduled_date,
        scheduled_departure_israel,
        estimated_departure_israel,
        gate_info,
        scraped_at
    )
    SELECT
        TRIM(flight_number) AS flight_number,
        airline,
        origin_city AS destination_city,
        terminal,
        scheduled_date,
        '2026-' || substr(scheduled_date,4,2) || '-' || substr(scheduled_date,1,2) || ' ' || scheduled_time || ':00' AS scheduled_departure_israel,
        CASE
          WHEN updated_time IS NOT NULL AND TRIM(updated_time) != ''
          THEN '2026-' || substr(scheduled_date,4,2) || '-' || substr(scheduled_date,1,2) || ' ' || updated_time || ':00'
          ELSE NULL
        END AS estimated_departure_israel,
        gate_info,
        scraped_at
    FROM flights_ben_gurion_departures_raw
    WHERE TRIM(flight_number) != ''
    """)

    conn.commit()
    conn.close()
    print("departures_current refreshed from Ben Gurion departures raw")


if __name__ == "__main__":
    main()
