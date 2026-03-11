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
        TRIM(r.flight_number) AS flight_number,
        r.airline,
        r.origin_city AS destination_city,
        r.terminal,
        r.scheduled_date,
        strftime('%Y','now') || '-' || substr(r.scheduled_date,4,2) || '-' || substr(r.scheduled_date,1,2) || ' ' || r.scheduled_time || ':00' AS scheduled_departure_israel,
        CASE
          WHEN r.updated_time IS NOT NULL AND TRIM(r.updated_time) != ''
          THEN strftime('%Y','now') || '-' || substr(r.scheduled_date,4,2) || '-' || substr(r.scheduled_date,1,2) || ' ' || r.updated_time || ':00'
          ELSE NULL
        END AS estimated_departure_israel,
        r.gate_info,
        r.scraped_at
    FROM flights_ben_gurion_departures_raw r
    JOIN (
        SELECT
            flight_number,
            scheduled_date,
            MAX(id) AS max_id
        FROM flights_ben_gurion_departures_raw
        WHERE datetime(scraped_at) >= datetime('now', '-8 hours')
        GROUP BY flight_number, scheduled_date
    ) latest
      ON r.id = latest.max_id
    WHERE TRIM(r.flight_number) != ''
    """)

    conn.commit()
    conn.close()
    print("departures_current refreshed from departures history")


if __name__ == "__main__":
    main()
