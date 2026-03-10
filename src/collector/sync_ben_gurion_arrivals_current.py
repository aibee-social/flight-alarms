import sqlite3
from pathlib import Path

DB_PATH = Path("data/flights.db")


def main():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    c.execute("""
    DELETE FROM flights_current
    """)

    c.execute("""
    INSERT OR REPLACE INTO flights_current (
        flight_number,
        airline,
        origin_airport,
        origin_city,
        scheduled_arrival_utc,
        scheduled_arrival_israel,
        real_arrival_utc,
        real_arrival_israel,
        status,
        first_seen_utc,
        first_seen_israel,
        last_collected_utc,
        last_collected_israel,
        last_changed_utc,
        last_changed_israel,
        estimated_arrival_utc,
        estimated_arrival_israel,
        callsign,
        registration,
        eta_utc,
        eta_israel
    )
    SELECT
        TRIM(flight_number) AS flight_number,
        airline,
        NULL AS origin_airport,
        origin_city,

        NULL AS scheduled_arrival_utc,
        '2026-' || substr(scheduled_date,4,2) || '-' || substr(scheduled_date,1,2) || ' ' || scheduled_time || ':00' AS scheduled_arrival_israel,

        NULL AS real_arrival_utc,
        CASE
          WHEN status='נחתה'
          THEN '2026-' || substr(scheduled_date,4,2) || '-' || substr(scheduled_date,1,2) || ' ' || updated_time || ':00'
          ELSE NULL
        END AS real_arrival_israel,

        CASE
          WHEN status='נחתה' THEN 'landed'
          WHEN status='בנחיתה' THEN 'estimated'
          WHEN status='עיכוב' THEN 'delayed'
          WHEN status='סופי' THEN 'estimated'
          ELSE 'scheduled'
        END AS status,

        strftime('%s','now'),
        datetime('now','localtime'),
        strftime('%s','now'),
        datetime('now','localtime'),
        strftime('%s','now'),
        datetime('now','localtime'),

        NULL AS estimated_arrival_utc,
        CASE
          WHEN updated_time IS NOT NULL AND TRIM(updated_time) != ''
          THEN '2026-' || substr(scheduled_date,4,2) || '-' || substr(scheduled_date,1,2) || ' ' || updated_time || ':00'
          ELSE NULL
        END AS estimated_arrival_israel,

        NULL AS callsign,
        NULL AS registration,
        NULL AS eta_utc,
        CASE
          WHEN updated_time IS NOT NULL AND TRIM(updated_time) != ''
          THEN '2026-' || substr(scheduled_date,4,2) || '-' || substr(scheduled_date,1,2) || ' ' || updated_time || ':00'
          ELSE NULL
        END AS eta_israel

    FROM flights_ben_gurion_raw
    WHERE TRIM(flight_number) != ''
    """)

    conn.commit()
    conn.close()
    print("flights_current refreshed from Ben Gurion arrivals raw")


if __name__ == "__main__":
    main()
