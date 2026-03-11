import sqlite3
from pathlib import Path

DB_PATH = Path("data/flights.db")


def main():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    c.execute("DELETE FROM flights_current")

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
        TRIM(r.flight_number) AS flight_number,
        r.airline,
        NULL AS origin_airport,
        r.origin_city,

        NULL AS scheduled_arrival_utc,
        strftime('%Y','now') || '-' || substr(r.scheduled_date,4,2) || '-' || substr(r.scheduled_date,1,2) || ' ' || r.scheduled_time || ':00' AS scheduled_arrival_israel,

        NULL AS real_arrival_utc,
        NULL AS real_arrival_israel,

        CASE
          WHEN r.status='נחתה' THEN 'landed'
          WHEN r.status='בנחיתה' THEN 'estimated'
          WHEN r.status='עיכוב' THEN 'delayed'
          WHEN r.status='סופי' THEN 'estimated'
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
          WHEN r.updated_time IS NOT NULL AND TRIM(r.updated_time) != ''
          THEN strftime('%Y','now') || '-' || substr(r.scheduled_date,4,2) || '-' || substr(r.scheduled_date,1,2) || ' ' || r.updated_time || ':00'
          ELSE NULL
        END AS estimated_arrival_israel,

        NULL AS callsign,
        NULL AS registration,
        NULL AS eta_utc,
        CASE
          WHEN r.updated_time IS NOT NULL AND TRIM(r.updated_time) != ''
          THEN strftime('%Y','now') || '-' || substr(r.scheduled_date,4,2) || '-' || substr(r.scheduled_date,1,2) || ' ' || r.updated_time || ':00'
          ELSE NULL
        END AS eta_israel

    FROM flights_ben_gurion_raw r
    JOIN (
        SELECT
            flight_number,
            scheduled_date,
            MAX(id) AS max_id
        FROM flights_ben_gurion_raw
        WHERE datetime(scraped_at) >= datetime('now', '-8 hours')
        GROUP BY flight_number, scheduled_date
    ) latest
      ON r.id = latest.max_id
    WHERE TRIM(r.flight_number) != ''
    """)

    conn.commit()
    conn.close()
    print("flights_current refreshed from arrivals history")


if __name__ == "__main__":
    main()
