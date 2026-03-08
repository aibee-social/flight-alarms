import sqlite3

DB_PATH = "data/flights.db"

def create_tables():

    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    c.execute("""
    CREATE TABLE IF NOT EXISTS flights_active (
        id INTEGER PRIMARY KEY AUTOINCREMENT,

        flight_number TEXT,
        airline TEXT,

        origin_airport TEXT,
        origin_city TEXT,

        scheduled_arrival_utc INTEGER,
        scheduled_arrival_israel TEXT,

        real_arrival_utc INTEGER,
        real_arrival_israel TEXT,

        status TEXT,

        first_seen_utc INTEGER,
        last_updated_utc INTEGER,

        first_seen_israel TEXT,
        last_updated_israel TEXT
    )
    """)

    c.execute("""
    CREATE TABLE IF NOT EXISTS flights_landed (
        id INTEGER PRIMARY KEY AUTOINCREMENT,

        flight_number TEXT,
        airline TEXT,

        origin_airport TEXT,
        origin_city TEXT,

        scheduled_arrival_utc INTEGER,
        scheduled_arrival_israel TEXT,

        real_arrival_utc INTEGER,
        real_arrival_israel TEXT,

        collected_at_utc INTEGER,
        collected_at_israel TEXT
    )
    """)

    c.execute("""
    CREATE TABLE IF NOT EXISTS flights_updates (
        id INTEGER PRIMARY KEY AUTOINCREMENT,

        flight_number TEXT,

        old_status TEXT,
        new_status TEXT,

        old_time INTEGER,
        new_time INTEGER,

        updated_at_utc INTEGER,
        updated_at_israel TEXT
    )
    """)

    conn.commit()
    conn.close()


if __name__ == "__main__":
    create_tables()
