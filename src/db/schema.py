import sqlite3

DB_PATH = "data/flights.db"

def create_tables():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    c.execute("""
    CREATE TABLE IF NOT EXISTS flights_current (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        flight_number TEXT NOT NULL UNIQUE,
        airline TEXT,
        origin_airport TEXT,
        origin_city TEXT,

        scheduled_arrival_utc INTEGER,
        scheduled_arrival_israel TEXT,

        real_arrival_utc INTEGER,
        real_arrival_israel TEXT,

        status TEXT,

        first_seen_utc INTEGER,
        first_seen_israel TEXT,

        last_collected_utc INTEGER,
        last_collected_israel TEXT,

        last_changed_utc INTEGER,
        last_changed_israel TEXT
    )
    """)

    c.execute("""
    CREATE TABLE IF NOT EXISTS flight_snapshots (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        flight_number TEXT NOT NULL,
        airline TEXT,
        origin_airport TEXT,
        origin_city TEXT,

        scheduled_arrival_utc INTEGER,
        scheduled_arrival_israel TEXT,

        real_arrival_utc INTEGER,
        real_arrival_israel TEXT,

        status TEXT,

        collected_at_utc INTEGER NOT NULL,
        collected_at_israel TEXT NOT NULL
    )
    """)

    c.execute("""
    CREATE TABLE IF NOT EXISTS flight_updates (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        flight_number TEXT NOT NULL,

        old_status TEXT,
        new_status TEXT,

        old_scheduled_utc INTEGER,
        new_scheduled_utc INTEGER,

        old_real_utc INTEGER,
        new_real_utc INTEGER,

        updated_at_utc INTEGER NOT NULL,
        updated_at_israel TEXT NOT NULL
    )
    """)

    c.execute("""
    CREATE TABLE IF NOT EXISTS flights_landed (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        flight_number TEXT NOT NULL UNIQUE,
        airline TEXT,
        origin_airport TEXT,
        origin_city TEXT,

        scheduled_arrival_utc INTEGER,
        scheduled_arrival_israel TEXT,

        real_arrival_utc INTEGER,
        real_arrival_israel TEXT,

        landed_detected_at_utc INTEGER NOT NULL,
        landed_detected_at_israel TEXT NOT NULL
    )
    """)

    c.execute("CREATE INDEX IF NOT EXISTS idx_snapshots_flight_number ON flight_snapshots(flight_number)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_snapshots_collected_at_utc ON flight_snapshots(collected_at_utc)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_updates_flight_number ON flight_updates(flight_number)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_current_status ON flights_current(status)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_current_sched_utc ON flights_current(scheduled_arrival_utc)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_landed_real_utc ON flights_landed(real_arrival_utc)")

    conn.commit()
    conn.close()

if __name__ == "__main__":
    create_tables()
    print("Schema created successfully.")
