import sqlite3
from pathlib import Path
from datetime import datetime
from zoneinfo import ZoneInfo
import pandas as pd

from src.predictor.predictor import load_all_traffic, _traffic_probability

DB_PATH = Path("data/flights.db")


def main():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    c.execute("""
    CREATE TABLE IF NOT EXISTS traffic_windows_history (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        calculated_at TEXT,
        window_start TEXT,
        window_end TEXT,
        flights_count INTEGER,
        arrivals_count INTEGER,
        departures_count INTEGER,
        updated_count INTEGER,
        avg_weight REAL,
        probability INTEGER
    )
    """)

    df = load_all_traffic().copy()
    if df.empty:
        conn.commit()
        conn.close()
        print("no traffic rows to save")
        return

    df["dt"] = pd.to_datetime(df["dt"], errors="coerce")
    df = df.dropna(subset=["dt"]).copy()

    now = datetime.now(ZoneInfo("Asia/Jerusalem")).replace(tzinfo=None)
    now_ts = pd.Timestamp(now)

    # נשמור רק חלונות רלוונטיים לזמן החישוב
    df = df[
        (df["dt"] >= now_ts - pd.Timedelta(minutes=30)) &
        (df["dt"] <= now_ts + pd.Timedelta(hours=24))
    ].copy()

    if df.empty:
        conn.commit()
        conn.close()
        print("no nearby traffic rows to save")
        return

    df["window"] = df["dt"].dt.floor("10min")

    grouped = (
        df.groupby("window")
          .agg(
              flights_count=("flight_number", "size"),
              arrivals_count=("type", lambda x: int((x == "arrival").sum())),
              departures_count=("type", lambda x: int((x == "departure").sum())),
              updated_count=("is_updated", "sum"),
              avg_weight=("weight", "mean")
          )
          .reset_index()
          .sort_values("window")
    )

    max_cluster = int(grouped["flights_count"].max()) if not grouped.empty else 1
    calculated_at = now.strftime("%Y-%m-%d %H:%M:%S")

    for _, row in grouped.iterrows():
        start = pd.Timestamp(row["window"])
        end = start + pd.Timedelta(minutes=10)

        probability = _traffic_probability(
            int(row["flights_count"]),
            int(row["updated_count"]),
            float(row["avg_weight"]),
            max_cluster
        )

        c.execute("""
        INSERT INTO traffic_windows_history (
            calculated_at,
            window_start,
            window_end,
            flights_count,
            arrivals_count,
            departures_count,
            updated_count,
            avg_weight,
            probability
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            calculated_at,
            start.strftime("%Y-%m-%d %H:%M:%S"),
            end.strftime("%Y-%m-%d %H:%M:%S"),
            int(row["flights_count"]),
            int(row["arrivals_count"]),
            int(row["departures_count"]),
            int(row["updated_count"]),
            float(row["avg_weight"]),
            int(probability)
        ))

    conn.commit()
    conn.close()
    print("traffic_windows_history saved")


if __name__ == "__main__":
    main()
