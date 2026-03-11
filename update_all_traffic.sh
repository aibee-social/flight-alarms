#!/bin/bash

cd ~/Projects/flight_alarms || exit 1

LOCKDIR="/tmp/flight_alarms_update.lock"

if ! mkdir "$LOCKDIR" 2>/dev/null; then
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] another update is already running, skipping"
  exit 0
fi

trap 'rm -rf "$LOCKDIR"' EXIT

echo "[$(date '+%Y-%m-%d %H:%M:%S')] starting combined update"

source .venv/bin/activate

echo "[$(date '+%Y-%m-%d %H:%M:%S')] arrivals..."
python src/collector/fetch_ben_gurion.py
python src/collector/sync_ben_gurion_arrivals_current.py

echo "[$(date '+%Y-%m-%d %H:%M:%S')] departures..."
python src/collector/fetch_ben_gurion_departures.py
python src/collector/sync_ben_gurion_departures_current.py
python -m src.collector.save_traffic_windows_history

echo "[$(date '+%Y-%m-%d %H:%M:%S')] combined update finished"
