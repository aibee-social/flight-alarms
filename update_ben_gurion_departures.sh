#!/bin/bash
cd ~/Projects/flight_alarms || exit 1
source .venv/bin/activate
python src/collector/fetch_ben_gurion_departures.py
python src/collector/sync_ben_gurion_departures_current.py
