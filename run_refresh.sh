#!/bin/bash
cd /Users/stavvaknin/Projects/flight_alarms || exit 1
source .venv/bin/activate
python3 YOUR_REFRESH_SCRIPT.py >> logs/refresh.log 2>&1
