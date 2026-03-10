#!/bin/bash

cd ~/Projects/flight_alarms || exit 1

echo "=== $(date) : updating flights data ==="

source .venv/bin/activate

python -m src.collector.collector

git add data/flights.db

if ! git diff --cached --quiet; then
  git commit -m "auto update flights data"
  git push
else
  echo "No changes in flights data"
fi
