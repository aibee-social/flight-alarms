#!/bin/zsh
cd ~/Projects/flight_alarms
source .venv/bin/activate
python -m src.collector.collector >> logs/collector.log 2>&1
