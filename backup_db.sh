#!/bin/bash
cd /Users/stavvaknin/Projects/flight_alarms || exit 1
TS=$(date +%Y%m%d_%H%M%S)
cp data/flights.db db_backups/flights_$TS.db
find db_backups -name "flights_*.db" -mtime +7 -delete
