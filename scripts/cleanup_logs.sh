#!/bin/zsh
cd ~/Projects/flight_alarms
find logs -name "*.log" -size +20M -exec truncate -s 0 {} \;
