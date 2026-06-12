#!/bin/bash
set -eu

until hdfs dfsadmin -safemode get 2>/dev/null | grep -q OFF; do
  echo "Waiting for HDFS safemode to turn off..."
  sleep 3
done

hdfs dfs -mkdir -p /bda/data/gtfs_bus /bda/data/vehicle_delays /bda/data/vehicle_delays_chck
hdfs dfs -chmod -R 777 /bda/data/vehicle_delays /bda/data/vehicle_delays_chck

GTFS_ZIP_URL="https://gitlab.com/LACMTA/gtfs_bus/-/raw/master/gtfs_bus.zip"

echo "Downloading GTFS data from $GTFS_ZIP_URL"
curl -sL "$GTFS_ZIP_URL" -o /tmp/gtfs_bus.zip
python -c "
import zipfile, os
with zipfile.ZipFile('/tmp/gtfs_bus.zip') as z:
    z.extract('stops.txt', '/tmp/gtfs_bus/')
    z.extract('stop_times.txt', '/tmp/gtfs_bus/')
    z.extract('trips.txt', '/tmp/gtfs_bus/')
    z.extract('routes.txt', '/tmp/gtfs_bus/')
"
hdfs dfs -put -f /tmp/gtfs_bus/stops.txt /tmp/gtfs_bus/stop_times.txt /tmp/gtfs_bus/trips.txt /tmp/gtfs_bus/routes.txt /bda/data/gtfs_bus/
rm -rf /tmp/gtfs_bus /tmp/gtfs_bus.zip

echo "GTFS files uploaded to HDFS"
hdfs dfs -ls /bda/data/gtfs_bus