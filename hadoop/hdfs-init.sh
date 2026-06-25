#!/bin/bash
set -eu

until hdfs dfsadmin -safemode get 2>/dev/null | grep -q OFF; do
  echo "Waiting for HDFS safemode to turn off..."
  sleep 3
done

GTFS_DATA_PATH="${GTFS_DATA_PATH:-/bda/data/gtfs_bus}"
GTFS_ZIP_URL="${GTFS_ZIP_URL:-https://gitlab.com/LACMTA/gtfs_bus/-/raw/master/gtfs_bus.zip}"
VEHICLE_DELAYS_PATH="${OUTPUT_PATH:-/bda/data/vehicle_delays}"
CHECKPOINT_PATH="${CHECKPOINT_PATH:-/bda/data/vehicle_delays_chck}"

# Strip hdfs:// prefix if present (hdfs dfs uses paths without scheme)
HDFS_BASE="${GTFS_DATA_PATH#hdfs://}"

echo "Creating HDFS directories..."
hdfs dfs -mkdir -p "$HDFS_BASE" "$VEHICLE_DELAYS_PATH" "$CHECKPOINT_PATH"
hdfs dfs -chmod -R 777 "$VEHICLE_DELAYS_PATH" "$CHECKPOINT_PATH"

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
hdfs dfs -put -f /tmp/gtfs_bus/stops.txt /tmp/gtfs_bus/stop_times.txt /tmp/gtfs_bus/trips.txt /tmp/gtfs_bus/routes.txt "$HDFS_BASE/"
rm -rf /tmp/gtfs_bus /tmp/gtfs_bus.zip

echo "GTFS files uploaded to HDFS"
hdfs dfs -ls "$HDFS_BASE"
