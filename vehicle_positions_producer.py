import time
import http.client
import threading
import queue
import os
import mysql.connector
import signal
import sys
import logging
import random
from datetime import datetime, timedelta
from google.protobuf.message import DecodeError
from google.transit import gtfs_realtime_pb2

# Configurar logging detallado
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)

# MySQL connection parameters
DB_HOST = "localhost"
DB_PORT = 3306
DB_NAME = "vehicles"
DB_USER = "root"
DB_PASSWORD = "pass"

# Time connection parameter (seconds)
TIME = 10

# Set para guardar la última parada de cada vehículo
last_stops = {}

# Conexión a MySQL
try:
    conn_db = mysql.connector.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME
    )
except Exception as e:
    logging.error(f"Database connection failed: {e}")
    exit(1)

cur = conn_db.cursor()

# Crear tabla si no existe
cur.execute("""
CREATE TABLE IF NOT EXISTS vehicle_positions (
    internal_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    id VARCHAR(255) NOT NULL,
    trip_route_id VARCHAR(255) NOT NULL,
    trip_trip_id VARCHAR(255) NOT NULL,
    position_latitude DOUBLE,
    position_longitude DOUBLE,
    current_stop_sequence INT NOT NULL,
    current_status VARCHAR(50),
    amount_people INT NOT NULL,
    timestamp TIMESTAMP NOT NULL
)
""")
conn_db.commit()

batch_queue = queue.Queue()
stop_event = threading.Event()

def check_stop_event(bus_id, vehicle_stop):
    if bus_id not in last_stops:
        amount_people = random.randint(0, 70)
        last_stops[bus_id] = (vehicle_stop, amount_people)
        return amount_people
    elif vehicle_stop != last_stops[bus_id][0]:
        new_amount_people = min(70, max(0, last_stops[bus_id][1] + random.randint(-5, 10)))
        last_stops[bus_id] = (vehicle_stop, new_amount_people)
        return new_amount_people
    else:
        return last_stops[bus_id][1]
    

def fetch_data():
    headers = {
        'Accept': "application/x-protobuf",
        'Authorization': os.getenv("API_KEY")
    }

    backoff = TIME
    max_backoff = 300  # 5 minutos
    consecutive_failures = 0

    while not stop_event.is_set():
        try:
            logging.info("Fetching data from API...")
            conn_api = http.client.HTTPSConnection("api.goswift.ly", timeout=10)
            conn_api.request("GET", "/real-time/lametro/gtfs-rt-vehicle-positions", headers=headers)
            res = conn_api.getresponse()
            data = res.read()
            conn_api.close()

            feed = gtfs_realtime_pb2.FeedMessage()
            feed.ParseFromString(data)

            records = []
            for entity in feed.entity:
                if entity.HasField('vehicle'):
                    vehicle = entity.vehicle
                    try:
                        status = ["INCOMING_AT", "STOPPED_AT", "IN_TRANSIT_TO"][vehicle.current_status]
                    except (IndexError, AttributeError):
                        status = None
                    
                    record = (
                        entity.id,
                        vehicle.trip.route_id,
                        vehicle.trip.trip_id,
                        vehicle.position.latitude,
                        vehicle.position.longitude,
                        vehicle.current_stop_sequence,
                        status,
                        check_stop_event(entity.id, vehicle.current_stop_sequence),
                        datetime.fromtimestamp(vehicle.timestamp) - timedelta(hours=9)
                    )
                    records.append(record)

            num_records = len(records)
            if num_records > 0:
                per_batch = max(1, num_records // TIME)
                batches = [records[i:i+per_batch] for i in range(0, num_records, per_batch)]

                while len(batches) < TIME:
                    batches.append([])
                if len(batches) > TIME:
                    extra = batches[TIME:]
                    batches = batches[:TIME]
                    for extra_batch in extra:
                        batches[-1].extend(extra_batch)

                for batch in batches:
                    batch_queue.put(batch)

                logging.info(f"Fetched {num_records} records. Split into {TIME} batches.")
            else:
                for _ in range(TIME):
                    batch_queue.put([])
                logging.info("Fetched 0 records. Sent empty batches.")

            backoff = TIME
            consecutive_failures = 0

        except DecodeError as e:
            logging.error(f"Protocol Buffer decode error: {e}")
        except Exception as e:
            logging.error(f"Fetch error: {e}")
            consecutive_failures += 1
            backoff = min(backoff * 2, max_backoff)
            logging.warning(f"Backoff increased to {backoff} seconds after {consecutive_failures} consecutive failures.")

        time.sleep(backoff)


def write_data():
    while not stop_event.is_set():
        try:
            batch = batch_queue.get(timeout=TIME)
            if batch:
                insert_query = """
                INSERT INTO vehicle_positions 
                (id, trip_route_id, trip_trip_id, position_latitude, position_longitude, current_stop_sequence, current_status, amount_people, timestamp)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                cur.executemany(insert_query, batch)
                conn_db.commit()
                logging.info(f"Inserted {len(batch)} records into DB.")
            else:
                logging.info("Empty batch. No insert.")
        except queue.Empty:
            logging.warning("No batch to insert (queue empty).")
        except Exception as e:
            logging.error(f"Insertion error: {e}")

        time.sleep(1)


def signal_handler(sig, frame):
    logging.info("\nFinalizando...")
    stop_event.set()
    fetcher_thread.join()
    writer_thread.join()
    cur.close()
    conn_db.close()
    logging.info("Conexión a base de datos cerrada.")
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

fetcher_thread = threading.Thread(target=fetch_data, daemon=True)
writer_thread = threading.Thread(target=write_data, daemon=True)
fetcher_thread.start()
writer_thread.start()

logging.info("Producer iniciado. Presiona Ctrl+C para salir.")
while not stop_event.is_set():
    time.sleep(1)

#docker run -d --name mysql-kafka -p 3306:3306 -e MYSQL_ROOT_PASSWORD=pass -e MYSQL_DATABASE=vehicles -v mysql_data:/var/lib/mysql mysql:latest