import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, unix_timestamp, when, concat_ws,
    abs, split, from_unixtime, concat, lit
)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, TimestampType

KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "kafka-broker-1:9092,kafka-broker-2:9093"
)
HDFS_NAMENODE = os.getenv("HDFS_NAMENODE", "hdfs://cluster-bda:8020")
GTFS_DATA_PATH = os.getenv("GTFS_DATA_PATH", "hdfs:///bda/data/gtfs_bus")
OUTPUT_PATH = os.getenv("OUTPUT_PATH", f"{HDFS_NAMENODE}/bda/data/vehicle_delays/")
CHECKPOINT_PATH = os.getenv(
    "CHECKPOINT_PATH",
    f"{HDFS_NAMENODE}/bda/data/vehicle_delays_chck/"
)

# Crear sesión de Spark
spark = SparkSession.builder \
    .appName("VehiclePositionsProcessor") \
    .config("spark.sql.sessionTimeZone", "America/Los_Angeles") \
    .getOrCreate()

# ==========================
# 1. Cargar datos estáticos
# ==========================

# stops.txt: stop_id → stop_name
stops_df = spark.read.option("header", "true").csv(f"{GTFS_DATA_PATH}/stops.txt") \
    .select("stop_id", "stop_name")

# stop_times.txt: relación entre trip_id, secuencia y horas
stop_times_df = spark.read.option("header", "true").csv(f"{GTFS_DATA_PATH}/stop_times.txt") \
    .withColumn("stop_sequence", col("stop_sequence").cast("long")) \
    .withColumnRenamed("trip_id", "trip_id_st") \
    .select("trip_id_st", "stop_sequence", "stop_id", "arrival_time", "departure_time")

# ================================
# 2. Leer stream desde Kafka
# ================================

raw_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
    .option("subscribe", "vehicle_positions") \
    .option("startingOffsets", "latest") \
    .load()

stream_schema = StructType([
    StructField("internal_id", LongType()),
    StructField("id", StringType()),
    StructField("trip_route_id", StringType()),
    StructField("trip_trip_id", StringType()),
    StructField("position_latitude", DoubleType()),
    StructField("position_longitude", DoubleType()),
    StructField("current_stop_sequence", LongType()),
    StructField("current_status", StringType()),
    StructField("amount_people", LongType()),
    StructField("timestamp", LongType()),
])

stream = raw_stream.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), stream_schema).alias("json")) \
    .select("json.*") \
    .withColumnRenamed("id", "vehicle_id") \
    .withColumnRenamed("trip_route_id", "route_id") \
    .withColumnRenamed("trip_trip_id", "trip_id") \
    .withColumnRenamed("position_latitude", "latitude") \
    .withColumnRenamed("position_longitude", "longitude") \
    .withColumn("timestamp", (col("timestamp") / 1000).cast(TimestampType()))

# ================================
# 3. Unir con stop_times y stops
# ================================

joined_df = stream.join(
    stop_times_df,
    (stream.trip_id == stop_times_df.trip_id_st) &
    (stream.current_stop_sequence == stop_times_df.stop_sequence),
    how="left"
)

# Extraer la fecha del timestamp en formato yyyy-MM-dd (epoch seconds a medianoche)
joined_df = joined_df.withColumn(
    "date_midnight",
    unix_timestamp(
        concat(
            from_unixtime(unix_timestamp(col("timestamp")), "yyyy-MM-dd"),
            lit(" 00:00:00 America/Los_Angeles")
        ),
        "yyyy-MM-dd HH:mm:ss z"
    )
)

# Parsear arrival_time / departure_time (soportan horas > 24 como "25:30:00")
def gtfs_time_to_seconds(col_name):
    parts = split(col(col_name), ":")
    return parts[0].cast("int") * 3600 + parts[1].cast("int") * 60 + parts[2].cast("int")

joined_df = joined_df \
    .withColumn("arrival_secs", gtfs_time_to_seconds("arrival_time")) \
    .withColumn("departure_secs", gtfs_time_to_seconds("departure_time"))

# Timestamp programado = medianoche de la fecha del vehículo + segundos del GTFS
joined_df = joined_df \
    .withColumn("arrival_ts", (col("date_midnight") + col("arrival_secs")).cast(TimestampType())) \
    .withColumn("departure_ts", (col("date_midnight") + col("departure_secs")).cast(TimestampType()))

# Calcular diferencia con respecto a hora estimada
joined_df = joined_df.withColumn(
    "time_diff_seconds",
    when(
        col("current_status") == "IN_TRANSIT_TO",
        unix_timestamp(col("timestamp")) - unix_timestamp(col("arrival_ts"))
    ).when(
        col("current_status") == "STOPPED_AT",
        unix_timestamp(col("timestamp")) - unix_timestamp(col("departure_ts"))
    ).otherwise(None)
)

# VALIDACIÓN: Filtrar diferencias de tiempo extremas (más de 12 horas)
joined_df = joined_df.withColumn(
    "time_diff_seconds",
    when(
        (col("time_diff_seconds").isNotNull()) &
        (abs(col("time_diff_seconds")) > 43200),  # 12 horas = 43200 segundos
        None
    ).otherwise(col("time_diff_seconds"))
)

# Unir con stops.txt por stop_id
joined_with_name_df = joined_df.join(stops_df, on="stop_id", how="left")

# ================================
# 4. Juntar latitud y longitud
# ================================

joined_with_name_df = joined_with_name_df.withColumn(
    "location", concat_ws(",", col("latitude"), col("longitude"))
)

# ================================
# 5. Seleccionar columnas finales
# ================================

output_df = joined_with_name_df.select(
    "vehicle_id",
    "route_id",
    "trip_id",
    "location",
    "current_status",
    "timestamp",
    "stop_id",
    "stop_name",
    "amount_people",
    "time_diff_seconds"
)

# ================================
# 6. Guardar como Parquet en HDFS
# ================================

query = output_df.writeStream \
    .format("parquet") \
    .option("path", OUTPUT_PATH) \
    .option("checkpointLocation", CHECKPOINT_PATH) \
    .outputMode("append") \
    .start()

query.awaitTermination()