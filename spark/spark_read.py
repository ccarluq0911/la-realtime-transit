from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, unix_timestamp, when, concat_ws, to_date, to_timestamp,
    date_format, expr, split, regexp_replace, format_string, abs
)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, TimestampType

# Crear sesión de Spark
spark = SparkSession.builder \
    .appName("VehiclePositionsProcessor") \
    .getOrCreate()

# ==========================
# 1. Cargar datos estáticos
# ==========================

# stops.txt: stop_id → stop_name
stops_df = spark.read.option("header", "true").csv("hdfs:///bda/data/gtfs_bus/stops.txt") \
    .select("stop_id", "stop_name")

# stop_times.txt: relación entre trip_id, secuencia y horas
stop_times_df = spark.read.option("header", "true").csv("hdfs:///bda/data/gtfs_bus/stop_times.txt") \
    .withColumn("stop_sequence", col("stop_sequence").cast("long")) \
    .withColumnRenamed("trip_id", "trip_id_st") \
    .select("trip_id_st", "stop_sequence", "stop_id", "arrival_time", "departure_time")

# ================================
# 2. Leer stream desde Kafka
# ================================

# Esquema del campo 'payload'
payload_schema = StructType([
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

kafka_schema = StructType().add("payload", payload_schema)

raw_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "192.168.56.10:9092,192.168.56.10:9093") \
    .option("subscribe", "vehicle_positions") \
    .option("startingOffsets", "latest") \
    .load()

stream = raw_stream.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), kafka_schema).alias("json")) \
    .select("json.payload.*") \
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

# Extraer la fecha del timestamp en formato yyyy-MM-dd
joined_df = joined_df.withColumn("date_only", date_format(col("timestamp"), "yyyy-MM-dd"))

# Combinar fecha con arrival_time y departure_time
joined_df = joined_df \
    .withColumn("arrival_ts", to_timestamp(concat_ws(" ", col("date_only"), col("arrival_time")), "yyyy-MM-dd HH:mm:ss")) \
    .withColumn("departure_ts", to_timestamp(concat_ws(" ", col("date_only"), col("departure_time")), "yyyy-MM-dd HH:mm:ss"))

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
    .option("path", "hdfs://cluster-bda:9000/bda/data/vehicle_delays/") \
    .option("checkpointLocation", "hdfs://cluster-bda:9000/bda/data/vehicle_delays_chck/") \
    .outputMode("append") \
    .start()

query.awaitTermination()