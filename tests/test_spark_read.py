import pytest
from datetime import datetime, timezone

pyspark = pytest.importorskip("pyspark", reason="PySpark no está instalado en este entorno")

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, DoubleType, TimestampType,
)
from pyspark.sql.functions import col, unix_timestamp, when, abs as pyspark_abs


@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder
        .master("local[1]")
        .appName("test_spark_read")
        .config("spark.sql.sessionTimeZone", "America/Los_Angeles")
        .getOrCreate()
    )


# ── Tests: gtfs_time_to_seconds() ───────────────────────────────

class TestGtfsTimeToSeconds:
    """Tests para la función Spark gtfs_time_to_seconds (spark_read.py:100-102)."""

    @pytest.fixture(scope="class")
    def func(self):
        from spark.spark_read import gtfs_time_to_seconds
        return gtfs_time_to_seconds

    def test_normal_times(self, spark, func):
        df = spark.createDataFrame(
            [("00:00:00",), ("00:00:01",), ("01:30:00",), ("12:00:00",)],
            ["t"],
        )
        result = df.select(func("t").alias("secs")).collect()
        assert result[0]["secs"] == 0
        assert result[1]["secs"] == 1
        assert result[2]["secs"] == 5400
        assert result[3]["secs"] == 43200

    def test_overflow_hours_gt_24(self, spark, func):
        df = spark.createDataFrame(
            [("25:30:00",), ("30:15:45",), ("99:00:00",)],
            ["t"],
        )
        result = df.select(func("t").alias("secs")).collect()
        assert result[0]["secs"] == 91800
        assert result[1]["secs"] == 108945
        assert result[2]["secs"] == 356400

    def test_malformed_returns_null(self, spark, func):
        df = spark.createDataFrame(
            [("",), ("abc",), ("00:00",)],
            ["t"],
        )
        result = df.select(func("t").alias("secs")).collect()
        for r in result:
            assert r["secs"] is None

    def test_edge_cases(self, spark, func):
        df = spark.createDataFrame(
            [("00:00:59",), ("23:59:59",), ("00:01:00",)],
            ["t"],
        )
        result = df.select(func("t").alias("secs")).collect()
        assert result[0]["secs"] == 59
        assert result[1]["secs"] == 86399
        assert result[2]["secs"] == 60


# ── Tests: Lógica de delay (STOPPED_AT / IN_TRANSIT_TO / filtro >12h) ──

class TestDelayLogic:
    """Tests para el cálculo de time_diff_seconds (spark_read.py:114-133)."""

    EPOCH = int(datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc).timestamp())

    @pytest.fixture(scope="class")
    def base_df(self, spark):
        schema = StructType([
            StructField("vehicle_id", StringType()),
            StructField("current_status", StringType()),
            StructField("timestamp", TimestampType()),
            StructField("date_midnight", LongType()),
            StructField("arrival_secs", LongType()),
            StructField("departure_secs", LongType()),
        ])
        # date_midnight = 2024-06-01 00:00 PDT = 2024-06-01 07:00 UTC
        midnight_utc = int(datetime(2024, 6, 1, 7, 0, 0, tzinfo=timezone.utc).timestamp())
        data = [
            ("v1", "IN_TRANSIT_TO", datetime.fromtimestamp(self.EPOCH, tz=timezone.utc),
             midnight_utc, 3600, 3660),
            ("v2", "STOPPED_AT", datetime.fromtimestamp(self.EPOCH, tz=timezone.utc),
             midnight_utc, 3600, 3660),
            ("v3", "INCOMING_AT", datetime.fromtimestamp(self.EPOCH, tz=timezone.utc),
             midnight_utc, 3600, 3660),
        ]
        return spark.createDataFrame(data, schema)

    def _compute_delay(self, spark, df):
        df = df.withColumn(
            "time_diff_seconds",
            when(
                col("current_status") == "IN_TRANSIT_TO",
                unix_timestamp(col("timestamp")) - unix_timestamp(
                    (col("date_midnight") + col("arrival_secs")).cast(TimestampType())
                ),
            ).when(
                col("current_status") == "STOPPED_AT",
                unix_timestamp(col("timestamp")) - unix_timestamp(
                    (col("date_midnight") + col("departure_secs")).cast(TimestampType())
                ),
            ).otherwise(None),
        )
        return df.select("vehicle_id", "time_diff_seconds").collect()

    def test_in_transit_to_uses_arrival_ts(self, spark, base_df):
        rows = self._compute_delay(spark, base_df)
        row = next(r for r in rows if r["vehicle_id"] == "v1")
        assert row["time_diff_seconds"] is not None
        # arrival_ts = midnight(07:00 UTC) + 3600s = 08:00 UTC
        # timestamp = 12:00 UTC → diff = 14400s
        assert row["time_diff_seconds"] == 14400

    def test_stopped_at_uses_departure_ts(self, spark, base_df):
        rows = self._compute_delay(spark, base_df)
        row = next(r for r in rows if r["vehicle_id"] == "v2")
        assert row["time_diff_seconds"] is not None
        # departure_ts = midnight(07:00 UTC) + 3660s = 08:01 UTC
        # timestamp = 12:00 UTC → diff = 14340s
        assert row["time_diff_seconds"] == 14340

    def test_other_status_returns_null(self, spark, base_df):
        rows = self._compute_delay(spark, base_df)
        row = next(r for r in rows if r["vehicle_id"] == "v3")
        assert row["time_diff_seconds"] is None

    def test_filter_extreme_delays_gt_12h(self, spark):
        midnight = int(datetime(2024, 6, 1, 7, 0, 0, tzinfo=timezone.utc).timestamp())
        schema = StructType([
            StructField("vehicle_id", StringType()),
            StructField("current_status", StringType()),
            StructField("timestamp", TimestampType()),
            StructField("date_midnight", LongType()),
            StructField("arrival_secs", LongType()),
            StructField("departure_secs", LongType()),
        ])
        data = [
            ("ok", "STOPPED_AT",
             datetime.fromtimestamp(midnight + 3660 + 3600, tz=timezone.utc),
             midnight, 0, 3660),   # delay = 3600s (1h) → OK
            ("extreme", "STOPPED_AT",
             datetime.fromtimestamp(midnight + 3660 + 50000, tz=timezone.utc),
             midnight, 0, 3660),   # delay = 50000s (>12h) → None
        ]
        df = spark.createDataFrame(data, schema)
        rows = self._compute_delay(spark, df)

        row_ok = next(r for r in rows if r["vehicle_id"] == "ok")
        assert row_ok["time_diff_seconds"] == 3600

        row_extreme = next(r for r in rows if r["vehicle_id"] == "extreme")
        df_with_filter = spark.createDataFrame(data, schema).withColumn(
            "time_diff_seconds",
            when(
                col("current_status") == "STOPPED_AT",
                unix_timestamp(col("timestamp")) - unix_timestamp(
                    (col("date_midnight") + col("departure_secs")).cast(TimestampType())
                ),
            ).otherwise(None),
        )
        df_filtered = df_with_filter.withColumn(
            "time_diff_seconds",
            when(
                (col("time_diff_seconds").isNotNull()) &
                (pyspark_abs(col("time_diff_seconds")) > 43200),
                None,
            ).otherwise(col("time_diff_seconds")),
        )
        result = df_filtered.select("vehicle_id", "time_diff_seconds").collect()
        extreme_row = next(r for r in result if r["vehicle_id"] == "extreme")
        assert extreme_row["time_diff_seconds"] is None

    def test_negative_delay_preserved(self, spark):
        """Delays negativos (adelanto) menores de 12h deben mantenerse."""
        midnight = int(datetime(2024, 6, 1, 7, 0, 0, tzinfo=timezone.utc).timestamp())
        schema = StructType([
            StructField("vehicle_id", StringType()),
            StructField("current_status", StringType()),
            StructField("timestamp", TimestampType()),
            StructField("date_midnight", LongType()),
            StructField("arrival_secs", LongType()),
            StructField("departure_secs", LongType()),
        ])
        data = [
            ("early", "STOPPED_AT",
             datetime.fromtimestamp(midnight + 3660 - 300, tz=timezone.utc),
             midnight, 0, 3660),  # -300s → -5 min
        ]
        df = spark.createDataFrame(data, schema)
        df = df.withColumn(
            "time_diff_seconds",
            when(
                col("current_status") == "STOPPED_AT",
                unix_timestamp(col("timestamp")) - unix_timestamp(
                    (col("date_midnight") + col("departure_secs")).cast(TimestampType())
                ),
            ).otherwise(None),
        )
        df = df.withColumn(
            "time_diff_seconds",
            when(
                (col("time_diff_seconds").isNotNull()) &
                (pyspark_abs(col("time_diff_seconds")) > 43200),
                None,
            ).otherwise(col("time_diff_seconds")),
        )
        result = df.collect()
        assert result[0]["time_diff_seconds"] == -300
