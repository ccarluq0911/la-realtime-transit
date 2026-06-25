import pytest
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from utils.gtfs import gtfs_time_to_seconds_str

LA_TZ = ZoneInfo("America/Los_Angeles")


# ── test helpers (réplica de lógica Spark que no existe en prod) ─

def compute_date_midnight(epoch_s: float) -> float:
    """
    Réplica de la lógica Spark en spark_read.py:88-97:

        unix_timestamp(
          concat(from_unixtime(unix_timestamp(timestamp), "yyyy-MM-dd"),
                 lit(" 00:00:00 America/Los_Angeles")),
          "yyyy-MM-dd HH:mm:ss z"
        )
    """
    utc_dt = datetime.fromtimestamp(epoch_s, tz=timezone.utc)
    la_dt = utc_dt.astimezone(LA_TZ)
    midnight_la = datetime(
        la_dt.year, la_dt.month, la_dt.day, 0, 0, 0, tzinfo=LA_TZ
    )
    return midnight_la.timestamp()


def compute_scheduled_ts(epoch_s: float, arrival_secs: int) -> float:
    return compute_date_midnight(epoch_s) + arrival_secs


# ── tests: gtfs_time_to_seconds_str ───────────────────────────

class TestGtfsTimeToSeconds:

    def test_normal_times(self):
        assert gtfs_time_to_seconds_str("00:00:00") == 0
        assert gtfs_time_to_seconds_str("00:00:01") == 1
        assert gtfs_time_to_seconds_str("01:30:00") == 5400
        assert gtfs_time_to_seconds_str("12:00:00") == 43200

    def test_overflow_hours_gt_24(self):
        assert gtfs_time_to_seconds_str("25:30:00") == 91800
        assert gtfs_time_to_seconds_str("30:15:45") == 108945
        assert gtfs_time_to_seconds_str("99:00:00") == 356400

    def test_fails_on_bad_format(self):
        with pytest.raises((ValueError, IndexError)):
            gtfs_time_to_seconds_str("")
        with pytest.raises((ValueError, IndexError)):
            gtfs_time_to_seconds_str("abc")
        with pytest.raises((ValueError, IndexError)):
            gtfs_time_to_seconds_str("00:00")


# ── tests: date_midnight y DST ──────────────────────────────────

class TestDateMidnightDst:

    # ── Fall Back (2 Nov → 3 Nov 2024) ─────────────────────────
    def test_fall_back_same_midnight_for_both_occurrences(self):
        """
        En Fall Back (2024-11-03), las 00:00 siguen en PDT
        (DST termina a las 02:00). Por tanto ambos vehículos,
        antes y después del cambio, tienen el mismo date_midnight.
        """
        epoch_pdt = datetime(2024, 11, 3, 8, 35, 0, tzinfo=timezone.utc).timestamp()  # 01:35 PDT
        epoch_pst = datetime(2024, 11, 3, 9, 35, 0, tzinfo=timezone.utc).timestamp()  # 01:35 PST

        mpdt = compute_date_midnight(epoch_pdt)
        mpst = compute_date_midnight(epoch_pst)

        assert mpdt == mpst
        expected = datetime(2024, 11, 3, 7, 0, 0, tzinfo=timezone.utc).timestamp()
        assert mpdt == pytest.approx(expected, abs=1)

    def test_fall_back_bug_delay_offset_by_one_hour(self):
        """
        ⚠️  COMPORTAMIENTO ACTUAL (probablemente un bug):

        En Fall Back, un vehículo en PST (post-transición) tiene su
        schedule computado contra midnight PDT (pre-transición), lo que
        añade 1 hora de error al delay.

        Ej: schedule 01:30, vehículo a 01:35 PST
          → schedule computado como 01:30 PDT = 08:30 UTC
          → vehículo a 01:35 PST = 09:35 UTC
          → delay = 65 min (en vez de 5 min)

        La corrección requeriría detectar si el timestamp del vehículo
        cae después del cambio DST y ajustar date_midnight en
        consecuencia.
        """
        arrival_secs = gtfs_time_to_seconds_str("01:30:00")

        # PDT: 01:35 PDT vs 01:30 PDT → 5 min tarde ✅
        epoch_pdt = datetime(2024, 11, 3, 8, 35, 0, tzinfo=timezone.utc).timestamp()
        sch_pdt = compute_scheduled_ts(epoch_pdt, arrival_secs)
        assert epoch_pdt - sch_pdt == pytest.approx(300, abs=2)

        # PST: 01:35 PST vs 01:30 PDT → ¡65 min! ❌
        epoch_pst = datetime(2024, 11, 3, 9, 35, 0, tzinfo=timezone.utc).timestamp()
        sch_pst = compute_scheduled_ts(epoch_pst, arrival_secs)
        assert epoch_pst - sch_pst == pytest.approx(3900, abs=2)

    # ── Spring Forward (9 Mar → 10 Mar 2024) ───────────────────
    def test_spring_forward_same_midnight_before_and_after(self):
        epoch_before = datetime(2024, 3, 10, 9, 59, 0, tzinfo=timezone.utc).timestamp()  # 01:59 PST
        epoch_after = datetime(2024, 3, 10, 10, 0, 0, tzinfo=timezone.utc).timestamp()   # 03:00 PDT

        m_before = compute_date_midnight(epoch_before)
        m_after = compute_date_midnight(epoch_after)
        assert m_before == pytest.approx(m_after, abs=1)

    def test_spring_forward_midnight_value(self):
        epoch = datetime(2024, 3, 10, 13, 0, 0, tzinfo=timezone.utc).timestamp()
        midnight = compute_date_midnight(epoch)
        expected = datetime(2024, 3, 10, 8, 0, 0, tzinfo=timezone.utc).timestamp()
        assert midnight == pytest.approx(expected, abs=1)

    def test_spring_forward_schedule_during_gap(self):
        arrival_secs = gtfs_time_to_seconds_str("02:30:00")
        epoch = datetime(2024, 3, 10, 10, 5, 0, tzinfo=timezone.utc).timestamp()
        sch = compute_scheduled_ts(epoch, arrival_secs)

        expected_sch = datetime(2024, 3, 10, 10, 30, 0, tzinfo=timezone.utc).timestamp()
        assert sch == pytest.approx(expected_sch, abs=1)
        assert epoch - sch == pytest.approx(-1500, abs=2)

    # ── Días normales sin transición ───────────────────────────
    def test_normal_day_no_dst_transition(self):
        arrival_secs = gtfs_time_to_seconds_str("14:00:00")
        epoch = datetime(2024, 6, 15, 21, 5, 0, tzinfo=timezone.utc).timestamp()
        sch = compute_scheduled_ts(epoch, arrival_secs)
        assert epoch - sch == pytest.approx(300, abs=2)

    def test_normal_day_negative_delay(self):
        arrival_secs = gtfs_time_to_seconds_str("14:00:00")
        epoch = datetime(2024, 6, 15, 20, 55, 0, tzinfo=timezone.utc).timestamp()
        sch = compute_scheduled_ts(epoch, arrival_secs)
        assert epoch - sch == pytest.approx(-300, abs=2)
