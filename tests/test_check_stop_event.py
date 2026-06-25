import pytest
import random
from unittest.mock import patch

from vehicle_positions_producer import check_stop_event, last_stops


def clear_last_stops():
    last_stops.clear()


class TestCheckStopEvent:

    def setup_method(self):
        clear_last_stops()

    def test_first_encounter_assigns_random_amount(self):
        with patch.object(random, "randint", return_value=42):
            amount = check_stop_event("bus_1", 1)
        assert amount == 42
        assert last_stops["bus_1"] == (1, 42)

    def test_same_bus_same_stop_returns_cached(self):
        last_stops["bus_1"] = (5, 30)
        amount = check_stop_event("bus_1", 5)
        assert amount == 30

    def test_new_stop_within_40pct_applies_delta(self):
        last_stops["bus_1"] = (5, 30)
        with patch.object(random, "random", return_value=0.3), \
             patch.object(random, "randint", return_value=4):
            amount = check_stop_event("bus_1", 10)
        assert amount == 34
        assert last_stops["bus_1"] == (10, 34)

    def test_new_stop_outside_40pct_keeps_amount(self):
        last_stops["bus_1"] = (5, 30)
        with patch.object(random, "random", return_value=0.5):
            amount = check_stop_event("bus_1", 10)
        assert amount == 30
        assert last_stops["bus_1"] == (10, 30)

    def test_multiple_buses_independent(self):
        with patch.object(random, "randint", side_effect=[10, 99]):
            a1 = check_stop_event("bus_a", 1)
            a2 = check_stop_event("bus_b", 1)
        assert a1 == 10
        assert a2 == 99
        assert last_stops["bus_a"] == (1, 10)
        assert last_stops["bus_b"] == (1, 99)

    def test_amount_clamped_at_zero(self):
        last_stops["bus_1"] = (5, 3)
        with patch.object(random, "random", return_value=0.3), \
             patch.object(random, "randint", return_value=-6):
            amount = check_stop_event("bus_1", 10)
        assert amount == 0

    def test_amount_clamped_at_seventy(self):
        last_stops["bus_1"] = (5, 68)
        with patch.object(random, "random", return_value=0.3), \
             patch.object(random, "randint", return_value=5):
            amount = check_stop_event("bus_1", 10)
        assert amount == 70

    def test_zero_people_no_negative_on_exit(self):
        last_stops["bus_1"] = (5, 0)
        with patch.object(random, "random", return_value=0.3), \
             patch.object(random, "randint", return_value=-1):
            amount = check_stop_event("bus_1", 10)
        assert amount == 0
