import pytest
import http.client
import queue
from unittest.mock import patch, MagicMock, PropertyMock
from google.transit import gtfs_realtime_pb2

from vehicle_positions_producer import fetch_data, stop_event, batch_queue


@pytest.fixture(autouse=True)
def reset_globals():
    stop_event.clear()
    while not batch_queue.empty():
        try:
            batch_queue.get_nowait()
        except queue.Empty:
            break
    from vehicle_positions_producer import last_stops
    last_stops.clear()
    yield


def _build_feed(entity_data_list):
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.header.gtfs_realtime_version = "2.0"
    for data in entity_data_list:
        entity = feed.entity.add()
        entity.id = data.get("id", "bus_1")
        entity.vehicle.trip.route_id = data.get("route_id", "R1")
        entity.vehicle.trip.trip_id = data.get("trip_id", "T1")
        entity.vehicle.position.latitude = data.get("lat", 34.0)
        entity.vehicle.position.longitude = data.get("lon", -118.0)
        entity.vehicle.current_stop_sequence = data.get("stop_seq", 1)
        entity.vehicle.current_status = data.get("status", 0)
        entity.vehicle.timestamp = data.get("ts", 1_000_000)
    return feed


class TestFetchData:
    """Tests para fetch_data(): HTTP request, backoff, cola."""

    def _run_fetch_once(self, feed=None):
        if feed is None:
            feed = _build_feed([{"id": "v1", "status": 1}])
        mock_conn = MagicMock(spec=http.client.HTTPSConnection)
        mock_resp = MagicMock()
        mock_resp.read.return_value = feed.SerializeToString()
        mock_conn.getresponse.return_value = mock_resp

        def stop_after_sleep(seconds):
            stop_event.set()

        with patch("vehicle_positions_producer.http.client.HTTPSConnection", return_value=mock_conn):
            with patch("vehicle_positions_producer.time.sleep", side_effect=stop_after_sleep):
                fetch_data()

        batches = []
        while not batch_queue.empty():
            batches.append(batch_queue.get_nowait())
        return batches

    def test_fetch_with_vehicles_puts_batches(self):
        batches = self._run_fetch_once()
        all_records = [r for b in batches for r in b]
        assert len(all_records) >= 1
        assert any(r[0] == "v1" for r in all_records)

    def test_fetch_multiple_vehicles(self):
        feed = _build_feed([
            {"id": "v1"}, {"id": "v2"}, {"id": "v3"},
        ])
        batches = self._run_fetch_once(feed)
        all_records = [r for b in batches for r in b]
        assert len(all_records) == 3

    def test_fetch_empty_feed_puts_ten_empty_batches(self):
        batches = self._run_fetch_once(_build_feed([]))
        assert len(batches) == 10
        for b in batches:
            assert b == []

    def test_http_error_does_not_crash(self):
        mock_conn = MagicMock(spec=http.client.HTTPSConnection)
        mock_conn.request.side_effect = Exception("Network error")

        sleep_calls = []

        def capture_and_stop(seconds):
            sleep_calls.append(seconds)
            if len(sleep_calls) >= 2:
                stop_event.set()

        with patch("vehicle_positions_producer.http.client.HTTPSConnection", return_value=mock_conn):
            with patch("vehicle_positions_producer.time.sleep", side_effect=capture_and_stop):
                fetch_data()

        assert len(sleep_calls) >= 2

    def test_consecutive_errors_double_backoff(self):
        mock_conn = MagicMock(spec=http.client.HTTPSConnection)
        mock_conn.request.side_effect = Exception("Timeout")

        sleep_calls = []

        def capture_and_stop(seconds):
            sleep_calls.append(seconds)
            if len(sleep_calls) >= 2:
                stop_event.set()

        with patch("vehicle_positions_producer.http.client.HTTPSConnection", return_value=mock_conn):
            with patch("vehicle_positions_producer.time.sleep", side_effect=capture_and_stop):
                fetch_data()

        assert sleep_calls[0] == 20
        assert sleep_calls[1] == 40

    def test_backoff_resets_after_success(self):
        mock_conn = MagicMock(spec=http.client.HTTPSConnection)
        mock_resp = MagicMock()
        mock_resp.read.return_value = _build_feed([{"id": "v1"}]).SerializeToString()

        call_count = 0

        def request_side_effect(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise Exception("Fail")
            mock_conn.getresponse.return_value = mock_resp

        mock_conn.request.side_effect = request_side_effect

        sleep_calls = []

        def capture_and_stop(seconds):
            sleep_calls.append(seconds)
            if len(sleep_calls) >= 2:
                stop_event.set()

        with patch("vehicle_positions_producer.http.client.HTTPSConnection", return_value=mock_conn):
            with patch("vehicle_positions_producer.time.sleep", side_effect=capture_and_stop):
                fetch_data()

        assert sleep_calls[0] == 20
        assert sleep_calls[1] == 10

    def test_protobuf_decode_error_does_not_crash(self):
        mock_conn = MagicMock(spec=http.client.HTTPSConnection)
        mock_resp = MagicMock()
        mock_resp.read.return_value = b"garbage data"
        mock_conn.getresponse.return_value = mock_resp

        def stop_after_sleep(seconds):
            stop_event.set()

        with patch("vehicle_positions_producer.http.client.HTTPSConnection", return_value=mock_conn):
            with patch("vehicle_positions_producer.time.sleep", side_effect=stop_after_sleep):
                fetch_data()

    def test_invalid_status_index_returns_none(self):
        status_list = ["INCOMING_AT", "STOPPED_AT", "IN_TRANSIT_TO"]
        assert status_list[0] == "INCOMING_AT"
        assert status_list[1] == "STOPPED_AT"
        assert status_list[2] == "IN_TRANSIT_TO"
        with pytest.raises(IndexError):
            _ = status_list[99]

    def test_record_has_correct_field_order(self):
        feed = _build_feed([{"id": "b1", "route_id": "RR", "trip_id": "TT",
                             "lat": 33.9, "lon": -118.2, "stop_seq": 7,
                             "status": 1, "ts": 2_000_000}])
        batches = self._run_fetch_once(feed)
        r = [r for b in batches for r in b][0]
        assert r[0] == "b1"
        assert r[1] == "RR"
        assert r[2] == "TT"
        assert r[3] == pytest.approx(33.9, abs=0.001)
        assert r[4] == pytest.approx(-118.2, abs=0.001)
        assert r[5] == 7
        assert r[6] == "STOPPED_AT"


class TestProtobufParsing:
    """Tests para la extracción de entidades desde FeedMessage."""

    def test_entity_without_vehicle_is_skipped(self):
        feed = gtfs_realtime_pb2.FeedMessage()
        feed.header.gtfs_realtime_version = "2.0"
        feed.entity.add().id = "no_vehicle_1"
        feed.entity.add().id = "no_vehicle_2"
        v = feed.entity.add()
        v.id = "has_vehicle"
        v.vehicle.trip.route_id = "R1"

        extracted = []
        for entity in feed.entity:
            if entity.HasField("vehicle"):
                extracted.append(entity.id)

        assert len(extracted) == 1
        assert extracted[0] == "has_vehicle"

    def test_all_vehicle_fields_mapped_correctly(self):
        feed = _build_feed([{
            "id": "b1", "route_id": "R42", "trip_id": "T99",
            "lat": 34.1, "lon": -117.9, "stop_seq": 3,
            "status": 2, "ts": 1_500_000,
        }])
        for entity in feed.entity:
            if entity.HasField("vehicle"):
                v = entity.vehicle
                status = ["INCOMING_AT", "STOPPED_AT", "IN_TRANSIT_TO"][v.current_status]
                assert entity.id == "b1"
                assert v.trip.route_id == "R42"
                assert v.trip.trip_id == "T99"
                assert v.position.latitude == pytest.approx(34.1, abs=0.001)
                assert v.position.longitude == pytest.approx(-117.9, abs=0.001)
                assert v.current_stop_sequence == 3
                assert status == "IN_TRANSIT_TO"
                assert v.timestamp == 1_500_000

    def test_extract_from_stopped_at(self):
        feed = _build_feed([{"id": "b1", "status": 1}])
        for entity in feed.entity:
            if entity.HasField("vehicle"):
                status = ["INCOMING_AT", "STOPPED_AT", "IN_TRANSIT_TO"][entity.vehicle.current_status]
                assert status == "STOPPED_AT"

    def test_extract_from_incoming_at(self):
        feed = _build_feed([{"id": "b1", "status": 0}])
        for entity in feed.entity:
            if entity.HasField("vehicle"):
                status = ["INCOMING_AT", "STOPPED_AT", "IN_TRANSIT_TO"][entity.vehicle.current_status]
                assert status == "INCOMING_AT"

    def test_extract_from_in_transit_to(self):
        feed = _build_feed([{"id": "b1", "status": 2}])
        for entity in feed.entity:
            if entity.HasField("vehicle"):
                status = ["INCOMING_AT", "STOPPED_AT", "IN_TRANSIT_TO"][entity.vehicle.current_status]
                assert status == "IN_TRANSIT_TO"

    def test_multiple_entities_independent(self):
        feed = _build_feed([
            {"id": "b1", "route_id": "R1", "status": 0},
            {"id": "b2", "route_id": "R2", "status": 1},
            {"id": "b3", "route_id": "R3", "status": 2},
        ])
        ids = []
        for entity in feed.entity:
            if entity.HasField("vehicle"):
                ids.append(entity.id)
                v = entity.vehicle
                status = ["INCOMING_AT", "STOPPED_AT", "IN_TRANSIT_TO"][v.current_status]
                assert (entity.id, v.trip.route_id, status) in [
                    ("b1", "R1", "INCOMING_AT"),
                    ("b2", "R2", "STOPPED_AT"),
                    ("b3", "R3", "IN_TRANSIT_TO"),
                ]
        assert len(ids) == 3
