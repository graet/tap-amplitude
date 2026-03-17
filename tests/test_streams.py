"""Tests for tap_amplitude.streams.EventsStream."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pendulum

from tap_amplitude.streams import EventsStream
from tap_amplitude.tap import TapAmplitude

# ── helpers ───────────────────────────────────────────────────────────────────

_BASE_CONFIG = {"api_key": "test_key", "secret_key": "test_secret"}


def _make_stream(extra_config: dict | None = None) -> EventsStream:
    config = {**_BASE_CONFIG, **(extra_config or {})}
    tap = TapAmplitude(config=config, parse_env_config=False)
    return tap.streams["events"]


# ── _to_amplitude_hour ────────────────────────────────────────────────────────


class TestToAmplitudeHour:
    def test_formats_correctly(self):
        dt = pendulum.datetime(2026, 3, 1, 14, 30, 0, tz="UTC")
        assert EventsStream._to_amplitude_hour(dt) == "20260301T14"

    def test_zero_pads_month_and_day(self):
        dt = pendulum.datetime(2026, 1, 5, 0, 0, 0, tz="UTC")
        assert EventsStream._to_amplitude_hour(dt) == "20260105T00"

    def test_midnight_hour(self):
        dt = pendulum.datetime(2026, 12, 31, 0, 0, 0, tz="UTC")
        assert EventsStream._to_amplitude_hour(dt) == "20261231T00"


# ── _normalise_timestamps ─────────────────────────────────────────────────────


class TestNormaliseTimestamps:
    def test_converts_amplitude_space_format(self):
        record = {"event_time": "2026-03-01 10:23:45.000000"}
        result = EventsStream._normalise_timestamps(record)
        assert result["event_time"] == "2026-03-01T10:23:45.000000+00:00"

    def test_converts_all_timestamp_fields(self):
        record = {
            "event_time": "2026-03-01 10:00:00.000000",
            "server_upload_time": "2026-03-01 10:00:01.000000",
            "client_event_time": "2026-03-01 09:59:59.000000",
            "client_upload_time": "2026-03-01 10:00:00.500000",
            "server_received_time": "2026-03-01 10:00:00.800000",
        }
        result = EventsStream._normalise_timestamps(record)
        for field, value in result.items():
            assert "T" in value and "+00:00" in value, f"{field} was not normalised"

    def test_leaves_already_iso_unchanged(self):
        record = {"event_time": "2026-03-01T10:23:45+00:00"}
        result = EventsStream._normalise_timestamps(record)
        assert result["event_time"] == "2026-03-01T10:23:45+00:00"

    def test_skips_null_values(self):
        record = {"event_time": None, "server_upload_time": "2026-03-01 10:00:00.000000"}
        result = EventsStream._normalise_timestamps(record)
        assert result["event_time"] is None

    def test_leaves_non_timestamp_fields_untouched(self):
        record = {
            "event_time": "2026-03-01 10:00:00.000000",
            "event_type": "some event with spaces",
        }
        result = EventsStream._normalise_timestamps(record)
        assert result["event_type"] == "some event with spaces"


# ── _get_start_datetime ───────────────────────────────────────────────────────


class TestGetStartDatetime:
    def test_uses_bookmark_when_present(self):
        stream = _make_stream()
        with patch.object(
            stream,
            "get_starting_replication_key_value",
            return_value="2026-03-10 08:00:00.000000",
        ):
            result = stream._get_start_datetime(None)

        assert result.year == 2026
        assert result.month == 3
        assert result.day == 10
        assert result.hour == 8

    def test_bookmark_already_iso8601(self):
        stream = _make_stream()
        with patch.object(
            stream,
            "get_starting_replication_key_value",
            return_value="2026-03-10T08:00:00+00:00",
        ):
            result = stream._get_start_datetime(None)

        assert result.hour == 8

    def test_uses_config_start_date_when_no_bookmark(self):
        stream = _make_stream({"start_date": "2026-01-15T00:00:00Z"})
        with patch.object(stream, "get_starting_replication_key_value", return_value=None):
            result = stream._get_start_datetime(None)

        assert result.year == 2026
        assert result.month == 1
        assert result.day == 15

    def test_defaults_to_30_days_ago_when_nothing_set(self):
        stream = _make_stream()
        with patch.object(stream, "get_starting_replication_key_value", return_value=None):
            result = stream._get_start_datetime(None)

        expected = pendulum.now("UTC").subtract(days=30)
        diff_days = abs((result - expected).total_seconds()) / 86400
        assert diff_days < 1, "Default start date should be approximately 30 days ago"


# ── get_records ───────────────────────────────────────────────────────────────


class TestGetRecords:
    def _frozen_now(self) -> pendulum.DateTime:
        return pendulum.datetime(2026, 3, 16, 12, 0, 0, tz="UTC")

    def test_yields_normalised_records(self):
        # start and end are on the same day → exactly one export() call
        stream = _make_stream({"start_date": "2026-03-16T08:00:00Z"})
        raw_event = {
            "uuid": "test-uuid-001",
            "event_type": "button_click",
            "event_time": "2026-03-16 08:30:00.000000",
        }
        mock_client = MagicMock()
        mock_client.export.return_value = [raw_event]
        stream._client = mock_client

        with patch("tap_amplitude.streams.pendulum.now", return_value=self._frozen_now()):
            records = list(stream.get_records(None))

        assert len(records) == 1
        assert records[0]["uuid"] == "test-uuid-001"
        assert records[0]["event_time"] == "2026-03-16T08:30:00.000000+00:00"

    def test_calls_export_with_correct_date_range(self):
        stream = _make_stream({"start_date": "2026-03-15T00:00:00Z"})
        mock_client = MagicMock()
        mock_client.export.return_value = []
        stream._client = mock_client

        with patch("tap_amplitude.streams.pendulum.now", return_value=self._frozen_now()):
            list(stream.get_records(None))

        calls = mock_client.export.call_args_list
        assert len(calls) >= 1
        first_start = calls[0][0][0]
        assert first_start == "20260315T00"

    def test_chunks_multi_day_range_by_day(self):
        stream = _make_stream({"start_date": "2026-03-13T00:00:00Z"})
        mock_client = MagicMock()
        mock_client.export.return_value = []
        stream._client = mock_client

        with patch("tap_amplitude.streams.pendulum.now", return_value=self._frozen_now()):
            list(stream.get_records(None))

        # start=Mar 13, end=Mar 16 12:00 - 2h = Mar 16 10:00 → 4 day chunks
        assert mock_client.export.call_count >= 3

    def test_yields_nothing_when_start_is_after_end(self):
        """If start_date is very recent the tap should emit nothing, not error."""
        stream = _make_stream({"start_date": "2026-03-16T11:30:00Z"})
        mock_client = MagicMock()
        stream._client = mock_client

        with patch("tap_amplitude.streams.pendulum.now", return_value=self._frozen_now()):
            records = list(stream.get_records(None))

        assert records == []
        mock_client.export.assert_not_called()
