"""Tests for tap_amplitude.tap.TapAmplitude."""

from __future__ import annotations

import pytest

from tap_amplitude.streams import EventsStream
from tap_amplitude.tap import TapAmplitude


class TestDiscoverStreams:
    def test_returns_events_stream(self):
        tap = TapAmplitude(
            config={"api_key": "k", "secret_key": "s"},
            parse_env_config=False,
        )
        streams = tap.discover_streams()
        assert len(streams) == 1
        assert isinstance(streams[0], EventsStream)

    def test_stream_accessible_by_name(self):
        tap = TapAmplitude(
            config={"api_key": "k", "secret_key": "s"},
            parse_env_config=False,
        )
        assert "events" in tap.streams


class TestConfigValidation:
    def test_valid_config_accepted(self):
        tap = TapAmplitude(
            config={
                "api_key": "my_api_key",
                "secret_key": "my_secret",
                "start_date": "2024-01-01T00:00:00Z",
            },
            parse_env_config=False,
        )
        assert tap.config["api_key"] == "my_api_key"

    def test_missing_api_key_raises(self):
        with pytest.raises(Exception):
            TapAmplitude(
                config={"secret_key": "s"},
                parse_env_config=False,
            )

    def test_missing_secret_key_raises(self):
        with pytest.raises(Exception):
            TapAmplitude(
                config={"api_key": "k"},
                parse_env_config=False,
            )

    def test_start_date_is_optional(self):
        tap = TapAmplitude(
            config={"api_key": "k", "secret_key": "s"},
            parse_env_config=False,
        )
        assert tap.config.get("start_date") is None
