"""Tests for tap_amplitude.client.AmplitudeClient."""

from __future__ import annotations

import gzip
import io
import json
import zipfile
from unittest.mock import MagicMock

import pytest
import requests

from tap_amplitude.client import AmplitudeClient

# ── helpers ───────────────────────────────────────────────────────────────────


def _make_zip_response(events: list[dict]) -> bytes:
    """Build the zip-of-gzip-JSONL bytes that Amplitude's Export API returns."""
    gz_buf = io.BytesIO()
    with gzip.GzipFile(fileobj=gz_buf, mode="wb") as gz:
        for event in events:
            gz.write((json.dumps(event) + "\n").encode())

    zip_buf = io.BytesIO()
    with zipfile.ZipFile(zip_buf, "w") as zf:
        zf.writestr("123456_20260301_0#1_1.json.gz", gz_buf.getvalue())

    return zip_buf.getvalue()


def _mock_response(status_code: int, content: bytes = b"") -> MagicMock:
    resp = MagicMock(spec=requests.Response)
    resp.status_code = status_code
    resp.content = content
    resp.raise_for_status = MagicMock(
        side_effect=None if status_code < 400 else requests.HTTPError(response=resp)
    )
    return resp


def _make_client() -> AmplitudeClient:
    return AmplitudeClient(api_key="test_key", secret_key="test_secret")


# ── tests ─────────────────────────────────────────────────────────────────────


class TestExportHappyPath:
    def test_yields_all_events(self):
        events = [
            {"uuid": "aaa", "event_type": "page_view", "event_time": "2026-03-01 10:00:00.000000"},
            {
                "uuid": "bbb",
                "event_type": "button_click",
                "event_time": "2026-03-01 10:05:00.000000",
            },
        ]
        client = _make_client()
        client._session.get = MagicMock(
            return_value=_mock_response(200, _make_zip_response(events))
        )

        result = list(client.export("20260301T10", "20260301T10"))

        assert result == events

    def test_passes_correct_params_to_api(self):
        client = _make_client()
        client._session.get = MagicMock(return_value=_mock_response(200, _make_zip_response([])))

        list(client.export("20260301T00", "20260301T23"))

        _, kwargs = client._session.get.call_args
        assert kwargs["params"] == {"start": "20260301T00", "end": "20260301T23"}

    def test_handles_zip_with_multiple_files(self):
        """Multiple hourly files in the zip are all read."""

        def _make_multi_zip() -> bytes:
            zip_buf = io.BytesIO()
            with zipfile.ZipFile(zip_buf, "w") as zf:
                for hour, uuid in [(0, "aaa"), (1, "bbb")]:
                    gz_buf = io.BytesIO()
                    with gzip.GzipFile(fileobj=gz_buf, mode="wb") as gz:
                        gz.write((json.dumps({"uuid": uuid}) + "\n").encode())
                    zf.writestr(f"proj_{hour:02d}.json.gz", gz_buf.getvalue())
            return zip_buf.getvalue()

        client = _make_client()
        client._session.get = MagicMock(return_value=_mock_response(200, _make_multi_zip()))

        result = list(client.export("20260301T00", "20260301T01"))

        assert {r["uuid"] for r in result} == {"aaa", "bbb"}

    def test_skips_blank_lines_in_gzip(self):
        gz_buf = io.BytesIO()
        with gzip.GzipFile(fileobj=gz_buf, mode="wb") as gz:
            gz.write(b'\n{"uuid": "aaa"}\n\n{"uuid": "bbb"}\n')

        zip_buf = io.BytesIO()
        with zipfile.ZipFile(zip_buf, "w") as zf:
            zf.writestr("data.json.gz", gz_buf.getvalue())

        client = _make_client()
        client._session.get = MagicMock(return_value=_mock_response(200, zip_buf.getvalue()))

        result = list(client.export("20260301T00", "20260301T00"))

        assert [r["uuid"] for r in result] == ["aaa", "bbb"]


class TestExportErrorHandling:
    def test_404_yields_nothing(self):
        client = _make_client()
        client._session.get = MagicMock(return_value=_mock_response(404))

        result = list(client.export("20260301T00", "20260301T23"))

        assert result == []

    def test_bad_zip_yields_nothing(self):
        client = _make_client()
        client._session.get = MagicMock(return_value=_mock_response(200, b"this is not a zip file"))

        result = list(client.export("20260301T00", "20260301T23"))

        assert result == []

    def test_5xx_raises_http_error(self):
        client = _make_client()
        client._session.get = MagicMock(return_value=_mock_response(500))

        with pytest.raises(requests.HTTPError):
            list(client.export("20260301T00", "20260301T23"))
