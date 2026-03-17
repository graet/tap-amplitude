"""Amplitude Singer streams."""

from __future__ import annotations

from functools import cached_property
from typing import Generator

import pendulum
from singer_sdk import Stream

from tap_amplitude.client import AmplitudeClient

# Amplitude returns timestamps as "YYYY-MM-DD HH:MM:SS.ffffff" (UTC, no tz marker).
# These fields get normalised to ISO-8601 before being emitted.
_TIMESTAMP_FIELDS = (
    "event_time",
    "server_upload_time",
    "client_event_time",
    "client_upload_time",
    "server_received_time",
)


class EventsStream(Stream):
    """Raw events exported from Amplitude via the Export API.

    Uses incremental replication keyed on ``event_time``.  On every run the
    tap starts from the bookmarked ``event_time`` (or ``start_date`` from
    config) and requests data up to two hours before *now* (giving Amplitude
    time to fully land the data).  The date range is broken into day-sized
    chunks to keep individual HTTP requests manageable.
    """

    name = "events"
    primary_keys = ["uuid"]
    replication_key = "event_time"

    schema = {
        "type": "object",
        "properties": {
            # ── identity ──────────────────────────────────────────────────────
            "uuid": {"type": "string"},
            "event_id": {"type": ["integer", "null"]},
            "amplitude_id": {"type": ["integer", "null"]},
            "user_id": {"type": ["string", "null"]},
            "device_id": {"type": ["string", "null"]},
            "session_id": {"type": ["integer", "null"]},
            "app": {"type": ["integer", "null"]},
            # ── event metadata ────────────────────────────────────────────────
            "event_type": {"type": ["string", "null"]},
            "event_time": {"type": ["string", "null"], "format": "date-time"},
            "server_upload_time": {"type": ["string", "null"], "format": "date-time"},
            "client_event_time": {"type": ["string", "null"], "format": "date-time"},
            "client_upload_time": {"type": ["string", "null"], "format": "date-time"},
            "server_received_time": {"type": ["string", "null"], "format": "date-time"},
            # ── geo / network ─────────────────────────────────────────────────
            "ip_address": {"type": ["string", "null"]},
            "city": {"type": ["string", "null"]},
            "region": {"type": ["string", "null"]},
            "country": {"type": ["string", "null"]},
            "dma": {"type": ["string", "null"]},
            "location_lat": {"type": ["number", "null"]},
            "location_lng": {"type": ["number", "null"]},
            # ── device / platform ─────────────────────────────────────────────
            "platform": {"type": ["string", "null"]},
            "os_name": {"type": ["string", "null"]},
            "os_version": {"type": ["string", "null"]},
            "device_brand": {"type": ["string", "null"]},
            "device_carrier": {"type": ["string", "null"]},
            "device_family": {"type": ["string", "null"]},
            "device_manufacturer": {"type": ["string", "null"]},
            "device_model": {"type": ["string", "null"]},
            "device_type": {"type": ["string", "null"]},
            # ── app version ───────────────────────────────────────────────────
            "version_name": {"type": ["string", "null"]},
            "start_version": {"type": ["string", "null"]},
            # ── user attributes ───────────────────────────────────────────────
            "language": {"type": ["string", "null"]},
            "library": {"type": ["string", "null"]},
            "paying": {"type": ["boolean", "null"]},
            "user_creation_time": {"type": ["string", "null"]},
            "is_attribution_event": {"type": ["boolean", "null"]},
            "adid": {"type": ["string", "null"]},
            "idfa": {"type": ["string", "null"]},
            # ── flexible payload fields ───────────────────────────────────────
            "event_properties": {
                "type": ["object", "null"],
                "additionalProperties": True,
            },
            "user_properties": {
                "type": ["object", "null"],
                "additionalProperties": True,
            },
            "groups": {
                "type": ["object", "null"],
                "additionalProperties": True,
            },
            "group_properties": {
                "type": ["object", "null"],
                "additionalProperties": True,
            },
            "data": {
                "type": ["object", "null"],
                "additionalProperties": True,
            },
        },
    }

    # ── internal helpers ──────────────────────────────────────────────────────

    @cached_property
    def _client(self) -> AmplitudeClient:
        return AmplitudeClient(
            api_key=self.config["api_key"],
            secret_key=self.config["secret_key"],
            logger=self.logger,
        )

    def _get_start_datetime(self, context: dict | None) -> pendulum.DateTime:
        """Return the earliest datetime to request, honouring the bookmark."""
        bookmark: str | None = self.get_starting_replication_key_value(context)
        if bookmark:
            # Amplitude stores timestamps as "YYYY-MM-DD HH:MM:SS.ffffff";
            # normalise to ISO-8601 so pendulum can parse it.
            normalised = bookmark.replace(" ", "T")
            if "+" not in normalised and not normalised.endswith("Z"):
                normalised += "+00:00"
            return pendulum.parse(normalised)

        start_date: str | None = self.config.get("start_date")
        if start_date:
            return pendulum.parse(start_date)

        return pendulum.now("UTC").subtract(days=30)

    @staticmethod
    def _to_amplitude_hour(dt: pendulum.DateTime) -> str:
        """Format a datetime as the ``YYYYMMDDTHH`` string expected by the API."""
        return f"{dt.year:04d}{dt.month:02d}{dt.day:02d}T{dt.hour:02d}"

    @staticmethod
    def _normalise_timestamps(record: dict) -> dict:
        """Convert Amplitude's space-separated UTC timestamps to ISO-8601."""
        for field in _TIMESTAMP_FIELDS:
            value = record.get(field)
            if value and isinstance(value, str) and " " in value:
                record[field] = value.replace(" ", "T") + "+00:00"
        return record

    # ── main data-fetching method ─────────────────────────────────────────────

    def get_records(self, context: dict | None) -> Generator[dict, None, None]:
        start_dt = self._get_start_datetime(context).start_of("hour")
        # Leave a 2-hour buffer so Amplitude has time to fully export the data.
        end_dt = pendulum.now("UTC").subtract(hours=2).start_of("hour")

        if start_dt >= end_dt:
            self.logger.info(
                "Nothing to sync: start (%s) is not before end (%s).",
                start_dt.isoformat(),
                end_dt.isoformat(),
            )
            return

        self.logger.info(
            "Syncing events from %s to %s",
            start_dt.isoformat(),
            end_dt.isoformat(),
        )

        current = start_dt
        while current <= end_dt:
            # Request at most one calendar day per API call.
            day_end = current.end_of("day")
            if day_end > end_dt:
                day_end = end_dt

            start_str = self._to_amplitude_hour(current)
            end_str = self._to_amplitude_hour(day_end)

            self.logger.info("Fetching events %s – %s", start_str, end_str)
            for record in self._client.export(start_str, end_str):
                yield self._normalise_timestamps(record)

            current = current.add(days=1).start_of("day")
