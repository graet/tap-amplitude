"""Singer tap for Amplitude."""

from singer_sdk import Tap
from singer_sdk import typing as th

from tap_amplitude.streams import EventsStream


class TapAmplitude(Tap):
    """Singer tap for the Amplitude Export API."""

    name = "tap-amplitude"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "api_key",
            th.StringType,
            required=True,
            secret=True,
            description="Amplitude API key (Project Settings → API Credentials).",
        ),
        th.Property(
            "secret_key",
            th.StringType,
            required=True,
            secret=True,
            description="Amplitude Secret key (Project Settings → API Credentials).",
        ),
        th.Property(
            "start_date",
            th.DateTimeType,
            description=(
                "Earliest date to sync in ISO-8601 format, e.g. 2024-01-01T00:00:00Z. "
                "Defaults to 30 days ago when not set."
            ),
        ),
    ).to_dict()

    def discover_streams(self) -> list:
        return [EventsStream(self)]


if __name__ == "__main__":
    TapAmplitude.cli()
