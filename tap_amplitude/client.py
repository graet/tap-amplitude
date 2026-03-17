"""Amplitude Export API client."""

from __future__ import annotations

import gzip
import io
import json
import logging
import zipfile
from typing import Generator

import requests


class AmplitudeClient:
    """Thin wrapper around Amplitude's Export API.

    The Export API returns a zip archive where every entry is a gzip-compressed
    newline-delimited JSON file (one file per project / hour segment).
    """

    BASE_URL = "https://amplitude.com/api/2"

    def __init__(
        self,
        api_key: str,
        secret_key: str,
        logger: logging.Logger | None = None,
    ) -> None:
        self.logger = logger or logging.getLogger(__name__)
        self._session = requests.Session()
        self._session.auth = (api_key, secret_key)

    def export(self, start: str, end: str) -> Generator[dict, None, None]:
        """Yield raw event dicts from the Amplitude Export API.

        Args:
            start: Range start in ``YYYYMMDDTHH`` format (UTC).
            end:   Range end   in ``YYYYMMDDTHH`` format (UTC).

        Yields:
            One dict per Amplitude event.
        """
        url = f"{self.BASE_URL}/export"
        self.logger.debug("GET %s  start=%s  end=%s", url, start, end)

        response = self._session.get(url, params={"start": start, "end": end})

        if response.status_code == 404:
            self.logger.info("No data for range %s – %s (404)", start, end)
            return

        response.raise_for_status()

        content = io.BytesIO(response.content)
        try:
            zf = zipfile.ZipFile(content)
        except zipfile.BadZipFile:
            self.logger.warning(
                "Response for %s – %s is not a valid zip file; skipping.", start, end
            )
            return

        with zf:
            for name in zf.namelist():
                self.logger.debug("Decompressing %s", name)
                with zf.open(name) as entry:
                    with gzip.GzipFile(fileobj=entry) as gz:
                        for raw_line in gz:
                            raw_line = raw_line.strip()
                            if raw_line:
                                yield json.loads(raw_line)
