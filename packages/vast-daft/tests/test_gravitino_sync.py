"""Tests for Gravitino sync helpers."""

from __future__ import annotations

import pytest

from vast_daft.gravitino_sync import GravitinoAPI, _parse_external_tables_json


class _DummyResponse:
    def __init__(self, status_code: int = 200, text: str = "") -> None:
        self.status_code = status_code
        self.text = text

    @property
    def ok(self) -> bool:
        return 200 <= self.status_code < 300


class _DummySession:
    def __init__(self) -> None:
        self.payload = None

    def post(self, _url, json):
        self.payload = json
        return _DummyResponse()


def test_parse_external_tables_json_valid():
    payload = """
    [
      {
        "schema": "raw",
        "name": "events",
        "format": "PARQUET",
        "location": "s3://bucket/events/",
        "columns": [{"name": "id", "type": "long", "nullable": false}]
      }
    ]
    """
    parsed = _parse_external_tables_json(payload)
    assert parsed[0]["schema"] == "raw"
    assert parsed[0]["format"] == "PARQUET"


def test_parse_external_tables_json_invalid():
    with pytest.raises(ValueError, match="valid JSON"):
        _parse_external_tables_json("{bad json")


def test_register_table_stores_format_in_properties_only():
    api = GravitinoAPI("http://example", "metalake", "catalog")
    dummy_session = _DummySession()
    api.session = dummy_session

    api.register_table(
        schema_name="raw",
        table_name="events",
        table_format="PARQUET",
        columns=[{"name": "id", "type": "long", "nullable": False, "comment": ""}],
        properties={"file.format": "PARQUET", "table.location": "s3://bucket/path/"},
    )

    assert "format" not in dummy_session.payload
    assert dummy_session.payload["properties"]["format"] == "PARQUET"
