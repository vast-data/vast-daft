from __future__ import annotations

from types import SimpleNamespace

import pytest

from vast_daft.sql_ep import VastDBProvider


def test_vastdb_provider_skips_anonymous() -> None:
    provider = VastDBProvider()
    auth = SimpleNamespace(username=None, password=None, access_key=None, secret_key=None, header=lambda *_n: None)
    assert provider.shared() is None
    assert provider.for_call(auth) is None
    assert provider.cache_key(auth) is None


def test_vastdb_provider_builds_catalog_from_call_auth(monkeypatch) -> None:
    captured: dict[str, object] = {}

    class _FakeCatalog:
        def __init__(self, config, alias=None) -> None:
            captured["config"] = config
            captured["alias"] = alias

    monkeypatch.setattr("vast_daft.sql_ep.VastDBCatalog", _FakeCatalog)
    provider = VastDBProvider()
    headers = {"x-daft-vastdb-endpoint": "http://vippool:80", "x-daft-bucket": "b", "x-daft-schema": "s"}
    auth = SimpleNamespace(
        username="ak",
        password="sk",
        access_key="ak",
        secret_key="sk",
        header=lambda *names: next((headers[n] for n in names if n in headers), None),
    )
    alias, catalog = provider.for_call(auth) or ("", None)
    assert alias == "vastdb"
    assert catalog is not None
    config = captured["config"]
    assert config.endpoint == "http://vippool:80"
    assert config.access_key == "ak"
    assert config.bucket == "b"
    assert captured["alias"] == "vastdb"
    assert provider.cache_key(auth) == ("ak", "sk", "http://vippool:80", "b", "s")


def test_vastdb_provider_requires_endpoint() -> None:
    provider = VastDBProvider()
    auth = SimpleNamespace(username="ak", password="sk", access_key="ak", secret_key="sk", header=lambda *_n: None)
    with pytest.raises(RuntimeError, match="endpoint"):
        provider.for_call(auth)
