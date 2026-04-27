"""Unit tests for vast_daft.config."""

import os
from unittest import mock

import pytest

from vast_daft.config import VastDBConfig


class TestVastDBConfig:
    def test_direct_construction(self):
        cfg = VastDBConfig(
            endpoint="http://host:9090",
            access_key="ak",
            secret_key="sk",
            bucket="b",
            schema="s",
        )
        assert cfg.endpoint == "http://host:9090"
        assert cfg.access_key == "ak"
        assert cfg.ssl_verify is True

    def test_endpoint_gets_scheme_prefix(self):
        cfg = VastDBConfig(
            endpoint="host:9090",
            access_key="ak",
            secret_key="sk",
            bucket="b",
            schema="s",
        )
        assert cfg.endpoint == "http://host:9090"

    def test_https_endpoint_unchanged(self):
        cfg = VastDBConfig(
            endpoint="https://host:9090",
            access_key="ak",
            secret_key="sk",
            bucket="b",
            schema="s",
        )
        assert cfg.endpoint == "https://host:9090"

    def test_from_env(self):
        env = {
            "VASTDB_ENDPOINT": "http://e:9090",
            "VASTDB_ACCESS_KEY": "a",
            "VASTDB_SECRET_KEY": "s",
            "VASTDB_BUCKET": "b",
            "VASTDB_SCHEMA": "sc",
            "VASTDB_SSL_VERIFY": "false",
        }
        with mock.patch.dict(os.environ, env, clear=False):
            cfg = VastDBConfig.from_env()
        assert cfg.endpoint == "http://e:9090"
        assert cfg.ssl_verify is False

    def test_from_env_missing_var_raises(self):
        with mock.patch.dict(os.environ, {}, clear=True):
            with pytest.raises(EnvironmentError, match="VASTDB_ENDPOINT"):
                VastDBConfig.from_env()


class TestDaftNativeExpressions:
    """Smoke tests for Daft native filter expressions (no VastDB needed).

    These expressions are now used directly instead of the removed
    ``vast_daft.predicates`` helpers — Daft's query planner pushes them
    down to VastDB automatically via ``_DaftToIbisVisitor``.
    """

    def test_equal(self):
        from daft import col

        expr = col("col") == 42
        assert expr is not None

    def test_is_in(self):
        from daft import col

        expr = col("col").is_in([1, 2, 3])
        assert expr is not None

    def test_between(self):
        from daft import col, lit

        expr = (col("col") >= lit(0)) & (col("col") <= lit(100))
        assert expr is not None

    def test_contains(self):
        from daft import col

        expr = col("col").contains("needle")
        assert expr is not None

    def test_and_or_combinators(self):
        from daft import col

        p1 = col("a") == 1
        p2 = col("b") == 2
        assert (p1 & p2) is not None
        assert (p1 | p2) is not None
