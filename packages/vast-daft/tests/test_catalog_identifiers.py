"""Unit tests for VastDBCatalog identifier resolution.

These tests exercise _strip_catalog_prefix and _resolve_bucket_schema across
all three configuration modes without requiring a live VastDB connection.
"""

from __future__ import annotations

import os
from unittest import mock

import pytest
from daft.catalog import Identifier

from vast_daft.config import VastDBConfig
from vast_daft.table import VastDBCatalog

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _catalog(bucket: str | None = None, schema: str | None = None, alias: str | None = None) -> VastDBCatalog:
    """Build a VastDBCatalog with a fake connection (no network calls)."""
    cfg = VastDBConfig(
        endpoint="http://fake:9090",
        access_key="ak",
        secret_key="sk",
        bucket=bucket,
        schema=schema,
    )
    cat = object.__new__(VastDBCatalog)
    cat._config = cfg
    cat._alias = alias
    # _connection is never touched by the pure identifier methods
    return cat


# ---------------------------------------------------------------------------
# VastDBConfig — optional bucket/schema
# ---------------------------------------------------------------------------


class TestVastDBConfigOptional:
    def test_no_bucket_no_schema_defaults_to_none(self):
        cfg = VastDBConfig(endpoint="http://h:9090", access_key="a", secret_key="s")
        assert cfg.bucket is None
        assert cfg.schema is None

    def test_bucket_only(self):
        cfg = VastDBConfig(endpoint="http://h:9090", access_key="a", secret_key="s", bucket="b")
        assert cfg.bucket == "b"
        assert cfg.schema is None

    def test_from_env_bucket_optional(self):
        """from_env succeeds even without VASTDB_BUCKET / VASTDB_SCHEMA."""
        env = {
            "VASTDB_ENDPOINT": "http://e:9090",
            "VASTDB_ACCESS_KEY": "a",
            "VASTDB_SECRET_KEY": "s",
        }
        with mock.patch.dict(os.environ, env, clear=True):
            cfg = VastDBConfig.from_env()
        assert cfg.bucket is None
        assert cfg.schema is None

    def test_from_env_with_bucket_and_schema(self):
        env = {
            "VASTDB_ENDPOINT": "http://e:9090",
            "VASTDB_ACCESS_KEY": "a",
            "VASTDB_SECRET_KEY": "s",
            "VASTDB_BUCKET": "my-bucket",
            "VASTDB_SCHEMA": "my-schema",
        }
        with mock.patch.dict(os.environ, env, clear=True):
            cfg = VastDBConfig.from_env()
        assert cfg.bucket == "my-bucket"
        assert cfg.schema == "my-schema"


# ---------------------------------------------------------------------------
# VastDBCatalog.name
# ---------------------------------------------------------------------------


class TestCatalogName:
    def test_name_with_alias(self):
        cat = _catalog(bucket="b", schema="s", alias="mycat")
        assert cat.name == "mycat"

    def test_name_bucket_and_schema(self):
        cat = _catalog(bucket="b", schema="s")
        assert cat.name == "b/s"

    def test_name_bucket_only(self):
        cat = _catalog(bucket="b")
        assert cat.name == "b"

    def test_name_nothing(self):
        cat = _catalog()
        assert cat.name == "vastdb"


# ---------------------------------------------------------------------------
# _strip_catalog_prefix
# ---------------------------------------------------------------------------


class TestStripCatalogPrefix:
    def test_strips_alias(self):
        cat = _catalog(bucket="b", schema="s", alias="mycat")
        ident = Identifier("mycat", "tbl")
        assert cat._strip_catalog_prefix(ident) == Identifier("tbl")

    def test_strips_catalog_name(self):
        cat = _catalog(bucket="b", schema="s")
        ident = Identifier("b/s", "tbl")
        assert cat._strip_catalog_prefix(ident) == Identifier("tbl")

    def test_no_prefix_unchanged(self):
        cat = _catalog(bucket="b", schema="s", alias="mycat")
        ident = Identifier("ns", "tbl")
        assert cat._strip_catalog_prefix(ident) == Identifier("ns", "tbl")

    def test_single_part_unchanged(self):
        cat = _catalog(bucket="b", schema="s", alias="mycat")
        ident = Identifier("tbl")
        assert cat._strip_catalog_prefix(ident) == Identifier("tbl")


# ---------------------------------------------------------------------------
# _resolve_bucket_schema — mode 1: both bucket & schema fixed
# ---------------------------------------------------------------------------


class TestResolveMode1BothFixed:
    """Config has bucket+schema; identifier is just "table" (or "ns.table")."""

    def setup_method(self):
        self.cat = _catalog(bucket="my-bucket", schema="my-schema")

    def test_single_part_table(self):
        bucket, schema, rel = self.cat._resolve_bucket_schema(Identifier("tbl"))
        assert bucket == "my-bucket"
        assert schema == "my-schema"
        assert rel == Identifier("tbl")

    def test_two_part_ns_table(self):
        bucket, schema, rel = self.cat._resolve_bucket_schema(Identifier("ns", "tbl"))
        assert bucket == "my-bucket"
        assert schema == "my-schema"
        assert rel == Identifier("ns", "tbl")

    def test_strips_alias_then_resolves(self):
        cat = _catalog(bucket="b", schema="s", alias="mycat")
        bucket, schema, rel = cat._resolve_bucket_schema(Identifier("mycat", "tbl"))
        assert bucket == "b"
        assert schema == "s"
        assert rel == Identifier("tbl")


# ---------------------------------------------------------------------------
# _resolve_bucket_schema — mode 2: bucket fixed, schema from identifier
# ---------------------------------------------------------------------------


class TestResolveMode2BucketFixed:
    """Config has bucket only; identifier must be "schema.table"."""

    def setup_method(self):
        self.cat = _catalog(bucket="my-bucket")

    def test_two_parts(self):
        bucket, schema, rel = self.cat._resolve_bucket_schema(Identifier("my-schema", "tbl"))
        assert bucket == "my-bucket"
        assert schema == "my-schema"
        assert rel == Identifier("tbl")

    def test_three_parts_ns_consumed(self):
        bucket, schema, rel = self.cat._resolve_bucket_schema(Identifier("my-schema", "ns", "tbl"))
        assert bucket == "my-bucket"
        assert schema == "my-schema"
        assert rel == Identifier("ns", "tbl")

    def test_single_part_raises(self):
        with pytest.raises(ValueError, match="at least 2 components"):
            self.cat._resolve_bucket_schema(Identifier("tbl"))


# ---------------------------------------------------------------------------
# _resolve_bucket_schema — mode 3: neither bucket nor schema fixed
# ---------------------------------------------------------------------------


class TestResolveMode3NoneFixed:
    """Config has no bucket/schema; identifier must be "bucket.schema.table"."""

    def setup_method(self):
        self.cat = _catalog()

    def test_three_parts(self):
        bucket, schema, rel = self.cat._resolve_bucket_schema(Identifier("my-bucket", "my-schema", "tbl"))
        assert bucket == "my-bucket"
        assert schema == "my-schema"
        assert rel == Identifier("tbl")

    def test_four_parts_ns_consumed(self):
        bucket, schema, rel = self.cat._resolve_bucket_schema(Identifier("my-bucket", "my-schema", "ns", "tbl"))
        assert bucket == "my-bucket"
        assert schema == "my-schema"
        assert rel == Identifier("ns", "tbl")

    def test_two_parts_raises(self):
        with pytest.raises(ValueError, match="at least 3 components"):
            self.cat._resolve_bucket_schema(Identifier("my-schema", "tbl"))

    def test_one_part_raises(self):
        with pytest.raises(ValueError, match="at least 3 components"):
            self.cat._resolve_bucket_schema(Identifier("tbl"))


# ---------------------------------------------------------------------------
# _list_tables / _list_namespaces raise when bucket is None
# ---------------------------------------------------------------------------


class TestListRequiresBucket:
    def test_list_tables_raises_without_bucket(self):
        cat = _catalog()
        with pytest.raises(NotImplementedError, match="requires config.bucket"):
            cat._list_tables()

    def test_list_namespaces_raises_without_bucket(self):
        cat = _catalog()
        with pytest.raises(NotImplementedError, match="requires config.bucket"):
            cat._list_namespaces()


# ---------------------------------------------------------------------------
# VastDBTable._schema_path — required by the metadata-cache key
# ---------------------------------------------------------------------------


class TestVastDBTableSchemaPath:
    def _table(self, *, namespace: tuple[str, ...] = ()):
        from vast_daft.table import VastDBTable

        cfg = VastDBConfig(endpoint="http://fake:9090", access_key="ak", secret_key="sk")
        return VastDBTable("docs", None, cfg, bucket="b", schema="s", namespace=namespace)

    def test_root_schema_path(self):
        assert self._table()._schema_path == "s"

    def test_nested_schema_path(self):
        assert self._table(namespace=("ns", "child"))._schema_path == "s/ns/child"

    def test_discover_schema_uses_schema_path(self, monkeypatch):
        import pyarrow as pa

        table = self._table(namespace=("ns",))
        captured: dict[str, object] = {}

        def fake_cached(key, loader):
            captured["key"] = key
            return pa.schema([("id", pa.int64())])

        monkeypatch.setattr("vast_daft.connection.cached_table_schema", fake_cached)
        result = table._discover_schema()
        assert captured["key"] == ("http://fake:9090", "ak", "b", "s/ns", "docs")
        assert result.names == ["id"]
