"""Sync VastDB table metadata into Apache Gravitino.

This script crawls VastDB (buckets → schemas → tables → columns) and
registers each table as a metadata-only entry in a Gravitino
``lakehouse-generic`` catalog. Since the generic catalog validates
Lance-style metadata, metadata-only registrations satisfy those required
properties while storing the real reader format in custom properties.

Run as::

    python -m vast_daft.gravitino_sync

Required environment variables::

    VASTDB_ENDPOINT    — VastDB API endpoint
    VASTDB_ACCESS_KEY  — VastDB access key
    VASTDB_SECRET_KEY  — VastDB secret key
    VASTDB_BUCKET      — VastDB bucket to crawl (single bucket per run)
    GRAVITINO_ENDPOINT — Gravitino REST API URL (e.g. http://gravitino-svc:8090)
    GRAVITINO_METALAKE — Gravitino metalake name
    GRAVITINO_CATALOG  — Gravitino catalog name for VastDB tables

Optional::

    VASTDB_SSL_VERIFY  — "true" (default) or "false"
    VASTDB_SCHEMA      — If set, only sync this single schema
"""

from __future__ import annotations

import json
import logging
import os
import sys

import pyarrow as pa
import requests
import vastdb

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("gravitino_sync")


# ---------------------------------------------------------------------------
# PyArrow → Gravitino type mapping
# ---------------------------------------------------------------------------


def _arrow_to_gravitino_type(arrow_type: pa.DataType) -> str:
    """Convert a PyArrow type to a Gravitino type string."""
    if pa.types.is_boolean(arrow_type):
        return "boolean"
    if pa.types.is_int8(arrow_type):
        return "byte"
    if pa.types.is_int16(arrow_type):
        return "short"
    if pa.types.is_int32(arrow_type):
        return "integer"
    if pa.types.is_int64(arrow_type):
        return "long"
    if pa.types.is_float32(arrow_type):
        return "float"
    if pa.types.is_float64(arrow_type):
        return "double"
    if pa.types.is_string(arrow_type) or pa.types.is_large_string(arrow_type):
        return "string"
    if pa.types.is_binary(arrow_type) or pa.types.is_large_binary(arrow_type):
        return "binary"
    if pa.types.is_date(arrow_type):
        return "date"
    if pa.types.is_timestamp(arrow_type):
        return "timestamp"
    if pa.types.is_decimal(arrow_type):
        return f"decimal({arrow_type.precision},{arrow_type.scale})"
    if pa.types.is_list(arrow_type) or pa.types.is_large_list(arrow_type):
        inner = _arrow_to_gravitino_type(arrow_type.value_type)
        return f"array<{inner}>"
    if pa.types.is_struct(arrow_type):
        return "struct"
    if pa.types.is_null(arrow_type):
        return "null"
    # Fallback
    return "string"


def _columns_to_gravitino(schema: pa.Schema) -> list[dict]:
    """Convert a PyArrow schema to a list of Gravitino column dicts."""
    columns = []
    for i, field in enumerate(schema):
        columns.append(
            {
                "name": field.name,
                "type": _arrow_to_gravitino_type(field.type),
                "comment": "",
                "nullable": field.nullable,
            }
        )
    return columns


# ---------------------------------------------------------------------------
# Gravitino REST helpers
# ---------------------------------------------------------------------------


class GravitinoAPI:
    """Thin wrapper around the Gravitino REST API."""

    def __init__(self, endpoint: str, metalake: str, catalog: str) -> None:
        self.base = endpoint.rstrip("/")
        self.metalake = metalake
        self.catalog = catalog
        self.session = requests.Session()
        self.session.headers.update(
            {
                "Content-Type": "application/json",
                "Accept": "application/vnd.gravitino.v1+json",
            }
        )

    def _url(self, *parts: str) -> str:
        path = "/".join(parts)
        return f"{self.base}/api/metalakes/{self.metalake}/{path}"

    # -- schemas --

    def list_schemas(self) -> set[str]:
        """Return the set of schema names in the catalog."""
        try:
            r = self.session.get(self._url("catalogs", self.catalog, "schemas"))
            r.raise_for_status()
            data = r.json()
            identifiers = data.get("identifiers", [])
            return {ident["name"] for ident in identifiers if "name" in ident}
        except Exception:
            return set()

    def ensure_schema(self, schema_name: str) -> None:
        """Create a schema if it doesn't exist."""
        r = self.session.post(
            self._url("catalogs", self.catalog, "schemas"),
            json={"name": schema_name, "comment": "VastDB schema (synced)"},
        )
        if r.status_code == 409:
            logger.debug("Schema %s already exists", schema_name)
        elif r.ok:
            logger.info("Created schema %s", schema_name)
        else:
            logger.warning("Failed to create schema %s: %s %s", schema_name, r.status_code, r.text)

    # -- tables --

    def list_tables(self, schema_name: str) -> set[str]:
        """Return the set of table names in a schema."""
        try:
            r = self.session.get(self._url("catalogs", self.catalog, "schemas", schema_name, "tables"))
            r.raise_for_status()
            data = r.json()
            identifiers = data.get("identifiers", [])
            return {ident["name"] for ident in identifiers if "name" in ident}
        except Exception:
            return set()

    def register_table(
        self,
        schema_name: str,
        table_name: str,
        table_format: str,
        columns: list[dict],
        properties: dict[str, str],
        comment: str = "",
    ) -> None:
        """Create or update a table registration in Gravitino."""
        payload = {
            "name": table_name,
            "comment": comment,
            "columns": columns,
            "properties": properties,
        }

        r = self.session.post(
            self._url("catalogs", self.catalog, "schemas", schema_name, "tables"),
            json=payload,
        )

        if r.status_code == 409:
            # Table already exists — could update columns here in the future
            logger.debug("Table %s.%s already registered", schema_name, table_name)
        elif r.ok:
            logger.info("Registered table %s.%s (%d columns)", schema_name, table_name, len(columns))
        else:
            logger.warning(
                "Failed to register %s.%s: %s %s",
                schema_name,
                table_name,
                r.status_code,
                r.text,
            )

    def drop_table(self, schema_name: str, table_name: str) -> None:
        """Remove a table registration from Gravitino."""
        r = self.session.delete(
            self._url("catalogs", self.catalog, "schemas", schema_name, "tables", table_name),
        )
        if r.ok:
            logger.info("Removed stale table %s.%s", schema_name, table_name)
        else:
            logger.debug("Could not remove %s.%s: %s", schema_name, table_name, r.status_code)


# ---------------------------------------------------------------------------
# Sync logic
# ---------------------------------------------------------------------------


def sync(
    vastdb_endpoint: str,
    vastdb_access_key: str,
    vastdb_secret_key: str,
    vastdb_bucket: str,
    vastdb_ssl_verify: bool,
    vastdb_schema_filter: str | None,
    gravitino_endpoint: str,
    gravitino_metalake: str,
    gravitino_catalog: str,
    external_tables_json: str | None = None,
) -> None:
    """Crawl VastDB and sync table metadata into Gravitino."""

    api = GravitinoAPI(gravitino_endpoint, gravitino_metalake, gravitino_catalog)

    # Table properties baked into every registered VastDB table.
    base_props: dict[str, str] = {
        "format": "lance",
        "lance.register": "true",
        "vast.table-format": "vastdb",
        "external": "true",
        "vastdb.endpoint": vastdb_endpoint,
        "vastdb.access_key": vastdb_access_key,
        "vastdb.secret_key": vastdb_secret_key,
        "vastdb.bucket": vastdb_bucket,
        "vastdb.ssl_verify": str(vastdb_ssl_verify).lower(),
    }

    logger.info(
        "Syncing VastDB → Gravitino  (endpoint=%s, bucket=%s, catalog=%s)",
        vastdb_endpoint,
        vastdb_bucket,
        gravitino_catalog,
    )

    session = vastdb.connect(
        endpoint=vastdb_endpoint,
        access=vastdb_access_key,
        secret=vastdb_secret_key,
        ssl_verify=vastdb_ssl_verify,
    )

    with session.transaction() as tx:
        bucket = tx.bucket(vastdb_bucket)

        for schema_obj in bucket.schemas():
            schema_name = schema_obj.name.split("/")[-1]

            if vastdb_schema_filter and schema_name != vastdb_schema_filter:
                continue

            api.ensure_schema(schema_name)

            # Collect VastDB tables in this schema
            vastdb_tables: set[str] = set()
            for table_name in schema_obj.tablenames():
                vastdb_tables.add(table_name)

                try:
                    table_obj = schema_obj.table(table_name, fail_if_missing=True)
                    pa_schema = table_obj.columns()
                    columns = _columns_to_gravitino(pa_schema)
                except Exception as e:
                    logger.warning("Could not read schema for %s.%s: %s", schema_name, table_name, e)
                    columns = []

                props = {
                    **base_props,
                    "location": f"file:///tmp/vastdb/{vastdb_bucket}/{schema_name}/{table_name}",
                    "vastdb.schema": schema_name,
                }

                api.register_table(
                    schema_name,
                    table_name,
                    table_format="VASTDB",
                    columns=columns,
                    properties=props,
                    comment="VastDB table (synced)",
                )

            # Remove tables from Gravitino that no longer exist in VastDB
            gravitino_tables = api.list_tables(schema_name)
            stale = gravitino_tables - vastdb_tables
            for stale_name in stale:
                api.drop_table(schema_name, stale_name)

    _sync_external_tables(
        api,
        external_tables_json=external_tables_json,
        s3_endpoint=vastdb_endpoint,
        s3_access_key=vastdb_access_key,
        s3_secret_key=vastdb_secret_key,
        s3_ssl_verify=vastdb_ssl_verify,
    )

    logger.info("Sync complete")


def _parse_external_tables_json(raw: str | None) -> list[dict]:
    if not raw:
        return []
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as e:
        raise ValueError(f"EXTERNAL_TABLES_JSON is not valid JSON: {e}") from e
    if not isinstance(payload, list):
        raise ValueError("EXTERNAL_TABLES_JSON must decode to a list of table definitions")
    return payload


def _sync_external_tables(
    api: GravitinoAPI,
    *,
    external_tables_json: str | None,
    s3_endpoint: str,
    s3_access_key: str,
    s3_secret_key: str,
    s3_ssl_verify: bool,
) -> None:
    for table_def in _parse_external_tables_json(external_tables_json):
        schema_name = table_def["schema"]
        table_name = table_def["name"]
        table_format = table_def["format"].upper()
        location = table_def["location"]
        comment = table_def.get("comment", f"External {table_format} table (synced)")
        columns = [
            {
                "name": col["name"],
                "type": col["type"],
                "nullable": col.get("nullable", True),
                "comment": col.get("comment", ""),
            }
            for col in table_def.get("columns", [])
        ]

        api.ensure_schema(schema_name)
        api.register_table(
            schema_name,
            table_name,
            table_format=table_format,
            columns=columns,
            properties={
                "format": "lance",
                "lance.register": "true",
                "external": "true",
                "file.format": table_format,
                "location": location,
                "table.location": location,
                "s3.endpoint": s3_endpoint,
                "s3.access_key": s3_access_key,
                "s3.secret_key": s3_secret_key,
                "s3.region": table_def.get("s3_region", "us-east-1"),
                "s3.use_ssl": str(table_def.get("s3_use_ssl", False)).lower(),
                "s3.ssl_verify": str(table_def.get("s3_ssl_verify", s3_ssl_verify)).lower(),
            },
            comment=comment,
        )


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------


def _require_env(name: str) -> str:
    val = os.environ.get(name)
    if not val:
        logger.error("Missing required environment variable: %s", name)
        sys.exit(1)
    return val


def main() -> None:
    sync(
        vastdb_endpoint=_require_env("VASTDB_ENDPOINT"),
        vastdb_access_key=_require_env("VASTDB_ACCESS_KEY"),
        vastdb_secret_key=_require_env("VASTDB_SECRET_KEY"),
        vastdb_bucket=_require_env("VASTDB_BUCKET"),
        vastdb_ssl_verify=os.environ.get("VASTDB_SSL_VERIFY", "true").lower() not in ("false", "0", "no"),
        vastdb_schema_filter=os.environ.get("VASTDB_SCHEMA"),
        gravitino_endpoint=_require_env("GRAVITINO_ENDPOINT"),
        gravitino_metalake=_require_env("GRAVITINO_METALAKE"),
        gravitino_catalog=_require_env("GRAVITINO_CATALOG"),
        external_tables_json=os.environ.get("EXTERNAL_TABLES_JSON"),
    )


if __name__ == "__main__":
    main()
