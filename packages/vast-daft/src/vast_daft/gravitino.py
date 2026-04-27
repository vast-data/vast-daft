"""Gravitino-aware Daft Catalog and Table that route VastDB-format tables to VastDBCatalog.

Usage::

    from daft.gravitino import GravitinoClient
    from vast_daft.gravitino import VastGravitinoCatalog

    client = GravitinoClient(
        endpoint="http://gravitino:8090",
        metalake_name="vast-data-lake",
        auth_type="simple",
        username="admin",
    )
    catalog = VastGravitinoCatalog.create(client)

    # Read a VastDB-native table (format=vastdb) or an Iceberg table — same API
    df = catalog.load_table("vastdb_catalog.my_schema.my_table").read()
"""

from __future__ import annotations

import logging
from typing import Any, cast

import daft
from daft.catalog import Identifier, NotFoundError
from daft.catalog.__gravitino import (
    GravitinoCatalog as _DaftGravitinoCatalog,
)
from daft.catalog.__gravitino import (
    GravitinoTable as _DaftGravitinoTable,
)
from daft.gravitino import GravitinoClient as InnerCatalog
from daft.gravitino import GravitinoTable as InnerTable
from daft.io import IOConfig, S3Config

from vast_daft.config import VastDBConfig
from vast_daft.table import VastDBCatalog, VastDBTable

logger = logging.getLogger(__name__)

# Canonical format string expected in Gravitino table properties
VASTDB_FORMAT = "VASTDB"
ICEBERG_FORMAT = "ICEBERG"
PARQUET_FORMAT = "PARQUET"
CSV_FORMAT = "CSV"
JSON_FORMAT = "JSON"
_SUPPORTED_FORMATS = ", ".join([ICEBERG_FORMAT, VASTDB_FORMAT, PARQUET_FORMAT, CSV_FORMAT, JSON_FORMAT])


class VastGravitinoTable(_DaftGravitinoTable):
    """A :class:`GravitinoTable` that knows how to read ``format=vastdb`` tables.

    For Iceberg tables the read is delegated to the upstream implementation.
    For VastDB tables the connection properties stored in Gravitino are used
    to construct a :class:`VastDBCatalog` and read through the native SDK.
    """

    _inner: InnerTable

    def __init__(self) -> None:
        raise RuntimeError("VastGravitinoTable.__init__ is not supported, use VastGravitinoTable._from_obj()")

    @staticmethod
    def _from_obj(obj: object) -> VastGravitinoTable:
        """Wrap an :class:`InnerTable` in a VastDB-aware table adapter."""
        if isinstance(obj, InnerTable):
            t = VastGravitinoTable.__new__(VastGravitinoTable)
            t._inner = obj
            return t
        raise ValueError(f"Unsupported gravitino table type: {type(obj)}")

    # ------------------------------------------------------------------
    # read
    # ------------------------------------------------------------------

    def read(self, **options: Any) -> Any:  # returns DataFrame
        fmt = self._format_key()

        if fmt == VASTDB_FORMAT:
            return self._read_vastdb(**options)

        if fmt == ICEBERG_FORMAT:
            return super().read(**options)

        if fmt == PARQUET_FORMAT:
            return self._read_parquet(**options)

        if fmt == CSV_FORMAT:
            return self._read_csv(**options)

        if fmt == JSON_FORMAT:
            return self._read_json(**options)

        raise NotImplementedError(
            f"Reading '{self._inner.table_info.format}' format tables is not supported. "
            f"Supported formats: {_SUPPORTED_FORMATS}."
        )

    def _format_key(self) -> str:
        """Return the canonical format key for dispatch."""
        props = self._inner.table_info.properties
        vast_fmt = props.get("vast.table-format", "").upper()
        if vast_fmt == VASTDB_FORMAT:
            return VASTDB_FORMAT

        file_fmt = props.get("file.format", "").upper()
        if file_fmt in {PARQUET_FORMAT, CSV_FORMAT, JSON_FORMAT}:
            return file_fmt

        fmt = self._inner.table_info.format.upper()
        if fmt.startswith(ICEBERG_FORMAT):
            return ICEBERG_FORMAT
        if fmt in {VASTDB_FORMAT, PARQUET_FORMAT, CSV_FORMAT, JSON_FORMAT}:
            return fmt
        return fmt

    def _require_property(self, name: str) -> str:
        value = self._inner.table_info.properties.get(name)
        if not value:
            raise ValueError(f"Missing required Gravitino table property: {name}")
        return value

    def _location(self) -> str:
        return self._inner.table_info.properties.get("table.location") or self._inner.table_info.storage_location

    def _build_io_config(self) -> IOConfig:
        endpoint = self._require_property("s3.endpoint")
        access_key = self._require_property("s3.access_key")
        secret_key = self._require_property("s3.secret_key")
        region = self._inner.table_info.properties.get("s3.region", "us-east-1")
        use_ssl = self._inner.table_info.properties.get("s3.use_ssl", "false").lower() in ("true", "1", "yes")
        ssl_verify = self._inner.table_info.properties.get("s3.ssl_verify", "false").lower() in (
            "true",
            "1",
            "yes",
        )

        return IOConfig(
            s3=S3Config(
                endpoint_url=endpoint,
                key_id=access_key,
                access_key=secret_key,
                region_name=region,
                use_ssl=use_ssl,
                verify_ssl=ssl_verify,
            )
        )

    def _validate_file_options(self, options: dict[str, Any], *, allowed: set[str]) -> dict[str, Any]:
        unknown = set(options) - allowed
        if unknown:
            raise ValueError(
                f"Unsupported read options for {self._format_key()} tables: {', '.join(sorted(unknown))}"
            )
        return options

    def _read_parquet(self, **options: Any) -> Any:
        options = self._validate_file_options(options, allowed={"columns"})
        location = self._location()
        if not location:
            raise ValueError("Missing required Gravitino table property: table.location")
        df = daft.read_parquet(location, io_config=self._build_io_config())
        columns = options.get("columns")
        if columns is not None:
            df = df.select(*columns)
        return df

    def _read_csv(self, **options: Any) -> Any:
        options = self._validate_file_options(options, allowed={"columns"})
        location = self._location()
        if not location:
            raise ValueError("Missing required Gravitino table property: table.location")
        df = daft.read_csv(location, io_config=self._build_io_config())
        columns = options.get("columns")
        if columns is not None:
            df = df.select(*columns)
        return df

    def _read_json(self, **options: Any) -> Any:
        options = self._validate_file_options(options, allowed={"columns"})
        location = self._location()
        if not location:
            raise ValueError("Missing required Gravitino table property: table.location")
        df = daft.read_json(location, io_config=self._build_io_config())
        columns = options.get("columns")
        if columns is not None:
            df = df.select(*columns)
        return df

    def _read_vastdb(self, **options: Any) -> Any:  # returns DataFrame
        """Read from VastDB using connection info stored in Gravitino properties."""
        props = self._inner.table_info.properties
        info = self._inner.table_info

        config = VastDBConfig(
            endpoint=props["vastdb.endpoint"],
            access_key=props["vastdb.access_key"],
            secret_key=props["vastdb.secret_key"],
            bucket=props.get("vastdb.bucket", info.catalog),
            schema=props.get("vastdb.schema", info.schema),
            ssl_verify=props.get("vastdb.ssl_verify", "true").lower() not in ("false", "0", "no"),
        )

        logger.debug(
            "VastGravitinoTable: reading %s via VastDB SDK (endpoint=%s, bucket=%s, schema=%s)",
            info.name,
            config.endpoint,
            config.bucket,
            config.schema,
        )

        catalog = VastDBCatalog(config)
        table = catalog.get_table(info.name)
        return table.read(**options)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _vastdb_table(self) -> VastDBTable:
        """Build a VastDBTable from the Gravitino properties."""
        props = self._inner.table_info.properties
        info = self._inner.table_info
        config = VastDBConfig(
            endpoint=props["vastdb.endpoint"],
            access_key=props["vastdb.access_key"],
            secret_key=props["vastdb.secret_key"],
            bucket=props.get("vastdb.bucket", info.catalog),
            schema=props.get("vastdb.schema", info.schema),
            ssl_verify=props.get("vastdb.ssl_verify", "true").lower() not in ("false", "0", "no"),
        )
        catalog = VastDBCatalog(config)
        return cast(VastDBTable, catalog.get_table(info.name))

    # ------------------------------------------------------------------
    # write — delegated or not-yet-implemented
    # ------------------------------------------------------------------

    def _is_vastdb(self) -> bool:
        """Check if this table is a VastDB-format table."""
        return self._format_key() == VASTDB_FORMAT

    def append(self, df: Any, **options: Any) -> None:
        if self._is_vastdb():
            self._vastdb_table().append(df, **options)
        elif self._format_key() in {PARQUET_FORMAT, CSV_FORMAT, JSON_FORMAT}:
            raise NotImplementedError(f"{self._format_key()} tables are read-only in vast-daft V1.")
        else:
            super().append(df, **options)

    def overwrite(self, df: Any, **options: Any) -> None:
        if self._is_vastdb():
            self._vastdb_table().overwrite(df, **options)
        elif self._format_key() in {PARQUET_FORMAT, CSV_FORMAT, JSON_FORMAT}:
            raise NotImplementedError(f"{self._format_key()} tables are read-only in vast-daft V1.")
        else:
            super().overwrite(df, **options)


class VastGravitinoCatalog(_DaftGravitinoCatalog):
    """A :class:`GravitinoCatalog` that returns :class:`VastGravitinoTable` instances.

    Use :meth:`create` instead of the constructor::

        catalog = VastGravitinoCatalog.create(client)
    """

    def __init__(self) -> None:
        raise RuntimeError("VastGravitinoCatalog.__init__ is not supported, use VastGravitinoCatalog.create()")

    @staticmethod
    def create(client: InnerCatalog) -> VastGravitinoCatalog:
        """Create a :class:`VastGravitinoCatalog` from a :class:`GravitinoClient`."""
        catalog = VastGravitinoCatalog.__new__(VastGravitinoCatalog)
        catalog._inner = client
        return catalog

    # ------------------------------------------------------------------
    # Override _get_table to return VastGravitinoTable
    # ------------------------------------------------------------------

    def _get_table(self, ident: Identifier) -> VastGravitinoTable:
        try:
            return VastGravitinoTable._from_obj(self._inner.load_table(str(ident)))
        except Exception as e:
            if "not found" in str(e).lower():
                raise NotFoundError(f"Table {ident} not found!")
            raise
