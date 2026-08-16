"""Daft Catalog and Table implementations backed by VastDB."""

from __future__ import annotations

import fnmatch
import logging
from contextlib import contextmanager
from typing import Any, cast

import pyarrow as pa
import vastdb
import vastdb.errors
import vastdb.schema
from daft.catalog import Catalog, Function, Identifier, NotFoundError, Table
from daft.daft import ScanOperatorHandle
from daft.dataframe import DataFrame
from daft.logical.builder import LogicalPlanBuilder
from daft.schema import Schema

from vast_daft.config import VastDBConfig
from vast_daft.connection import VastDBConnection, clear_metadata_cache
from vast_daft.scan import VastDBScanOperator
from vast_daft.sink import VastDBDataSink

logger = logging.getLogger(__name__)


def _vastdb_safe_schema(pa_schema: pa.Schema) -> pa.Schema:
    """Normalize a PyArrow schema for VastDB compatibility.

    Daft's ``Schema.to_pyarrow_schema()`` maps Utf8/String to ``large_string``,
    which VastDB does not support.  This helper down-casts ``large_string`` to
    ``string`` so that table creation succeeds.
    """
    fields = []
    for field in pa_schema:
        if pa.types.is_large_string(field.type):
            fields.append(pa.field(field.name, pa.string(), nullable=field.nullable))
        else:
            fields.append(field)
    return pa.schema(fields)


class VastDBTable(Table):
    """A Daft :class:`Table` backed by a single VastDB table.

    Parameters
    ----------
    name : str
        Table name (leaf name, not the full path).
    connection : VastDBConnection
        Shared VastDB connection.
    config : VastDBConfig
        VastDB configuration (used to construct DataSource/DataSink).
    bucket : str
        VastDB bucket containing this table.
    schema : str
        VastDB schema (within *bucket*) containing this table.
    namespace : tuple[str, ...]
        Sub-schema path components under the root schema.
        Empty tuple means the table lives directly under the root schema.
    """

    def __init__(
        self,
        name: str,
        connection: VastDBConnection,
        config: VastDBConfig,
        *,
        bucket: str,
        schema: str,
        namespace: tuple[str, ...] = (),
    ) -> None:
        self._name = name
        self._connection = connection
        self._config = config
        self._bucket = bucket
        self._schema = schema
        self._namespace = namespace

    # -- abstract interface --------------------------------------------------

    @property
    def name(self) -> str:
        return self._name

    def schema(self) -> Schema:
        pa_schema = self._discover_schema()
        return Schema.from_pyarrow_schema(pa_schema)

    def read(self, **options: Any) -> DataFrame:
        self._validate_options("read", options, {"columns", "query_config", "num_splits"})
        columns = options.pop("columns", None)
        pa_schema = self._discover_schema(columns=columns)
        scan_op = VastDBScanOperator(
            self._config,
            self._name,
            pa_schema,
            bucket=self._bucket,
            db_schema=self._schema,
            columns=columns,
            **options,
        )
        handle = ScanOperatorHandle.from_python_scan_operator(scan_op)
        builder = LogicalPlanBuilder.from_tabular_scan(scan_operator=handle)
        return DataFrame(builder)

    def append(self, df: DataFrame, **options: Any) -> None:
        pa_schema = self.schema().to_pyarrow_schema()
        sink = VastDBDataSink(
            self._config,
            self._name,
            pa_schema,
            bucket=self._bucket,
            schema=self._schema,
        )
        df.write_sink(sink)

    def overwrite(self, df: DataFrame, **options: Any) -> None:
        pa_schema = self.schema().to_pyarrow_schema()
        self._drop_if_exists()
        sink = VastDBDataSink(
            self._config,
            self._name,
            pa_schema,
            bucket=self._bucket,
            schema=self._schema,
        )
        df.write_sink(sink)

    # -- helpers --------------------------------------------------------------

    @contextmanager
    def _vastdb_table(self):
        """Yield the underlying VastDB Table inside a transaction."""
        with self._connection.session.transaction() as tx:
            db_bucket = tx.bucket(self._bucket)
            db_schema = db_bucket.schema(self._schema, fail_if_missing=True)
            for part in self._namespace:
                db_schema = db_schema.schema(part, fail_if_missing=True)
            table = db_schema.table(self._name, fail_if_missing=True)
            yield table

    def _discover_schema(self, *, columns: list[str] | None = None) -> pa.Schema:
        # Called for every table reference in every query (schema() and read()),
        # and the interactive path costs a bucket HEAD + schema/table listing +
        # columns(). Query planning is dominated by these, so memoise briefly.
        from vast_daft.connection import cached_table_schema

        def _load() -> pa.Schema:
            with self._vastdb_table() as table:
                return table.columns()

        key = (
            self._config.endpoint,
            self._config.access_key,
            self._bucket,
            self._schema_path,
            self._name,
        )
        full_schema = cached_table_schema(key, _load)
        if columns is None:
            return full_schema
        return pa.schema([full_schema.field(name) for name in columns])

    def _drop_if_exists(self) -> None:
        with self._connection.session.transaction() as tx:
            db_bucket = tx.bucket(self._bucket)
            db_schema = db_bucket.schema(self._schema, fail_if_missing=False)
            if db_schema is None:
                return
            for part in self._namespace:
                db_schema = db_schema.schema(part, fail_if_missing=False)
                if db_schema is None:
                    return
            table = db_schema.table(self._name, fail_if_missing=False)
            if table is not None:
                table.drop()
                clear_metadata_cache()


class VastDBCatalog(Catalog):
    """A Daft :class:`Catalog` backed by VastDB.

    The VastDB hierarchy maps to Daft's catalog model as follows:

    .. code-block:: text

        VastDB bucket  →  catalog  (top level)
        VastDB schema  →  namespace
        VastDB table   →  table

    Mirroring the Unity Catalog convention, table identifiers use dot
    notation: ``"bucket.schema.table"``.

    ``bucket`` and ``schema`` can be fixed in :class:`VastDBConfig` as
    defaults — any components already fixed in the config are consumed
    from the *left* of the identifier automatically:

    +-----------------+------------------+---------------------------------------+
    | config.bucket   | config.schema    | Identifier format required            |
    +=================+==================+=======================================+
    | set             | set              | ``"table"`` or ``"ns.table"``         |
    +-----------------+------------------+---------------------------------------+
    | set             | ``None``         | ``"schema.table"``                    |
    +-----------------+------------------+---------------------------------------+
    | ``None``        | ``None``         | ``"bucket.schema.table"``             |
    +-----------------+------------------+---------------------------------------+

    Parameters
    ----------
    config : VastDBConfig
        VastDB connection configuration.  ``bucket`` and ``schema`` are
        optional defaults.
    alias : str | None
        Optional short name used in SQL ``FROM`` clauses.

    Examples
    --------
    Fully-qualified config (backward-compatible):

    >>> config = VastDBConfig(
    ...     endpoint="http://vastdb:9090",
    ...     access_key="...",
    ...     secret_key="...",
    ...     bucket="my-bucket",
    ...     schema="my-schema",
    ... )
    >>> catalog = VastDBCatalog(config)
    >>> catalog.list_tables()
    [Identifier('my_table')]

    Bucket-level catalog — schema supplied in identifier:

    >>> config = VastDBConfig(
    ...     endpoint="http://vastdb:9090",
    ...     access_key="...",
    ...     secret_key="...",
    ...     bucket="my-bucket",
    ... )
    >>> catalog = VastDBCatalog(config, alias="vastdb")
    >>> catalog.load_table("my-schema.my_table")

    Endpoint-level catalog — bucket and schema in identifier:

    >>> config = VastDBConfig(
    ...     endpoint="http://vastdb:9090",
    ...     access_key="...",
    ...     secret_key="...",
    ... )
    >>> catalog = VastDBCatalog(config, alias="vastdb")
    >>> catalog.load_table("my-bucket.my-schema.my_table")
    """

    def __init__(self, config: VastDBConfig, alias: str | None = None) -> None:
        self._config = config
        self._connection = VastDBConnection(config)
        self._alias = alias

    # -- abstract interface --------------------------------------------------

    @property
    def name(self) -> str:
        if self._alias:
            return self._alias
        parts = [p for p in (self._config.bucket, self._config.schema) if p]
        return "/".join(parts) if parts else "vastdb"

    def _create_function(self, ident: Identifier, function: Function | Any) -> None:
        raise NotImplementedError("VastDBCatalog does not support functions")

    def _get_function(self, ident: Identifier) -> Function:
        raise NotFoundError(f"Function {ident!r} not found")

    # -- identifier resolution -----------------------------------------------

    def _strip_catalog_prefix(self, ident: Identifier) -> Identifier:
        """Strip the catalog name/alias that Daft prepends to every identifier."""
        if len(ident) > 1:
            first = str(ident[0])
            if first == self._alias or first == self.name:
                return Identifier(*list(ident)[1:])
        return ident

    def _resolve_bucket_schema(self, ident: Identifier) -> tuple[str, str, Identifier]:
        """Resolve (bucket, schema, remaining_ident) from *ident*.

        Components are consumed from the *left* of *ident* to fill in
        whichever of bucket/schema are not already fixed in the config.

        Raises
        ------
        ValueError
            If *ident* does not have enough components.
        """
        ident = self._strip_catalog_prefix(ident)
        parts = list(ident)

        bucket = self._config.bucket
        schema = self._config.schema

        if bucket is None and schema is None:
            if len(parts) < 3:
                raise ValueError(
                    f"Identifier {ident!r} must have at least 3 components "
                    f"(bucket.schema.table) when config has no bucket or schema."
                )
            bucket = parts[0]
            schema = parts[1]
            remaining = Identifier(*parts[2:])
        elif bucket is not None and schema is None:
            if len(parts) < 2:
                raise ValueError(
                    f"Identifier {ident!r} must have at least 2 components (schema.table) when config has no schema."
                )
            schema = parts[0]
            remaining = Identifier(*parts[1:])
        else:
            # Both fixed — nothing consumed from ident
            remaining = Identifier(*parts)

        # After all branches, bucket and schema are guaranteed to be str.
        assert bucket is not None and schema is not None
        return bucket, schema, remaining

    # -- tables --------------------------------------------------------------

    def _get_table(self, ident: Identifier) -> VastDBTable:
        bucket, schema, rel = self._resolve_bucket_schema(ident)
        table_name = str(rel[-1])
        namespace = tuple(str(p) for p in list(rel)[:-1])
        if not self._table_exists(bucket, schema, namespace, table_name):
            raise NotFoundError(f"Table {ident!r} not found")
        return VastDBTable(
            table_name,
            self._connection,
            self._config,
            bucket=bucket,
            schema=schema,
            namespace=namespace,
        )

    def _has_table(self, ident: Identifier) -> bool:
        try:
            bucket, schema, rel = self._resolve_bucket_schema(ident)
        except ValueError:
            return False
        table_name = str(rel[-1])
        namespace = tuple(str(p) for p in list(rel)[:-1])
        return self._table_exists(bucket, schema, namespace, table_name)

    def _table_exists(
        self,
        bucket: str,
        schema: str,
        namespace: tuple[str, ...],
        table_name: str,
    ) -> bool:
        from vast_daft.connection import cached_exists

        def _probe() -> bool:
            with self._connection.session.transaction() as tx:
                db_bucket = tx.bucket(bucket)
                db_schema = db_bucket.schema(schema, fail_if_missing=False)
                if db_schema is None:
                    return False
                for part in namespace:
                    db_schema = db_schema.schema(part, fail_if_missing=False)
                    if db_schema is None:
                        return False
                return db_schema.table(table_name, fail_if_missing=False) is not None

        key = (self._config.endpoint, self._config.access_key, bucket, schema, namespace, table_name)
        return cached_exists(key, _probe)

    def _list_tables(self, pattern: str | None = None) -> list[Identifier]:
        if self._config.bucket is None:
            raise NotImplementedError(
                "list_tables() requires config.bucket to be set. "
                "Use list_tables('bucket.schema') to specify the namespace."
            )
        bucket = self._config.bucket
        results: list[Identifier] = []
        if self._config.schema is not None:
            self._collect_tables(bucket, self._config.schema, (), results)
        else:
            with self._connection.session.transaction() as tx:
                db_bucket = tx.bucket(bucket)
                for s in db_bucket.schemas():
                    schema_name = s.name.split("/")[-1]
                    self._collect_tables(bucket, schema_name, (), results)
        if pattern is not None:
            results = [i for i in results if fnmatch.fnmatch(str(i), pattern)]
        return cast(list[Identifier], sorted(results, key=str))

    def _create_table(
        self,
        ident: Identifier,
        schema: Schema,
        properties: Any | None = None,
        partition_fields: list[Any] | None = None,
    ) -> VastDBTable:
        bucket, schema_name, rel = self._resolve_bucket_schema(ident)
        table_name = str(rel[-1])
        namespace = tuple(str(p) for p in list(rel)[:-1])
        pa_schema = _vastdb_safe_schema(schema.to_pyarrow_schema())
        with self._connection.session.transaction() as tx:
            db_bucket = tx.bucket(bucket)
            db_schema = db_bucket.schema(schema_name, fail_if_missing=False)
            if db_schema is None:
                db_schema = db_bucket.create_schema(schema_name, fail_if_exists=False)
            for part in namespace:
                child = db_schema.schema(part, fail_if_missing=False)
                if child is None:
                    child = db_schema.create_schema(part, fail_if_exists=False)
                db_schema = child
            db_schema.create_table(table_name, columns=pa_schema)
        return VastDBTable(
            table_name,
            self._connection,
            self._config,
            bucket=bucket,
            schema=schema_name,
            namespace=namespace,
        )

    def _drop_table(self, ident: Identifier) -> None:
        bucket, schema_name, rel = self._resolve_bucket_schema(ident)
        table_name = str(rel[-1])
        namespace = tuple(str(p) for p in list(rel)[:-1])
        with self._connection.session.transaction() as tx:
            db_bucket = tx.bucket(bucket)
            db_schema = db_bucket.schema(schema_name, fail_if_missing=False)
            if db_schema is None:
                return
            for part in namespace:
                db_schema = db_schema.schema(part, fail_if_missing=False)
                if db_schema is None:
                    return
            table = db_schema.table(table_name, fail_if_missing=False)
            if table is not None:
                table.drop()
                clear_metadata_cache()

    def drop_table_if_exists(self, identifier: Identifier | str) -> None:
        """Drop a table if it exists, silently succeeding otherwise."""
        if isinstance(identifier, str):
            identifier = Identifier(identifier)
        if self.has_table(identifier):
            self.drop_table(identifier)

    # -- namespaces ----------------------------------------------------------

    def _has_namespace(self, ident: Identifier) -> bool:
        try:
            ident = self._strip_catalog_prefix(ident)
            # Append a sentinel to let _resolve_bucket_schema consume the
            # bucket/schema prefix, then treat what remains as sub-schema path.
            bucket, schema_name, rel = self._resolve_bucket_schema(Identifier(*list(ident), "__sentinel__"))
            ns_parts = list(rel)[:-1]  # drop sentinel
            with self._connection.session.transaction() as tx:
                db_bucket = tx.bucket(bucket)
                db_schema = db_bucket.schema(schema_name, fail_if_missing=False)
                if db_schema is None:
                    return False
                for part in ns_parts:
                    db_schema = db_schema.schema(part, fail_if_missing=False)
                    if db_schema is None:
                        return False
            return True
        except (ValueError, vastdb.errors.MissingBucket):
            return False

    def _list_namespaces(self, pattern: str | None = None) -> list[Identifier]:
        if self._config.bucket is None:
            raise NotImplementedError("list_namespaces() requires config.bucket to be set.")
        bucket = self._config.bucket
        results: list[Identifier] = []
        if self._config.schema is not None:
            # Sub-schemas within the configured schema
            self._collect_namespaces(bucket, self._config.schema, (), results)
        else:
            # Schemas are the top-level namespaces
            with self._connection.session.transaction() as tx:
                db_bucket = tx.bucket(bucket)
                for s in db_bucket.schemas():
                    schema_name = s.name.split("/")[-1]
                    results.append(Identifier(schema_name))
                    self._collect_namespaces(bucket, schema_name, (), results)
        if pattern is not None:
            results = [i for i in results if fnmatch.fnmatch(str(i), pattern)]
        return cast(list[Identifier], sorted(results, key=str))

    def _create_namespace(self, ident: Identifier) -> None:
        ident = self._strip_catalog_prefix(ident)
        # Use sentinel trick to resolve bucket/schema prefix
        bucket, schema_name, rel = self._resolve_bucket_schema(Identifier(*list(ident), "__sentinel__"))
        ns_parts = list(rel)[:-1]  # drop sentinel
        with self._connection.session.transaction() as tx:
            db_bucket = tx.bucket(bucket)
            db_schema = db_bucket.schema(schema_name, fail_if_missing=False)
            if db_schema is None:
                db_schema = db_bucket.create_schema(schema_name, fail_if_exists=False)
            for part in ns_parts[:-1]:
                db_schema = db_schema.schema(part, fail_if_missing=True)
            if ns_parts:
                db_schema.create_schema(ns_parts[-1], fail_if_exists=True)

    def _drop_namespace(self, ident: Identifier) -> None:
        ident = self._strip_catalog_prefix(ident)
        bucket, schema_name, rel = self._resolve_bucket_schema(Identifier(*list(ident), "__sentinel__"))
        ns_parts = list(rel)[:-1]  # drop sentinel
        with self._connection.session.transaction() as tx:
            db_bucket = tx.bucket(bucket)
            db_schema = db_bucket.schema(schema_name, fail_if_missing=False)
            if db_schema is None:
                return
            if not ns_parts:
                # Dropping a root schema
                tables = list(db_schema.tablenames())
                if tables:
                    raise ValueError(f"Namespace {ident!r} is not empty; drop tables first: {tables}")
                db_schema.drop()
                return
            for part in ns_parts[:-1]:
                db_schema = db_schema.schema(part, fail_if_missing=True)
            child = db_schema.schema(ns_parts[-1], fail_if_missing=False)
            if child is not None:
                tables = list(child.tablenames())
                if tables:
                    raise ValueError(f"Namespace {ident!r} is not empty; drop tables first: {tables}")
                child.drop()

    # -- helpers -------------------------------------------------------------

    def _collect_tables(
        self,
        bucket_name: str,
        schema_name: str,
        sub_prefix: tuple[str, ...],
        out: list[Identifier],
    ) -> None:
        """Recursively collect all tables under the given bucket/schema."""
        with self._connection.session.transaction() as tx:
            db_bucket = tx.bucket(bucket_name)
            s = db_bucket.schema(schema_name, fail_if_missing=True)
            for part in sub_prefix:
                s = s.schema(part, fail_if_missing=True)

            # Build identifier prefix — omit parts already fixed in config
            id_prefix: tuple[str, ...] = ()
            if self._config.bucket is None:
                id_prefix = (bucket_name, schema_name, *sub_prefix)
            elif self._config.schema is None:
                id_prefix = (schema_name, *sub_prefix)
            else:
                id_prefix = sub_prefix

            for name in s.tablenames():
                out.append(Identifier(*id_prefix, name))

            for child in s.schemas():
                child_name = child.name.split("/")[-1]
                self._collect_tables(bucket_name, schema_name, (*sub_prefix, child_name), out)

    def _collect_namespaces(
        self,
        bucket_name: str,
        schema_name: str,
        sub_prefix: tuple[str, ...],
        out: list[Identifier],
    ) -> None:
        """Recursively collect sub-schemas under the given bucket/schema."""
        with self._connection.session.transaction() as tx:
            db_bucket = tx.bucket(bucket_name)
            s = db_bucket.schema(schema_name, fail_if_missing=True)
            for part in sub_prefix:
                s = s.schema(part, fail_if_missing=True)

            id_prefix: tuple[str, ...] = ()
            if self._config.schema is not None:
                # Root schema is fixed; sub-schemas appear relative to it
                id_prefix = sub_prefix
            else:
                id_prefix = (schema_name, *sub_prefix)

            for child in s.schemas():
                child_name = child.name.split("/")[-1]
                full_id = (*id_prefix, child_name)
                out.append(Identifier(*full_id))
                self._collect_namespaces(bucket_name, schema_name, (*sub_prefix, child_name), out)
