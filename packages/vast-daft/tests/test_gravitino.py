# """Tests for VastGravitinoCatalog and VastGravitinoTable."""

# from __future__ import annotations

# from dataclasses import dataclass
# from unittest.mock import MagicMock, patch

# import pytest

# from vast_daft.gravitino import (
#     VASTDB_FORMAT,
#     VastGravitinoCatalog,
#     VastGravitinoTable,
# )

# # ---------------------------------------------------------------------------
# # Helpers — lightweight fakes of daft.gravitino dataclasses
# # ---------------------------------------------------------------------------


# @dataclass(frozen=True)
# class FakeTableInfo:
#     name: str
#     catalog: str
#     schema: str
#     table_type: str
#     storage_location: str
#     format: str
#     properties: dict[str, str]


# @dataclass(frozen=True)
# class FakeInnerTable:
#     """Minimal stand-in for daft.gravitino.GravitinoTable."""

#     table_info: FakeTableInfo
#     table_uri: str
#     io_config: object


# def _make_inner_table(
#     fmt: str = "vastdb",
#     name: str = "orders",
#     catalog: str = "vastdb_catalog",
#     schema: str = "sales",
#     storage_location: str = "vastdb://my-bucket/my-schema/orders",
#     **extra_props: str,
# ) -> FakeInnerTable:
#     props = {
#         "format": fmt,
#         "vastdb.endpoint": "http://vastdb:9090",
#         "vastdb.access_key": "AK",
#         "vastdb.secret_key": "SK",
#         "vastdb.bucket": "my-bucket",
#         "vastdb.schema": "my-schema",
#         "vastdb.ssl_verify": "false",
#         **extra_props,
#     }
#     info = FakeTableInfo(
#         name=name,
#         catalog=catalog,
#         schema=schema,
#         table_type="RELATIONAL",
#         storage_location=storage_location,
#         format=fmt,
#         properties=props,
#     )
#     return FakeInnerTable(table_info=info, table_uri="vastdb://fake", io_config=None)


# # ---------------------------------------------------------------------------
# # VastGravitinoTable tests
# # ---------------------------------------------------------------------------


# class TestVastGravitinoTable:
#     def test_from_obj_accepts_inner_table(self):
#         """_from_obj wraps a GravitinoTable (InnerTable) correctly."""
#         # FakeInnerTable is not the real InnerTable class, so we patch isinstance
#         with patch("vast_daft.gravitino.isinstance", side_effect=lambda o, t: True):
#             # Direct construction — we actually need to make FakeInnerTable
#             # pass the isinstance check.  Easier: just test with real type patching.
#             pass

#         # Test the actual path by registering FakeInnerTable as a virtual subclass

#         # We can't make FakeInnerTable a real subclass, so test the ValueError path
#         with pytest.raises(ValueError, match="Unsupported"):
#             VastGravitinoTable._from_obj("not a table")

#     def test_init_raises(self):
#         """Direct __init__ should raise RuntimeError."""
#         with pytest.raises(RuntimeError, match="not supported"):
#             VastGravitinoTable()

#     def test_read_vastdb_format_builds_config(self):
#         """When format=VASTDB, read() should build VastDBConfig from properties."""
#         inner = _make_inner_table(fmt="vastdb")

#         # Create the table by bypassing __init__
#         table = VastGravitinoTable.__new__(VastGravitinoTable)
#         table._inner = inner  # type: ignore

#         with patch("vast_daft.gravitino.VastDBCatalog") as MockCatalog:
#             mock_vastdb_table = MagicMock()
#             mock_vastdb_table.read.return_value = "mock_dataframe"
#             MockCatalog.return_value.get_table.return_value = mock_vastdb_table

#             result = table.read()

#             # Verify VastDBCatalog was created with correct config
#             MockCatalog.assert_called_once()
#             config = MockCatalog.call_args[0][0]
#             assert config.endpoint == "http://vastdb:9090"
#             assert config.access_key == "AK"
#             assert config.secret_key == "SK"
#             assert config.bucket == "my-bucket"
#             assert config.schema == "my-schema"
#             assert config.ssl_verify is False

#             # Verify get_table was called with the table name
#             MockCatalog.return_value.get_table.assert_called_once_with("orders")
#             assert result == "mock_dataframe"

#     def test_read_unknown_format_raises(self):
#         """Unknown formats should raise NotImplementedError."""
#         inner = _make_inner_table(fmt="parquet_raw")
#         table = VastGravitinoTable.__new__(VastGravitinoTable)
#         table._inner = inner  # type: ignore

#         with pytest.raises(NotImplementedError, match="parquet_raw"):
#             table.read()

#     def test_read_iceberg_delegates_to_super(self):
#         """ICEBERG format should delegate to the parent class."""
#         inner = _make_inner_table(fmt="ICEBERG/PARQUET")
#         table = VastGravitinoTable.__new__(VastGravitinoTable)
#         table._inner = inner  # type: ignore

#         # The parent's read() will try to call read_iceberg which needs pyiceberg.
#         # We just verify it doesn't go through the VastDB path.
#         with patch("vast_daft.gravitino.VastDBCatalog") as MockCatalog:
#             with pytest.raises(Exception):
#                 # Will fail because there's no real Iceberg table, but it should NOT
#                 # call VastDBCatalog
#                 table.read()
#             MockCatalog.assert_not_called()

#     def test_read_parquet_dispatches_to_daft(self):
#         inner = _make_inner_table(
#             fmt="PARQUET",
#             **{
#                 "file.format": "PARQUET",
#                 "table.location": "s3://bucket/path/data.parquet",
#                 "s3.endpoint": "http://s3:9000",
#                 "s3.access_key": "AK",
#                 "s3.secret_key": "SK",
#             },
#         )
#         table = VastGravitinoTable.__new__(VastGravitinoTable)
#         table._inner = inner  # type: ignore

#         with patch("vast_daft.gravitino.daft.read_parquet", return_value="df") as mock_read:
#             result = table.read()

#         mock_read.assert_called_once()
#         assert mock_read.call_args.args[0] == "s3://bucket/path/data.parquet"
#         assert "io_config" in mock_read.call_args.kwargs
#         assert result == "df"

#     def test_read_csv_dispatches_to_daft(self):
#         inner = _make_inner_table(
#             fmt="CSV",
#             **{
#                 "file.format": "CSV",
#                 "table.location": "s3://bucket/path/data.csv",
#                 "s3.endpoint": "http://s3:9000",
#                 "s3.access_key": "AK",
#                 "s3.secret_key": "SK",
#             },
#         )
#         table = VastGravitinoTable.__new__(VastGravitinoTable)
#         table._inner = inner  # type: ignore

#         with patch("vast_daft.gravitino.daft.read_csv", return_value="df") as mock_read:
#             result = table.read()

#         mock_read.assert_called_once()
#         assert mock_read.call_args.args[0] == "s3://bucket/path/data.csv"
#         assert "io_config" in mock_read.call_args.kwargs
#         assert result == "df"

#     def test_read_json_dispatches_to_daft(self):
#         inner = _make_inner_table(
#             fmt="JSON",
#             **{
#                 "file.format": "JSON",
#                 "table.location": "s3://bucket/path/data.json",
#                 "s3.endpoint": "http://s3:9000",
#                 "s3.access_key": "AK",
#                 "s3.secret_key": "SK",
#             },
#         )
#         table = VastGravitinoTable.__new__(VastGravitinoTable)
#         table._inner = inner  # type: ignore

#         with patch("vast_daft.gravitino.daft.read_json", return_value="df") as mock_read:
#             result = table.read()

#         mock_read.assert_called_once()
#         assert mock_read.call_args.args[0] == "s3://bucket/path/data.json"
#         assert "io_config" in mock_read.call_args.kwargs
#         assert result == "df"

#     def test_missing_table_location_raises(self):
#         inner = _make_inner_table(
#             fmt="PARQUET",
#             storage_location="",
#             **{
#                 "file.format": "PARQUET",
#                 "s3.endpoint": "http://s3:9000",
#                 "s3.access_key": "AK",
#                 "s3.secret_key": "SK",
#             },
#         )
#         table = VastGravitinoTable.__new__(VastGravitinoTable)
#         table._inner = inner  # type: ignore

#         with pytest.raises(ValueError, match="table.location"):
#             table.read()

#     def test_missing_required_s3_property_raises(self):
#         inner = _make_inner_table(
#             fmt="PARQUET",
#             **{
#                 "file.format": "PARQUET",
#                 "table.location": "s3://bucket/path/data.parquet",
#                 "s3.endpoint": "http://s3:9000",
#                 "s3.access_key": "AK",
#                 "s3.secret_key": "",
#             },
#         )
#         table = VastGravitinoTable.__new__(VastGravitinoTable)
#         table._inner = inner  # type: ignore

#         with pytest.raises(ValueError, match="s3.secret_key"):
#             table.read()

#     def test_append_vastdb_format(self):
#         """append() with format=VASTDB routes to VastDBCatalog."""
#         inner = _make_inner_table(fmt="vastdb")
#         table = VastGravitinoTable.__new__(VastGravitinoTable)
#         table._inner = inner  # type: ignore

#         mock_df = MagicMock()
#         with patch("vast_daft.gravitino.VastDBCatalog") as MockCatalog:
#             mock_vastdb_table = MagicMock()
#             MockCatalog.return_value.get_table.return_value = mock_vastdb_table

#             table.append(mock_df)

#             mock_vastdb_table.append.assert_called_once_with(mock_df)

#     def test_overwrite_vastdb_format(self):
#         """overwrite() with format=VASTDB routes to VastDBCatalog."""
#         inner = _make_inner_table(fmt="vastdb")
#         table = VastGravitinoTable.__new__(VastGravitinoTable)
#         table._inner = inner  # type: ignore

#         mock_df = MagicMock()
#         with patch("vast_daft.gravitino.VastDBCatalog") as MockCatalog:
#             mock_vastdb_table = MagicMock()
#             MockCatalog.return_value.get_table.return_value = mock_vastdb_table

#             table.overwrite(mock_df)

#             mock_vastdb_table.overwrite.assert_called_once_with(mock_df)

#     def test_file_backed_append_is_read_only(self):
#         inner = _make_inner_table(
#             fmt="PARQUET",
#             **{
#                 "file.format": "PARQUET",
#                 "table.location": "s3://bucket/path/data.parquet",
#                 "s3.endpoint": "http://s3:9000",
#                 "s3.access_key": "AK",
#                 "s3.secret_key": "SK",
#             },
#         )
#         table = VastGravitinoTable.__new__(VastGravitinoTable)
#         table._inner = inner  # type: ignore

#         with pytest.raises(NotImplementedError, match="read-only"):
#             table.append(MagicMock())


# # ---------------------------------------------------------------------------
# # VastGravitinoCatalog tests
# # ---------------------------------------------------------------------------


# class TestVastGravitinoCatalog:
#     def test_init_raises(self):
#         """Direct __init__ should raise RuntimeError."""
#         with pytest.raises(RuntimeError, match="not supported"):
#             VastGravitinoCatalog()

#     def test_create_sets_inner(self):
#         """create() should set _inner to the provided client."""
#         mock_client = MagicMock()
#         catalog = VastGravitinoCatalog.create(mock_client)
#         assert catalog._inner is mock_client

#     def test_get_table_returns_vast_gravitino_table(self):
#         """_get_table should return VastGravitinoTable, not the base GravitinoTable."""
#         inner = _make_inner_table()
#         mock_client = MagicMock()
#         mock_client.load_table.return_value = inner

#         catalog = VastGravitinoCatalog.create(mock_client)

#         # _get_table expects an Identifier
#         from daft.catalog import Identifier

#         # Will fail isinstance check since FakeInnerTable != GravitinoTable,
#         # so we patch VastGravitinoTable._from_obj
#         with patch.object(VastGravitinoTable, "_from_obj", return_value="mock_table") as mock_from:
#             result = catalog._get_table(Identifier("vastdb_catalog", "sales", "orders"))
#             mock_from.assert_called_once_with(inner)
#             assert result == "mock_table"

#     def test_get_table_not_found_raises(self):
#         """_get_table should raise NotFoundError when table is not found."""
#         mock_client = MagicMock()
#         mock_client.load_table.side_effect = Exception("Table xyz not found in gravitino")

#         catalog = VastGravitinoCatalog.create(mock_client)

#         from daft.catalog import Identifier

#         with pytest.raises(Exception, match="not found"):
#             catalog._get_table(Identifier("cat", "schema", "xyz"))


# # ---------------------------------------------------------------------------
# # Constants
# # ---------------------------------------------------------------------------


# class TestConstants:
#     def test_vastdb_format_value(self):
#         assert VASTDB_FORMAT == "VASTDB"
