"""vast-daft: Daft custom connector for VastDB.

Provides ``DataSource`` / ``DataSink`` implementations for reading from
and writing to VastDB, a Daft catalog integration for native SQL queries,
and a Gravitino-aware catalog that routes VastDB-format tables through
the native SDK.
"""

from vast_daft.config import VastDBConfig
from vast_daft.connection import VastDBConnection
from vast_daft.scan import VastDBScanOperator
from vast_daft.sink import VastDBDataSink
from vast_daft.source import VastDBDataSource
from vast_daft.table import VastDBCatalog, VastDBTable

try:
    from vast_daft.gravitino import VastGravitinoCatalog, VastGravitinoTable
except ModuleNotFoundError:
    VastGravitinoCatalog = None
    VastGravitinoTable = None

__all__ = [
    # Core
    "VastDBConfig",
    "VastDBConnection",
    # Daft connectors
    "VastDBDataSource",
    "VastDBDataSink",
    "VastDBScanOperator",
    # Catalog / Table (Daft interfaces — use with daft.attach_catalog + daft.sql)
    "VastDBCatalog",
    "VastDBTable",
    # Gravitino-aware catalog (routes format=vastdb to VastDBCatalog)
    "VastGravitinoCatalog",
    "VastGravitinoTable",
]
