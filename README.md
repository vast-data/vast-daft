# vast-daft

Daft custom connector (`DataSource` / `DataSink`) for [VastDB](https://vastdata.com), with SQL query execution via ADBC and two-stage vector similarity search.

## Features

- **`VastDBDataSource`** — Read from any VastDB table into a Daft DataFrame, with column projection and ibis predicate pushdown.
- **`VastDBDataSink`** — Write Daft DataFrames to VastDB tables.
- **`SQLDataSource`** — Execute arbitrary SQL against VastDB via the ADBC driver and get a Daft DataFrame back.
- **`VectorSearchDataSource`** — Two-stage vector similarity search (ADBC `array_distance` for top-k, then SDK for full rows).
- **`VastDBCatalog`** — Daft `Catalog` backed by VastDB: create, drop, list, and read/write tables via the standard Daft catalog API.
- **`VastDBTable`** — Daft `Table` backed by a single VastDB table (read/append/overwrite).
- **Predicate helpers** — `where_equal`, `where_in`, `where_between`, `where_contains`, and combinators (`and_`, `or_`).

## Installation

```bash
uv add vast-daft
```

Or with development dependencies:

```bash
uv sync --all-extras
```

## Quick Start

### Reading from VastDB

```python
import pyarrow as pa
from vast_daft import VastDBConfig, VastDBDataSource

config = VastDBConfig(
    endpoint="http://vastdb:9090",
    access_key="YOUR_ACCESS_KEY",
    secret_key="YOUR_SECRET_KEY",
    bucket="my-bucket",
    schema="my-schema",
)

# Define the table schema (users provide their own)
schema = pa.schema([
    ("id", pa.string()),
    ("name", pa.string()),
    ("value", pa.float64()),
])

# Read the entire table
source = VastDBDataSource(config, "my_table", schema)
df = source.read()
df.show()

# Read with column projection and predicate
from vast_daft import where_equal

source = VastDBDataSource(
    config,
    "my_table",
    schema,
    columns=["id", "value"],
    predicate=where_equal("name", "alice"),
)
df = source.read()
```

### Writing to VastDB

```python
import daft
from vast_daft import VastDBConfig, VastDBDataSink

config = VastDBConfig(...)
schema = pa.schema([("id", pa.string()), ("value", pa.float64())])

sink = VastDBDataSink(config, "my_table", schema)
daft.from_pydict({"id": ["a", "b"], "value": [1.0, 2.0]}).write_sink(sink).show()
```

### SQL Queries via ADBC

```python
from vast_daft import VastDBConfig, SQLDataSource

config = VastDBConfig(
    ...,
    adbc_driver_path="/path/to/libadbc_driver_vastdb.so",
)
result_schema = pa.schema([("id", pa.string()), ("total", pa.int64())])

source = SQLDataSource(
    config,
    "SELECT id, count(*) as total FROM \"bucket/schema\".\"table\" GROUP BY id",
    result_schema,
)
df = source.read()
```

### Vector Similarity Search

```python
from vast_daft import VastDBConfig, VectorSearchDataSource

config = VastDBConfig(..., adbc_driver_path="/path/to/libadbc_driver_vastdb.so")
table_schema = pa.schema([
    ("pk", pa.string()),
    ("pk_hash", pa.string()),
    ("text", pa.string()),
    ("vector", pa.list_(pa.float32(), 768)),
])

source = VectorSearchDataSource(
    config,
    table_name="embeddings",
    table_schema=table_schema,
    query_vector=[0.1] * 768,
    k=10,
)
df = source.read()  # includes a 'distance' column
```

### Catalog & Table Management

```python
from vast_daft import VastDBCatalog, VastDBConfig
from daft.schema import Schema
import pyarrow as pa

config = VastDBConfig(...)
catalog = VastDBCatalog(config)

# List tables
print(catalog.list_tables())

# Create a table
schema = Schema.from_pyarrow_schema(pa.schema([("id", pa.int64()), ("name", pa.utf8())]))
table = catalog.create_table_if_not_exists("my_table", schema)

# Read into a DataFrame
df = catalog.read_table("my_table")

# Write back
catalog.write_table("my_table", df, mode="append")

# Drop
catalog.drop_table("old_table")
```

## Configuration

`VastDBConfig` can be built from environment variables:

```python
config = VastDBConfig.from_env()
```

| Env Var | Required | Description |
|---------|----------|-------------|
| `VASTDB_ENDPOINT` | Yes | VastDB API endpoint |
| `VASTDB_ACCESS_KEY` | Yes | Access key |
| `VASTDB_SECRET_KEY` | Yes | Secret key |
| `VASTDB_BUCKET` | Yes | Bucket name |
| `VASTDB_SCHEMA` | Yes | Schema name |
| `VASTDB_SSL_VERIFY` | No | `true`/`false` (default `true`) |
| `VASTDB_ADBC_DRIVER_PATH` | No | Path to `libadbc_driver_vastdb.so` |

## Development

```bash
# Install with dev dependencies
uv sync --all-extras

# Run tests
uv run pytest

# Lint
uv run ruff check src/ tests/

# Type check
uv run mypy src/
```

## License

Apache-2.0
