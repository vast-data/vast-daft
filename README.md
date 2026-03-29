# vast-daft

Daft custom connector (`DataSource` / `DataSink`) for [VastDB](https://vastdata.com).

## Features

- **`VastDBDataSource`** — Read from any VastDB table into a Daft DataFrame, with column projection and ibis predicate pushdown.
- **`VastDBDataSink`** — Write Daft DataFrames to VastDB tables.
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

## Deployment (Kubernetes + Ray + Marimo)

Deploy a Ray cluster and Marimo notebook server on Kubernetes with a single command:

```bash
make deploy
```

This builds a Docker image with `vast_daft` + dependencies, pushes it to the Zarf in-cluster registry, and deploys:
- **Ray cluster** (1 head + 2 workers) via KubeRay operator
- **Marimo notebook** server for interactive development
- **Ingress** for browser access (nginx)

| Service | URL |
|---------|-----|
| Marimo notebook | `http://marimo.ray-system.v141.lc` |
| Ray dashboard | `http://ray-dashboard.ray-system.v141.lc` |

### Using Ray from Marimo

```python
import os, daft
from vast_daft import VastDBCatalog, VastDBConfig

daft.set_runner_ray(os.environ["RAY_ADDRESS"])

config = VastDBConfig(...)
catalog = VastDBCatalog(config)
df = catalog.read_table("my_table")
df.show()
```

### Other Make targets

```bash
make build      # Build Docker image only
make push       # Build + push to Zarf registry
make status     # Show pods, services, ingress
make logs       # Tail marimo logs
make undeploy   # Remove Helm release (keeps operator)
make clean      # Remove everything including namespace
```

See [RAY_DEPLOYMENT.md](RAY_DEPLOYMENT.md) for detailed architecture and troubleshooting.

## Architecture

### End-to-end flow on Ray

```
┌─────────────────────────────────────────────────────────────────────┐
│                        User Code (notebook)                          │
│  df = sess.read_table("orders")                                      │
│         .where(col("status") == "active")                            │
│         .groupby("product").agg(sum("revenue"))                      │
│         .collect()                                                   │
└────────────────────────────┬────────────────────────────────────────┘
                             │  lazy — builds a logical plan
                             ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    Daft Logical Plan (driver)                        │
│                                                                      │
│   Aggregate [groupby product, sum revenue]                           │
│       └── Filter [status == "active"]                                │
│               └── VastDBScan [bucket/schema/orders]  ← lazy node    │
└────────────────────────────┬────────────────────────────────────────┘
                             │  .collect() triggers execution
                             ▼
┌─────────────────────────────────────────────────────────────────────┐
│              Daft Optimizer  →  to_scan_tasks(pushdowns)             │
│                                                                      │
│  pushdowns = { filters: status=="active", limit: None }             │
│  → translate filter to ibis predicate  (_pushdown.py)               │
│  → create N ScanTask objects  (one per split)                        │
└──────┬──────────────────────────────────────────────────────────────┘
       │  N pickled ScanTasks sent to Ray
       ▼
┌──────────────────────────────────────────────────────────────────────────┐
│                           Ray Cluster                                     │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐    │
│  │  Worker 0   │  │  Worker 1   │  │  Worker 2   │  │  Worker 3   │    │
│  │  split 0/4  │  │  split 1/4  │  │  split 2/4  │  │  split 3/4  │    │
│  │ VastDB conn │  │ VastDB conn │  │ VastDB conn │  │ VastDB conn │    │
│  │ select_splits(num_splits=4) ─┤  │ select_splits(num_splits=4) ─┤    │
│  │ → keep [0]  │  │ → keep [1]  │  │ → keep [2]  │  │ → keep [3]  │    │
│  └──────┬──────┘  └──────┬──────┘  └──────┬──────┘  └──────┬──────┘    │
│         │ MicroPartition │ MicroPartition  │ MicroPartition │           │
│         ▼                ▼                 ▼                ▼           │
│  ┌──────────────────────────────────────────────────────────────────┐   │
│  │        Daft groupby/agg — local partial → shuffle → final        │   │
│  └──────────────────────────────────────────────────────────────────┘   │
└──────────────────────────────────────────────────────────────────────────┘
                             │  result MicroPartition
                             ▼
                     driver: .collect() returns
```

### Object model

```
VastDBConfig  (frozen dataclass — picklable, sent to every worker)
    endpoint, access_key, secret_key, bucket, schema
         │
         │  instantiated per-worker
         ▼
VastDBConnection
    ├── .get_table()                interactive path (create-if-missing)
    │       HEAD bucket  +  list schemas  +  get/create table  = 3 RPCs
    │
    └── .get_table_from_metadata()  non-interactive hot path
            TableMetadata + load_stats(tx) = 1 RPC
            → skips 2 RPCs vs the interactive path
            → used in: every read split, every write micro-partition
```

### `VastDBScanOperator` — Daft integration

Implements `daft.io.scan.ScanOperator` directly (not `DataSource → _DataSourceShim`)
to unlock optimisation hooks unavailable through the shim:

| Hook | Value | Effect |
|---|---|---|
| `supports_count_pushdown()` | `True` | `df.count()` = 1 metadata RPC, no data scan |
| `can_absorb_filter()` | `True` | WHERE predicate pushed to VastDB server |
| `can_absorb_limit()` | `True` | LIMIT pushed to VastDB server |
| `can_absorb_select()` | `False` | Column projection disabled to prevent join schema failures |

### Split estimation

```
_resolve_num_splits()
        │
        ├─ explicit num_splits passed?     ──► use it
        ├─ query_config.num_splits?        ──► use it
        ├─ auto-estimate from table stats
        │       load_stats(tx)             1 RPC
        │       estimated = num_rows // rows_per_split  (default 4M)
        │       n = min(estimated, cluster_cpus, 64)    ──► use n
        ├─ Ray available, no stats?        ──► cluster_cpus (max 64)
        └─ fallback                        ──► 4
```

### Filter pushdown

Daft filter expressions are translated to ibis predicates via `_DaftToIbisVisitor`
and passed to `table.select_splits(predicate=...)` for server-side filtering:

| Daft expression | ibis predicate |
|---|---|
| `col("x") == 5` | `ibis._["x"] == 5` |
| `col("s").startswith("foo")` | `ibis._["s"].startswith("foo")` |
| `col("a").is_in([1,2,3])` | `ibis._["a"].isin([1,2,3])` |
| `col("a").between(0, 10)` | `ibis._["a"].between(0, 10)` |
| `(col("a") > 0) & (col("b") < 10)` | `(ibis._["a"] > 0) & (ibis._["b"] < 10)` |
| `col("x").cast(...)` | unsupported → Daft applies client-side |

### Write flow

```
df.write_sink(VastDBDataSink(...))

DRIVER ── start() ─────────────────────────────────────────────
    get_table(..., create_if_missing=True)
    HEAD bucket + list schemas + get/create table  = 3 RPCs
    Table guaranteed to exist before any writes.

RAY WORKERS (concurrent, one call per micro-partition)
    for mp in micropartitions:
        arrow_table = mp.to_arrow()
        get_table_from_metadata(...)     ← 1 RPC (load_stats)
        table.insert(arrow_table)

DRIVER ── finalize() ──────────────────────────────────────────
    aggregate WriteResults → summary MicroPartition
```

### RPC budget

```
Operation                            RPCs   Path
─────────────────────────────────────────────────────────────────────
Scan operator construction            1     load_stats (split estimate)
Each read split  (per Ray worker)     1     load_stats (table_type)
df.count()  (entire query!)           1     load_stats → num_rows
write start()  (once, on driver)      3     HEAD bucket + schema + table
Each write micro-partition            1     load_stats (table_type)

10M-row read, 4 splits:
  OLD (interactive per split):  1 + 4×3 = 13 RPCs
  NEW (non-interactive):        1 + 4×1 =  5 RPCs   ← 62% fewer
```

See [`examples/notebooks/architecture_notebook.py`](examples/notebooks/architecture_notebook.py) for the full interactive walkthrough.

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
