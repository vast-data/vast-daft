# vast-daft

[![PyPI](https://img.shields.io/pypi/v/vast-daft)](https://pypi.org/project/vast-daft/) [![GitHub](https://img.shields.io/badge/GitHub-vast--data%2Fvast--daft-blue?logo=github)](https://github.com/vast-data/vast-daft)

Daft custom connector (`DataSource` / `DataSink`) for [VastDB](https://vastdata.com).

- **PyPI**: https://pypi.org/project/vast-daft/
- **GitHub**: https://github.com/vast-data/vast-daft

## Features

- **`VastDBDataSource`** — Read from any VastDB table into a Daft DataFrame, with column projection and ibis predicate pushdown.
- **`VastDBDataSink`** — Write Daft DataFrames to VastDB tables.
- **`VastDBCatalog`** — Daft `Catalog` backed by VastDB: create, drop, list, and read/write tables via the standard Daft catalog API.
- **`VastDBTable`** — Daft `Table` backed by a single VastDB table (read/append/overwrite).
- **Predicate helpers** — `where_equal`, `where_in`, `where_between`, `where_contains`, and combinators (`and_`, `or_`).

## Installation

```bash
pip install vast-daft
```

Or with uv:

```bash
uv add vast-daft
```

For development in this monorepo:

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

## Deploying on Kubernetes with Ray

`vast-daft` is a regular Python package — no custom container image is required. A typical setup:

1. **Ray cluster on Kubernetes.** Install the [KubeRay](https://github.com/ray-project/kuberay) operator and create a `RayCluster` using the stock `rayproject/ray` image (e.g. `rayproject/ray:2.57.0-py312`).

2. **Install `vast-daft` on the Ray pods.** The package must be importable on every Ray pod (head + workers), since tasks execute on the workers. Options, in order of simplicity:

   **a. PyPI.** Use Ray's `runtime_env` or KubeRay's `runtimeEnvYAML`:
   ```yaml
   runtimeEnvYAML: |
     pip: ["vast-daft"]
   ```

   **b. Wheel via ConfigMap (no registry needed).** Build the wheel, ship it as a `ConfigMap`, mount it, and `pip install` from the mount on pod startup:
   ```bash
   uv build --wheel                                  # produces dist/vast_daft-*.whl
   kubectl create configmap vast-daft-wheel \
       --from-file=dist/vast_daft-*.whl -n ray-system
   ```
   Then in the `RayCluster` pod spec, mount the ConfigMap and run `pip install /mnt/wheel/vast_daft-*.whl` in an init container (or in the container's `command`).

   **c. Custom image.** `FROM rayproject/ray:2.57.0-py312` + `RUN pip install vast-daft`. Heaviest option (registry, rebuilds), but fully reproducible.

3. **Credentials.** Expose VastDB and S3 credentials as env vars on the Ray pods: `VASTDB_ENDPOINT`, `VASTDB_ACCESS_KEY`, `VASTDB_SECRET_KEY`, `S3_ACCESS_KEY`, `S3_SECRET_KEY`. See `.env.example`.

4. **Connect from a client.**
   ```python
   import daft
   from vast_daft import VastDBCatalog, VastDBConfig

   daft.context.set_runner_ray("ray://<head-svc>:10001")
   catalog = VastDBCatalog(VastDBConfig(...))
   catalog.read_table("my_table").show()
   ```

### Fault tolerance on Ray

Running Daft on Ray provides partition-level resilience automatically:

| Capability | Notes |
|---|---|
| Task retry on worker crash | Ray retries failed tasks up to 3× on system failure (node death, OOMKill) |
| Worker node failure recovery | Lost partitions are rescheduled on surviving nodes |
| Lineage-based reconstruction | Lost objects are rebuilt by re-running the producing task (assumes idempotent reads) |
| OOMKill recovery | Ray detects OOM-killed workers and retries the affected work |
| Object spilling | Datasets larger than aggregate cluster RAM spill to disk automatically |

**Limitations** — what Ray does _not_ provide:

- **No job-level checkpointing** — a job that fails after 90% completion restarts from scratch
- **Head node is a SPOF** by default; requires KubeRay + HA Redis for head-node fault tolerance
- **No exactly-once guarantees** — retried tasks may produce duplicate writes unless the sink is idempotent
- **No stateful streaming** — Daft is a batch engine; incremental/streaming ingestion is not supported
- For long-running pipelines where partial failure recovery is critical, pair with an external orchestrator (Dagster, Airflow, Prefect)

## Architecture

### End-to-end flow on Ray

```
┌─────────────────────────────────────────────────────────────────────┐
│                        User Code (notebook)                         │
│  df = sess.read_table("orders")                                     │
│         .where(col("status") == "active")                           │
│         .groupby("product").agg(sum("revenue"))                     │
│         .collect()                                                  │
└────────────────────────────┬────────────────────────────────────────┘
                             │  lazy — builds a logical plan
                             ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    Daft Logical Plan (driver)                       │
│                                                                     │
│   Aggregate [groupby product, sum revenue]                          │
│       └── Filter [status == "active"]                               │
│               └── VastDBScan [bucket/schema/orders]  ← lazy node    │
└────────────────────────────┬────────────────────────────────────────┘
                             │  .collect() triggers execution
                             ▼
┌─────────────────────────────────────────────────────────────────────┐
│              Daft Optimizer  →  to_scan_tasks(pushdowns)            │
│                                                                     │
│  pushdowns = { filters: status=="active", limit: None }             │
│  → translate filter to ibis predicate  (_pushdown.py)               │
│  → create N ScanTask objects  (one per split)                       │
└──────┬──────────────────────────────────────────────────────────────┘
       │  N pickled ScanTasks sent to Ray
       ▼
┌────────────────────────────────────────────────────────────────────────┐
│                           Ray Cluster                                  │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐    │
│  │  Worker 0   │  │  Worker 1   │  │  Worker 2   │  │  Worker 3   │    │
│  │  split 0/4  │  │  split 1/4  │  │  split 2/4  │  │  split 3/4  │    │
│  │ VastDB conn │  │ VastDB conn │  │ VastDB conn │  │ VastDB conn │    │
│  │ select_splits(num_splits=4) ─┤  │ select_splits(num_splits=4) ─┤    │
│  │ → keep [0]  │  │ → keep [1]  │  │ → keep [2]  │  │ → keep [3]  │    │
│  └──────┬──────┘  └──────┬──────┘  └──────┬──────┘  └──────┬──────┘    │
│         │ MicroPartition │ MicroPartition │ MicroPartition │           │
│         ▼                ▼                ▼                ▼           │
│  ┌──────────────────────────────────────────────────────────────────┐  │
│  │        Daft groupby/agg — local partial → shuffle → final        │  │
│  └──────────────────────────────────────────────────────────────────   │
└────────────────────────────────────────────────────────────────────────┘
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
| `can_absorb_select()` | `False` | Column projection disabled — workaround for a Daft bug ([#6500](https://github.com/Eventual-Inc/Daft/issues/6500)) where partial column pushdown triggers schema assertion failures in hash-join; all columns are fetched and Daft projects above the scan node |

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

### Catalog Explorer

The [`examples/notebooks/catalog_explorer_notebook.py`](examples/notebooks/catalog_explorer_notebook.py) notebook provides an interactive UI for browsing VastDB, Iceberg, and Kafka catalogs side-by-side:

- **Catalog & table browser** — select a catalog and explore its tables
- **SQL editor** — run ad-hoc SQL queries with execution timing
- **Python editor** — execute arbitrary Daft/Python code against the session
- **Schema viewer** — inspect column names and types for any table

## Development

```bash
# Install with dev dependencies
uv sync --all-extras

# Run tests
uv run pytest

# Type check and lint
uv make check
```
