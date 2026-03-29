import marimo  # type: ignore

__generated_with = "0.13.0"
app = marimo.App(width="medium")


@app.cell
def _(mo):
    mo.md(
        """
        # vast_daft Architecture

        How Daft integrates with VastDB and distributes work across Ray workers.

        Covers: object model, split estimation, read (select / filter / agg / join),
        count pushdown, filter pushdown, writes, and Ray fault tolerance — all in the Ray context.
        """
    )
    return


@app.cell
def _():
    import marimo as mo

    return (mo,)


@app.cell
def _(mo):
    mo.md(
        r"""
        ## End-to-end flow

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
        │               └── VastDBScan [bucket/schema/orders]  ◄── lazy node  │
        └────────────────────────────┬────────────────────────────────────────┘
                                     │  .collect() triggers execution
                                     ▼
        ┌─────────────────────────────────────────────────────────────────────┐
        │              Daft Optimizer  →  to_scan_tasks(pushdowns)            │
        │                                                                     │
        │  pushdowns = { filters: status=="active", limit: None }             │
        │                                                                     │
        │  → translate filter to ibis predicate  (via _pushdown.py)           │
        │  → create N ScanTask objects  (one per split)                       │
        └──────┬──────────────────────────────────────────────────────────────┘
               │  N pickled ScanTasks sent to Ray
               ▼
        ┌────────────────────────────────────────────────────────────────────────┐
        │                         Ray Cluster                                    │
        │                                                                        │
        │  ┌─────────────┐   ┌─────────────┐   ┌─────────────┐   ┌─────────────┐ │
        │  │  Worker 0   │   │  Worker 1   │   │  Worker 2   │   │  Worker 3   │ │
        │  │  split 0/4  │   │  split 1/4  │   │  split 2/4  │   │  split 3/4  │ │
        │  │             │   │             │   │             │   │             │ │
        │  │ VastDB conn │   │ VastDB conn │   │ VastDB conn │   │ VastDB conn │ │
        │  │ select_splits(num_splits=4)   │   │ select_splits(num_splits=4)   │ │
        │  │ → keep [0]  │   │ → keep [1]  │   │ → keep [2]  │   │ → keep [3]  │ │
        │  └──────┬──────┘   └──────┬──────┘   └──────┬──────┘   └──────┬──────┘ │
        │         │ MicroPartition  │ MicroPartition  │ MicroPartition  │        │
        │         ▼                 ▼                 ▼                 ▼        │
        │  ┌──────────────────────────────────────────────────────────────────┐  │
        │  │        Daft groupby/agg — local partial → shuffle → final        │  │
        │  └──────────────────────────────────────────────────────────────────┘  │
        └────────────────────────────────────────────────────────────────────────┘
                                     │  result MicroPartition
                                     ▼
                             driver: .collect() returns
        ```
        """
    )
    return


@app.cell
def _(mo):
    mo.md(
        r"""
        ## Object model

        ```
        VastDBConfig  (frozen dataclass — picklable, sent to every worker)
            endpoint, access_key, secret_key, bucket, schema
                 │
                 │  instantiated per-worker
                 ▼
        VastDBConnection
            ├── .get_table()               interactive path (create-if-missing)
            │       HEAD bucket            3 RPCs
            │       list schemas           ↑
            │       get/create table       ↑
            │
            └── .get_table_from_metadata() non-interactive path (hot path)
                    TableMetadata(TableRef(bucket, schema, table), arrow_schema)
                    load_stats(tx)         1 RPC  ← sets table_type (needed by insert)
                    tx.table_from_metadata(table_md)

                 ┌─────────────────────────────────────────┐
                 │  non-interactive saves 2 RPCs per call  │
                 │  used in: every read split, every write │
                 └─────────────────────────────────────────┘
        ```

        `VastDBConfig` is the only thing that travels between driver and workers.
        Every worker opens its own `VastDBConnection` and its own SDK session.
        """
    )
    return


@app.cell
def _(mo):
    mo.md(
        r"""
        ## Daft integration — `VastDBScanOperator`

        `scan.py` implements `daft.io.scan.ScanOperator` directly.
        Going through `DataSource → _DataSourceShim` would lock out key optimisation hooks.

        ```
        ┌──────────────────────────────────────────────────────────────┐
        │  Hook                      │ Value │ Effect                  │
        │────────────────────────────┼───────┼─────────────────────────│
        │  supports_count_pushdown() │ True  │ df.count() = 1 RPC,     │
        │                            │       │ no data scan            │
        │  can_absorb_filter()       │ True  │ WHERE pushed to VastDB  │
        │  can_absorb_limit()        │ True  │ LIMIT pushed to VastDB  │
        │  can_absorb_select()       │ False │ bug workaround #6500    │
        └──────────────────────────────────────────────────────────────┘
        ```

        Wired into Daft's logical plan:

        ```python
        scan_op = VastDBScanOperator(config, table_name, pa_schema, ...)
        handle  = ScanOperatorHandle.from_python_scan_operator(scan_op)
        builder = LogicalPlanBuilder.from_tabular_scan(scan_operator=handle)
        return DataFrame(builder)   # lazy — nothing runs yet
        ```

        **Why `can_absorb_select() = False`?**
        Workaround for a Daft bug ([#6500](https://github.com/Eventual-Inc/Daft/issues/6500)).
        When enabled, Daft may push a partial column set to the scan (e.g. only
        `["product_id"]` for a join), omitting columns that downstream operators need,
        triggering schema assertion failures inside Daft's hash-join.
        Disabled until the bug is fixed upstream — VastDB returns all columns and
        Daft projects above the scan node. Cost: extra data over the wire.
        """
    )
    return


@app.cell
def _(mo):
    mo.md(
        r"""
        ## Split estimation

        Runs at `VastDBScanOperator.__init__()` — before any user-triggered execution.

        ```
        _resolve_num_splits()
                │
                ├─ explicit num_splits passed?      ──► use it
                │
                ├─ query_config.num_splits?         ──► use it
                │
                ├─ auto-estimate from table stats
                │       load_stats(tx)              1 RPC
                │       estimated = num_rows // rows_per_split   (default 4M)
                │
                │       cluster_cpus = ray.cluster_resources()["CPU"]
                │       cap = min(cluster_cpus, 64)
                │       n   = min(estimated, cap)   ──► use n
                │
                ├─ Ray available, no stats?         ──► cluster_cpus (max 64)
                │
                └─ fallback                         ──► 4

        Example: 10M rows, 4 CPUs
            10_000_000 // 4_000_000 = 2  →  min(2, 4) = 2 splits
        ```

        The formula (`num_rows // rows_per_split`) is the same one VastDB uses
        internally when `num_splits` is not specified.  Capping at cluster CPUs
        avoids creating more tasks than available cores.
        """
    )
    return


@app.cell
def _(mo):
    mo.md(
        r"""
        ## Read flow (SELECT + filter)

        ```
        DRIVER
        ──────
        VastDBScanOperator.to_scan_tasks(pushdowns)
            │
            ├── pushdowns.aggregation?  →  no  →  _create_split_tasks()
            │
            ├── pushdowns_to_predicate(pds)     (_pushdown.py — see below)
            │       returns ibis predicate  or  None
            │
            └── for i in range(num_splits):
                    task = VastDBDataSourceTask(
                        split_index  = i,
                        total_splits = num_splits,
                        predicate    = ibis._["status"] == "active",
                        ...
                    )
                    yield ScanTask.python_factory_func_scan_task(
                        func      = "_read_vastdb_split",   ← module-level fn (picklable)
                        func_args = (task,),                ← task is pickled here
                    )

                    ┌────────────────────────────────────────────────────┐
                    │  ScanTask = pointer to (func_name, pickled_args)   │
                    │  No data is read at this point.                    │
                    └────────────────────────────────────────────────────┘

        RAY WORKER  (one per ScanTask, all run concurrently)
        ──────────
        _read_vastdb_split(task)
            └── task.get_micro_partitions()

                VastDBConnection(config)              ← fresh connection per worker

                with get_table_from_metadata(...):    ← 1 RPC (load_stats)

                    split_readers = table.select_splits(
                        predicate = ibis predicate,   ← server filters rows
                        config    = QueryConfig(num_splits=4),
                    )
                    # VastDB server partitions table into 4 equal chunks.
                    # Every worker requests all 4; each picks its own index.

                    reader = split_readers[self.split_index]
                    for r in split_readers:
                        if r is not reader: r.close()   ← free server cursors

                    for batch in reader:                ← streams Arrow RecordBatches
                        yield MicroPartition.from_arrow(...)
        ```
        """
    )
    return


@app.cell
def _(mo):
    mo.md(
        r"""
        ## Count pushdown

        `df.count()` never reads any data.

        ```
        df.count()
            │
            ▼
        Daft optimizer puts aggregation = CountMode.All in pushdowns

        VastDBScanOperator.to_scan_tasks(pushdowns)
            │
            ├── aggregation_count_mode() == CountMode.All ?  →  YES
            │
            └── _create_count_task()
                    │
                    ├── _fetch_row_count()
                    │       TableMetadata(TableRef(...), arrow_schema)
                    │       load_stats(tx)           ← 1 RPC only
                    │       return stats.num_rows    ← e.g. 10_000_000
                    │
                    └── yield one ScanTask wrapping _vastdb_count_result(10_000_000)
                            → single-row RecordBatch: [{"count": 10000000}]
                            → NO data scan, NO split tasks created

        Total cost: 1 RPC  ~1.4s
        vs full scan: N splits × stream all rows  ~minutes
        ```
        """
    )
    return


@app.cell
def _(mo):
    mo.md(
        r"""
        ## Filter pushdown — expression translation

        `_pushdown.py` walks Daft's `Expression` AST via `_DaftToIbisVisitor`
        and produces an ibis predicate passed to `table.select_splits(predicate=...)`.

        ```
        Daft Expression                      ibis predicate (sent to VastDB server)
        ────────────────────────────────────────────────────────────────────────────
        col("x") == 5                    →   ibis._["x"] == 5
        col("s").startswith("foo")       →   ibis._["s"].startswith("foo")
        col("s").endswith("bar")         →   ibis._["s"].endswith("bar")
        col("s").contains("baz")         →   ibis._["s"].contains("baz")
        col("a").is_in([1, 2, 3])        →   ibis._["a"].isin([1, 2, 3])
        col("a").between(0, 10)          →   ibis._["a"].between(0, 10)
        col("x").is_null()               →   ibis._["x"].isnull()
        col("x").not_null()              →   ibis._["x"].notnull()
        (col("a") > 0) & (col("b") < 10) →  (ibis._["a"] > 0) & (ibis._["b"] < 10)
        (col("a") > 0) | (col("b") < 10) →  (ibis._["a"] > 0) | (ibis._["b"] < 10)
        ~col("x").is_null()              →   ~ibis._["x"].isnull()

        col("x").cast(IntType)           →   ✗ unsupported → returns None
                                              → Daft applies filter client-side
        ```

        If any node in the tree is unsupported, the visitor returns `_UNSUPPORTED`
        and `pushdowns_to_predicate()` returns `None`.  In that case no predicate
        is sent to VastDB; Daft post-filters the raw rows after the scan.
        """
    )
    return


@app.cell
def _(mo):
    mo.md(
        r"""
        ## Write flow

        ```
        df.write_sink(VastDBDataSink(config, "orders", schema))

        DRIVER ── start() ────────────────────────────────────────────────────
            connection.get_table(table_name, schema, create_if_missing=True)
            │   HEAD bucket        ┐
            │   list schemas       ├  3 RPCs (interactive — table may not exist yet)
            │   get/create table   ┘
            └── table guaranteed to exist before any writes

        Daft partitions df into micro-partitions and dispatches to Ray workers

        RAY WORKERS (concurrent, one call per micro-partition)
        ──────────
        VastDBDataSink.write(micropartitions)
            │
            └── for mp in micropartitions:
                    arrow_table = mp.to_arrow()

                    with get_table_from_metadata(...):   ← 1 RPC (load_stats)
                        table.insert(arrow_table)        ← sends data to VastDB

                    yield WriteResult(rows_written=N, bytes_written=B)

        DRIVER ── finalize(write_results) ────────────────────────────────────
            sum all rows_written, bytes_written across workers
            return summary MicroPartition:

                rows_written  │ bytes_written
                ──────────────┼───────────────
                10_000_000    │ 2_400_000_000
        ```

        `start()` uses the **interactive** path because the table may not exist.
        Every subsequent `write()` call uses the **non-interactive** path because
        the table is guaranteed to exist after `start()`.
        """
    )
    return


@app.cell
def _(mo):
    mo.md(
        r"""
        ## Join flow

        ```
        orders.join(products, on="product_id")

        DRIVER: two scan operators constructed independently
            ┌─────────────────────────┐    ┌─────────────────────────┐
            │  VastDBScanOperator     │    │  VastDBScanOperator     │
            │  table: orders          │    │  table: products        │
            │  10M rows → 4 splits    │    │  200 rows → 1 split     │
            └────────────┬────────────┘    └────────────┬────────────┘
                         │                              │
                         │  to_scan_tasks()             │  to_scan_tasks()
                         │  4 ScanTasks                 │  1 ScanTask
                         ▼                              ▼
        RAY WORKERS:  all 5 tasks dispatched and run concurrently

          Worker 0: orders split 0/4    Worker 4: products split 0/1
          Worker 1: orders split 1/4    (builds hash table in memory)
          Worker 2: orders split 2/4
          Worker 3: orders split 3/4

        DAFT join strategy (hash join):
          ┌──────────────────────────────────────────────────┐
          │  products (small) → broadcast hash table         │
          │  each orders partition probes the hash table     │
          │  matched rows emitted as joined MicroPartitions  │
          └──────────────────────────────────────────────────┘

        WHY can_absorb_select() = False matters here:
          ┌────────────────────────────────────────────────────────────────────┐
          │  Daft bug #6500: partial column pushdown into a scan that feeds    │
          │  a hash-join causes schema assertion failures.                     │
          │                                                                    │
          │  If True:  Daft might push ["product_id"] to the orders scan       │
          │            → other columns (order_total, status …) missing         │
          │            → HashJoin schema assertion failure  ← bug              │
          │  If False: orders scan returns ALL columns every time              │
          │            → Daft projects above the join node — always safe       │
          │            → cost: extra columns over the wire until bug is fixed  │
          └────────────────────────────────────────────────────────────────────┘
        ```
        """
    )
    return


@app.cell
def _(mo):
    mo.md(
        r"""
        ## RPC budget

        ```
        Operation                            RPCs   Path
        ─────────────────────────────────────────────────────────────────────
        Scan operator construction            1     load_stats (split estimate)
        Each read split  (per Ray worker)     1     load_stats (table_type)
        df.count()  (entire query!)           1     load_stats → num_rows
        write start()  (once, on driver)      3     HEAD bucket + schema + table
        Each write micro-partition            1     load_stats (table_type)
        Schema discovery (VastDBTable)        3     interactive (bucket+schema+cols)
        ─────────────────────────────────────────────────────────────────────

        10M-row read, 4 splits:
          OLD (interactive per split):  1 + 4×3 = 13 RPCs
          NEW (non-interactive):        1 + 4×1 =  5 RPCs   ← 62% fewer
        ```
        """
    )
    return


@app.cell
def _(mo):
    mo.md(
        r"""
        ## Fault tolerance on Ray

        Running Daft on Ray provides partition-level resilience automatically.

        **What you get for free:**

        ```
        ┌──────────────────────────────────┬──────────────────────────────────────────┐
        │  Capability                      │  What happens                            │
        ├──────────────────────────────────┼──────────────────────────────────────────┤
        │  Task retry on worker crash      │  Ray retries up to 3× on system failure  │
        │                                  │  (node death, OOMKill)                   │
        │  Worker node failure recovery    │  Lost partitions rescheduled on          │
        │                                  │  surviving nodes automatically           │
        │  Lineage reconstruction          │  Lost objects rebuilt by re-running the  │
        │                                  │  producing task (reads must be idempotent)│
        │  OOMKill recovery                │  Ray detects and retries OOM-killed work │
        │  Object spilling                 │  Datasets > aggregate RAM spill to disk  │
        └──────────────────────────────────┴──────────────────────────────────────────┘
        ```

        **What you do NOT get:**

        ```
        ✗  No job-level checkpointing
              A job that fails at 90% restarts from scratch.

        ✗  Head node is a SPOF by default
              Requires KubeRay + HA Redis for head-node fault tolerance.

        ✗  No exactly-once guarantees
              Retried tasks may produce duplicate writes unless the sink is idempotent.

        ✗  No stateful streaming / incremental ingestion
              Daft is a batch engine. For CDC or streaming, use a dedicated tool.

        ✗  No automatic actor state recovery
              Swordfish workers on Ray have no built-in checkpoint of in-progress state;
              a failed task restarts the partition scan from the beginning.
        ```

        For production long-running pipelines, pair with an external orchestrator
        (Dagster, Airflow, Prefect) and ensure write operations are idempotent.
        """
    )
    return


if __name__ == "__main__":
    app.run()
