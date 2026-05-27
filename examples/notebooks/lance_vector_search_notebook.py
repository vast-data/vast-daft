import marimo

__generated_with = "0.21.1"
app = marimo.App(width="medium")


@app.cell
def _(mo):
    mo.md("""
    # Lance Vector & Full-Text Search on VAST S3

    This notebook demonstrates using Daft's native Lance integration for vector 
    and full-text search, with data stored on VAST S3.

    1. **Copy chunks to Lance** -- read the VastDB `chunks` table and write it
       as a Lance dataset on S3 using Daft's `write_lance()`.
    2. **Build indexes** -- create an IVF_PQ vector index *and* an INVERTED
       full-text search index (using Lance APIs, as Daft doesn't yet support this).
    3. **ANN vector search** -- use Daft's `default_scan_options["nearest"]` to find
       the 10 most similar chunks using the ANN index.
    4. **Full-text search** -- use Daft to query the Lance dataset.
    5. **Read via Daft** -- load and query the Lance dataset using the Daft DataFrame API.
    """)
    return


@app.cell
def _():
    import marimo as mo  # type: ignore

    return (mo,)


@app.cell
def _():
    import math
    import os
    import random
    import time

    import daft
    import lance
    import pyarrow as pa
    from daft.io import IOConfig, S3Config
    from helpers import configure_daft_runner, get_s3_credentials  # type: ignore

    from vast_daft import VastDBCatalog, VastDBConfig

    return (
        IOConfig,
        S3Config,
        VastDBCatalog,
        VastDBConfig,
        configure_daft_runner,
        daft,
        get_s3_credentials,
        lance,
        math,
        os,
        pa,
        random,
        time,
    )


@app.cell
def _(configure_daft_runner):
    runner_status = configure_daft_runner()
    print(runner_status)
    return


@app.cell
def _(IOConfig, S3Config, VastDBCatalog, VastDBConfig, daft, get_s3_credentials, os):
    ENDPOINT = os.environ.get("VASTDB_ENDPOINT", "")
    BUCKET = "collections-bucket"
    SCHEMA = "collections-schema"
    ACCESS_KEY, SECRET_KEY = get_s3_credentials()

    LANCE_URI = f"s3://{BUCKET}/lance/chunks.daft"
    LANCE_STORAGE_OPTIONS = {
        "region": "us-east-1",
        "endpoint": ENDPOINT,
        "access_key_id": ACCESS_KEY,
        "secret_access_key": SECRET_KEY,
        "allow_http": "true",
        "allow_invalid_certificates": "true",
    }

    io_config = IOConfig(
        s3=S3Config(
            endpoint_url=ENDPOINT,
            key_id=ACCESS_KEY,
            access_key=SECRET_KEY,
            region_name="us-east-1",
            use_ssl=not ENDPOINT.startswith("http://"),
            verify_ssl=False,
        ),
    )

    config = VastDBConfig(
        endpoint=ENDPOINT,
        access_key=ACCESS_KEY,
        secret_key=SECRET_KEY,
        bucket=BUCKET,
        schema=SCHEMA,
        ssl_verify=False,
    )
    catalog = VastDBCatalog(config, alias="vast")

    sess = daft.session()
    sess.attach_catalog(catalog)
    sess.set_catalog("vast")

    print(f"Connected to VastDB at {ENDPOINT}")
    print(f"Lance URI: {LANCE_URI}")
    return (
        ACCESS_KEY,
        ENDPOINT,
        LANCE_STORAGE_OPTIONS,
        LANCE_URI,
        SECRET_KEY,
        catalog,
        daft,
        io_config,
        sess,
    )


# ---------------------------------------------------------------------------
# Step 1 -- Copy chunks table to Lance on S3 using Daft
# ---------------------------------------------------------------------------
@app.cell
def _(mo):
    mo.md("""
    ## Step 1 -- Copy chunks table to Lance on S3 using Daft

    Read the full `chunks` table from VastDB and persist it as a Lance dataset
    on VAST S3 using Daft's `df.write_lance()` API.
    """)
    return


@app.cell
def _(
    ACCESS_KEY, ENDPOINT, LANCE_URI, LANCE_STORAGE_OPTIONS, S3Config, SECRET_KEY, catalog, daft, io_config, math, time
):
    _t0 = time.perf_counter()

    # Check if the Lance dataset already exists using Daft.
    try:
        _existing_df = daft.read_lance(LANCE_URI, io_config=io_config)
        _row_count = _existing_df.count().collect().to_pydict()["count"][0]
        lance_created = False
        print(f"Lance dataset already exists ({_row_count:,} rows) -- skipping write")
    except Exception:
        print("Lance dataset not found -- reading chunks from VastDB ...")
        _chunks_df = catalog.get_table("chunks").read()
        _collected = _chunks_df.collect()
        _num_rows = int(_collected.count().collect().to_pydict()["count"][0])
        print(f"Collected {_num_rows:,} rows from VastDB")

        # Use Daft's write_lance() API.
        _meta = _collected.write_lance(
            LANCE_URI,
            mode="overwrite",
            io_config=io_config,
        )
        lance_created = True
        print(f"Wrote {_num_rows:,} rows to {LANCE_URI}")
        print(f"Write metadata: {_meta}")

    _elapsed = time.perf_counter() - _t0
    print(f"Step 1 elapsed: {_elapsed:.1f}")

    # Open with Daft for later steps.
    df_lance = daft.read_lance(LANCE_URI, io_config=io_config)
    num_rows = df_lance.count().collect().to_pydict()["count"][0]
    num_partitions = max(16, int(math.sqrt(num_rows)))

    print(f"Daft Lance DataFrame: {num_rows:,} rows")
    return df_lance, lance_created, num_partitions, num_rows


# ---------------------------------------------------------------------------
# Step 2 -- Build vector index (IVF_PQ) using Lance
# ---------------------------------------------------------------------------
@app.cell
def _(mo):
    mo.md("""
    ## Step 2 -- Build vector index (IVF_PQ)

    Create an IVF_PQ approximate nearest-neighbour index on the `vector` column.
    Daft doesn't yet have native index creation, so we use Lance directly.
    """)
    return


@app.cell
def _(LANCE_STORAGE_OPTIONS, LANCE_URI, df_lance, lance, lance_created, num_partitions, time):
    _t0 = time.perf_counter()

    # Open with Lance for index creation.
    _ds = lance.dataset(LANCE_URI, storage_options=LANCE_STORAGE_OPTIONS)

    # Detect vector dimension from the schema.
    _vec_field = _ds.schema.field("vector")
    _vec_dim = _vec_field.type.list_size
    _num_sub = max(1, _vec_dim // 128)

    _indices = _ds.list_indices()
    _has_vec_idx = any(idx.get("fields", idx.get("columns", [""]))[0] == "vector" for idx in _indices)

    if _has_vec_idx and not lance_created:
        print("Vector index already exists -- skipping")
    else:
        print(f"Building IVF_PQ index: dim={_vec_dim}, partitions={num_partitions}, sub_vectors={_num_sub} ...")
        _ds.create_index(
            "vector",
            index_type="IVF_PQ",
            num_partitions=num_partitions,
            num_sub_vectors=_num_sub,
            replace=True,
        )
        print("Vector index created")

    _elapsed = time.perf_counter() - _t0
    print(f"Step 2 elapsed: {_elapsed:.1f}s")
    return


# ---------------------------------------------------------------------------
# Step 3 -- Build full-text search index (INVERTED)
# ---------------------------------------------------------------------------
@app.cell
def _(mo):
    mo.md("""
    ## Step 3 -- Build full-text search index (INVERTED)

    Create an inverted index on the `raw_text` column for BM25-ranked keyword searches.
    Daft doesn't yet have native index creation, so we use Lance directly.
    """)
    return


@app.cell
def _(LANCE_STORAGE_OPTIONS, LANCE_URI, _ds, lance, lance_created, time):
    _t0 = time.perf_counter()

    # Re-open with Lance for index creation.
    _ds = lance.dataset(LANCE_URI, storage_options=LANCE_STORAGE_OPTIONS)

    _indices = _ds.list_indices()
    _has_fts_idx = any(idx.get("fields", idx.get("columns", [""]))[0] == "raw_text" for idx in _indices)

    if _has_fts_idx and not lance_created:
        print("FTS index already exists -- skipping")
    else:
        print("Building INVERTED full-text index on raw_text ...")
        _ds.create_scalar_index(
            column="raw_text",
            index_type="INVERTED",
            with_position=True,
            replace=True,
        )
        print("FTS index created")

    _elapsed = time.perf_counter() - _t0
    print(f"Step 3 elapsed: {_elapsed:.1f}s")

    # Re-open to pick up all newly created indices.
    _ds_indexed = lance.dataset(LANCE_URI, storage_options=LANCE_STORAGE_OPTIONS)
    print(f"Indices: {_ds_indexed.list_indices()}")
    return (_ds_indexed,)


# ---------------------------------------------------------------------------
# Step 4 -- ANN vector search using Daft's default_scan_options
# ---------------------------------------------------------------------------
@app.cell
def _(mo):
    mo.md("""
    ## Step 4 -- ANN vector search using Daft

    Use Daft's `default_scan_options` with `nearest` to run vector search.
    This pushes the ANN query down to Lance's index.
    """)
    return


@app.cell
def _(
    ACCESS_KEY,
    ENDPOINT,
    LANCE_URI,
    LANCE_STORAGE_OPTIONS,
    S3Config,
    SECRET_KEY,
    daft,
    df_lance,
    io_config,
    lance,
    random,
    time,
):
    _t0 = time.perf_counter()

    # Get a random sample of 1 row using sample.
    _query_df = df_lance.select("pk", "source", "vector").limit(1)
    _query_row = _query_df.collect().to_pydict()
    _query_pk = _query_row["pk"][0]
    _query_source = _query_row["source"][0]
    _query_vector = _query_row["vector"][0]

    print(f"Random query PK: {_query_pk}")
    print(f"Random query source: {_query_source}")
    print(f"Vector dim: {len(_query_vector)}")

    # Use Lance directly for ANN search (Daft's vector search has some bugs with fixed_size_list).
    _ds = lance.dataset(LANCE_URI, storage_options=LANCE_STORAGE_OPTIONS)
    _ann_result = _ds.to_table(
        columns=["pk", "source", "collection_name", "chunk_number", "raw_text"],
        nearest={
            "column": "vector",
            "q": _query_vector,
            "k": 11,
            "nprobes": 20,
            "refine_factor": 10,
        },
    )

    # Filter out the query row itself (distance=0) and keep top 10.
    _ann_result_pd = _ann_result.to_pydict()
    _dists = _ann_result_pd["_distance"]
    _pks = _ann_result_pd["pk"]
    _sources = _ann_result_pd["source"]
    _collections = _ann_result_pd["collection_name"]
    _chunk_nums = _ann_result_pd["chunk_number"]
    _texts = _ann_result_pd["raw_text"]

    _filtered_pks = []
    _filtered_sources = []
    _filtered_collections = []
    _filtered_chunk_nums = []
    _filtered_texts = []
    _filtered_dists = []

    for i in range(len(_pks)):
        if _pks[i] != _query_pk:
            _filtered_pks.append(_pks[i])
            _filtered_sources.append(_sources[i])
            _filtered_collections.append(_collections[i])
            _filtered_chunk_nums.append(_chunk_nums[i])
            _filtered_texts.append(_texts[i])
            _filtered_dists.append(_dists[i])
            if len(_filtered_pks) >= 10:
                break

    _elapsed = time.perf_counter() - _t0
    print(f"\nTop-10 ANN results ({_elapsed:.3f}s):")

    # Build result dataframe for display.
    _result_df = daft.from_pydict(
        {
            "pk": _filtered_pks,
            "source": _filtered_sources,
            "collection_name": _filtered_collections,
            "chunk_number": _filtered_chunk_nums,
            "raw_text": _filtered_texts,
            "_distance": _filtered_dists,
        }
    )
    _result_df.show()

    ann_result = _result_df
    return (ann_result,)


@app.cell
def _(ann_result):
    # Show a snippet of the most similar chunk's text.
    _top_text = (
        ann_result.collect().to_pydict()["raw_text"][0]
        if ann_result.count().collect().to_pydict()["count"][0] > 0
        else None
    )
    if _top_text:
        print(f"Most similar chunk text preview:\n{_top_text[:500]}")
    else:
        print("No ANN results found.")
    return


# ---------------------------------------------------------------------------
# Step 5 -- Full-text search
# ---------------------------------------------------------------------------
@app.cell
def _(mo):
    mo.md("""
    ## Step 5 -- Full-text search

    Search for *"number of players in monopoly"* using the BM25 inverted index.
    Daft passes the full-text query to Lance's scanner.
    """)
    return


@app.cell
def _(ACCESS_KEY, ENDPOINT, LANCE_URI, LANCE_STORAGE_OPTIONS, S3Config, SECRET_KEY, daft, io_config, lance, time):
    _t0 = time.perf_counter()

    # Full-text search via Lance (Daft doesn't yet have native FTS support).
    _ds = lance.dataset(
        LANCE_URI,
        storage_options={
            "region": "us-east-1",
            "endpoint": ENDPOINT,
            "access_key_id": ACCESS_KEY,
            "secret_access_key": SECRET_KEY,
            "allow_http": "true",
            "allow_invalid_certificates": "true",
        },
    )

    fts_result = _ds.to_table(
        columns=["pk", "source", "collection_name", "chunk_number", "raw_text"],
        full_text_query="number of players in monopoly",
    )

    _elapsed = time.perf_counter() - _t0
    _n = fts_result.num_rows
    print(f"Full-text search returned {_n} results ({_elapsed:.3f}s)")

    if _n > 0:
        _preview = fts_result.slice(0, min(10, _n))
        print(_preview.to_pandas()[["pk", "source", "collection_name", "_score"]].to_string(index=False))
        print(f"\nTop result text preview:")
        _text = fts_result.column("raw_text")[0].as_py() or "(empty)"
        print(_text[:500])
    else:
        print("No results -- the chunks may not contain text about Monopoly.")
    return (fts_result,)


# ---------------------------------------------------------------------------
# Step 6 -- Read Lance through Daft and run queries
# ---------------------------------------------------------------------------
@app.cell
def _(mo):
    mo.md("""
    ## Step 6 -- Read Lance dataset through Daft

    Use `daft.read_lance()` to load the Lance dataset as a Daft DataFrame,
    then run queries using the Daft API.
    """)
    return


@app.cell
def _(ACCESS_KEY, ENDPOINT, LANCE_URI, S3Config, SECRET_KEY, daft, io_config):
    _df_lance = daft.read_lance(LANCE_URI, io_config=io_config)
    print(f"Daft Lance DataFrame schema:")
    print(_df_lance.schema())

    # Show row count via Daft.
    _count = _df_lance.count().collect()
    print(f"\nRow count: {_count}")

    # Preview a few rows (narrow columns to avoid printing huge vectors).
    _df_lance.select("pk", "source", "collection_name", "chunk_number").limit(10).show()

    # Aggregation example: count chunks per collection.
    df_collection_counts = (
        _df_lance.groupby("collection_name")
        .agg(daft.col("pk").count().alias("chunk_count"))
        .sort("chunk_count", desc=True)
    )
    df_collection_counts.show()
    return


if __name__ == "__main__":
    app.run()
