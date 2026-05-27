import marimo

__generated_with = "0.21.1"
app = marimo.App(width="medium")


@app.cell
def _(mo):
    mo.md("""
    # Chunks & Ingestion Analysis

    This notebook operates on the **chunks** and **ingestion_status** tables
    in the VastDB `collections-bucket/collections-schema` catalog.

    1. **Join & Aggregate** — join chunks with ingestion status, group by
       status, and count the number of chunks per ingestion status.
    2. **Cosine Similarity Search (DataFrame API)** — pick a reference chunk
       vector and find the most similar chunks using `cosine_similarity`
       with an explicit cast to `FixedSizeList[Float32]`.
    3. **Cosine Similarity Search (SQL)** — the same search in pure SQL,
       where no cast is needed because both sides are typed columns.
    """)
    return


@app.cell
def _():
    import marimo as mo  # type: ignore

    return (mo,)


@app.cell
def _():
    import os
    import time

    import daft
    import pyarrow as pa
    from daft import DataType
    from helpers import configure_daft_runner, get_s3_credentials  # type: ignore

    from vast_daft import VastDBCatalog, VastDBConfig

    return (
        DataType,
        VastDBCatalog,
        VastDBConfig,
        configure_daft_runner,
        daft,
        get_s3_credentials,
        os,
        pa,
        time,
    )


@app.cell
def _(configure_daft_runner):
    runner_status = configure_daft_runner()
    print(runner_status)
    return


@app.cell
def _(VastDBCatalog, VastDBConfig, daft, get_s3_credentials, os):
    ENDPOINT = os.environ.get("VASTDB_ENDPOINT", "")
    BUCKET = "collections-bucket"
    SCHEMA = "collections-schema"
    ACCESS_KEY, SECRET_KEY = get_s3_credentials()

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
    print(f"Catalog tables: {catalog.list_tables()}")
    return catalog, sess


@app.cell
def _(mo):
    mo.md("""
    ## Step 1 — Read chunks and ingestion_status tables
    """)
    return


@app.cell
def _(catalog, time):
    _t0 = time.perf_counter()

    df_chunks = catalog.get_table("chunks").read()
    df_ingestion = catalog.get_table("ingestion_status").read()
    df_chunks_join = catalog.get_table("chunks").read(
        columns=["collection_name"],
    )
    df_ingestion_join = catalog.get_table("ingestion_status").read(
        columns=["collection_name", "status", "handler_type"],
    )
    df_chunks_probe = catalog.get_table("chunks").read(
        columns=["pk", "source"],
    )
    df_chunks_query_vector = catalog.get_table("chunks").read(
        columns=["pk", "vector"],
    )
    df_chunks_similarity = catalog.get_table("chunks").read(
        columns=["pk", "source", "collection_name", "chunk_number", "raw_text", "vector"],
    )

    _chunks_count = df_chunks.count().collect()
    _ingestion_count = df_ingestion.count().collect()
    _elapsed = time.perf_counter() - _t0
    _chunks_count, _ingestion_count
    # print(f"Loaded chunks: {_chunks_count:,} rows")
    # print(f"Loaded ingestion_status: {_ingestion_count:,} rows")
    # print(f"Elapsed: {_elapsed:.2f}s")
    return (
        df_chunks,
        df_chunks_join,
        df_chunks_probe,
        df_chunks_query_vector,
        df_chunks_similarity,
        df_ingestion,
        df_ingestion_join,
    )


@app.cell
def _(mo):
    mo.md("""
    ## Step 2 — Join chunks with ingestion_status, group by status

    Join on `collection_name` to associate each chunk with its ingestion
    status, then count how many chunks belong to each status category.
    """)
    return


@app.cell
def _(daft, df_chunks_join, df_ingestion_join):
    # Pre-aggregate chunk rows to a collection-level dimension table before
    # joining. This avoids materializing one row per chunk through the join.
    df_chunk_counts = df_chunks_join.groupby("collection_name").agg(
        daft.col("collection_name").count().alias("chunk_count"),
    )

    df_joined = df_chunk_counts.join(
        df_ingestion_join.select(
            daft.col("collection_name").alias("ing_collection_name"),
            "status",
            "handler_type",
        ),
        left_on="collection_name",
        right_on="ing_collection_name",
        how="inner",
    )

    df_joined.limit(10).show()
    return (df_joined,)


@app.cell
def _(daft, df_joined):
    df_status_counts = (
        df_joined.groupby("status")
        .agg(
            daft.col("chunk_count").sum().alias("chunk_count"),
        )
        .sort("chunk_count", desc=True)
    )

    df_status_counts.show()
    return


@app.cell
def _(daft, df_joined):
    df_status_handler = (
        df_joined.groupby("status", "handler_type")
        .agg(
            daft.col("chunk_count").sum().alias("chunk_count"),
        )
        .sort(["status", "handler_type"])
    )

    df_status_handler.show()
    return


@app.cell
def _(mo):
    mo.md("""
    ## Step 3 — Cosine Similarity Search (DataFrame API)

    Pick the first chunk's vector as the query vector, then compute
    cosine similarity against all other chunks using the DataFrame API.

    Note: `daft.lit()` produces a Python-typed literal, so we must cast
    it to `FixedSizeList[Float32, 2048]` for `cosine_similarity` to work.
    """)
    return


@app.cell
def _(daft, df_chunks_probe, df_chunks_query_vector):
    # Resolve the first row from a narrow probe, then fetch its vector by PK.
    _first_row = df_chunks_probe.limit(1).collect().to_pydict()
    query_pk = _first_row["pk"][0]
    query_source = _first_row["source"][0]
    _vector_row = (
        df_chunks_query_vector
        .filter(daft.col("pk") == daft.lit(query_pk))
        .limit(1)
        .collect()
        .to_pydict()
    )
    query_vector = _vector_row["vector"][0]

    print(f"Query chunk PK: {query_pk}")
    print(f"Query source: {query_source}")
    print(f"Vector dimension: {len(query_vector)}")
    return query_pk, query_vector


@app.cell
def _(DataType, daft, df_chunks_similarity, query_pk, query_vector):
    # Build a fixed-size list literal for the query vector (dimension=2048)
    _query_lit = daft.lit(query_vector).cast(DataType.fixed_size_list(DataType.float32(), 2048))

    # Compute cosine similarity for every chunk
    df_similarity = (
        df_chunks_similarity
        .with_column(
            "similarity",
            daft.col("vector").cosine_similarity(_query_lit),
        )
        .filter(daft.col("pk") != daft.lit(query_pk))
        .sort("similarity", desc=True)
        .limit(10)
    )

    df_similarity.select("pk", "source", "collection_name", "chunk_number", "similarity").show()
    return (df_similarity,)


@app.cell
def _(mo):
    mo.md("""
    ## Step 4 — Cosine Similarity Search (SQL)

    The same search expressed in SQL. We register `query_vector` and
    `query_pk` as a temp table (`query_ref`) so the SQL query can
    reference the Python-side vector without any cast — both sides of
    `cosine_similarity()` are typed columns.

    Daft SQL doesn't support `CROSS JOIN`, so we use a dummy `join_key`
    to broadcast the query vector to every row via an equality join.
    """)
    return


@app.cell
def _(daft, pa, query_pk, query_vector, sess):
    # Register the query vector as a temp table with the correct Arrow type
    _fsl_type = pa.list_(pa.float32(), 2048)
    _query_df = daft.from_pydict(
        {
            "query_vec": pa.array([query_vector], type=_fsl_type),
            "query_pk": [query_pk],
            "join_key": [1],
        }
    )
    sess.create_temp_table("query_ref", _query_df)

    df_similarity_sql = sess.sql("""
        SELECT c.pk, c.source, c.collection_name, c.chunk_number,
               cosine_similarity(c.vector, q.query_vec) AS similarity
        FROM (SELECT *, 1 AS join_key FROM chunks) c
        JOIN query_ref q ON c.join_key = q.join_key
        WHERE c.pk != q.query_pk
        ORDER BY similarity DESC
        LIMIT 10
    """)

    df_similarity_sql.show()
    return (df_similarity_sql,)


@app.cell
def _(mo):
    mo.md("""
    ## Step 5 — Inspect top similar chunk content
    """)
    return


@app.cell
def _(df_similarity):
    # Show the raw_text of the most similar chunk
    _top = df_similarity.limit(1).collect()
    _top_dict = _top.to_pydict()

    if _top_dict["raw_text"]:
        _text = _top_dict["raw_text"][0]
        _sim = _top_dict["similarity"][0]
        _src = _top_dict["source"][0]
        print(f"Most similar chunk (similarity={_sim:.4f})")
        print(f"Source: {_src}")
        print(f"Text preview (first 500 chars):\n{_text[:500] if _text else '(empty)'}")
    else:
        print("No similar chunks found.")
    return


if __name__ == "__main__":
    app.run()
