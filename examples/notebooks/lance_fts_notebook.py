import marimo

__generated_with = "0.21.1"
app = marimo.App(width="medium")


@app.cell
def _(mo):
    mo.md(
        """
        # Lance + Daft Full-Text Search

        End-to-end demo of building an inverted index on a Lance dataset and
        running BM25 queries through it — driven by the Daft DataFrame API
        wherever Daft 0.7.x exposes the operation.

        Two flavours, controlled by the ``CORPUS`` constant in the next cell:

        * **small** — eight in-memory documents, useful as a smoke test.
        * **ag_news** — streams ~120K news articles from the public Hugging
          Face mirror.

        Two storage targets, controlled by the ``WHERE`` constant:

        * **local** — writes Lance under ``./data/lance/<name>``
        * **remote** — writes to ``s3://$VASTDB_BUCKET/lance/<name>`` using
          VAST S3 credentials.

        References:
        * <https://lance.org/quickstart/full-text-search/>
        * <https://docs.daft.ai/en/stable/connectors/lance/>
        """
    )
    return


@app.cell
def _():
    import marimo as mo  # type: ignore

    return (mo,)


@app.cell
def _():
    import os
    import sys
    import time
    from pathlib import Path

    import daft
    import lance
    from daft import col

    # Allow ``from lance_fts_example import ...`` whether the notebook runs
    # from the repo (examples/notebooks/) or the marimo pod (/home/ray/examples/notebooks/).
    _examples_dir = Path(__file__).resolve().parent.parent
    if str(_examples_dir) not in sys.path:
        sys.path.insert(0, str(_examples_dir))

    from helpers import configure_daft_runner  # type: ignore
    from lance_fts_example import (  # type: ignore
        AG_NEWS_QUERIES,
        SMALL_QUERIES,
        load_ag_news,
        load_small,
        resolve_target,
        step_build_index,
        step_explore_with_daft,
        step_query,
        step_write,
    )

    return (
        AG_NEWS_QUERIES,
        SMALL_QUERIES,
        col,
        configure_daft_runner,
        daft,
        lance,
        load_ag_news,
        load_small,
        os,
        resolve_target,
        step_build_index,
        step_explore_with_daft,
        step_query,
        step_write,
        time,
    )


@app.cell
def _(configure_daft_runner):
    runner_status = configure_daft_runner()
    print(runner_status)
    return


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
@app.cell
def _(mo):
    mo.md(
        """
        ## Configuration

        Flip ``CORPUS`` to ``"ag_news"`` to ingest the public AG News corpus,
        and ``WHERE`` to ``"remote"`` to land the Lance dataset on
        ``s3://$VASTDB_BUCKET/lance/...`` instead of the local ``./data/`` dir.
        """
    )
    return


@app.cell
def _(os, resolve_target):
    CORPUS = "small"  # "small" | "ag_news"
    WHERE = "local"  # "local" | "remote"
    REBUILD = False  # set True to overwrite an existing dataset

    NAME = f"fts_{CORPUS}"
    URI, LANCE_STORAGE_OPTIONS, IO_CONFIG = resolve_target(WHERE, NAME)

    print(f"corpus = {CORPUS}")
    print(f"where  = {WHERE}")
    print(f"uri    = {URI}")
    if WHERE == "remote":
        print(f"bucket = {os.environ.get('VASTDB_BUCKET')}")
    return CORPUS, IO_CONFIG, LANCE_STORAGE_OPTIONS, NAME, REBUILD, URI


# ---------------------------------------------------------------------------
# Step 1 — load the corpus through Daft
# ---------------------------------------------------------------------------
@app.cell
def _(mo):
    mo.md(
        """
        ## Step 1 — load the corpus into a Daft DataFrame

        For ``small`` we build the DataFrame from a Python list.  For
        ``ag_news`` we point ``daft.read_parquet`` at the Hugging Face URLs
        and join a tiny labels table to map the integer ``label`` to a
        human-readable ``category`` — both pure Daft.
        """
    )
    return


@app.cell
def _(AG_NEWS_QUERIES, CORPUS, SMALL_QUERIES, load_ag_news, load_small):
    if CORPUS == "small":
        df_corpus = load_small()
        text_column = "text"
        return_columns = ["id", "title", "category", "text"]
        queries = SMALL_QUERIES
    else:
        df_corpus = load_ag_news()
        text_column = "text"
        return_columns = ["id", "category", "text"]
        queries = AG_NEWS_QUERIES

    print("schema:")
    print(df_corpus.schema())
    df_corpus.limit(3).show()
    return df_corpus, queries, return_columns, text_column


# ---------------------------------------------------------------------------
# Step 2 — write to Lance via Daft
# ---------------------------------------------------------------------------
@app.cell
def _(mo):
    mo.md(
        """
        ## Step 2 — write the corpus to Lance via ``df.write_lance``

        The write goes through Daft's native ``write_lance`` API.  When the
        target dataset already exists we skip the write unless ``REBUILD`` is
        set, so re-running the notebook is cheap.
        """
    )
    return


@app.cell
def _(
    IO_CONFIG,
    LANCE_STORAGE_OPTIONS,
    REBUILD,
    URI,
    df_corpus,
    step_write,
):
    num_rows = step_write(
        df_corpus,
        uri=URI,
        io_config=IO_CONFIG,
        storage_options=LANCE_STORAGE_OPTIONS,
        rebuild=REBUILD,
    )
    print(f"dataset rows: {num_rows:,}")
    return (num_rows,)


# ---------------------------------------------------------------------------
# Step 3 — build an INVERTED index
# ---------------------------------------------------------------------------
@app.cell
def _(mo):
    mo.md(
        """
        ## Step 3 — build an inverted (BM25) index

        Daft 0.7.x doesn't have a native API for index creation, so this step
        opens the dataset with the Lance SDK and calls
        ``create_scalar_index(..., index_type="INVERTED", with_position=True)``.
        ``with_position=True`` enables phrase queries.
        """
    )
    return


@app.cell
def _(LANCE_STORAGE_OPTIONS, URI, step_build_index, text_column):
    step_build_index(uri=URI, text_column=text_column, storage_options=LANCE_STORAGE_OPTIONS)
    return


# ---------------------------------------------------------------------------
# Step 4 — run BM25 queries
# ---------------------------------------------------------------------------
@app.cell
def _(mo):
    mo.md(
        """
        ## Step 4 — run BM25 queries

        Each query goes through the Lance scanner (so we get the ``_score``
        column) and the Arrow result is wrapped back into a Daft DataFrame
        for display — everything downstream stays in Daft.
        """
    )
    return


@app.cell
def _(LANCE_STORAGE_OPTIONS, URI, queries, return_columns, step_query):
    step_query(
        uri=URI,
        queries=queries,
        return_columns=return_columns,
        storage_options=LANCE_STORAGE_OPTIONS,
        k=5,
    )
    return


# ---------------------------------------------------------------------------
# Step 5 — explore with Daft
# ---------------------------------------------------------------------------
@app.cell
def _(mo):
    mo.md(
        """
        ## Step 5 — explore the indexed dataset with Daft

        With the dataset on disk, the rest is plain Daft: ``read_lance`` →
        ``groupby`` → ``agg``.  The Lance index is invisible here — it only
        kicks in when an FTS query is issued.
        """
    )
    return


@app.cell
def _(IO_CONFIG, URI, step_explore_with_daft):
    step_explore_with_daft(uri=URI, io_config=IO_CONFIG)
    return


# ---------------------------------------------------------------------------
# Step 6 — combine FTS hits with a Daft groupby
# ---------------------------------------------------------------------------
@app.cell
def _(mo):
    mo.md(
        """
        ## Step 6 — combine FTS hits with a Daft groupby

        Run a query through Lance, wrap the result as a Daft DataFrame, then
        bucket the BM25 hits by ``category`` to see which topics dominate
        the result set.
        """
    )
    return


@app.cell
def _(LANCE_STORAGE_OPTIONS, URI, col, daft, lance, queries):
    _q = queries[0]
    _ds = lance.dataset(URI, storage_options=LANCE_STORAGE_OPTIONS)
    _hits = _ds.to_table(
        columns=["id", "category", "_score"],
        full_text_query=_q,
        limit=200,
        disable_scoring_autoprojection=True,
    )

    fts_summary = (
        daft.from_arrow(_hits)
        .groupby("category")
        .agg(
            col("id").count().alias("hits"),
            col("_score").mean().alias("avg_score"),
            col("_score").max().alias("max_score"),
        )
        .sort("max_score", desc=True)
    )
    print(f"category breakdown for query={_q!r}:")
    fts_summary.show()
    return (fts_summary,)


if __name__ == "__main__":
    app.run()
