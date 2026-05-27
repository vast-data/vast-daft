#!/usr/bin/env python3
"""Lance + Daft full-text search demo.

Two modes:

  --corpus=small     (default) — a handful of in-memory documents
  --corpus=ag_news   — downloads the AG News corpus (~120K news articles,
                       ~30MB) from the public Hugging Face mirror.

Two storage targets:

  --where=local      (default) — writes Lance under ./data/lance/<name>
  --where=remote     — writes to s3://$VASTDB_BUCKET/lance/<name> using
                       VAST S3 credentials from the environment.

The whole pipeline (read → write_lance → build inverted index → FTS query)
goes through Daft wherever the API exposes it.  Index creation and the FTS
query path that returns BM25 scores still go through Lance directly because
Daft 0.7.x does not yet have a native API for those.

References
----------
* https://lance.org/quickstart/full-text-search/
* https://docs.daft.ai/en/stable/connectors/lance/
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from pathlib import Path
from typing import Any

import daft
import lance
from daft import col
from daft.io import IOConfig, S3Config

REPO_ROOT = Path(__file__).resolve().parent.parent
LOCAL_DATA_DIR = REPO_ROOT / "data" / "lance"


# ---------------------------------------------------------------------------
# Storage helpers
# ---------------------------------------------------------------------------
def resolve_target(where: str, name: str) -> tuple[str, dict[str, str] | None, IOConfig | None]:
    """Return (uri, lance_storage_options, daft_io_config) for the chosen target."""
    if where == "local":
        LOCAL_DATA_DIR.mkdir(parents=True, exist_ok=True)
        uri = str(LOCAL_DATA_DIR / name)
        return uri, None, None

    bucket = os.environ.get("VASTDB_BUCKET")
    if not bucket:
        sys.exit("ERROR: --where=remote requires VASTDB_BUCKET in the environment.")

    endpoint = os.environ.get("VASTDB_ENDPOINT", "")
    if not endpoint:
        sys.exit("ERROR: --where=remote requires VASTDB_ENDPOINT in the environment.")
    access_key = os.environ.get("S3_ACCESS_KEY") or os.environ.get("VASTDB_ACCESS_KEY")
    secret_key = os.environ.get("S3_SECRET_KEY") or os.environ.get("VASTDB_SECRET_KEY")
    if not access_key or not secret_key:
        sys.exit("ERROR: --where=remote requires S3_ACCESS_KEY and S3_SECRET_KEY.")

    uri = f"s3://{bucket}/lance/{name}"
    storage_options = {
        "region": "us-east-1",
        "endpoint": endpoint,
        "access_key_id": access_key,
        "secret_access_key": secret_key,
        "allow_http": "true",
        "allow_invalid_certificates": "true",
    }
    io_config = IOConfig(
        s3=S3Config(
            endpoint_url=endpoint,
            key_id=access_key,
            access_key=secret_key,
            region_name="us-east-1",
            use_ssl=not endpoint.startswith("http://"),
            verify_ssl=False,
        ),
    )
    return uri, storage_options, io_config


# ---------------------------------------------------------------------------
# Corpus loaders
# ---------------------------------------------------------------------------
SMALL_DOCS: list[dict[str, object]] = [
    {
        "id": 1,
        "title": "Lance: a columnar format for AI",
        "text": (
            "Lance is a modern columnar data format optimized for ML workloads. "
            "It supports fast random access, vector indexes, and inverted indexes "
            "for full-text search."
        ),
        "category": "infra",
    },
    {
        "id": 2,
        "title": "Daft: distributed dataframes in Python",
        "text": (
            "Daft is a distributed query engine that runs on Ray and integrates "
            "natively with Lance, Iceberg, Delta, and other modern table formats."
        ),
        "category": "infra",
    },
    {
        "id": 3,
        "title": "Monopoly rules for two players",
        "text": (
            "The classic Monopoly board game supports two to eight players. "
            "With only two players the game is much shorter; trading is "
            "essentially impossible because there is no third party to bid up prices."
        ),
        "category": "games",
    },
    {
        "id": 4,
        "title": "Inverted index basics",
        "text": (
            "An inverted index maps each token to the list of documents it "
            "appears in, enabling sub-linear keyword search. BM25 ranks results "
            "by term frequency and inverse document frequency."
        ),
        "category": "search",
    },
    {
        "id": 5,
        "title": "Why columnar storage wins for analytics",
        "text": (
            "Columnar formats like Parquet, Arrow, and Lance store values from "
            "the same column together. This boosts compression ratios and lets "
            "vectorised query engines skip irrelevant columns entirely."
        ),
        "category": "infra",
    },
    {
        "id": 6,
        "title": "BM25 scoring explained",
        "text": (
            "BM25 is a probabilistic ranking function. It generalises TF-IDF by "
            "saturating term frequency and normalising for document length, "
            "which makes it robust on mixed-length corpora."
        ),
        "category": "search",
    },
    {
        "id": 7,
        "title": "How to play Settlers of Catan",
        "text": (
            "Catan is a board game for three to four players (six with the "
            "expansion). Players collect wood, brick, sheep, wheat, and ore to "
            "build roads, settlements, and cities."
        ),
        "category": "games",
    },
    {
        "id": 8,
        "title": "Approximate nearest neighbour search",
        "text": (
            "ANN indexes such as IVF_PQ trade exact recall for orders of "
            "magnitude faster vector search. They are the standard backbone of "
            "production semantic search and recommender systems."
        ),
        "category": "search",
    },
]


def load_small() -> daft.DataFrame:
    """Build a tiny in-memory DataFrame entirely through the Daft API."""
    return daft.from_pylist(SMALL_DOCS)


AG_NEWS_PARQUET_URLS = [
    "https://huggingface.co/datasets/fancyzhx/ag_news/resolve/refs%2Fconvert%2Fparquet/default/train/0000.parquet",
    "https://huggingface.co/datasets/fancyzhx/ag_news/resolve/refs%2Fconvert%2Fparquet/default/test/0000.parquet",
]
AG_NEWS_LABELS = ["World", "Sports", "Business", "Sci/Tech"]


def load_ag_news() -> daft.DataFrame:
    """Stream AG News parquet files from Hugging Face directly into Daft.

    The AG News dataset has two columns: ``text`` (the news body) and ``label``
    (an int 0..3).  We add a stable ``id`` column and join a tiny labels
    DataFrame to map ``label`` → string ``category``, all through the Daft API.
    """
    labels = daft.from_pylist(
        [{"label": i, "category": name} for i, name in enumerate(AG_NEWS_LABELS)]
    )
    docs = daft.read_parquet(AG_NEWS_PARQUET_URLS).with_column(
        "id", daft.functions.monotonically_increasing_id()
    )
    return docs.join(labels, on="label", how="inner").select("id", "text", "category", "label")


# ---------------------------------------------------------------------------
# Pipeline steps
# ---------------------------------------------------------------------------
def _dataset_exists(uri: str, storage_options: dict[str, str] | None) -> bool:
    try:
        lance.dataset(uri, storage_options=storage_options)
        return True
    except (ValueError, OSError, FileNotFoundError):
        return False


def step_write(
    df: daft.DataFrame,
    uri: str,
    io_config: IOConfig | None,
    storage_options: dict[str, str] | None,
    rebuild: bool,
) -> int:
    """Write the DataFrame as a Lance dataset using ``df.write_lance``.

    Skips the write when the dataset already exists, unless ``rebuild`` is set.
    """
    print(f"\n[write] target = {uri}")
    if not rebuild and _dataset_exists(uri, storage_options):
        rt = daft.read_lance(uri, io_config=io_config) if io_config else daft.read_lance(uri)
        n = int(rt.count().collect().to_pydict()["count"][0])
        print(f"[write] dataset already exists ({n:,} rows) — skipping (pass --rebuild to overwrite)")
        return n

    t0 = time.perf_counter()
    if io_config is not None:
        df.write_lance(uri, mode="overwrite", io_config=io_config)
    else:
        df.write_lance(uri, mode="overwrite")
    dt = time.perf_counter() - t0

    rt = daft.read_lance(uri, io_config=io_config) if io_config else daft.read_lance(uri)
    n = int(rt.count().collect().to_pydict()["count"][0])
    print(f"[write] wrote {n:,} rows in {dt:.2f}s")
    return n


def _index_fields(idx: Any) -> list[str]:
    """Lance returns Index TypedDicts; both "fields" and "columns" appear in the wild."""
    return list(idx.get("fields") or idx.get("columns") or [])


def _index_name(idx: Any) -> str:
    return idx.get("name") or idx.get("uuid") or "?"


def step_build_index(
    uri: str,
    text_column: str,
    storage_options: dict[str, str] | None,
) -> None:
    """Create an INVERTED full-text index on ``text_column``."""
    print(f"\n[index] opening Lance dataset at {uri}")
    ds = lance.dataset(uri, storage_options=storage_options)

    has_fts = any(text_column in _index_fields(idx) for idx in ds.list_indices())
    if has_fts:
        print(f"[index] inverted index on '{text_column}' already exists — skipping")
    else:
        print(f"[index] building INVERTED index on '{text_column}' ...")
        t0 = time.perf_counter()
        ds.create_scalar_index(
            column=text_column,
            index_type="INVERTED",
            with_position=True,
            replace=True,
        )
        print(f"[index] built in {time.perf_counter() - t0:.2f}s")

    ds = lance.dataset(uri, storage_options=storage_options)
    summary = [(_index_name(i), _index_fields(i)) for i in ds.list_indices()]
    print(f"[index] indices now: {summary}")


def step_query(
    uri: str,
    queries: list[str],
    return_columns: list[str],
    storage_options: dict[str, str] | None,
    k: int = 5,
) -> None:
    """Run BM25 full-text queries against the inverted index.

    Daft 0.7.x does not expose a native FTS expression for Lance, so we run
    the FTS leg through Lance's scanner directly.  Each result Arrow table is
    immediately wrapped back into a Daft DataFrame via ``daft.from_arrow``,
    so any downstream filtering / aggregation can keep using the Daft API.
    """
    ds = lance.dataset(uri, storage_options=storage_options)
    # Adopt Lance's future behaviour: explicitly request _score, and disable
    # the auto-projection that emits a deprecation warning when it isn't asked for.
    columns_with_score = [*return_columns, "_score"]
    for q in queries:
        print(f"\n[fts] query={q!r}")
        t0 = time.perf_counter()
        tbl = ds.to_table(
            columns=columns_with_score,
            full_text_query=q,
            limit=k,
            disable_scoring_autoprojection=True,
        )
        dt = time.perf_counter() - t0
        print(f"[fts] {tbl.num_rows} hits in {dt:.3f}s")
        daft.from_arrow(tbl).show()


def step_explore_with_daft(uri: str, io_config: IOConfig | None) -> None:
    """A few Daft-only operations on the indexed dataset."""
    print("\n[explore] Daft DataFrame on the Lance dataset")
    df = daft.read_lance(uri, io_config=io_config) if io_config else daft.read_lance(uri)
    print("schema:")
    print(df.schema())
    print("\nrows per category:")
    df.groupby("category").agg(col("id").count().alias("n")).sort("n", desc=True).show()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
SMALL_QUERIES = [
    "monopoly players",
    "BM25 ranking",
    "vector index",
]
AG_NEWS_QUERIES = [
    "stock market crash",
    "olympic gold medal",
    "linux kernel security",
    "presidential election",
]


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--corpus", choices=["small", "ag_news"], default="small")
    p.add_argument("--where", choices=["local", "remote"], default="local")
    p.add_argument(
        "--name",
        default=None,
        help="Lance dataset name (default: derived from --corpus).",
    )
    p.add_argument(
        "--rebuild",
        action="store_true",
        help="Overwrite the Lance dataset even if it already exists.",
    )
    args = p.parse_args()

    name = args.name or f"fts_{args.corpus}"
    uri, storage_options, io_config = resolve_target(args.where, name)

    if args.corpus == "small":
        df = load_small()
        text_column = "text"
        return_columns = ["id", "title", "category", "text"]
        queries = SMALL_QUERIES
    else:
        print("[load] streaming AG News parquet from Hugging Face ...")
        df = load_ag_news()
        text_column = "text"
        return_columns = ["id", "category", "text"]
        queries = AG_NEWS_QUERIES

    step_write(
        df,
        uri=uri,
        io_config=io_config,
        storage_options=storage_options,
        rebuild=args.rebuild,
    )
    step_build_index(uri=uri, text_column=text_column, storage_options=storage_options)
    step_query(
        uri=uri,
        queries=queries,
        return_columns=return_columns,
        storage_options=storage_options,
    )
    step_explore_with_daft(uri=uri, io_config=io_config)
    print("\nDone.")


if __name__ == "__main__":
    main()
