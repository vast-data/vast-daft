"""Minimal Sail/Spark Connect example for the VastDB Python data source."""

from __future__ import annotations

import os

from pyspark.sql import SparkSession

from vast_sail import register_vastdb


def main() -> None:
    spark = SparkSession.builder.remote(os.environ["SPARK_REMOTE"]).getOrCreate()
    register_vastdb(spark)

    df = (
        spark.read.format("vastdb")
        .option("table", os.environ["VASTDB_TABLE"])
        .option("num_splits", os.environ.get("VASTDB_NUM_SPLITS", "4"))
        .load()
    )
    df.limit(10).show()


if __name__ == "__main__":
    main()
