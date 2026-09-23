"""The job: wire IO to the transformations in transform.py, and nothing else.

Everything here is either a read, a write, or a call into a pure function. That
split is why the test suite can cover the logic against a local SparkSession in
seconds without touching S3.
"""

from __future__ import annotations

import argparse
import json
import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from .schema import LCA_SCHEMA
from .transform import (
    annualise_wage,
    certified_h1b,
    employer_summary,
    normalise_employer,
    quarantine_reason,
)


def build_session(app_name: str = "lca-pipeline") -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        # Adaptive query execution: Spark re-plans mid-flight using real
        # statistics. It is what turns a skewed employer (one filer with 100x the
        # rows of the median) from a straggler task into a split one, and it is
        # the single highest-value setting on a job that groups by a natural key.
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        # Parquet written by an older Spark stores timestamps differently; this
        # makes the read explicit rather than surprising.
        .config("spark.sql.parquet.int96RebaseModeInRead", "CORRECTED")
        .getOrCreate()
    )


def read_raw(spark: SparkSession, path: str) -> DataFrame:
    """Read with an explicit schema — never inferSchema. See schema.py."""
    return (
        spark.read.option("header", "true")
        .option("mode", "PERMISSIVE")
        .schema(LCA_SCHEMA)
        .csv(path)
    )


def prepare(df: DataFrame) -> DataFrame:
    """Everything up to the split into clean and quarantined."""
    return quarantine_reason(annualise_wage(normalise_employer(df)))


def run(spark: SparkSession, input_path: str, output_path: str) -> dict:
    raw = read_raw(spark, input_path)
    prepared = prepare(raw)

    # Cached because both branches below scan it, and the parse work above is the
    # expensive part. Without this Spark recomputes the whole lineage twice.
    prepared.cache()

    rejects = prepared.filter(F.col("reject_reason").isNotNull())
    clean = certified_h1b(prepared.filter(F.col("reject_reason").isNull()))

    summary = employer_summary(clean)

    # Partitioned by state so the common query ("who sponsors in TX?") reads one
    # directory instead of the whole dataset. Partitioning by employer would
    # produce tens of thousands of tiny files — the small-files problem, which
    # costs more than the scan it saves.
    clean.write.mode("overwrite").partitionBy("WORKSITE_STATE").parquet(f"{output_path}/certified")
    summary.write.mode("overwrite").parquet(f"{output_path}/employer_summary")
    rejects.select("CASE_NUMBER", "EMPLOYER_NAME", "WAGE_RATE_OF_PAY_FROM",
                   "WAGE_UNIT_OF_PAY", "reject_reason") \
        .write.mode("overwrite").partitionBy("reject_reason").parquet(f"{output_path}/quarantine")

    stats = {
        "rows_read": prepared.count(),
        "rows_quarantined": rejects.count(),
        "rows_certified_h1b": clean.count(),
        "employers": summary.count(),
    }
    prepared.unpersist()

    # A reject RATE, not just a count: 400 bad rows out of 400 is a broken feed,
    # 400 out of 4,000,000 is Tuesday. The rate is what a threshold can be set on.
    stats["quarantine_rate"] = (
        round(stats["rows_quarantined"] / stats["rows_read"], 6) if stats["rows_read"] else 0.0
    )
    return stats


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="Annualise and aggregate DOL LCA disclosure data.")
    ap.add_argument("--input", required=True, help="Path or glob of raw LCA CSVs")
    ap.add_argument("--output", required=True, help="Output root (local path or s3://)")
    ap.add_argument("--max-quarantine-rate", type=float, default=0.05,
                    help="Fail the job above this reject rate — a silent 40%% reject is a broken feed.")
    args = ap.parse_args(argv)

    spark = build_session()
    try:
        stats = run(spark, args.input, args.output)
        print(json.dumps(stats))
        if stats["quarantine_rate"] > args.max_quarantine_rate:
            print(json.dumps({
                "error": "quarantine rate above threshold",
                "rate": stats["quarantine_rate"], "threshold": args.max_quarantine_rate,
            }))
            return 1
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
