"""The transformations, as pure DataFrame functions.

Every function here takes a DataFrame and returns one, so each is testable
against a two-row fixture without a cluster, a file, or a mock. That is the whole
reason the job in `job.py` is thin: it wires IO to these, and nothing else.

Two rules the code follows deliberately:

* **No Python UDFs.** A UDF serialises every row out of the JVM into a Python
  process and back, which drops Catalyst's optimisations on the floor and is
  routinely 10-100x slower than the equivalent built-in. Everything here is
  expressed with `pyspark.sql.functions`.
* **Bad rows are quarantined, not dropped.** A filter that silently discards
  malformed input is how a pipeline reports 90% of the truth with total
  confidence. Rejects go to their own path with the reason attached.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from .schema import PAY_UNIT_MULTIPLIER

# A wage below this is almost certainly a data-entry error (a monthly figure
# filed as annual); above it, a typo with extra zeros. Real LCA wages sit well
# inside the band, and rows outside it are quarantined rather than deleted so
# the number is auditable.
MIN_PLAUSIBLE_ANNUAL = 15_000.0
MAX_PLAUSIBLE_ANNUAL = 2_000_000.0


def normalise_employer(df: DataFrame) -> DataFrame:
    """Collapse the many spellings of one employer into a join key.

    DOL files contain "AMAZON.COM SERVICES LLC", "Amazon.com Services, LLC." and
    "AMAZON COM SERVICES LLC" as separate strings. Grouping on the raw name
    reports one employer as three, which is the most common way these datasets
    get quoted wrongly.
    """
    key = F.upper(F.trim(F.col("EMPLOYER_NAME")))
    # Punctuation becomes a SPACE, not nothing. Deleting it turns "AMAZON.COM"
    # into "AMAZONCOM", which then never matches the "AMAZON COM" spelling of the
    # same employer — the exact merge this function exists to perform.
    key = F.regexp_replace(key, r"[.,]", " ")
    # Collapse whitespace BEFORE stripping the suffix, or a doubled space leaves
    # the suffix pattern unmatched.
    key = F.trim(F.regexp_replace(key, r"\s+", " "))
    key = F.regexp_replace(key, r"\s+(INC|LLC|LTD|CORP|CORPORATION|CO|LP|LLP|PLC)$", "")
    return df.withColumn("employer_key", F.trim(key))


def annualise_wage(df: DataFrame) -> DataFrame:
    """Derive an annual wage from the published value and its unit.

    `create_map` builds the lookup as a Spark expression rather than a UDF, so
    Catalyst can fold it. The raw columns stay, so the derivation is auditable.
    """
    multiplier_map = F.create_map(
        *[x for unit, m in PAY_UNIT_MULTIPLIER.items() for x in (F.lit(unit), F.lit(m))]
    )
    cleaned = F.regexp_replace(F.col("WAGE_RATE_OF_PAY_FROM"), r"[$,]", "")
    return (
        # try_cast, NOT cast. Spark 4 enables ANSI mode by default, so a plain
        # cast of "N/A" RAISES CAST_INVALID_INPUT and kills the job — it does not
        # produce null. The entire quarantine design below depends on bad values
        # becoming null, so on Spark 4 a plain cast means the pipeline crashes on
        # the first malformed row instead of isolating it. Found by running the
        # tests, not by reading the code.
        df.withColumn("wage_raw", cleaned.try_cast("double"))
        .withColumn("pay_multiplier", multiplier_map[F.trim(F.col("WAGE_UNIT_OF_PAY"))])
        .withColumn("annual_wage", F.col("wage_raw") * F.col("pay_multiplier"))
    )


def quarantine_reason(df: DataFrame) -> DataFrame:
    """Tag each row with why it is unusable, or null if it is fine.

    A single column computed once, rather than four filters each scanning the
    data. The first matching reason wins, so the label is deterministic.
    """
    return df.withColumn(
        "reject_reason",
        F.when(F.col("CASE_NUMBER").isNull() | (F.trim(F.col("CASE_NUMBER")) == ""), "missing_case_number")
        .when(F.col("wage_raw").isNull(), "unparseable_wage")
        .when(F.col("pay_multiplier").isNull(), "unknown_pay_unit")
        .when(
            (F.col("annual_wage") < MIN_PLAUSIBLE_ANNUAL) | (F.col("annual_wage") > MAX_PLAUSIBLE_ANNUAL),
            "implausible_wage",
        )
        .otherwise(F.lit(None).cast("string")),
    )


def certified_h1b(df: DataFrame) -> DataFrame:
    """Certified H-1B only.

    `startswith("Certified")` is deliberate: DOL uses both "Certified" and
    "Certified - Withdrawn", and both represent an employer that was willing to
    file. Matching equality drops roughly a tenth of the real signal.
    """
    return df.filter(
        F.col("CASE_STATUS").startswith("Certified") & (F.trim(F.col("VISA_CLASS")) == "H-1B")
    )


def employer_summary(df: DataFrame) -> DataFrame:
    """Per-employer aggregate with a wage percentile and a top job title.

    The top title uses a window function rather than a self-join: one shuffle
    instead of two, and no risk of the join fanning out on ties.
    """
    per_title = (
        df.groupBy("employer_key", "JOB_TITLE")
        .agg(F.count(F.lit(1)).alias("title_filings"))
    )
    ranked = per_title.withColumn(
        "rk",
        F.row_number().over(
            Window.partitionBy("employer_key").orderBy(F.desc("title_filings"), F.asc("JOB_TITLE"))
        ),
    )
    top_title = ranked.filter(F.col("rk") == 1).select(
        "employer_key", F.col("JOB_TITLE").alias("top_job_title")
    )

    agg = df.groupBy("employer_key").agg(
        F.count(F.lit(1)).alias("filings"),
        F.round(F.avg("annual_wage"), 2).alias("avg_annual_wage"),
        # percentile_approx: an exact percentile needs a full sort per group,
        # which is the expensive part of this job at real volume.
        F.round(F.percentile_approx("annual_wage", 0.5, 1000), 2).alias("median_annual_wage"),
        F.countDistinct("WORKSITE_STATE").alias("states"),
        F.collect_set("PW_WAGE_LEVEL").alias("wage_levels"),
        F.max("EMPLOYER_NAME").alias("employer_display_name"),
    )

    # Broadcast: top_title has one row per employer — tens of thousands at most,
    # a few MB. Broadcasting avoids shuffling the large side across the cluster.
    return agg.join(F.broadcast(top_title), on="employer_key", how="left")
