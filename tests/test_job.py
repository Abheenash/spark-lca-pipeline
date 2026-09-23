"""End-to-end: the real job, real CSV in, real Parquet out.

Everything else tests a transformation in isolation. This runs `run()` exactly as
EMR Serverless would — read, split, aggregate, write partitioned Parquet — against
a temp directory, and asserts on what actually landed on disk.
"""
import csv

import pytest
from lca.job import prepare, read_raw, run
from lca.schema import LCA_SCHEMA

HEADER = [f.name for f in LCA_SCHEMA.fields]


def write_csv(path, records):
    with open(path, "w", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(HEADER)
        w.writerows(records)
    return str(path)


def rec(case, employer="ACME INC", wage="100000", unit="Year", status="Certified",
        visa="H-1B", state="TX", title="Engineer"):
    return [case, status, visa, employer, title, "Software Developers", "II",
            wage, unit, state, "Austin", "2025-01-01"]


@pytest.fixture
def dataset(tmp_path):
    return write_csv(tmp_path / "lca.csv", [
        rec("1", "AMAZON.COM SERVICES LLC", "180000"),
        rec("2", "Amazon.com Services, LLC.", "90.00", unit="Hour"),
        rec("3", "ACME INC", "150000"),
        rec("4", "ACME INC", "120000", state="CA"),
        rec("5", "DENIED CO", status="Denied"),
        rec("6", "E3 CO", visa="E-3"),
        rec("7", "BAD WAGE CO", wage="N/A"),          # quarantined
        rec("8", "BAD UNIT CO", unit="Fortnight"),    # quarantined
    ])


def test_end_to_end_writes_all_three_outputs(spark, dataset, tmp_path):
    out = str(tmp_path / "out")
    stats = run(spark, dataset, out)

    assert stats["rows_read"] == 8
    assert stats["rows_quarantined"] == 2
    # 8 read - 2 quarantined = 6 clean; of those, Denied and E-3 are filtered out.
    assert stats["rows_certified_h1b"] == 4
    assert stats["employers"] == 2          # Amazon (2 spellings merged) + ACME
    assert stats["quarantine_rate"] == pytest.approx(0.25)

    summary = {r.employer_key: r for r in spark.read.parquet(f"{out}/employer_summary").collect()}
    assert set(summary) == {"AMAZON COM SERVICES", "ACME"}
    # 180000 and 90/hr -> 187200, so mean is 183600
    assert summary["AMAZON COM SERVICES"].filings == 2
    assert summary["AMAZON COM SERVICES"].avg_annual_wage == pytest.approx(183600.0)


def test_output_is_partitioned_by_state(spark, dataset, tmp_path):
    """Partitioning is what makes 'who sponsors in TX?' read one directory
    instead of the whole dataset."""
    out = str(tmp_path / "out")
    run(spark, dataset, out)

    parts = {p.name for p in (tmp_path / "out" / "certified").iterdir() if p.is_dir()}
    assert parts == {"WORKSITE_STATE=TX", "WORKSITE_STATE=CA"}

    # Reading one partition must not require reading the others.
    tx = spark.read.parquet(f"{out}/certified/WORKSITE_STATE=TX")
    assert tx.count() == 3


def test_quarantined_rows_are_written_with_their_reason(spark, dataset, tmp_path):
    out = str(tmp_path / "out")
    run(spark, dataset, out)

    q = spark.read.parquet(f"{out}/quarantine")
    reasons = {r.reject_reason for r in q.collect()}
    assert reasons == {"unparseable_wage", "unknown_pay_unit"}
    # The original value is preserved so a human can see what DOL actually published.
    bad = {r.CASE_NUMBER: r.WAGE_RATE_OF_PAY_FROM for r in q.collect()}
    assert bad["7"] == "N/A"


def test_a_malformed_wage_does_not_kill_the_job(spark, tmp_path):
    """Spark 4 runs ANSI by default: a plain cast of 'N/A' to double RAISES and
    takes the whole job with it. This is the regression test for that — the row
    must be isolated, not fatal."""
    path = write_csv(tmp_path / "bad.csv", [rec("1", wage="N/A"), rec("2", wage="100000")])
    df = prepare(read_raw(spark, path))
    reasons = {r.CASE_NUMBER: r.reject_reason for r in df.collect()}
    assert reasons["1"] == "unparseable_wage"
    assert reasons["2"] is None


def test_quarantine_rate_is_a_rate_not_a_count(spark, tmp_path):
    """400 bad rows out of 400 is a broken feed; 400 out of 4,000,000 is Tuesday.
    Only the rate can carry a threshold."""
    path = write_csv(tmp_path / "all-bad.csv", [rec(str(i), wage="N/A") for i in range(4)])
    stats = run(spark, path, str(tmp_path / "out"))
    assert stats["quarantine_rate"] == 1.0
