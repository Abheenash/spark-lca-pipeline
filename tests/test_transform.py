"""Transformations, against a real local SparkSession.

These are not mocked. Every assertion here runs through Catalyst and a real
shuffle, which is the only way to catch the class of bug that only appears once
data is split across partitions.
"""
from lca.schema import LCA_SCHEMA
from lca.transform import (
    annualise_wage,
    certified_h1b,
    employer_summary,
    normalise_employer,
    quarantine_reason,
)


def rows(spark, data):
    return spark.createDataFrame(data, schema=LCA_SCHEMA)


def rec(case="C-1", status="Certified", visa="H-1B", employer="ACME INC", title="Engineer",
        soc="Software Developers", level="II", wage="100000", unit="Year", state="TX",
        city="Austin", begin="2025-01-01"):
    return (case, status, visa, employer, title, soc, level, wage, unit, state, city, begin)


# --- employer normalisation ------------------------------------------------

def test_spelling_variants_collapse_to_one_employer(spark):
    """DOL publishes the same company several ways. Grouping on the raw name
    reports one employer as three — the most common way this data gets quoted
    wrongly."""
    df = rows(spark, [
        rec(case="1", employer="AMAZON.COM SERVICES LLC"),
        rec(case="2", employer="Amazon.com Services, LLC."),
        rec(case="3", employer="AMAZON COM SERVICES  LLC"),
    ])
    keys = {r.employer_key for r in normalise_employer(df).collect()}
    assert keys == {"AMAZON COM SERVICES"}, keys


def test_normalisation_does_not_merge_genuinely_different_employers(spark):
    df = rows(spark, [rec(case="1", employer="ACME INC"), rec(case="2", employer="ACME LABS INC")])
    assert len({r.employer_key for r in normalise_employer(df).collect()}) == 2


# --- wage annualisation ----------------------------------------------------

def test_hourly_wage_annualises_at_2080_hours(spark):
    df = annualise_wage(rows(spark, [rec(wage="60.00", unit="Hour")]))
    assert df.collect()[0].annual_wage == 60.0 * 2080


def test_currency_formatting_is_stripped(spark):
    df = annualise_wage(rows(spark, [rec(wage="$145,000.00", unit="Year")]))
    assert df.collect()[0].annual_wage == 145000.0


def test_every_published_pay_unit_is_handled(spark):
    units = [("Year", 1), ("Hour", 2080), ("Week", 52), ("Bi-Weekly", 26), ("Month", 12)]
    df = annualise_wage(rows(spark, [rec(case=str(i), wage="100", unit=u) for i, (u, _) in enumerate(units)]))
    got = {r.WAGE_UNIT_OF_PAY: r.annual_wage for r in df.collect()}
    for unit, mult in units:
        assert got[unit] == 100.0 * mult, unit


# --- quarantine ------------------------------------------------------------

def test_bad_rows_are_quarantined_with_a_reason_not_dropped(spark):
    """A filter that silently discards malformed input is how a pipeline reports
    90% of the truth with total confidence."""
    df = quarantine_reason(annualise_wage(normalise_employer(rows(spark, [
        rec(case="ok"),
        rec(case=""),                       # missing case number
        rec(case="b", wage="N/A"),          # unparseable
        rec(case="c", unit="Fortnight"),    # unit DOL does not publish
        rec(case="d", wage="10", unit="Year"),        # implausibly low
        rec(case="e", wage="9000000", unit="Year"),   # implausibly high
    ]))))
    by_case = {r.CASE_NUMBER: r.reject_reason for r in df.collect()}
    assert by_case["ok"] is None
    assert by_case[""] == "missing_case_number"
    assert by_case["b"] == "unparseable_wage"
    assert by_case["c"] == "unknown_pay_unit"
    assert by_case["d"] == "implausible_wage"
    assert by_case["e"] == "implausible_wage"


# --- filtering -------------------------------------------------------------

def test_certified_withdrawn_is_kept(spark):
    """DOL uses both 'Certified' and 'Certified - Withdrawn'. Matching on equality
    drops roughly a tenth of the real signal — an employer that was willing to
    file is the thing being measured."""
    df = rows(spark, [
        rec(case="1", status="Certified"),
        rec(case="2", status="Certified - Withdrawn"),
        rec(case="3", status="Denied"),
        rec(case="4", status="Withdrawn"),
    ])
    assert {r.CASE_NUMBER for r in certified_h1b(df).collect()} == {"1", "2"}


def test_non_h1b_visa_classes_are_excluded(spark):
    df = rows(spark, [rec(case="1", visa="H-1B"), rec(case="2", visa="E-3"), rec(case="3", visa="H-1B1 Chile")])
    assert {r.CASE_NUMBER for r in certified_h1b(df).collect()} == {"1"}


# --- aggregation -----------------------------------------------------------

def test_employer_summary_counts_and_averages(spark):
    df = certified_h1b(annualise_wage(normalise_employer(rows(spark, [
        rec(case="1", employer="ACME INC", wage="100000", state="TX"),
        rec(case="2", employer="Acme, Inc.", wage="200000", state="CA"),
        rec(case="3", employer="OTHER LLC", wage="150000", state="TX"),
    ]))))
    out = {r.employer_key: r for r in employer_summary(df).collect()}
    assert out["ACME"].filings == 2
    assert out["ACME"].avg_annual_wage == 150000.0
    assert out["ACME"].states == 2
    assert out["OTHER"].filings == 1


def test_top_job_title_is_the_most_frequent_and_ties_break_deterministically(spark):
    df = certified_h1b(annualise_wage(normalise_employer(rows(spark, [
        rec(case="1", employer="ACME INC", title="Engineer"),
        rec(case="2", employer="ACME INC", title="Engineer"),
        rec(case="3", employer="ACME INC", title="Analyst"),
        rec(case="4", employer="TIE CO", title="Beta"),
        rec(case="5", employer="TIE CO", title="Alpha"),
    ]))))
    out = {r.employer_key: r.top_job_title for r in employer_summary(df).collect()}
    assert out["ACME"] == "Engineer"
    # A tie must not be arbitrary, or the same input produces different output
    # between runs and nobody can diff two reports.
    assert out["TIE"] == "Alpha"
