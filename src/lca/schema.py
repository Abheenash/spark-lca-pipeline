"""The LCA disclosure schema, declared explicitly.

`inferSchema` is the single most expensive habit in Spark: it reads the entire
input once just to guess types, then reads it again to do the work — so every job
costs double before it computes anything. Worse, the guess is not stable. DOL
publishes one file per fiscal year and the column set drifts between them, so an
inferred `WAGE_RATE_OF_PAY_FROM` can be a double in FY2024 and a string in FY2025
(one file has a stray "N/A"), and a downstream cast starts silently producing
nulls.

Declaring it means a shape change is an error at read time, in this file, with
the column named — not a wrong number three stages later.
"""

from pyspark.sql import types as T

# Only the columns this pipeline actually uses. DOL's files carry ~90; reading
# the other ~75 is IO and memory spent on data nothing consumes.
LCA_SCHEMA = T.StructType([
    T.StructField("CASE_NUMBER", T.StringType(), nullable=False),
    T.StructField("CASE_STATUS", T.StringType(), nullable=True),
    T.StructField("VISA_CLASS", T.StringType(), nullable=True),
    T.StructField("EMPLOYER_NAME", T.StringType(), nullable=True),
    T.StructField("JOB_TITLE", T.StringType(), nullable=True),
    T.StructField("SOC_TITLE", T.StringType(), nullable=True),
    T.StructField("PW_WAGE_LEVEL", T.StringType(), nullable=True),
    T.StructField("WAGE_RATE_OF_PAY_FROM", T.StringType(), nullable=True),
    T.StructField("WAGE_UNIT_OF_PAY", T.StringType(), nullable=True),
    T.StructField("WORKSITE_STATE", T.StringType(), nullable=True),
    T.StructField("WORKSITE_CITY", T.StringType(), nullable=True),
    T.StructField("BEGIN_DATE", T.StringType(), nullable=True),
])

# Wage is published in six different units. Annualising in the pipeline rather
# than at read time keeps the raw value auditable — a reviewer can see what DOL
# actually published next to what we derived from it.
PAY_UNIT_MULTIPLIER = {
    "Year": 1.0,
    "Hour": 2080.0,   # 40h x 52w, the DOL convention
    "Week": 52.0,
    "Bi-Weekly": 26.0,
    "Month": 12.0,
}

QUARANTINE_REASONS = (
    "missing_case_number",
    "unparseable_wage",
    "unknown_pay_unit",
    "implausible_wage",
)
