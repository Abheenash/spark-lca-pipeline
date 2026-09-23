# Spark LCA Pipeline — the DOL disclosure data, done the way it scales

> **Sep 2026:** first release — PySpark on EMR Serverless, explicit schema, quarantine-not-drop, skew-aware aggregation. 15 tests against a **real local Spark session**, including an end-to-end run that writes partitioned Parquet. Two genuine bugs found by running it.

[`h1b-sponsor-intel`](https://github.com/Abheenash/h1b-sponsor-intel) builds
`sponsors.json` from DOL LCA disclosure files with a single-threaded `openpyxl`
loop. That is the right tool at the size I run it, and the wrong one at the size
the data actually is: DOL publishes **millions of rows per fiscal year**, in one
file per quarter, with a column set that drifts between them.

This is that pipeline expressed as a Spark job — and, more to the point, the
decisions that only matter once the data is too big to hold in memory.

## It actually runs

Unlike the rest of my infrastructure projects, this one has **measured output**,
because PySpark runs locally and needs no cloud account:

```
$ python -m lca.job --input lca.csv --output out/
{"rows_read": 5000, "rows_quarantined": 52, "rows_certified_h1b": 4498,
 "employers": 4, "quarantine_rate": 0.0104}

$ ls out/certified/
WORKSITE_STATE=CA  WORKSITE_STATE=NY  WORKSITE_STATE=TX  WORKSITE_STATE=WA
```

Six employer spellings merged to four. Fifty-two malformed rows isolated, not
dropped. Output partitioned by the column people actually filter on.

## Two bugs that only surfaced by running it

**Spark 4 defaults to ANSI mode, so `cast("double")` on `"N/A"` raises instead of
returning null.** The entire quarantine design depends on bad values becoming
null — so on Spark 4 a plain cast means the job *crashes on the first malformed
row* rather than isolating it. `try_cast` is the fix. Reading the code would
never have caught this; a test that feeds it `"N/A"` caught it immediately.

**Stripping punctuation turned `AMAZON.COM` into `AMAZONCOM`**, which then never
matched the `AMAZON COM` spelling of the same employer — defeating the exact
merge the function existed to do. Punctuation has to become a *space*.

Both are pinned by tests.

## The decisions worth defending

| | Why |
|---|---|
| **Explicit schema, never `inferSchema`** | Inference reads the whole input once just to guess, then again to work — every job costs double before computing anything. And the guess is not stable: one stray `"N/A"` makes a FY2025 column a string where FY2024 was a double, and downstream casts start silently nulling. |
| **Quarantine, don't drop** | A filter that discards malformed rows is how a pipeline reports 90% of the truth with total confidence. Rejects go to their own partitioned path *with the reason and the original value*. |
| **A reject *rate*, not a count** | 400 bad rows out of 400 is a broken feed; 400 out of 4,000,000 is Tuesday. Only a rate can carry a threshold, and `--max-quarantine-rate` fails the job above it. |
| **No Python UDFs** | A UDF serialises every row out of the JVM and back, discarding Catalyst's optimisations — routinely 10–100× slower. The pay-unit lookup is a `create_map` expression, not a function. |
| **Adaptive query execution + skew join** | Grouping by employer is skewed by construction: the largest filer has orders of magnitude more rows than the median. AQE splits that straggler instead of waiting for it. |
| **Window function, not a self-join** | Top job title per employer: one shuffle instead of two, and no fan-out on ties. Ties break deterministically, so two runs of the same input diff cleanly. |
| **Broadcast the small side** | One row per employer is a few MB. Broadcasting it avoids shuffling the large side across the cluster. |
| **Partition by state, not employer** | `WORKSITE_STATE` has ~50 values and is what every query filters on. Partitioning by employer would produce tens of thousands of tiny files — the small-files problem costs more than the scan it saves. |

## Testing a Spark job

`tests/` runs a **real local SparkSession** — not a mock. Every assertion goes
through Catalyst and a real shuffle, which is the only way to catch bugs that
appear once data is split across partitions (`local[2]`, deliberately, not
`local[1]`).

The logic lives in `transform.py` as pure DataFrame→DataFrame functions, so each
is testable against a two-row fixture. `job.py` is thin on purpose: it wires IO
to those functions and does nothing else.

```bash
pip install -r tests/requirements.txt
python -m pytest tests -q        # 15 tests, ~9s, no cloud account
```

## Infrastructure

**EMR Serverless**, because this job runs when DOL publishes — a handful of times
a year. A persistent EMR cluster would idle at full price between runs.
`auto_stop` releases capacity after 15 minutes and there is no pre-initialised
capacity, trading a cold start for zero idle cost.

The job role can read `raw/` and write the three output prefixes. It cannot read
or write anything else in the bucket, and the trust policy carries an
`aws:SourceAccount` condition so another account's EMR application cannot assume
it.

## Status

**The job is proven; the infrastructure is not applied.** 15 tests pass against
real Spark, 4 terraform tests pass, `terraform validate` and checkov are clean.
EMR Serverless costs nothing idle but bills per vCPU-second on any real run, and
the local numbers above demonstrate everything a cloud run would.

## Not affiliated with Apache — a personal learning + portfolio project by
[Rajolu Abheenash](https://abheenash.com).
