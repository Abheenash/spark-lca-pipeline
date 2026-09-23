import pathlib
import sys

import pytest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1] / "src"))


@pytest.fixture(scope="session")
def spark():
    """One local SparkSession for the whole suite.

    Session-scoped because starting a JVM costs a few seconds and the tests are
    otherwise milliseconds. local[2] rather than local[1] so partition-boundary
    bugs — the ones that only appear when data is split — can actually surface.
    """
    from pyspark.sql import SparkSession

    s = (
        SparkSession.builder.master("local[2]")
        .appName("lca-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")  # default 200 is absurd for 6 rows
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )
    s.sparkContext.setLogLevel("ERROR")
    yield s
    s.stop()
