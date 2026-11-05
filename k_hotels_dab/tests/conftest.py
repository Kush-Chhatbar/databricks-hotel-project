import pytest

from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    """
    Create a local SparkSession for GitHub Actions tests.
    """

    spark_session = (
        SparkSession.builder
        .master("local[2]")
        .appName("Hotel-Project-Tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )

    yield spark_session

    spark_session.stop()