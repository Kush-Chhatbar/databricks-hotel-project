import os
import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():

    is_databricks = (
        "SPARK_REMOTE" in os.environ
        or "DATABRICKS_RUNTIME_VERSION" in os.environ
    )

    builder = SparkSession.builder.appName("hotel-silver-tests")

    if not is_databricks:
        builder = builder.master("local[2]")
        builder = builder.config("spark.ui.enabled", "false")

    session = builder.getOrCreate()

    yield session

    if not is_databricks:
        session.stop()