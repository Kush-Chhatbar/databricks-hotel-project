import pytest

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    BooleanType,
    IntegerType,
    FloatType,
    DecimalType,
)
from decimal import Decimal

from DLT_SILVER_LAYER.transformations.my_transformation import (
    get_silver_expectations,
)


def test_silver_expectations_contains_required_rules():
    rules = get_silver_expectations()

    assert "valid_cancellation_flag" in rules
    assert "valid_guest_count" in rules
    assert "valid_adr" in rules

    assert len(rules) == 3


def test_silver_expectations_are_sql_expressions(spark):

    rules = get_silver_expectations()

    schema = StructType([
        StructField("is_canceled", BooleanType(), True),
        StructField("adults", IntegerType(), True),
        StructField("children", FloatType(), True),
        StructField("babies", IntegerType(), True),
        StructField("adr", DecimalType(10, 2), True),
    ])

    data = [
        (False, 2, 0.0, 0, Decimal("75.50"))
    ]

    test_df = spark.createDataFrame(data, schema)

    for expression in rules.values():

        test_df.select(
            F.expr(expression).alias("result")
        ).collect()


def test_valid_records_pass_all_expectations(spark):
    rules = get_silver_expectations()

    data = [
        (False, 2, 0.0, 0, 75.50),
        (True, 1, 1.0, 0, 100.00),
    ]

    columns = [
        "is_canceled",
        "adults",
        "children",
        "babies",
        "adr",
    ]

    df = spark.createDataFrame(data, columns)

    combined_rule = " AND ".join(
        f"({expression})"
        for expression in rules.values()
    )

    valid_records = df.filter(F.expr(combined_rule))

    assert valid_records.count() == 2


def test_invalid_records_fail_expectations(spark):
    rules = get_silver_expectations()

    data = [
        # Negative adults
        (False, -1, 0.0, 0, 75.50),

        # Negative children
        (False, 2, -1.0, 0, 75.50),

        # Negative ADR
        (False, 2, 0.0, 0, -10.00),

        # NULL cancellation flag
        (None, 2, 0.0, 0, 75.50),

        # NULL guest count
        (False, None, 0.0, 0, 75.50),

        # NULL ADR
        (False, 2, 0.0, 0, None),
    ]

    columns = [
        "is_canceled",
        "adults",
        "children",
        "babies",
        "adr",
    ]

    df = spark.createDataFrame(data, columns)

    combined_rule = " AND ".join(
        f"({expression})"
        for expression in rules.values()
    )

    valid_records = df.filter(F.expr(combined_rule))
    rejected_records = df.filter(~F.expr(combined_rule))

    assert valid_records.count() == 0
    assert rejected_records.count() == 6