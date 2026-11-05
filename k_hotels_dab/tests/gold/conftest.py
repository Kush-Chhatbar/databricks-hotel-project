import pytest

from decimal import Decimal

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    BooleanType,
    IntegerType,
    DecimalType,
)


@pytest.fixture
def gold_input_df(spark):

    schema = StructType([
        StructField("hotel", StringType(), True),
        StructField("is_canceled", BooleanType(), True),
        StructField("arrival_date_year", IntegerType(), True),
        StructField("arrival_date_month", StringType(), True),
        StructField("arrival_date_day_of_month", IntegerType(), True),
        StructField("stays_in_weekend_nights", IntegerType(), True),
        StructField("stays_in_week_nights", IntegerType(), True),
        StructField("reservation_status", StringType(), True),
        StructField("adr", DecimalType(10, 2), True),
    ])

    data = [
        (
            "Resort Hotel", False, 2015, "July", 1,
            1, 2, "Check-Out", Decimal("100.00")
        ),
        (
            "Resort Hotel", True, 2015, "July", 2,
            0, 3, "Canceled", Decimal("80.00")
        ),
        (
            "City Hotel", False, 2015, "August", 3,
            2, 1, "Check-Out", Decimal("120.00")
        ),
        (
            "City Hotel", False, 2015, "July", 4,
            None, 2, "Check-Out", Decimal("90.00")
        ),
    ]

    return spark.createDataFrame(data, schema)