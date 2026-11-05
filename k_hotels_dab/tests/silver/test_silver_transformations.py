import pytest
from decimal import Decimal

from pyspark.sql.types import (
    BooleanType,
    IntegerType,
    FloatType,
    DecimalType,
    TimestampType,
    StructType,
    StructField,
    StringType,
)

from DLT_SILVER_LAYER.transformations.my_transformation import (
    cast_hotel_booking_columns,
    clean_hotel_booking_columns,
    transform_hotel_bookings,
)


@pytest.fixture
def bronze_df(spark):

    schema = StructType([
        StructField("is_canceled", StringType(), True),
        StructField("lead_time", StringType(), True),
        StructField("arrival_date_year", StringType(), True),
        StructField("arrival_date_week_number", StringType(), True),
        StructField("arrival_date_day_of_month", StringType(), True),
        StructField("stays_in_weekend_nights", StringType(), True),
        StructField("stays_in_week_nights", StringType(), True),
        StructField("adults", StringType(), True),
        StructField("children", StringType(), True),
        StructField("babies", StringType(), True),
        StructField("is_repeated_guest", StringType(), True),
        StructField("previous_cancellations", StringType(), True),
        StructField("previous_bookings_not_canceled", StringType(), True),
        StructField("booking_changes", StringType(), True),
        StructField("agent", StringType(), True),
        StructField("company", StringType(), True),
        StructField("adr", StringType(), True),
        StructField("days_in_waiting_list", StringType(), True),
        StructField("required_car_parking_spaces", StringType(), True),
        StructField("total_of_special_requests", StringType(), True),
        StructField("reservation_status_date", StringType(), True),
        StructField("_rescued_data", StringType(), True),
    ])

    data = [(
        "0", "342", "2015", "27", "1",
        "0", "2", "2", "0.0", "0",
        "0", "0", "3", "0", "0",
        "0", "75.50", "0", "0", "1",
        "2015-07-01", None
    )]

    return spark.createDataFrame(data, schema)

def test_cast_hotel_booking_columns_schema(bronze_df):
    result = cast_hotel_booking_columns(bronze_df)

    schema = {
        field.name: field.dataType
        for field in result.schema.fields
    }

    assert isinstance(schema["is_canceled"], BooleanType)
    assert isinstance(schema["lead_time"], IntegerType)
    assert isinstance(schema["arrival_date_year"], IntegerType)
    assert isinstance(schema["adults"], IntegerType)
    assert isinstance(schema["children"], FloatType)
    assert isinstance(schema["agent"], DecimalType)
    assert isinstance(schema["company"], DecimalType)
    assert isinstance(schema["adr"], DecimalType)
    assert isinstance(schema["reservation_status_date"], TimestampType)

def test_cast_hotel_booking_columns_values(bronze_df):
    result = cast_hotel_booking_columns(bronze_df)

    row = result.first()

    assert row["is_canceled"] is False
    assert row["lead_time"] == 342
    assert row["arrival_date_year"] == 2015
    assert row["adults"] == 2
    assert row["children"] == 0.0
    assert row["adr"] == Decimal("75.50")
    assert row["reservation_status_date"] is not None

def test_clean_hotel_booking_columns(bronze_df):
    result = clean_hotel_booking_columns(bronze_df)

    assert "_rescued_data" not in result.columns
    assert "modified_date" in result.columns

    row = result.first()

    assert row["modified_date"] is not None

def test_transform_hotel_bookings(bronze_df):
    result = transform_hotel_bookings(bronze_df)

    row = result.first()

    assert result.count() == 1

    assert isinstance(
        result.schema["lead_time"].dataType,
        IntegerType
    )

    assert isinstance(
        result.schema["adr"].dataType,
        DecimalType
    )

    assert "_rescued_data" not in result.columns
    assert row["modified_date"] is not None
    assert row["adr"] == Decimal("75.50")