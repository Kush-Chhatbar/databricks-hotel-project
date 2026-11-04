from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    IntegerType,
    FloatType,
    DecimalType,
    TimestampType,
)


def cast_hotel_booking_columns(df: DataFrame):
    """
    Convert hotel booking columns from source types
    into the required Silver layer data types.
    """
    df = (
        df
        .withColumn("is_canceled", F.col("is_canceled").cast(BooleanType()))
        .withColumn("lead_time", F.col("lead_time").cast(IntegerType()))
        .withColumn("arrival_date_year", F.col("arrival_date_year").cast(IntegerType()))
        .withColumn("arrival_date_week_number", F.col("arrival_date_week_number").cast(IntegerType()))
        .withColumn("arrival_date_day_of_month", F.col("arrival_date_day_of_month").cast(IntegerType()))
        .withColumn("stays_in_weekend_nights", F.col("stays_in_weekend_nights").cast(IntegerType()))
        .withColumn("stays_in_week_nights", F.col("stays_in_week_nights").cast(IntegerType()))
        .withColumn("adults", F.col("adults").cast(IntegerType()))
        .withColumn("children", F.col("children").cast(FloatType()))
        .withColumn("babies", F.col("babies").cast(IntegerType()))
        .withColumn("is_repeated_guest", F.col("is_repeated_guest").cast(BooleanType()))
        .withColumn("previous_cancellations", F.col("previous_cancellations").cast(IntegerType()))
        .withColumn("previous_bookings_not_canceled", F.col("previous_bookings_not_canceled").cast(IntegerType()))
        .withColumn("booking_changes", F.col("booking_changes").cast(IntegerType()))
        .withColumn("agent", F.col("agent").cast(DecimalType(10, 2)))
        .withColumn("company", F.col("company").cast(DecimalType(10, 2)))
        .withColumn("adr", F.col("adr").cast(DecimalType(10, 2)))
        .withColumn("days_in_waiting_list", F.col("days_in_waiting_list").cast(IntegerType()))
        .withColumn("required_car_parking_spaces", F.col("required_car_parking_spaces").cast(IntegerType()))
        .withColumn("total_of_special_requests", F.col("total_of_special_requests").cast(IntegerType()))
        .withColumn("reservation_status_date", F.col("reservation_status_date").cast(TimestampType()))
    )

    return df


def clean_hotel_booking_columns(df: DataFrame) -> DataFrame:
    """
    Apply common Silver cleaning operations.
    """
    df = (
        df\
        .withColumn("modified_date", F.current_timestamp())\
        .drop("_rescued_data")
    )

    return df


def transform_hotel_bookings(df: DataFrame):
    """
    Apply all Silver transformations in sequence.
    """
    df = cast_hotel_booking_columns(df)
    df = clean_hotel_booking_columns(df)

    return df


def get_silver_expectations():
    """
    Return the data quality rules for the Silver layer.
    """
    return {
        "valid_cancellation_flag": """
            is_canceled IS NOT NULL
        """,

        "valid_guest_count": """
            adults IS NOT NULL
            AND children IS NOT NULL
            AND babies IS NOT NULL
            AND adults >= 0
            AND children >= 0
            AND babies >= 0
        """,

        "valid_adr": """
            adr IS NOT NULL
            AND adr >= 0
        """
    }