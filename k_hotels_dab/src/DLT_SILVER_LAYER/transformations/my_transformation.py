import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

@dlt.table(name="stage_hotel_bookings")
def stage_hotel_bookings():
    df = spark.readStream.format("delta")\
        .load("/Volumes/hotel_project/bronze/bronzevolume/hotel_bookings/data/")
    return df

@dlt.view(name="trans_hotel_bookings")
def trans_hotel_bookings():
    df = spark.readStream.table("stage_hotel_bookings")
    df = df\
    .withColumn("is_canceled", col("is_canceled").cast(BooleanType()))\
    .withColumn("lead_time", col("lead_time").cast(IntegerType()))\
    .withColumn("arrival_date_year",col("arrival_date_year").cast(IntegerType()))\
    .withColumn("arrival_date_week_number",col("arrival_date_week_number").cast(IntegerType()))\
    .withColumn("arrival_date_day_of_month",col("arrival_date_day_of_month").cast(IntegerType()))\
    .withColumn("stays_in_weekend_nights",col("stays_in_weekend_nights").cast(IntegerType()))\
    .withColumn("stays_in_week_nights",col("stays_in_week_nights").cast(IntegerType()))\
    .withColumn("adults",col("adults").cast(IntegerType()))\
    .withColumn("children",col("children").cast(FloatType()))\
    .withColumn("babies",col("babies").cast(IntegerType()))\
    .withColumn("is_repeated_guest", col("is_repeated_guest").cast(BooleanType()))\
    .withColumn("previous_cancellations",col("previous_cancellations").cast(IntegerType()))\
    .withColumn("previous_bookings_not_canceled",col("previous_bookings_not_canceled").cast(IntegerType()))\
    .withColumn("booking_changes",col("booking_changes").cast(IntegerType()))\
    .withColumn("agent", col("agent").cast(DecimalType(10,2)))\
    .withColumn("company", col("company").cast(DecimalType(10,2)))\
    .withColumn("adr",col("adr").cast(DecimalType(10,2)))\
    .withColumn("days_in_waiting_list",col("days_in_waiting_list").cast(IntegerType()))\
    .withColumn("required_car_parking_spaces",col("required_car_parking_spaces").cast(IntegerType()))\
    .withColumn("total_of_special_requests",col("total_of_special_requests").cast(IntegerType()))\
    .withColumn("reservation_status_date", col("reservation_status_date").cast(TimestampType()))\
    .withColumn("modified_date", current_timestamp())\
    .drop("_rescued_data")
    
    return df

rules = {
    "valid_cancellation_flag": (
        col("is_canceled").isNotNull()
        & col("is_canceled").isin(False, True)
    ),
    "valid_guest_count": (
        col("adults").isNotNull()
        & col("children").isNotNull()
        & col("babies").isNotNull()
        & (col("adults") >= 0)
        & (col("children") >= 0)
        & (col("babies") >= 0)
    ),
    "valid_adr": (
        col("adr").isNotNull()
        & (col("adr") >= 0)
    )
}

@dlt.table(name="silver_hotel_bookings")
@dlt.expect_all_or_drop(rules)
def silver_hotel_bookings():
    df = spark.readStream.table("trans_hotel_bookings")
    return df