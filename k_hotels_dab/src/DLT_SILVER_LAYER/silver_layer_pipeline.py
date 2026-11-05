import dlt

from transformations.my_transformation import (
    transform_hotel_bookings,
    get_silver_expectations
)


@dlt.table(name="stage_hotel_bookings")
def stage_hotel_bookings():
    df = (
        spark.readStream
        .format("delta")
        .load("/Volumes/hotel_project/bronze/bronzevolume/hotel_bookings/data/")
    )
    return df


@dlt.view(name="trans_hotel_bookings")
def trans_hotel_bookings():
    df = spark.readStream.table("stage_hotel_bookings")
    df = transform_hotel_bookings(df)

    return df


rules = get_silver_expectations()


@dlt.table(name="silver_hotel_bookings")
@dlt.expect_all_or_drop(rules)
def silver_hotel_bookings():
    df = spark.readStream.table("trans_hotel_bookings")

    return df