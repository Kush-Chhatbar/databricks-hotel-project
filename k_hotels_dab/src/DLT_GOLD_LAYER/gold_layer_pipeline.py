import dlt

from transformations.my_transformation import (
    aggregate_hotel_booking_summary,
    aggregate_monthly_booking_trends,
    aggregate_monthly_room_revenue,
    prepare_gold_hotel_bookings,
)


@dlt.view(name="gold_hotel_bookings_base")
def gold_hotel_bookings_base():
    df = spark.readStream.table(
        "workspace.silver.silver_hotel_bookings"
    )
    return prepare_gold_hotel_bookings(df)


@dlt.table(name="gold_hotel_booking_summary")
def gold_hotel_booking_summary():
    df = spark.readStream.table("gold_hotel_bookings_base")
    return aggregate_hotel_booking_summary(df)


@dlt.table(name="gold_monthly_booking_trends")
def gold_monthly_booking_trends():
    df = spark.readStream.table("gold_hotel_bookings_base")
    return aggregate_monthly_booking_trends(df)


@dlt.table(name="gold_monthly_room_revenue")
def gold_monthly_room_revenue():
    df = spark.readStream.table("gold_hotel_bookings_base")
    return aggregate_monthly_room_revenue(df)
