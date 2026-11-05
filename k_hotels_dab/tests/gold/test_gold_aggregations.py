from DLT_GOLD_LAYER.transformations.my_transformation import (
    prepare_gold_hotel_bookings,
    aggregate_hotel_booking_summary,
    aggregate_monthly_booking_trends,
    aggregate_monthly_room_revenue,
)


def test_hotel_booking_summary_counts_correctly(gold_input_df):

    result = aggregate_hotel_booking_summary(gold_input_df)

    rows = {
        row["hotel"]: row
        for row in result.collect()
    }

    resort = rows["Resort Hotel"]

    assert resort["total_bookings"] == 2
    assert resort["canceled_bookings"] == 1
    assert resort["non_canceled_bookings"] == 1

    city = rows["City Hotel"]

    assert city["total_bookings"] == 2
    assert city["canceled_bookings"] == 0
    assert city["non_canceled_bookings"] == 2