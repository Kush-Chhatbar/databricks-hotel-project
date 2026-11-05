from decimal import Decimal

from DLT_GOLD_LAYER.transformations.my_transformation import (
    prepare_gold_hotel_bookings
)


def test_prepare_gold_adds_required_columns(gold_input_df):

    result = prepare_gold_hotel_bookings(gold_input_df)

    expected_columns = [
        "arrival_date",
        "total_nights",
        "estimated_room_revenue",
    ]

    for column in expected_columns:
        assert column in result.columns


def test_arrival_date_is_created_correctly(gold_input_df):

    result = prepare_gold_hotel_bookings(gold_input_df)

    row = (
        result
        .filter(
            (result.hotel == "Resort Hotel")
            & (result.arrival_date_day_of_month == 1)
        )
        .first()
    )

    assert str(row["arrival_date"]) == "2015-07-01"


def test_total_nights_is_calculated_correctly(gold_input_df):

    result = prepare_gold_hotel_bookings(gold_input_df)

    row = (
        result
        .filter(
            (result.hotel == "Resort Hotel")
            & (result.arrival_date_day_of_month == 1)
        )
        .first()
    )

    assert row["total_nights"] == 3


def test_null_night_values_are_treated_as_zero(gold_input_df):

    result = prepare_gold_hotel_bookings(gold_input_df)

    row = (
        result
        .filter(
            (result.hotel == "City Hotel")
            & (result.arrival_date_day_of_month == 4)
        )
        .first()
    )

    assert row["total_nights"] == 2


def test_room_revenue_is_calculated_for_checked_out_bookings(
    gold_input_df
):

    result = prepare_gold_hotel_bookings(gold_input_df)

    row = (
        result
        .filter(
            (result.hotel == "Resort Hotel")
            & (result.arrival_date_day_of_month == 1)
        )
        .first()
    )

    assert row["estimated_room_revenue"] == Decimal("300.00")


def test_room_revenue_is_null_for_canceled_bookings(gold_input_df):

    result = prepare_gold_hotel_bookings(gold_input_df)

    row = (
        result
        .filter(
            (result.hotel == "Resort Hotel")
            & (result.arrival_date_day_of_month == 2)
        )
        .first()
    )

    assert row["estimated_room_revenue"] is None