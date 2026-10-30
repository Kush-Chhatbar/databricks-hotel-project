import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))
sys.modules.pop("bronze", None)

from bronze.ingestion import (
    validate_source,
    get_source_paths
)

@pytest.mark.parametrize(
    "source",
    [
        "hotel_bookings",
        "customers",
        "orders",
        "hotel-bookings",
        "source_123",
    ],
)
def test_valid_source_name(source):
    validate_source(source)

@pytest.mark.parametrize(
    "source",
    [
        "",
        "../hotel_bookings",
        "hotel/bookings",
        "hotel bookings",
        "hotel.bookings",
        "hotel@bookings",
    ],
)
def test_invalid_source_name(source):
    with pytest.raises(ValueError):
        validate_source(source)


def test_empty_source_name():
    with pytest.raises(ValueError):
        validate_source("")


def test_source_paths():
    paths = get_source_paths("hotel_bookings")

    assert paths["raw_path"] == (
        "/Volumes/hotel_project/raw_data/raw_files/hotel_bookings/"
    )

    assert paths["schema_path"] == (
        "/Volumes/hotel_project/bronze/bronzevolume/hotel_bookings/schema/"
    )

    assert paths["checkpoint_path"] == (
        "/Volumes/hotel_project/bronze/bronzevolume/hotel_bookings/checkpoints/"
    )

    assert paths["bronze_path"] == (
        "/Volumes/hotel_project/bronze/bronzevolume/hotel_bookings/data/"
    )