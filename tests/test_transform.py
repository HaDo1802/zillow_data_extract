import os
import sys

# Fix imports
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, project_root)
sys.path.insert(0, os.path.join(project_root, "etl"))

import transform as transform_module  # noqa: E402
from transform import (  # noqa: E402
    convert_unix_timestamp,
    extract_address_components,
    extract_listing_subtype_info,
    extract_vegas_district,
    normalize_lot_area_value,
)


def test_parse_complete_address():
    """Test parsing a normal, complete address."""
    address = "123 Main St, Las Vegas, NV 89101"

    result = extract_address_components(address)

    assert result["street_address"] == "123 Main St"
    assert result["city"] == "Las Vegas"
    assert result["state"] == "NV"
    assert result["zip_code"] == "89101"


def test_empty_address():
    """Test that empty string is handled safely."""
    result = extract_address_components("")

    # All fields should be None
    assert result["street_address"] is None
    assert result["city"] is None
    assert result["state"] is None
    assert result["zip_code"] is None


def test_convert_valid_timestamp():
    """Test converting a valid Unix timestamp."""
    # January 1, 2024, 00:00:00 UTC = 1704067200000 milliseconds
    timestamp = 1704067200000

    result = convert_unix_timestamp(timestamp)

    assert result is not None
    assert result.year == 2024
    assert result.month == 1
    assert result.day == 1


def test_convert_timestamp_string():
    """Test that string timestamps work."""
    timestamp = "1704067200000"

    result = convert_unix_timestamp(timestamp)

    assert result is not None
    assert result.year == 2024


def test_empty_string_timestamp():
    """Test that empty string returns None."""
    result = convert_unix_timestamp("")
    assert result is None


def test_invalid_string_timestamp():
    """Test that invalid string returns None."""
    result = convert_unix_timestamp("not_a_number")
    assert result is None


def test_one_acre_to_sqft():
    """Test converting 1 acre to square feet."""
    result = normalize_lot_area_value(1.0, "acres")

    # 1 acre = 43,560 square feet
    assert result == 43560.0


def test_sqft_unchanged():
    result = normalize_lot_area_value(5000.0, "sqft")

    # Should not convert, return same value
    assert result == 5000.0


def test_none_area_value():
    """Test that None area returns None."""
    result = normalize_lot_area_value(None, "acres")
    assert result is None


def test_summerlin_district(monkeypatch):
    """Test that Summerlin is detected."""
    monkeypatch.setattr(
        transform_module,
        "load_district_mapping",
        lambda: {"summerlin": "Summerlin"},
    )
    address = "123 Main St, Summerlin, NV 89135"
    result = extract_vegas_district(address, "Summerlin")

    assert result == "Summerlin"


def test_fallback_to_city(monkeypatch):
    """Test fallback when no district keyword found."""
    monkeypatch.setattr(transform_module, "load_district_mapping", lambda: {})
    address = "999 Unknown St"
    city = "Las Vegas"

    result = extract_vegas_district(address, city)

    assert result == "Las Vegas"


def test_open_house_true():
    """Test dictionary with open house True."""
    listing = {"is_FSBA": False, "is_openHouse": True}

    result = extract_listing_subtype_info(listing)

    assert result["is_fsba"] is False
    assert result["is_open_house"] is True


def test_none_listing():
    """Test that None returns default False values."""
    result = extract_listing_subtype_info(None)

    assert result["is_fsba"] is False
    assert result["is_open_house"] is False
