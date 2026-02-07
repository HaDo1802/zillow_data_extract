"""
BEGINNER TEST: Simple tests for extract functions
NO MOCKING - Just testing basic logic
"""

from datetime import datetime, timezone

import pandas as pd


def test_dataframe_creation():
    """Test creating a simple DataFrame (like what extract.py returns)."""
    # Arrange: Create sample data
    data = {"zpid": ["123", "456"], "price": [350000, 425000], "bedrooms": [3, 4]}

    # Act: Create DataFrame
    df = pd.DataFrame(data)

    # Assert: Check it worked
    assert len(df) == 2
    assert df.iloc[0]["price"] == 350000
    assert df.iloc[1]["bedrooms"] == 4


def test_remove_duplicates():
    """Test removing duplicate property IDs."""
    # Arrange: Create data with duplicates
    data = {"zpid": ["123", "123", "456"], "price": [350000, 350000, 425000]}
    df = pd.DataFrame(data)

    # Act: Remove duplicates
    df_clean = df.drop_duplicates(subset=["zpid"], keep="first")

    # Assert: Should only have 2 rows now
    assert len(df_clean) == 2
    assert list(df_clean["zpid"]) == ["123", "456"]


def test_add_timestamp():
    """Test adding a timestamp column (like extracted_at)."""
    # Arrange: Create simple DataFrame
    data = {"zpid": ["123"]}
    df = pd.DataFrame(data)

    # Act: Add timestamp
    df["extracted_at"] = datetime.now(timezone.utc)

    # Assert: Column exists
    assert "extracted_at" in df.columns
    assert df.iloc[0]["extracted_at"] is not None


def test_filter_by_price():
    """Test filtering properties by price."""
    # Arrange: Create sample data
    data = {"zpid": ["123", "456", "789"], "price": [100000, 500000, 250000]}
    df = pd.DataFrame(data)

    # Act: Filter expensive properties (over $300k)
    expensive = df[df["price"] > 300000]

    # Assert: Should only have 1 property
    assert len(expensive) == 1
    assert expensive.iloc[0]["zpid"] == "456"
