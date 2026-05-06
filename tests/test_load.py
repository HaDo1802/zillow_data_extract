"""
BEGINNER TEST: Simple tests for load/database functions
NO REAL DATABASE - Just testing data preparation
"""

import pandas as pd


def test_prepare_data_for_database():
    """Test preparing DataFrame for database insert."""
    # Arrange: Create sample data
    data = {"zpid": ["123", "456"], "price": [350000, 425000], "bedrooms": [3, 4]}
    df = pd.DataFrame(data)

    # Act: Get column names
    columns = list(df.columns)

    # Assert: Should have expected columns
    assert "zpid" in columns
    assert "price" in columns
    assert "bedrooms" in columns


def test_convert_dataframe_to_dict():
    """Test converting DataFrame rows to dictionaries."""
    # Arrange: Create sample data
    data = {"zpid": ["123"], "price": [350000]}
    df = pd.DataFrame(data)

    # Act: Convert first row to dict
    row_dict = df.iloc[0].to_dict()

    # Assert: Should be a dictionary
    assert isinstance(row_dict, dict)
    assert row_dict["zpid"] == "123"
    assert row_dict["price"] == 350000


def test_count_rows_to_insert():
    """Test counting how many rows will be inserted."""
    # Arrange: Create sample data
    data = {"zpid": ["123", "456", "789"], "price": [350000, 425000, 500000]}
    df = pd.DataFrame(data)

    # Act: Count rows
    row_count = len(df)

    # Assert: Should have 3 rows
    assert row_count == 3


def test_create_csv_for_database():
    """Test creating CSV file (like for COPY command)."""
    # Arrange: Sample data
    data = {"zpid": ["123", "456"], "price": [350000, 425000]}
    df = pd.DataFrame(data)

    # Act: Convert to CSV string
    csv_string = df.to_csv(index=False)

    # Assert: Should contain data
    assert "zpid,price" in csv_string
    assert "123,350000" in csv_string
    assert "456,425000" in csv_string
