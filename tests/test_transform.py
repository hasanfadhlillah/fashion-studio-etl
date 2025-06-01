import pytest
import pandas as pd
import numpy as np
from datetime import datetime
from utils.transform import (
    clean_price, convert_price_to_idr, clean_rating, clean_colors,
    clean_size, clean_gender, transform_data
)
from utils.config import USD_TO_IDR_EXCHANGE_RATE

@pytest.fixture
def sample_raw_df():
    """Provides a sample raw DataFrame for testing transformations."""
    now = datetime.now()
    data = {
        'Title': ["Product A", "Product B", "Unknown Product", "Product C", "Product A"], # Duplicate Product A
        'Price': ["$10.50", "Price Unavailable", "$5.00", "$20", "$10.50"],
        'Rating': ["Rating: 4.5 / 5", "Rating: Invalid Rating / 5", "Rating: 3.0 / 5", "Rating: 2 / 5", "Rating: 4.5 / 5"],
        'Colors': ["3 Colors", "2 Colors", "1 Color", "No Colors Here", "3 Colors"], # "1 Color" variation
        'Size': ["Size: M", "Size: L", "Size: S", "Size: XL", "Size: M"],
        'Gender': ["Gender: Men", "Gender: Women", "Gender: Unisex", "Gender: Men", "Gender: Men"],
        'Timestamp': [now, now, now, now, now]
    }
    return pd.DataFrame(data)

@pytest.fixture
def empty_df():
    return pd.DataFrame()

# --- Individual cleaning function tests ---
def test_clean_price():
    assert clean_price("$10.50") == 10.50
    assert clean_price("Price Unavailable") is np.nan
    assert clean_price(None) is np.nan
    assert clean_price("") is np.nan
    assert clean_price("abc") is np.nan # Invalid value

def test_convert_price_to_idr():
    assert convert_price_to_idr(10.50) == int(10.50 * USD_TO_IDR_EXCHANGE_RATE)
    assert pd.isna(convert_price_to_idr(np.nan))
    with pytest.raises(TypeError, match=".*unsupported operand type.*"): # Example error propagation
        convert_price_to_idr("abc")


def test_clean_rating():
    assert clean_rating("Rating: 4.5 / 5") == 4.5
    assert clean_rating("Rating: Invalid Rating / 5") is np.nan
    assert clean_rating("Rating: 3/5") == 3.0 # Test without decimal
    assert clean_rating(None) is np.nan
    assert clean_rating("") is np.nan
    assert clean_rating("Some other text") is np.nan

def test_clean_colors():
    assert clean_colors("3 Colors") == 3
    assert clean_colors("1 Color") == 1 # Handle singular
    assert clean_colors("No Colors Here") == 0 # Default for unknown format
    assert clean_colors(None) == 0
    assert clean_colors("") == 0
    assert clean_colors("abc") == 0 # Default for invalid

def test_clean_size():
    assert clean_size("Size: M") == "M"
    assert clean_size("Size: Extra Large") == "Extra Large"
    assert clean_size(None) == "Unknown"
    assert clean_size("") == "Unknown"
    assert clean_size("No Size Prefix") == "Unknown"

def test_clean_gender():
    assert clean_gender("Gender: Men") == "Men"
    assert clean_gender(None) == "Unknown"
    assert clean_gender("") == "Unknown"
    assert clean_gender("No Gender Prefix") == "Unknown"

# --- Main transform_data test ---
def test_transform_data_success(sample_raw_df):
    df_transformed = transform_data(sample_raw_df)

    # Check shape (Unknown Product and Price Unavailable/Invalid Rating should be dropped, one duplicate of Product A removed)
    # Product A (valid): 1 instance
    # Product C (valid): 1 instance
    # Total expected = 2
    assert len(df_transformed) == 2

    # Check data types
    assert df_transformed['Title'].dtype == 'object' # Pandas uses 'object' for strings
    assert df_transformed['Price'].dtype == 'int64'
    assert df_transformed['Rating'].dtype == 'float64'
    assert df_transformed['Colors'].dtype == 'int64'
    assert df_transformed['Size'].dtype == 'object'
    assert df_transformed['Gender'].dtype == 'object'
    assert df_transformed['Timestamp'].dtype == 'datetime64[ns]'

    # Check values of the first valid product (Product A)
    product_a = df_transformed[df_transformed['Title'] == "Product A"].iloc[0]
    assert product_a['Price'] == int(10.50 * USD_TO_IDR_EXCHANGE_RATE)
    assert product_a['Rating'] == 4.5
    assert product_a['Colors'] == 3
    assert product_a['Size'] == "M"
    assert product_a['Gender'] == "Men"

    # Check no "Unknown Product"
    assert "Unknown Product" not in df_transformed['Title'].values

    # Check no NaN in critical columns
    assert not df_transformed[['Title', 'Price', 'Rating']].isnull().any().any()

    # Check specific transformation for "Product C" (Rating '2 / 5', Price '20')
    product_c = df_transformed[df_transformed['Title'] == "Product C"].iloc[0]
    assert product_c['Price'] == int(20 * USD_TO_IDR_EXCHANGE_RATE)
    assert product_c['Rating'] == 2.0
    assert product_c['Colors'] == 0 # "No Colors Here" becomes 0

def test_transform_data_empty_input(empty_df, caplog):
    df_transformed = transform_data(empty_df)
    assert df_transformed.empty
    assert "Input DataFrame for transformation is empty. Skipping." in caplog.text

def test_transform_data_all_invalid(caplog):
    """Test transformation when all data is invalid and gets dropped."""
    now = datetime.now()
    data = {
        'Title': ["Unknown Product", "Product X"],
        'Price': ["$5.00", "Price Unavailable"],
        'Rating': ["Rating: 3.0 / 5", "Rating: Invalid Rating / 5"], # Product X has invalid rating
        'Colors': ["1 Color", "2 Colors"],
        'Size': ["Size: S", "Size: M"],
        'Gender': ["Gender: Unisex", "Gender: Men"],
        'Timestamp': [now, now]
    }
    df_all_invalid = pd.DataFrame(data)
    df_transformed = transform_data(df_all_invalid)
    assert df_transformed.empty
    # Check logs if needed, e.g. transformation completion log
    assert "Transformation complete. Products after cleaning: 0" in caplog.text


def test_transform_data_type_error_handling(sample_raw_df, caplog, monkeypatch):
    """Test error handling during a specific transformation step."""
    
    # Introduce an error in one of the cleaning functions
    def faulty_clean_size(size_str):
        if size_str == "Size: L": # Target a specific row to cause partial failure
            raise TypeError("Simulated type error in clean_size")
        return size_str.replace('Size: ', '').strip() if size_str else "Unknown"

    monkeypatch.setattr('utils.transform.clean_size', faulty_clean_size)
    
    # We expect the overall transform_data to catch this and return an empty DF
    # because the error is not handled within clean_size itself to allow the row to proceed.
    # If clean_size handled it and returned 'Unknown', the row might proceed.
    # This tests the outer try-except in transform_data.
    
    df_transformed = transform_data(sample_raw_df.copy()) # Use a copy
    
    assert "An error occurred during data transformation" in caplog.text
    assert "Simulated type error in clean_size" in caplog.text # Ensure our specific error is logged
    assert df_transformed.empty # As per current error handling, returns empty on critical error