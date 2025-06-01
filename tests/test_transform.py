import pytest
import pandas as pd
import numpy as np
from datetime import datetime
from utils.transform import (
    clean_price, convert_price_to_idr, clean_rating, clean_colors,
    clean_size, clean_gender, transform_data
)
from utils.config import USD_TO_IDR_EXCHANGE_RATE # Pastikan ini diimpor
import logging # Untuk caplog

@pytest.fixture
def sample_raw_df():
    """Provides a sample raw DataFrame for testing transformations."""
    now = datetime.now()
    data = {
        'Title': ["Product A", "Product B", "Unknown Product", "Product C", "Product A"],
        'Price': ["$10.50", "Price Unavailable", "$5.00", "$20", "$10.50"],
        'Rating': ["Rating: ⭐ 4.5 / 5", "Rating: Invalid Rating / 5", "Rating: ⭐ 3.0 / 5", "Rating: 2 / 5", "Rating: ⭐ 4.5 / 5"],
        'Colors': ["3 Colors", "2 Colors", "1 Color", "No Colors Here", "3 Colors"],
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
    assert pd.isna(clean_price("Price Unavailable")) # Gunakan pd.isna untuk cek NaN dari Pandas/Numpy
    assert pd.isna(clean_price(None))
    assert pd.isna(clean_price(""))
    assert pd.isna(clean_price("abc"))

def test_convert_price_to_idr(caplog): # Tambah caplog jika mau cek log
    assert convert_price_to_idr(10.50) == int(10.50 * USD_TO_IDR_EXCHANGE_RATE)
    assert pd.isna(convert_price_to_idr(np.nan))
    # Fungsi sekarang menangani error dan return NaN, jadi tes tidak mengharapkan TypeError lagi
    with caplog.at_level(logging.WARNING):
        assert pd.isna(convert_price_to_idr("abc"))
    assert "Could not convert price 'abc' to IDR" in caplog.text


def test_clean_rating():
    assert clean_rating("Rating: ⭐ 4.5 / 5") == 4.5
    assert pd.isna(clean_rating("Rating: Invalid Rating / 5"))
    assert clean_rating("Rating: 3/5") == 3.0 
    assert pd.isna(clean_rating(None))
    assert pd.isna(clean_rating(""))
    assert pd.isna(clean_rating("Some other text"))
    assert clean_rating("Rating: ⭐ 3 / 5") == 3.0 # Tes dengan emoji dan integer

def test_clean_colors(caplog): # Tambah caplog jika perlu
    assert clean_colors("3 Colors") == 3
    assert clean_colors("1 Color") == 1 # Ini seharusnya sekarang lolos dengan perbaikan di utils/transform.py
    assert clean_colors("No Colors Here") == 0
    assert clean_colors(None) == 0
    assert clean_colors("") == 0
    with caplog.at_level(logging.WARNING): # Jika fungsi Anda log warning untuk "abc"
      assert clean_colors("abc") == 0
    # Anda bisa cek log jika clean_colors("abc") menghasilkan warning
    # assert "Could not parse numeric colors from 'abc'" in caplog.text 

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
    df_transformed = transform_data(sample_raw_df.copy())
    assert len(df_transformed) == 2 # Hanya 2 produk valid yang tersisa setelah pembersihan

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
    with caplog.at_level(logging.WARNING): # Pastikan level log sesuai
        df_transformed = transform_data(empty_df)
    assert df_transformed.empty
    assert "Input DataFrame for transformation is empty. Skipping." in caplog.text

def test_transform_data_all_invalid(caplog):
    """Test transformation when all data is invalid and gets dropped."""
    now = datetime.now()
    data = {
        'Title': ["Unknown Product", "Product X"],
        'Price': ["$5.00", "Price Unavailable"],
        'Rating': ["Rating: ⭐ 3.0 / 5", "Rating: Invalid Rating / 5"], 
        'Colors': ["1 Color", "2 Colors"],
        'Size': ["Size: S", "Size: M"],
        'Gender': ["Gender: Unisex", "Gender: Men"],
        'Timestamp': [now, now]
    }
    df_all_invalid = pd.DataFrame(data)
    
    # Menangkap log pada level INFO
    with caplog.at_level(logging.INFO):
        df_transformed = transform_data(df_all_invalid)
        
    assert df_transformed.empty
    # Periksa apakah pesan log yang diharapkan ada di caplog.text
    # Pesan lognya adalah: "Transformation complete. Products after cleaning: {len(df)}"
    assert "Transformation complete. Products after cleaning: 0" in caplog.text

def test_transform_data_type_error_handling(sample_raw_df, caplog, monkeypatch):
    """Test error handling during a specific transformation step."""
    def faulty_clean_size(size_str):
        if size_str == "Size: L": 
            raise TypeError("Simulated type error in clean_size")
        return str(size_str).replace('Size: ', '').strip() if pd.notna(size_str) and "Size: " in str(size_str) else "Unknown"

    monkeypatch.setattr('utils.transform.clean_size', faulty_clean_size)
    
    with caplog.at_level(logging.ERROR): # Tangkap ERROR dari transform_data
        df_transformed = transform_data(sample_raw_df.copy()) 
    
    assert "An error occurred during data transformation" in caplog.text
    assert "Simulated type error in clean_size" in caplog.text 
    assert df_transformed.empty 