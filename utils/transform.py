import pandas as pd
import numpy as np
import re # Impor modul regex
from .config import USD_TO_IDR_EXCHANGE_RATE
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def clean_price(price_str):
    """Cleans and converts price string to float (USD) or NaN."""
    try:
        if "Price Unavailable" in price_str or not price_str:
            return np.nan
        return float(price_str.replace('$', '').replace(',', ''))
    except (ValueError, TypeError) as e:
        logging.warning(f"Could not parse price '{price_str}': {e}")
        return np.nan

def convert_price_to_idr(price_usd):
    """Converts USD price to IDR."""
    try:
        if pd.isna(price_usd):
            return np.nan
        return int(price_usd * USD_TO_IDR_EXCHANGE_RATE)
    except (ValueError, TypeError) as e:
        logging.warning(f"Could not convert price '{price_usd}' to IDR: {e}")
        return np.nan

def clean_rating(rating_str):
    """Cleans rating string (e.g., 'Rating: ⭐ 4.8 / 5' or 'Rating: Not Rated') to float or NaN."""
    try:
        if pd.isna(rating_str) or not rating_str: # Handle NaN atau string kosong
            return np.nan

        # Ubah semua jadi string untuk konsistensi (jika ada tipe lain)
        rating_str = str(rating_str)

        # Kasus khusus seperti "Invalid Rating" atau "Not Rated"
        if "Invalid Rating" in rating_str or "Not Rated" in rating_str:
            return np.nan

        # Hilangkan bagian "Rating: " dan emoji bintang "⭐" serta spasi ekstra
        # Menggunakan regex untuk mencari angka desimal (atau integer)
        # Pola ini mencari satu atau lebih digit, diikuti opsional oleh titik dan satu atau lebih digit
        match = re.search(r'(\d+(\.\d+)?)', rating_str)
        
        if match:
            rating_value_str = match.group(1) # Ambil angka yang cocok
            return float(rating_value_str)
        else:
            # Jika tidak ada angka yang cocok setelah pembersihan awal
            logging.warning(f"Could not extract numeric rating from '{rating_str}' after basic cleaning.")
            return np.nan

    except Exception as e: # Tangkap exception yang lebih umum juga
        logging.warning(f"Could not parse rating '{rating_str}': {e}")
        return np.nan

def clean_colors(colors_str):
    """Cleans colors string (e.g., '5 Colors' or '1 Color') to int or 0."""
    try:
        if pd.isna(colors_str) or not colors_str: # Handle NaN atau string kosong
             return 0
        
        # Ubah semua jadi string untuk konsistensi
        s = str(colors_str).strip()
        
        # Cari angka di awal string, diikuti oleh "Color" atau "Colors"
        match = re.match(r"(\d+)\s*(Color|Colors)", s, re.IGNORECASE) # re.IGNORECASE untuk menangani "color" atau "colors"
        if match:
            return int(match.group(1))
        
        logging.warning(f"Could not parse numeric colors from '{colors_str}', defaulting to 0.")
        return 0 # Jika format tidak cocok atau tidak ada angka
    except (ValueError, TypeError) as e:
        logging.warning(f"Error parsing colors '{colors_str}': {e}, defaulting to 0.")
        return 0

def clean_size(size_str):
    """Cleans size string (e.g., 'Size: M') to string or 'Unknown'."""
    try:
        if not size_str or "Size: " not in size_str:
            return "Unknown"
        return size_str.replace('Size: ', '').strip()
    except TypeError as e:
        logging.warning(f"Could not parse size '{size_str}': {e}")
        return "Unknown"


def clean_gender(gender_str):
    """Cleans gender string (e.g., 'Gender: Men') to string or 'Unknown'."""
    try:
        if not gender_str or "Gender: " not in gender_str:
            return "Unknown"
        return gender_str.replace('Gender: ', '').strip()
    except TypeError as e:
        logging.warning(f"Could not parse gender '{gender_str}': {e}")
        return "Unknown"

def transform_data(df_raw):
    """
    Transforms the raw DataFrame: cleans data, converts types, removes duplicates/nulls.
    Includes error handling for overall transformation process.
    """
    if df_raw.empty:
        logging.warning("Input DataFrame for transformation is empty. Skipping.")
        return pd.DataFrame()
    
    logging.info("Starting data transformation...")
    try:
        df = df_raw.copy()

        # Clean and convert Price
        df['Price_USD'] = df['Price'].apply(clean_price)
        df['Price'] = df['Price_USD'].apply(convert_price_to_idr)
        
        # Clean other columns
        df['Rating'] = df['Rating'].apply(clean_rating)
        df['Colors'] = df['Colors'].apply(clean_colors)
        df['Size'] = df['Size'].apply(clean_size)
        df['Gender'] = df['Gender'].apply(clean_gender)
        
        # Remove "Unknown Product" titles
        df = df[df['Title'] != "Unknown Product"]
        
        # Drop rows with NaN in critical columns that make the data unusable
        df.dropna(subset=['Price', 'Rating', 'Title'], inplace=True)

        # Remove duplicates
        df.drop_duplicates(subset=['Title', 'Price', 'Size', 'Gender', 'Colors'], keep='first', inplace=True)
        
        # Ensure correct data types
        df['Timestamp'] = pd.to_datetime(df['Timestamp'])
        df['Price'] = df['Price'].astype('int64') # Price is now IDR
        df['Rating'] = df['Rating'].astype('float64')
        df['Colors'] = df['Colors'].astype('int64')
        df['Title'] = df['Title'].astype(str)
        df['Size'] = df['Size'].astype(str)
        df['Gender'] = df['Gender'].astype(str)

        # Select and reorder columns for the final dataset
        final_columns = ['Title', 'Price', 'Rating', 'Colors', 'Size', 'Gender', 'Timestamp']
        df = df[final_columns]

        logging.info(f"Transformation complete. Products after cleaning: {len(df)}")
        return df
    except Exception as e:
        logging.error(f"An error occurred during data transformation: {e}")
        # Return an empty DataFrame or the partially transformed one depending on desired robustness
        return pd.DataFrame() # Safest to return empty on major error