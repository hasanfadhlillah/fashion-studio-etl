import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import time
from .config import BASE_URL, MAX_PAGES
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def fetch_page_content(page_number):
    """
    Fetches the HTML content of a specific page.
    Includes error handling for network requests.
    """
    url = f"{BASE_URL}/page/{page_number}"
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()  # Raises an HTTPError for bad responses (4XX or 5XX)
        return response.content
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching page {page_number}: {e}")
        return None

def parse_product_data(html_content, page_number):
    """
    Parses product data from the HTML content of a page.
    Includes error handling for parsing issues.
    """
    products_on_page = []
    extraction_timestamp = datetime.now()
    try:
        soup = BeautifulSoup(html_content, 'html.parser')
        product_cards = soup.find_all('div', class_='collection-card')

        if not product_cards:
            logging.warning(f"No product cards found on page {page_number}.")
            return []

        for card in product_cards:
            title_tag = card.find('h3', class_='product-title')
            title = title_tag.text.strip() if title_tag else "Unknown Product"

            price_container = card.find('div', class_='price-container')
            price_tag_p = card.find('p', class_='price') # For "Price Unavailable"

            if price_container and price_container.find('span', class_='price'):
                price = price_container.find('span', class_='price').text.strip()
            elif price_tag_p:
                price = price_tag_p.text.strip()
            else:
                price = "Price Unavailable" # Fallback

            rating_tag = card.find('p', string=lambda text: text and "Rating:" in text)
            rating = rating_tag.text.strip() if rating_tag else "Rating: Invalid Rating / 5"
            
            colors_tag = card.find('p', string=lambda text: text and "Colors" in text)
            colors = colors_tag.text.strip() if colors_tag else "0 Colors"

            size_tag = card.find('p', string=lambda text: text and "Size:" in text)
            size = size_tag.text.strip() if size_tag else "Size: Unknown"

            gender_tag = card.find('p', string=lambda text: text and "Gender:" in text)
            gender = gender_tag.text.strip() if gender_tag else "Gender: Unknown"

            products_on_page.append({
                'Title': title,
                'Price': price,
                'Rating': rating,
                'Colors': colors,
                'Size': size,
                'Gender': gender,
                'Timestamp': extraction_timestamp
            })
        return products_on_page
    except Exception as e:
        logging.error(f"Error parsing product data on page {page_number}: {e}")
        return [] # Return empty list on parsing error for this page

def extract_all_products():
    """
    Extracts product data from all pages (1 to MAX_PAGES).
    Returns a Pandas DataFrame.
    Includes error handling for overall extraction process.
    """
    all_products_data = []
    logging.info(f"Starting extraction from {BASE_URL}...")
    try:
        for page_num in range(1, MAX_PAGES + 1):
            logging.info(f"Fetching data from page {page_num}/{MAX_PAGES}...")
            html_content = fetch_page_content(page_num)
            if html_content:
                products_from_page = parse_product_data(html_content, page_num)
                if products_from_page:
                    all_products_data.extend(products_from_page)
                else:
                    logging.warning(f"No products extracted from page {page_num}.")
            else:
                logging.warning(f"Skipping page {page_num} due to fetch error.")
            time.sleep(0.5) # Be respectful to the server

        if not all_products_data:
            logging.warning("No data was extracted from any page.")
            return pd.DataFrame() # Return empty DataFrame if nothing was extracted

        df = pd.DataFrame(all_products_data)
        logging.info(f"Extraction complete. Total products scraped initially: {len(df)}")
        return df
    except Exception as e:
        logging.error(f"An critical error occurred during the extraction process: {e}")
        return pd.DataFrame() # Return empty DataFrame on critical failure