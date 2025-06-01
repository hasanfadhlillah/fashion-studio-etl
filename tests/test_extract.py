import pytest
import requests_mock
from utils.extract import fetch_page_content, parse_product_data, extract_all_products
from utils.config import BASE_URL # Pastikan BASE_URL diimpor dari config
import pandas as pd
from datetime import datetime
import logging # Untuk caplog

# Definisikan MOCK HTML di sini (saya asumsikan ini sudah ada dan benar di file Anda)
MOCK_HTML_PAGE_1_CONTENT = """
<html><body>
    <div class="collection-card">
        <h3 class="product-title">Cool T-Shirt</h3>
        <div class="price-container"><span class="price">$25.99</span></div>
        <p>Rating: ⭐ 4.5 / 5</p>
        <p>3 Colors</p>
        <p>Size: M</p>
        <p>Gender: Men</p>
    </div>
    <div class="collection-card">
        <h3 class="product-title">Awesome Jeans</h3>
        <p class="price">Price Unavailable</p>
        <p>Rating: Invalid Rating / 5</p>
        <p>2 Colors</p>
        <p>Size: L</p>
        <p>Gender: Unisex</p>
    </div>
</body></html>
"""

MOCK_HTML_PAGE_2_CONTENT = """
<html><body>
    <div class="collection-card">
        <h3 class="product-title">Another Product</h3>
        <div class="price-container"><span class="price">$10.00</span></div>
        <p>Rating: ⭐ 3.0 / 5</p>
        <p>1 Color</p>
        <p>Size: S</p>
        <p>Gender: Women</p>
    </div>
</body></html>
"""
MOCK_HTML_PAGE_EMPTY_CONTENT = "<html><body></body></html>"


@pytest.fixture
def mock_requests_session(): # Mengganti nama fixture agar lebih jelas
    with requests_mock.Mocker() as m:
        yield m

def test_fetch_page_content_success(mock_requests_session):
    """Test fetching page content successfully for page 1 and page > 1."""
    # Test Page 1
    page_num_1 = 1
    url_page_1 = BASE_URL # URL untuk halaman 1
    mock_requests_session.get(url_page_1, text="<html>Page 1 Data</html>", status_code=200)
    content_1 = fetch_page_content(page_num_1)
    assert content_1 == b"<html>Page 1 Data</html>"

    # Test Page 2
    page_num_2 = 2
    url_page_2 = f"{BASE_URL}/page{page_num_2}" # URL untuk halaman 2 dst.
    mock_requests_session.get(url_page_2, text="<html>Page 2 Data</html>", status_code=200)
    content_2 = fetch_page_content(page_num_2)
    assert content_2 == b"<html>Page 2 Data</html>"

def test_fetch_page_content_failure(mock_requests_session, caplog):
    """Test fetching page content with a network error for page 1."""
    page_num = 1
    url_to_mock = BASE_URL # URL untuk halaman 1
    mock_requests_session.get(url_to_mock, status_code=500)
    
    with caplog.at_level(logging.ERROR): # Pastikan menangkap log ERROR
        content = fetch_page_content(page_num)
    
    assert content is None
    assert f"Error fetching page {page_num} from URL {url_to_mock}" in caplog.text

def test_parse_product_data_success():
    """Test parsing product data from valid HTML."""
    products = parse_product_data(MOCK_HTML_PAGE_1_CONTENT, 1)
    assert len(products) == 2
    assert products[0]['Title'] == "Cool T-Shirt"
    assert products[0]['Price'] == "$25.99"
    assert products[0]['Rating'] == "Rating: 4.5 / 5"
    assert products[0]['Colors'] == "3 Colors"
    assert products[0]['Size'] == "Size: M"
    assert products[0]['Gender'] == "Gender: Men"
    assert isinstance(products[0]['Timestamp'], datetime)

    assert products[1]['Title'] == "Awesome Jeans"
    assert products[1]['Price'] == "Price Unavailable" # Correctly captured
    assert products[1]['Rating'] == "Rating: Invalid Rating / 5"

def test_parse_product_data_no_cards(caplog):
    """Test parsing when no product cards are found."""
    with caplog.at_level(logging.WARNING): # Menangkap log WARNING
        products = parse_product_data(MOCK_HTML_PAGE_EMPTY_CONTENT, 1)
    assert len(products) == 0
    assert "No product cards found on page 1" in caplog.text


def test_parse_product_data_parsing_error(caplog):
    """Test parsing with malformed HTML (simulated by passing None)."""
    with caplog.at_level(logging.ERROR): # Menangkap log ERROR
        products = parse_product_data(None, 1) # Ini akan menyebabkan error di BeautifulSoup
    assert len(products) == 0
    assert "Error parsing product data on page 1" in caplog.text


def test_extract_all_products_success(mock_requests_session, monkeypatch):
    """Test the full extraction process across multiple mocked pages."""
    monkeypatch.setattr('utils.extract.MAX_PAGES', 2)
    
    url_page1_mock = BASE_URL 
    url_page2_mock = f"{BASE_URL}/page2"
    
    mock_requests_session.get(url_page1_mock, text=MOCK_HTML_PAGE_1_CONTENT, status_code=200)
    mock_requests_session.get(url_page2_mock, text=MOCK_HTML_PAGE_2_CONTENT, status_code=200)

    df = extract_all_products()
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 3 # 2 dari MOCK_HTML_PAGE_1_CONTENT, 1 dari MOCK_HTML_PAGE_2_CONTENT
    assert df.iloc[0]['Title'] == "Cool T-Shirt"
    assert df.iloc[2]['Title'] == "Another Product"


def test_extract_all_products_fetch_failure_one_page(mock_requests_session, caplog, monkeypatch):
    """Test extraction when one page fails to fetch but others succeed."""
    monkeypatch.setattr('utils.extract.MAX_PAGES', 2)
    url_page1_mock = BASE_URL
    url_page2_mock = f"{BASE_URL}/page2"

    mock_requests_session.get(url_page1_mock, text=MOCK_HTML_PAGE_1_CONTENT, status_code=200)
    mock_requests_session.get(url_page2_mock, status_code=500) # Page 2 fails

    with caplog.at_level(logging.WARNING): # Menangkap log WARNING untuk "Skipping page"
        df = extract_all_products()
    
    assert len(df) == 2 # Hanya produk dari halaman 1 (MOCK_HTML_PAGE_1_CONTENT)
    assert f"Error fetching page 2 from URL {url_page2_mock}" in caplog.text # Cek log error spesifik
    assert f"Skipping page 2 due to fetch error" in caplog.text


def test_extract_all_products_no_data_extracted(mock_requests_session, caplog, monkeypatch):
    """Test extraction when no data is extracted from any page."""
    monkeypatch.setattr('utils.extract.MAX_PAGES', 1)
    url_page1_mock = BASE_URL
    # Mock halaman 1 mengembalikan HTML kosong
    mock_requests_session.get(url_page1_mock, text=MOCK_HTML_PAGE_EMPTY_CONTENT, status_code=200)

    with caplog.at_level(logging.WARNING):
        df = extract_all_products()
    
    assert df.empty
    assert "No products extracted from page 1" in caplog.text # Pesan ketika halaman tidak ada produk
    assert "No data was extracted from any page." in caplog.text # Pesan akhir jika DF kosong