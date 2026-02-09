import os
import requests
import pandas as pd
from bs4 import BeautifulSoup
from typing import List, Dict, Any
from datetime import datetime

if 'data_loader' not in dir():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in dir():
    from mage_ai.data_preparation.decorators import test


class EcommerceScraper:
    """
    E-commerce product scraper using Bright Data.

    This scraper extracts product information from e-commerce websites
    using Bright Data's proxy infrastructure for reliable access.
    """

    def __init__(self):
        self.api_key = os.getenv('BRIGHT_DATA_API_KEY')
        self.zone = os.getenv('BRIGHT_DATA_ZONE', 'web_unlocker')

        if not self.api_key:
            raise ValueError("BRIGHT_DATA_API_KEY environment variable is required")

        self.proxy_url = f"http://brd-customer-{self.api_key}-zone-{self.zone}:@brd.superproxy.io:22225"
        self.proxies = {
            'http': self.proxy_url,
            'https': self.proxy_url
        }

    def scrape_product(self, url: str) -> Dict[str, Any]:
        """
        Scrape a single product page.

        Args:
            url: Product page URL

        Returns:
            Dictionary with product data
        """
        try:
            response = requests.get(
                url,
                proxies=self.proxies,
                timeout=60,
                headers={
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
                },
                verify=False
            )

            if response.status_code != 200:
                return self._error_result(url, f"HTTP {response.status_code}")

            return self._parse_product_page(url, response.text)

        except Exception as e:
            return self._error_result(url, str(e))

    def _parse_product_page(self, url: str, html: str) -> Dict[str, Any]:
        """
        Parse product data from HTML.

        Customize this method based on your target website's structure.
        """
        soup = BeautifulSoup(html, 'lxml')

        # Generic selectors - customize for your target site
        product_data = {
            'url': url,
            'scraped_at': datetime.utcnow().isoformat(),
            'success': True,

            # Extract title (common selectors)
            'title': self._extract_text(soup, [
                'h1.product-title',
                'h1[data-testid="product-title"]',
                '#productTitle',
                'h1'
            ]),

            # Extract price
            'price': self._extract_text(soup, [
                '.price-current',
                '[data-testid="price"]',
                '.product-price',
                '#priceblock_ourprice',
                '.price'
            ]),

            # Extract description
            'description': self._extract_text(soup, [
                '.product-description',
                '#productDescription',
                '[data-testid="description"]',
                '.description'
            ]),

            # Extract rating
            'rating': self._extract_text(soup, [
                '.rating',
                '[data-testid="rating"]',
                '.star-rating',
                '#averageCustomerReviews'
            ]),

            # Extract availability
            'availability': self._extract_text(soup, [
                '.availability',
                '#availability',
                '[data-testid="availability"]',
                '.stock-status'
            ]),

            # Store raw HTML for further processing if needed
            'raw_html_length': len(html)
        }

        return product_data

    def _extract_text(self, soup: BeautifulSoup, selectors: List[str]) -> str:
        """Try multiple CSS selectors and return first match."""
        for selector in selectors:
            element = soup.select_one(selector)
            if element:
                return element.get_text(strip=True)
        return None

    def _error_result(self, url: str, error: str) -> Dict[str, Any]:
        """Return error result dictionary."""
        return {
            'url': url,
            'scraped_at': datetime.utcnow().isoformat(),
            'success': False,
            'error': error,
            'title': None,
            'price': None,
            'description': None,
            'rating': None,
            'availability': None,
            'raw_html_length': 0
        }


@data_loader
def load_data(*args, **kwargs) -> pd.DataFrame:
    """
    Scrape e-commerce product data using Bright Data.

    Pass URLs via pipeline variables:
        urls: List of product URLs to scrape
    """
    # Get URLs from pipeline variables or use defaults
    urls = kwargs.get('urls', [
        # Add your product URLs here
        'https://www.example.com/product/1',
    ])

    scraper = EcommerceScraper()
    results = []

    for i, url in enumerate(urls):
        print(f"Scraping [{i+1}/{len(urls)}]: {url}")
        result = scraper.scrape_product(url)
        results.append(result)

    df = pd.DataFrame(results)

    # Summary
    success_count = df['success'].sum()
    print(f"\nScraping complete: {success_count}/{len(df)} successful")

    return df


@test
def test_output(output, *args) -> None:
    """Validate scraped data."""
    assert output is not None, 'Output is undefined'
    assert len(output) > 0, 'No products scraped'
    assert 'title' in output.columns, 'Missing title column'
    assert 'price' in output.columns, 'Missing price column'
