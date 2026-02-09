"""
Bright Data Web Scraper - supports multiple scraping methods.
"""

import os
import requests
import pandas as pd
from typing import List, Dict, Any, Optional
from datetime import datetime
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

if 'data_loader' not in dir():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in dir():
    from mage_ai.data_preparation.decorators import test


def scrape_with_residential_proxy(
    urls: List[str],
    country: Optional[str] = None,
    timeout: int = 60
) -> List[Dict[str, Any]]:
    """
    Scrape URLs using Bright Data Residential Proxy.

    Best for: General scraping, geo-targeting.
    """
    customer_id = os.getenv('BRIGHT_DATA_CUSTOMER_ID', 'hl_49a5c300')
    zone = os.getenv('BRIGHT_DATA_ZONE', 'mls_scraping_proxy')
    password = os.getenv('BRIGHT_DATA_PASSWORD', '8d18168m2hzn')

    username = f"brd-customer-{customer_id}-zone-{zone}"
    if country:
        username += f"-country-{country}"

    proxy_url = f"http://{username}:{password}@brd.superproxy.io:33335"
    proxies = {"http": proxy_url, "https": proxy_url}

    results = []
    for url in urls:
        try:
            response = requests.get(url, proxies=proxies, timeout=timeout, verify=False)
            results.append({
                'url': url,
                'status_code': response.status_code,
                'content': response.text,
                'content_length': len(response.text),
                'success': response.status_code == 200,
                'method': 'residential_proxy',
                'scraped_at': datetime.utcnow().isoformat()
            })
        except Exception as e:
            results.append({
                'url': url,
                'status_code': None,
                'content': None,
                'content_length': 0,
                'success': False,
                'error': str(e),
                'method': 'residential_proxy',
                'scraped_at': datetime.utcnow().isoformat()
            })

    return results


def scrape_with_web_unlocker(
    urls: List[str],
    country: Optional[str] = None,
    timeout: int = 60
) -> List[Dict[str, Any]]:
    """
    Scrape URLs using Bright Data Web Unlocker API.

    Best for: Sites with anti-bot protection, CAPTCHAs.
    """
    api_token = os.getenv('BRIGHT_DATA_API_TOKEN', 'c28d7f9089ed865d80b868c34265f9d79c85ce3381f6c476ee38a5c8c8cb6d3e')
    zone = os.getenv('BRIGHT_DATA_UNLOCKER_ZONE', 'web_unlocker')

    results = []
    for url in urls:
        try:
            payload = {"zone": zone, "url": url, "format": "raw"}
            if country:
                payload["country"] = country

            response = requests.post(
                "https://api.brightdata.com/request",
                headers={
                    "Authorization": f"Bearer {api_token}",
                    "Content-Type": "application/json"
                },
                json=payload,
                timeout=timeout
            )

            results.append({
                'url': url,
                'status_code': response.status_code,
                'content': response.text,
                'content_length': len(response.text),
                'success': response.status_code == 200,
                'method': 'web_unlocker',
                'scraped_at': datetime.utcnow().isoformat()
            })
        except Exception as e:
            results.append({
                'url': url,
                'status_code': None,
                'content': None,
                'content_length': 0,
                'success': False,
                'error': str(e),
                'method': 'web_unlocker',
                'scraped_at': datetime.utcnow().isoformat()
            })

    return results


@data_loader
def load_data(*args, **kwargs) -> pd.DataFrame:
    """
    Scrape URLs using Bright Data.

    Pipeline variables:
        urls: List of URLs to scrape
        method: 'residential_proxy' or 'web_unlocker' (default: web_unlocker)
        country: Optional country code for geo-targeting (e.g., 'us', 'uk')
    """
    urls = kwargs.get('urls', [
        'https://httpbin.org/ip',
        'https://example.com',
    ])

    method = kwargs.get('method', 'web_unlocker')
    country = kwargs.get('country', None)

    print(f"Scraping {len(urls)} URLs using {method}...")

    if method == 'residential_proxy':
        results = scrape_with_residential_proxy(urls, country=country)
    else:
        results = scrape_with_web_unlocker(urls, country=country)

    df = pd.DataFrame(results)

    success_count = df['success'].sum()
    print(f"Completed: {success_count}/{len(df)} successful")

    return df


@test
def test_output(output, *args) -> None:
    """Validate output."""
    assert output is not None, 'Output is undefined'
    assert len(output) > 0, 'No data scraped'
    assert 'url' in output.columns, 'Missing url column'
    assert 'success' in output.columns, 'Missing success column'
