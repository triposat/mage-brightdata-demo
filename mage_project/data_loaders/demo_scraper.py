"""
Demo scraper - scrapes sample websites to demonstrate Bright Data integration.
"""

import os
import requests
import pandas as pd
from datetime import datetime

if 'data_loader' not in dir():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in dir():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data(*args, **kwargs) -> pd.DataFrame:
    """
    Demo: Scrape sample websites using Bright Data Web Unlocker.
    """
    api_token = os.getenv('BRIGHT_DATA_API_TOKEN')

    # Demo URLs
    urls = [
        'https://httpbin.org/ip',
        'https://httpbin.org/headers',
        'https://quotes.toscrape.com/',
        'https://books.toscrape.com/',
    ]

    print(f"Scraping {len(urls)} URLs with Bright Data Web Unlocker...")
    print("-" * 50)

    results = []
    for i, url in enumerate(urls, 1):
        print(f"[{i}/{len(urls)}] Scraping: {url}")

        try:
            response = requests.post(
                'https://api.brightdata.com/request',
                headers={
                    'Authorization': f'Bearer {api_token}',
                    'Content-Type': 'application/json'
                },
                json={
                    'zone': 'web_unlocker',
                    'url': url,
                    'format': 'raw'
                },
                timeout=60
            )

            results.append({
                'url': url,
                'status_code': response.status_code,
                'content_length': len(response.text),
                'content_preview': response.text[:500],
                'success': response.status_code == 200,
                'scraped_at': datetime.utcnow().isoformat()
            })

            status = "OK" if response.status_code == 200 else f"FAILED ({response.status_code})"
            print(f"    Status: {status}, Size: {len(response.text)} bytes")

        except Exception as e:
            results.append({
                'url': url,
                'status_code': None,
                'content_length': 0,
                'content_preview': None,
                'success': False,
                'error': str(e),
                'scraped_at': datetime.utcnow().isoformat()
            })
            print(f"    ERROR: {e}")

    print("-" * 50)

    df = pd.DataFrame(results)
    success_count = df['success'].sum()
    print(f"Summary: {success_count}/{len(df)} URLs scraped successfully")

    return df


@test
def test_output(output, *args) -> None:
    assert output is not None, 'Output is undefined'
    assert len(output) > 0, 'No data'
