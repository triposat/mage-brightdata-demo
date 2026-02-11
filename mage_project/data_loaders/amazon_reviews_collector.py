"""
Amazon Reviews Collector using Bright Data Web Scraper API.

Collects reviews for specific Amazon product URLs.
Use this AFTER discovering products to get detailed review data.
"""

import os
import requests
import pandas as pd
import time
from typing import List, Dict, Any

if 'data_loader' not in dir():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in dir():
    from mage_ai.data_preparation.decorators import test


REVIEWS_DATASET_ID = "gd_le8e811kzy4ggddlq"
API_BASE = "https://api.brightdata.com/datasets/v3"


def collect_reviews(
    product_urls: List[str],
    api_token: str,
    max_wait_seconds: int = 300
) -> List[Dict[str, Any]]:
    """
    Collect reviews for given Amazon product URLs.

    Args:
        product_urls: List of Amazon product URLs
        api_token: Bright Data API token
        max_wait_seconds: Max time to wait for results

    Returns:
        List of review dictionaries
    """
    if not product_urls:
        print("No product URLs provided")
        return []

    # Prepare input - each URL as separate input
    input_data = [{"url": url} for url in product_urls]

    print(f"Collecting reviews for {len(product_urls)} products...")
    print("-" * 50)

    # Trigger scrape
    response = requests.post(
        f"{API_BASE}/scrape",
        headers={
            "Authorization": f"Bearer {api_token}",
            "Content-Type": "application/json"
        },
        params={
            "dataset_id": REVIEWS_DATASET_ID,
            "notify": "false",
            "include_errors": "true"
        },
        json={"input": input_data}
    )

    result = response.json()

    if "snapshot_id" not in result:
        raise Exception(f"Failed to start review collection: {result}")

    snapshot_id = result["snapshot_id"]
    print(f"Snapshot ID: {snapshot_id}")

    # Poll for results
    start_time = time.time()
    while time.time() - start_time < max_wait_seconds:
        print(f"Waiting for reviews... ({int(time.time() - start_time)}s)")
        time.sleep(20)

        snapshot_response = requests.get(
            f"{API_BASE}/snapshot/{snapshot_id}",
            headers={"Authorization": f"Bearer {api_token}"},
            params={"format": "json"}
        )

        # Handle both JSON array and NDJSON (newline-delimited JSON) responses
        try:
            data = snapshot_response.json()
        except Exception:
            # Bright Data sometimes returns NDJSON
            import json as _json
            lines = snapshot_response.text.strip().split('\n')
            data = []
            for line in lines:
                line = line.strip()
                if line:
                    try:
                        data.append(_json.loads(line))
                    except _json.JSONDecodeError:
                        continue

        if isinstance(data, list):
            reviews = [d for d in data if "error" not in d]
            errors = [d for d in data if "error" in d]

            print(f"Got {len(reviews)} reviews ({len(errors)} errors)")
            return reviews

        if isinstance(data, dict) and data.get("status") in ["running", "closing"]:
            continue

    print(f"Timeout waiting for reviews after {max_wait_seconds}s")
    return []


@data_loader
def load_data(data: pd.DataFrame = None, *args, **kwargs) -> pd.DataFrame:
    """
    Collect Amazon reviews for products.

    Can be used in two ways:
    1. Standalone: Pass product_urls in pipeline variables
    2. Chained: Receives DataFrame from upstream block with 'url' column

    Pipeline variables:
    - product_urls: List of Amazon product URLs (if standalone)
    - top_n_products: How many top products to get reviews for (default: 5)
    - sort_by: Column to sort by when selecting top products (default: reviews_count)
    - reviews_per_product: Max reviews to collect per product (default: 25)
    """
    api_token = os.getenv('BRIGHT_DATA_API_TOKEN')

    if not api_token:
        raise ValueError("BRIGHT_DATA_API_TOKEN not set")

    top_n = kwargs.get('top_n_products', 4)
    sort_by = kwargs.get('sort_by', 'reviews_count')
    reviews_per_product = kwargs.get('reviews_per_product', 25)

    # Get product URLs either from upstream data or pipeline variables
    if data is not None and len(data) > 0 and 'url' in data.columns:
        print(f"Received {len(data)} products from upstream block")

        # Sort and pick top N products
        if sort_by in data.columns:
            data_sorted = data.sort_values(sort_by, ascending=False)
        else:
            data_sorted = data

        top_products = data_sorted.head(top_n)
        product_urls = top_products['url'].dropna().tolist()

        print(f"Selected top {len(product_urls)} products by {sort_by}")
        for _, row in top_products.iterrows():
            title = row.get('title', 'N/A')[:40] if 'title' in row else 'N/A'
            count = row.get('reviews_count', 'N/A')
            print(f"  - {title}... ({count} reviews)")

    else:
        # Standalone mode - get URLs from pipeline variables
        product_urls = kwargs.get('product_urls', [])

        if not product_urls:
            print("No product URLs provided. Either:")
            print("  1. Connect this block to a product discovery block")
            print("  2. Set 'product_urls' pipeline variable")
            return pd.DataFrame()

    # Collect reviews
    reviews = collect_reviews(
        product_urls=product_urls,
        api_token=api_token
    )

    if not reviews:
        print("No reviews collected")
        return pd.DataFrame()

    df = pd.DataFrame(reviews)
    total_collected = len(df)

    # Limit reviews per product to save Gemini API quota
    asin_col = None
    for col in ['asin', 'product_asin', 'url']:
        if col in df.columns:
            asin_col = col
            break

    if asin_col and reviews_per_product > 0:
        df = df.groupby(asin_col).head(reviews_per_product).reset_index(drop=True)
        print(f"Limited to {reviews_per_product} reviews per product: {total_collected} → {len(df)}")

    print("-" * 50)
    print(f"Total reviews: {len(df)}")

    if 'rating' in df.columns:
        print(f"Average rating: {df['rating'].mean():.2f}")
        print(f"Rating distribution:")
        print(df['rating'].value_counts().sort_index(ascending=False).to_string())

    return df


@test
def test_output(output, *args) -> None:
    """Validate Bright Data review collection results."""
    assert output is not None, 'Output is undefined'
    assert len(output) > 0, 'No reviews collected -- check product URLs and BRIGHT_DATA_API_TOKEN'

    # Must have review text for downstream AI analysis
    has_text = any(col in output.columns for col in ['review_text', 'text', 'body', 'content'])
    assert has_text, 'No review text column found -- AI analysis will have nothing to analyze'

    # Ratings should be 1-5
    if 'rating' in output.columns:
        ratings = pd.to_numeric(output['rating'], errors='coerce').dropna()
        if len(ratings) > 0:
            assert ratings.between(1, 5).all(), 'Found ratings outside 1-5 range'

    # Should have reviews from multiple products (if top_n > 1)
    asin_col = next((c for c in ['asin', 'product_asin'] if c in output.columns), None)
    if asin_col:
        n_products = output[asin_col].nunique()
        print(f'Reviews span {n_products} unique products')
