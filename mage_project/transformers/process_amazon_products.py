"""
Process and clean Amazon product data from Bright Data Web Scraper API.
"""

import pandas as pd
import numpy as np

if 'transformer' not in dir():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in dir():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(data: pd.DataFrame, *args, **kwargs) -> pd.DataFrame:
    """
    Clean and enrich Amazon product data.

    Operations:
    - Calculate discount percentage
    - Extract keyword from discovery_input
    - Clean price fields
    - Add price tier categories
    - Filter out unavailable products (optional)
    """
    if len(data) == 0:
        print("No data to transform")
        return data

    df = data.copy()
    print(f"Processing {len(df)} Amazon products...")

    # Extract search keyword from discovery_input
    if 'discovery_input' in df.columns:
        df['search_keyword'] = df['discovery_input'].apply(
            lambda x: x.get('keyword', None) if isinstance(x, dict) else None
        )

    # Clean price fields
    if 'initial_price' in df.columns:
        df['initial_price'] = pd.to_numeric(df['initial_price'], errors='coerce')

    if 'final_price' in df.columns:
        df['final_price'] = pd.to_numeric(df['final_price'], errors='coerce')

    # Calculate discount
    if 'initial_price' in df.columns and 'final_price' in df.columns:
        df['discount_amount'] = df['initial_price'] - df['final_price']
        df['discount_percent'] = (
            (df['discount_amount'] / df['initial_price'] * 100)
            .round(1)
            .fillna(0)
        )
        print(f"  - Products with discount: {(df['discount_percent'] > 0).sum()}")

    # Get best price (final or initial)
    if 'final_price' in df.columns and 'initial_price' in df.columns:
        df['best_price'] = df['final_price'].fillna(df['initial_price'])
    elif 'final_price' in df.columns:
        df['best_price'] = df['final_price']
    elif 'initial_price' in df.columns:
        df['best_price'] = df['initial_price']

    # Price tier categories
    if 'best_price' in df.columns:
        df['price_tier'] = pd.cut(
            df['best_price'],
            bins=[0, 25, 50, 100, 250, float('inf')],
            labels=['Budget (<$25)', 'Mid ($25-50)', 'Premium ($50-100)', 'High-end ($100-250)', 'Luxury ($250+)']
        )

    # Rating categories
    if 'rating' in df.columns:
        df['rating'] = pd.to_numeric(df['rating'], errors='coerce')
        df['rating_category'] = pd.cut(
            df['rating'],
            bins=[0, 3, 4, 4.5, 5],
            labels=['Low (<3)', 'Average (3-4)', 'Good (4-4.5)', 'Excellent (4.5-5)']
        )

    # Reviews count
    if 'reviews_count' in df.columns:
        df['reviews_count'] = pd.to_numeric(df['reviews_count'], errors='coerce')
        df['popularity'] = pd.cut(
            df['reviews_count'],
            bins=[0, 100, 500, 1000, 5000, float('inf')],
            labels=['New (<100)', 'Growing (100-500)', 'Popular (500-1K)', 'Very Popular (1K-5K)', 'Best Seller (5K+)']
        )

    # Extract main category
    if 'categories' in df.columns:
        df['main_category'] = df['categories'].apply(
            lambda x: x[0] if isinstance(x, list) and len(x) > 0 else None
        )

    # Filter options (optional - controlled by pipeline variable)
    filter_unavailable = kwargs.get('filter_unavailable', False)
    if filter_unavailable and 'availability' in df.columns:
        before = len(df)
        df = df[df['availability'].str.contains('In Stock', case=False, na=False)]
        print(f"  - Filtered unavailable: {before} -> {len(df)} products")

    # Summary statistics
    print("-" * 50)
    print("Summary:")

    if 'best_price' in df.columns:
        print(f"  Price range: ${df['best_price'].min():.2f} - ${df['best_price'].max():.2f}")
        print(f"  Average price: ${df['best_price'].mean():.2f}")

    if 'rating' in df.columns:
        print(f"  Average rating: {df['rating'].mean():.2f}★")

    if 'search_keyword' in df.columns:
        print(f"  Products by keyword:")
        for kw, count in df['search_keyword'].value_counts().items():
            print(f"    - {kw}: {count}")

    return df


@test
def test_output(output, *args) -> None:
    """Validate enriched product data quality."""
    assert output is not None, 'Output is undefined'
    assert len(output) > 0, 'Transformer returned empty DataFrame'

    # Enriched columns must exist after transformation
    assert 'best_price' in output.columns, 'best_price column missing -- price calculation failed'
    assert 'price_tier' in output.columns, 'price_tier column missing -- categorization failed'

    # Data quality: best_price should be positive
    valid_prices = pd.to_numeric(output['best_price'], errors='coerce').dropna()
    assert len(valid_prices) > 0, 'No valid prices after transformation'
    assert (valid_prices > 0).all(), f'Found {(valid_prices <= 0).sum()} products with zero/negative price'

    # Data quality: ratings should be 0-5
    if 'rating' in output.columns:
        valid_ratings = pd.to_numeric(output['rating'], errors='coerce').dropna()
        if len(valid_ratings) > 0:
            assert valid_ratings.between(0, 5).all(), 'Found ratings outside 0-5 range'

    # Data quality: discount_percent should be 0-100
    if 'discount_percent' in output.columns:
        discounts = pd.to_numeric(output['discount_percent'], errors='coerce').dropna()
        if len(discounts) > 0:
            assert (discounts >= 0).all(), 'Found negative discount percentages'
