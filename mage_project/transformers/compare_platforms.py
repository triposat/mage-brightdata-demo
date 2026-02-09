"""
Compare Prices Across Platforms.

Analyzes products scraped from multiple ecommerce platforms to find:
- Price differences for similar products
- Best deals by platform
- Market positioning insights
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
    Compare and analyze products across platforms.

    Adds:
    - Normalized price field
    - Platform price ranking
    - Price comparison vs average
    - Best deal flag
    """
    if len(data) == 0:
        print("No data to compare")
        return data

    df = data.copy()
    print(f"Comparing {len(df)} products across platforms...")

    # Ensure we have price data
    if 'final_price' not in df.columns:
        # Try to find a price column
        price_cols = [c for c in df.columns if 'price' in c.lower()]
        if price_cols:
            df['final_price'] = pd.to_numeric(df[price_cols[0]], errors='coerce')
        else:
            print("Warning: No price column found")
            df['final_price'] = None

    # Clean price data
    df['final_price'] = pd.to_numeric(df['final_price'], errors='coerce')

    # Get title for matching
    if 'title' not in df.columns:
        title_cols = [c for c in df.columns if 'title' in c.lower() or 'name' in c.lower()]
        if title_cols:
            df['title'] = df[title_cols[0]]

    # Calculate platform statistics
    if 'source_platform' in df.columns and 'final_price' in df.columns:
        print("\n" + "="*50)
        print("PLATFORM COMPARISON")
        print("="*50)

        platform_stats = df.groupby('source_platform').agg({
            'final_price': ['count', 'mean', 'min', 'max', 'std']
        }).round(2)

        print("\nPrice Statistics by Platform:")
        for platform in df['source_platform'].unique():
            platform_data = df[df['source_platform'] == platform]['final_price']
            print(f"\n{platform.upper()}:")
            print(f"  Products: {len(platform_data)}")
            print(f"  Avg Price: ${platform_data.mean():.2f}")
            print(f"  Min Price: ${platform_data.min():.2f}")
            print(f"  Max Price: ${platform_data.max():.2f}")

        # Calculate price index (vs overall average)
        overall_avg = df['final_price'].mean()
        df['price_vs_avg'] = ((df['final_price'] / overall_avg) * 100).round(1)
        df['price_vs_avg_label'] = df['price_vs_avg'].apply(
            lambda x: 'Below Avg' if x < 95 else ('Above Avg' if x > 105 else 'Average')
        )

        # Rank by price within each keyword/category
        if 'search_keyword' in df.columns or 'keyword' in df.columns:
            keyword_col = 'search_keyword' if 'search_keyword' in df.columns else 'keyword'

            # Find cheapest option for each keyword by platform
            df['price_rank'] = df.groupby([keyword_col, 'source_platform'])['final_price'].rank()

            # Best deal flag (cheapest per keyword across platforms)
            idx = df.groupby(keyword_col)['final_price'].idxmin()
            df['is_best_deal'] = False
            df.loc[idx, 'is_best_deal'] = True

            print("\n" + "="*50)
            print("BEST DEALS BY KEYWORD")
            print("="*50)

            for keyword in df[keyword_col].unique():
                best = df[(df[keyword_col] == keyword) & (df['is_best_deal'] == True)]
                if len(best) > 0:
                    row = best.iloc[0]
                    print(f"\n{keyword}:")
                    print(f"  Best: {row.get('title', 'N/A')[:40]}...")
                    print(f"  Platform: {row.get('source_platform', 'N/A')}")
                    print(f"  Price: ${row.get('final_price', 0):.2f}")

    # Add price tier
    if 'final_price' in df.columns:
        df['price_tier'] = pd.cut(
            df['final_price'],
            bins=[0, 25, 50, 100, 250, float('inf')],
            labels=['Budget', 'Mid-Range', 'Premium', 'High-End', 'Luxury']
        )

    # Summary
    print("\n" + "="*50)
    print("SUMMARY")
    print("="*50)
    print(f"Total products: {len(df)}")

    if 'source_platform' in df.columns:
        print(f"Platforms: {df['source_platform'].nunique()}")
        print(f"Products by platform: {df['source_platform'].value_counts().to_dict()}")

    if 'is_best_deal' in df.columns:
        print(f"Best deals identified: {df['is_best_deal'].sum()}")

    return df


@test
def test_output(output, *args) -> None:
    """Validate comparison output."""
    assert output is not None, 'Output is undefined'
