"""
Export Amazon product data to CSV for sharing and backup.
"""

import os
import pandas as pd
from datetime import datetime

if 'data_exporter' not in dir():
    from mage_ai.data_preparation.decorators import data_exporter
if 'test' not in dir():
    from mage_ai.data_preparation.decorators import test


@data_exporter
def export_data(data: pd.DataFrame, *args, **kwargs) -> None:
    """
    Export Amazon products to CSV file.

    Files are timestamped for historical tracking.
    """
    if len(data) == 0:
        print("No data to export")
        return

    output_dir = kwargs.get('output_dir', '/home/src/mage_project/output')
    os.makedirs(output_dir, exist_ok=True)

    # Create timestamped filename
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

    # Get keywords for filename
    if 'search_keyword' in data.columns:
        keywords = data['search_keyword'].dropna().unique()
        keyword_str = '_'.join(keywords[:3]).replace(' ', '-')[:30]
        filename = f"amazon_{keyword_str}_{timestamp}.csv"
    else:
        filename = f"amazon_products_{timestamp}.csv"

    filepath = os.path.join(output_dir, filename)

    # Select columns to export
    export_columns = [
        'title', 'brand', 'asin', 'url',
        'initial_price', 'final_price', 'best_price', 'currency',
        'discount_percent', 'price_tier',
        'rating', 'reviews_count', 'popularity',
        'availability', 'main_category', 'search_keyword',
        'seller_name', 'image_url'
    ]

    available = [c for c in export_columns if c in data.columns]
    df_export = data[available].copy()

    df_export.to_csv(filepath, index=False)
    print(f"Exported {len(df_export)} products to: {filepath}")


@test
def test_output(*args, **kwargs) -> None:
    """Data exporters don't return output, so no validation needed."""
    pass
