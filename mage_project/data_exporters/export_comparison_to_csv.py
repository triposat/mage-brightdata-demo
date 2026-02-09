"""
Export multi-platform comparison to CSV.
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
    """Export platform comparison to CSV."""
    if len(data) == 0:
        print("No data to export")
        return

    output_dir = kwargs.get('output_dir', '/home/src/mage_project/output')
    os.makedirs(output_dir, exist_ok=True)

    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

    # Get platforms for filename
    if 'source_platform' in data.columns:
        platforms = '_'.join(data['source_platform'].unique()[:3])
        filename = f"comparison_{platforms}_{timestamp}.csv"
    else:
        filename = f"comparison_{timestamp}.csv"

    filepath = os.path.join(output_dir, filename)

    # Select key columns
    priority_cols = [
        'title', 'source_platform', 'final_price', 'price_tier',
        'price_vs_avg', 'is_best_deal', 'rating', 'reviews_count',
        'search_keyword', 'url', 'asin', 'brand'
    ]

    available = [c for c in priority_cols if c in data.columns]
    other_cols = [c for c in data.columns if c not in priority_cols][:10]

    df_export = data[available + other_cols].copy()
    df_export.to_csv(filepath, index=False)

    print(f"Exported {len(df_export)} products to: {filepath}")

    # Also export a summary
    if 'source_platform' in data.columns:
        summary_file = os.path.join(output_dir, f"comparison_summary_{timestamp}.csv")
        summary = data.groupby('source_platform').agg({
            'final_price': ['count', 'mean', 'min', 'max']
        }).round(2)
        summary.to_csv(summary_file)
        print(f"Exported summary to: {summary_file}")


@test
def test_output(*args, **kwargs) -> None:
    pass
