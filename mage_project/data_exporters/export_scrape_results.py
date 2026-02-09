"""
Export Web Unlocker scrape results to CSV.
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
    Export Web Unlocker scrape results to CSV.
    """
    if len(data) == 0:
        print("No data to export")
        return

    output_dir = kwargs.get('output_dir', '/home/src/mage_project/output')
    os.makedirs(output_dir, exist_ok=True)

    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = f"web_scrape_{timestamp}.csv"
    filepath = os.path.join(output_dir, filename)

    # Select columns to export (exclude raw HTML if present)
    export_cols = [c for c in data.columns if c != 'html']
    df_export = data[export_cols].copy()

    df_export.to_csv(filepath, index=False)

    print(f"Exported {len(df_export)} scrape results to: {filepath}")
    print(f"Success rate: {data['success'].sum()}/{len(data)}")


@test
def test_output(*args, **kwargs) -> None:
    """Data exporters don't return output."""
    pass
