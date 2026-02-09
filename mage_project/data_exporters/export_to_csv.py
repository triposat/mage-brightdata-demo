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
    Export scraped data to CSV file.

    Configuration via pipeline variables:
        output_dir: Directory for CSV files (default: '/home/src/mage_project/output')
        filename_prefix: Prefix for filename (default: 'scraped_products')
        include_timestamp: Add timestamp to filename (default: True)
    """
    if len(data) == 0:
        print("No data to export")
        return

    output_dir = kwargs.get('output_dir', '/home/src/mage_project/output')
    filename_prefix = kwargs.get('filename_prefix', 'scraped_products')
    include_timestamp = kwargs.get('include_timestamp', True)

    # Ensure output directory exists
    os.makedirs(output_dir, exist_ok=True)

    # Build filename
    if include_timestamp:
        timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
        filename = f"{filename_prefix}_{timestamp}.csv"
    else:
        filename = f"{filename_prefix}.csv"

    filepath = os.path.join(output_dir, filename)

    # Export to CSV
    data.to_csv(filepath, index=False)

    print(f"Exported {len(data)} rows to: {filepath}")


@test
def test_output(output, *args) -> None:
    """Verify export completed."""
    pass
