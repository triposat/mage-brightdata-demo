import pandas as pd
from datetime import datetime

if 'transformer' not in dir():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in dir():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(data: pd.DataFrame, *args, **kwargs) -> pd.DataFrame:
    """
    Aggregate product statistics by domain/category.

    Creates summary statistics useful for price monitoring
    and competitive analysis.
    """
    if len(data) == 0:
        return pd.DataFrame()

    aggregations = {}

    # Price statistics
    if 'price' in data.columns and data['price'].notna().any():
        aggregations['price'] = ['count', 'mean', 'median', 'min', 'max', 'std']

    # Rating statistics
    if 'rating' in data.columns and data['rating'].notna().any():
        aggregations['rating'] = ['mean', 'median', 'min', 'max']

    # Data quality
    if 'data_quality_score' in data.columns:
        aggregations['data_quality_score'] = ['mean']

    if not aggregations:
        print("No numeric columns to aggregate")
        return data

    # Group by domain if available
    group_col = 'domain' if 'domain' in data.columns else None

    if group_col and data[group_col].notna().any():
        stats = data.groupby(group_col).agg(aggregations)
        stats.columns = ['_'.join(col).strip() for col in stats.columns.values]
        stats = stats.reset_index()
    else:
        # Global statistics
        stats = data.agg(aggregations)
        stats = pd.DataFrame(stats).T
        stats.columns = ['_'.join(col).strip() for col in stats.columns.values]
        stats['domain'] = 'all'
        stats = stats.reset_index(drop=True)

    # Add metadata
    stats['total_products'] = len(data)
    stats['aggregated_at'] = datetime.utcnow().isoformat()

    print(f"Aggregated stats for {len(stats)} group(s)")

    return stats


@test
def test_output(output, *args) -> None:
    """Validate aggregation output."""
    assert output is not None, 'Output is undefined'
    if len(output) > 0:
        assert 'aggregated_at' in output.columns, 'Missing timestamp'
