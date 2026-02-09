"""
Conditional: Check Data Quality Before Export

Only proceeds to export if data meets quality thresholds.
"""

if 'condition' not in dir():
    from mage_ai.data_preparation.decorators import condition


@condition
def evaluate_condition(data, *args, **kwargs) -> bool:
    """
    Check if data quality is sufficient for export.

    Conditions:
    - At least 10 products scraped
    - At least 50% have valid prices
    - At least 50% have valid ratings
    """
    if data is None or len(data) == 0:
        print("CONDITION FAILED: No data")
        return False

    min_products = kwargs.get('min_products', 10)
    min_price_rate = kwargs.get('min_price_rate', 0.5)
    min_rating_rate = kwargs.get('min_rating_rate', 0.5)

    # Check minimum products
    if len(data) < min_products:
        print(f"CONDITION FAILED: Only {len(data)} products (min: {min_products})")
        return False

    # Check price validity
    if 'best_price' in data.columns:
        price_valid = data['best_price'].notna().sum() / len(data)
        if price_valid < min_price_rate:
            print(f"CONDITION FAILED: Only {price_valid:.1%} have valid prices (min: {min_price_rate:.1%})")
            return False

    # Check rating validity
    if 'rating' in data.columns:
        rating_valid = data['rating'].notna().sum() / len(data)
        if rating_valid < min_rating_rate:
            print(f"CONDITION FAILED: Only {rating_valid:.1%} have valid ratings (min: {min_rating_rate:.1%})")
            return False

    print(f"CONDITION PASSED: {len(data)} products with good data quality")
    return True
