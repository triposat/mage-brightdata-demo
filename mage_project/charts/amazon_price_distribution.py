"""
Chart: Amazon Product Price Distribution

Visualizes the price distribution of scraped Amazon products.
"""

from mage_ai.data_preparation.decorators import chart


@chart
def visualize(data, *args, **kwargs):
    """
    Create a histogram of product prices.

    This chart shows:
    - Price distribution across all products
    - Helps identify price clusters and outliers
    """
    import plotly.express as px

    if data is None or len(data) == 0:
        return None

    # Use best_price if available, otherwise final_price or initial_price
    price_col = None
    for col in ['best_price', 'final_price', 'initial_price']:
        if col in data.columns:
            price_col = col
            break

    if not price_col:
        return None

    # Filter valid prices
    df = data[data[price_col].notna() & (data[price_col] > 0)].copy()

    if len(df) == 0:
        return None

    # Create histogram
    fig = px.histogram(
        df,
        x=price_col,
        nbins=30,
        title='Amazon Product Price Distribution',
        labels={price_col: 'Price (USD)'},
        color_discrete_sequence=['#6B50D7']
    )

    fig.update_layout(
        xaxis_title='Price (USD)',
        yaxis_title='Number of Products',
        showlegend=False
    )

    return fig
