"""
Chart: Top Amazon Brands by Product Count

Shows which brands have the most products in the scraped data.
"""

from mage_ai.data_preparation.decorators import chart


@chart
def visualize(data, *args, **kwargs):
    """
    Create a bar chart of top brands.
    """
    import plotly.express as px

    if data is None or len(data) == 0:
        return None

    if 'brand' not in data.columns:
        return None

    # Count products per brand
    brand_counts = data['brand'].value_counts().head(15).reset_index()
    brand_counts.columns = ['Brand', 'Product Count']

    # Create bar chart
    fig = px.bar(
        brand_counts,
        x='Product Count',
        y='Brand',
        orientation='h',
        title='Top 15 Brands by Product Count',
        color='Product Count',
        color_continuous_scale='Blues'
    )

    fig.update_layout(
        xaxis_title='Number of Products',
        yaxis_title='Brand',
        yaxis={'categoryorder': 'total ascending'},
        showlegend=False
    )

    return fig
