"""
Chart: Amazon Product Ratings by Search Keyword

Compares average ratings across different search keywords.
"""

from mage_ai.data_preparation.decorators import chart


@chart
def visualize(data, *args, **kwargs):
    """
    Create a bar chart of average ratings by keyword.
    """
    import plotly.express as px
    import pandas as pd

    if data is None or len(data) == 0:
        return None

    if 'rating' not in data.columns or 'search_keyword' not in data.columns:
        return None

    # Calculate average rating per keyword
    avg_ratings = data.groupby('search_keyword').agg({
        'rating': 'mean',
        'asin': 'count'
    }).reset_index()

    avg_ratings.columns = ['Keyword', 'Avg Rating', 'Product Count']
    avg_ratings = avg_ratings.sort_values('Avg Rating', ascending=True)

    # Create bar chart
    fig = px.bar(
        avg_ratings,
        x='Avg Rating',
        y='Keyword',
        orientation='h',
        title='Average Rating by Search Keyword',
        text='Product Count',
        color='Avg Rating',
        color_continuous_scale='Viridis'
    )

    fig.update_layout(
        xaxis_title='Average Rating',
        yaxis_title='Search Keyword',
        showlegend=False
    )

    fig.update_traces(texttemplate='%{text} products', textposition='outside')

    return fig
