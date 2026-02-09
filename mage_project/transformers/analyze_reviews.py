"""
Analyze Amazon Reviews: Extract insights from review data.

Performs:
- Sentiment categorization
- Rating trend analysis
- Keyword extraction from review text
- Alert detection (negative review spikes)
"""

import pandas as pd
import re
from datetime import datetime, timedelta

if 'transformer' not in dir():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in dir():
    from mage_ai.data_preparation.decorators import test


def extract_sentiment(rating: float) -> str:
    """Categorize sentiment based on rating."""
    if pd.isna(rating):
        return 'Unknown'
    if rating >= 4:
        return 'Positive'
    elif rating >= 3:
        return 'Neutral'
    else:
        return 'Negative'


def extract_keywords(text: str, negative_keywords: list = None) -> list:
    """Extract important keywords from review text."""
    if not text or not isinstance(text, str):
        return []

    if negative_keywords is None:
        negative_keywords = [
            'broken', 'defective', 'waste', 'terrible', 'awful', 'worst',
            'returned', 'refund', 'disappointed', 'cheap', 'flimsy',
            'stopped working', 'not working', 'doesn\'t work', 'fake',
            'scam', 'poor quality', 'fell apart', 'broke'
        ]

    text_lower = text.lower()
    found = [kw for kw in negative_keywords if kw in text_lower]
    return found


@transformer
def transform(data: pd.DataFrame, *args, **kwargs) -> pd.DataFrame:
    """
    Analyze Amazon reviews and extract insights.

    Adds:
    - sentiment: Positive/Neutral/Negative based on rating
    - negative_keywords: Problem keywords found in review
    - review_age_days: How old the review is
    - is_recent: Whether review is from last 30 days
    - is_verified: Whether purchase is verified

    Also calculates aggregate metrics in DataFrame attrs.
    """
    if len(data) == 0:
        print("No reviews to analyze")
        return data

    df = data.copy()
    print(f"Analyzing {len(df)} reviews...")
    print("=" * 50)

    # 1. Sentiment categorization
    if 'rating' in df.columns:
        df['rating'] = pd.to_numeric(df['rating'], errors='coerce')
        df['sentiment'] = df['rating'].apply(extract_sentiment)

        sentiment_counts = df['sentiment'].value_counts()
        print("\nSentiment Distribution:")
        for sentiment, count in sentiment_counts.items():
            pct = count / len(df) * 100
            print(f"  {sentiment}: {count} ({pct:.1f}%)")

    # 2. Extract negative keywords from review text
    review_col = None
    for col in ['review_text', 'text', 'body', 'content', 'review']:
        if col in df.columns:
            review_col = col
            break

    if review_col:
        df['negative_keywords'] = df[review_col].apply(
            lambda x: extract_keywords(x) if pd.notna(x) else []
        )
        df['has_negative_keywords'] = df['negative_keywords'].apply(len) > 0

        negative_count = df['has_negative_keywords'].sum()
        print(f"\nReviews with negative keywords: {negative_count} ({negative_count/len(df)*100:.1f}%)")

        # Most common negative keywords
        all_keywords = []
        for kws in df['negative_keywords']:
            all_keywords.extend(kws)

        if all_keywords:
            from collections import Counter
            keyword_counts = Counter(all_keywords).most_common(5)
            print("Top negative keywords:")
            for kw, count in keyword_counts:
                print(f"  '{kw}': {count} mentions")

    # 3. Review age analysis
    date_col = None
    for col in ['date', 'review_date', 'created_at', 'timestamp']:
        if col in df.columns:
            date_col = col
            break

    if date_col:
        df['review_date'] = pd.to_datetime(df[date_col], errors='coerce')
        df['review_age_days'] = (datetime.now() - df['review_date']).dt.days
        df['is_recent'] = df['review_age_days'] <= 30

        recent_count = df['is_recent'].sum()
        print(f"\nRecent reviews (last 30 days): {recent_count}")

        # Check for recent negative spike
        if 'sentiment' in df.columns:
            recent_negative = df[(df['is_recent'] == True) & (df['sentiment'] == 'Negative')]
            if len(recent_negative) > 0:
                print(f"⚠️  Recent NEGATIVE reviews: {len(recent_negative)}")

    # 4. Verified purchase analysis
    if 'verified_purchase' in df.columns:
        verified_count = df['verified_purchase'].sum()
        print(f"\nVerified purchases: {verified_count} ({verified_count/len(df)*100:.1f}%)")

    # 5. Product-level aggregation
    if 'asin' in df.columns or 'product_asin' in df.columns:
        asin_col = 'asin' if 'asin' in df.columns else 'product_asin'

        product_stats = df.groupby(asin_col).agg({
            'rating': ['count', 'mean'],
            'sentiment': lambda x: (x == 'Negative').sum() if 'sentiment' in df.columns else 0
        }).round(2)

        print("\nPer-Product Summary:")
        print(product_stats.to_string())

    # 6. Store summary metrics in attrs for downstream use
    summary = {
        'total_reviews': len(df),
        'avg_rating': df['rating'].mean() if 'rating' in df.columns else None,
        'negative_pct': (df['sentiment'] == 'Negative').mean() * 100 if 'sentiment' in df.columns else None,
        'recent_negative_count': len(df[(df.get('is_recent', False) == True) & (df.get('sentiment', '') == 'Negative')]) if 'sentiment' in df.columns and 'is_recent' in df.columns else 0
    }

    df.attrs['review_summary'] = summary

    print("\n" + "=" * 50)
    print("SUMMARY")
    print(f"  Total reviews: {summary['total_reviews']}")
    if summary['avg_rating']:
        print(f"  Average rating: {summary['avg_rating']:.2f}")
    if summary['negative_pct']:
        print(f"  Negative reviews: {summary['negative_pct']:.1f}%")
    print("=" * 50)

    return df


@test
def test_output(output, *args) -> None:
    """Validate review analysis."""
    assert output is not None, 'Output is undefined'
    if len(output) > 0:
        assert 'sentiment' in output.columns, 'Sentiment column missing'
