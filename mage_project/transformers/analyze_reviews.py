"""
Analyze Amazon Reviews using Google Gemini AI.

Uses multiple Gemini models with automatic rotation to maximize free tier quota:
- gemini-2.5-flash-lite (primary): 15 RPM / 1,000 RPD
- gemini-2.5-flash (secondary): 10 RPM / 250 RPD
- gemini-2.5-pro (tertiary): 5 RPM / 100 RPD

Combined free tier capacity: ~1,350 requests/day.

Falls back to rating-based analysis if all models are exhausted or Gemini is unavailable.
"""

import os
import json
import time
import pandas as pd
from collections import Counter

if 'transformer' not in dir():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in dir():
    from mage_ai.data_preparation.decorators import test


# Models ordered by free tier quota (highest first)
GEMINI_MODELS = [
    "gemini-2.5-flash-lite",  # 15 RPM / 1,000 RPD
    "gemini-2.5-flash",       # 10 RPM / 250 RPD
    "gemini-2.5-pro",         # 5 RPM / 100 RPD
]


def analyze_batch_with_gemini(reviews: list, api_key: str, models: list) -> list:
    """
    Send a batch of reviews to Gemini for AI analysis.
    Automatically rotates through models on 429 rate limit errors.

    Args:
        reviews: List of dicts with 'text' and 'rating' keys
        api_key: Gemini API key
        models: List of model IDs to try in order

    Returns:
        List of dicts with AI-generated analysis per review
    """
    from google import genai

    client = genai.Client(api_key=api_key)

    # Build the prompt
    reviews_text = ""
    for i, r in enumerate(reviews):
        text = str(r.get('text', ''))[:500]
        rating = r.get('rating', 'N/A')
        reviews_text += f"\n[Review {i+1}] (Rating: {rating}/5)\n{text}\n"

    prompt = f"""Analyze these Amazon product reviews. For EACH review, return a JSON array with one object per review containing:

- "index": the review number (1-based)
- "sentiment": exactly one of "Positive", "Neutral", or "Negative" based on the actual text tone (not just the star rating -- a 4-star review with complaints is Negative)
- "issues": array of specific product issues mentioned (e.g., "battery drains fast", "screen scratches easily"). Empty array if none.
- "themes": array of 1-3 topic tags (e.g., "build quality", "value for money", "customer service")
- "summary": one sentence summarizing the review

IMPORTANT: Return ONLY the JSON array, no markdown, no explanation.

Reviews to analyze:
{reviews_text}"""

    # Try each model in order
    for model in models:
        try:
            response = client.models.generate_content(
                model=model,
                contents=prompt
            )

            response_text = response.text.strip()
            if response_text.startswith("```"):
                lines = response_text.split("\n", 1)
                response_text = lines[1] if len(lines) > 1 else response_text
                if response_text.endswith("```"):
                    response_text = response_text[:-3].rstrip()
                elif "```" in response_text:
                    response_text = response_text.rsplit("```", 1)[0]

            results = json.loads(response_text)
            return results

        except Exception as e:
            error_str = str(e)
            if '429' in error_str or 'RESOURCE_EXHAUSTED' in error_str:
                print(f"  {model} rate limited, rotating to next model...")
                continue
            else:
                print(f"  Gemini API error ({model}): {e}")
                return []

    # All models exhausted
    print("  All Gemini models rate limited")
    return []


def fallback_analysis(rating) -> dict:
    """Simple rating-based fallback when Gemini is unavailable."""
    if pd.isna(rating):
        return {'sentiment': 'Unknown', 'issues': [], 'themes': [], 'summary': ''}
    if rating >= 4:
        sentiment = 'Positive'
    elif rating >= 3:
        sentiment = 'Neutral'
    else:
        sentiment = 'Negative'
    return {'sentiment': sentiment, 'issues': [], 'themes': [], 'summary': ''}


@transformer
def transform(data: pd.DataFrame, *args, **kwargs) -> pd.DataFrame:
    """
    Analyze Amazon reviews using Gemini AI with model rotation.

    Adds columns:
    - sentiment: AI-detected sentiment from review text
    - issues: specific product issues mentioned
    - themes: topic tags for the review
    - ai_summary: one-sentence summary of each review

    Falls back to rating-based analysis if Gemini API key is not set.
    """
    if len(data) == 0:
        print("No reviews to analyze")
        return data

    df = data.copy()
    print(f"Analyzing {len(df)} reviews...")
    print("=" * 50)

    # Get Gemini API key
    gemini_key = os.getenv('GEMINI_API_KEY')
    use_ai = gemini_key is not None and gemini_key != ''

    if use_ai:
        models_str = " → ".join(GEMINI_MODELS)
        print(f"Gemini AI models: {models_str}")
        print(f"Auto-rotation on rate limit (combined ~1,350 RPD free tier)")
    else:
        print("GEMINI_API_KEY not set -- falling back to rating-based analysis")
        print("Set GEMINI_API_KEY in .env for AI-powered insights")

    # Clean rating column
    if 'rating' in df.columns:
        df['rating'] = pd.to_numeric(df['rating'], errors='coerce')

    # Find the review text column
    review_col = None
    for col in ['review_text', 'text', 'body', 'content', 'review']:
        if col in df.columns:
            review_col = col
            break

    # Initialize result columns
    df['sentiment'] = 'Unknown'
    df['issues'] = [[] for _ in range(len(df))]
    df['themes'] = [[] for _ in range(len(df))]
    df['ai_summary'] = ''

    if use_ai and review_col:
        batch_size = kwargs.get('ai_batch_size', 10)
        ai_analyzed = 0
        fallback_count = 0

        for batch_start in range(0, len(df), batch_size):
            batch_end = min(batch_start + batch_size, len(df))
            batch = df.iloc[batch_start:batch_end]
            batch_num = batch_start // batch_size + 1

            print(f"\n  Batch {batch_num} (reviews {batch_start+1}-{batch_end})...")

            # Prepare reviews for Gemini
            reviews_for_ai = []
            for _, row in batch.iterrows():
                reviews_for_ai.append({
                    'text': str(row.get(review_col, '')),
                    'rating': row.get('rating', None)
                })

            # Call Gemini with model rotation
            results = analyze_batch_with_gemini(reviews_for_ai, gemini_key, GEMINI_MODELS)

            if results:
                # Match results by index -- handle partial results gracefully
                matched = 0
                for result in results:
                    ri = result.get('index', None)
                    if ri is not None and 1 <= ri <= len(batch):
                        idx = batch_start + ri - 1
                    elif matched < len(batch):
                        idx = batch_start + matched
                    else:
                        continue
                    df.at[df.index[idx], 'sentiment'] = result.get('sentiment', 'Unknown')
                    df.at[df.index[idx], 'issues'] = result.get('issues', [])
                    df.at[df.index[idx], 'themes'] = result.get('themes', [])
                    df.at[df.index[idx], 'ai_summary'] = result.get('summary', '')
                    matched += 1
                ai_analyzed += matched
                # Fallback for any unmatched reviews in this batch
                unmatched = len(batch) - matched
                if unmatched > 0:
                    fallback_count += unmatched
            else:
                # All models exhausted -- fallback for this batch
                print(f"  Using fallback for reviews {batch_start+1}-{batch_end}")
                for i in range(len(batch)):
                    idx = batch_start + i
                    fb = fallback_analysis(batch.iloc[i].get('rating'))
                    df.at[df.index[idx], 'sentiment'] = fb['sentiment']
                fallback_count += len(batch)

            # Small delay between batches to respect rate limits
            time.sleep(1)

        print(f"\n  AI analyzed: {ai_analyzed} reviews")
        if fallback_count > 0:
            print(f"  Fallback (rate limited): {fallback_count} reviews")

    else:
        # Fallback: rating-based sentiment
        if 'rating' in df.columns:
            df['sentiment'] = df['rating'].apply(
                lambda r: fallback_analysis(r)['sentiment']
            )

    # --- Standard analysis (always runs) ---

    # Sentiment distribution
    sentiment_counts = df['sentiment'].value_counts()
    print("\nSentiment Distribution:")
    for sentiment, count in sentiment_counts.items():
        pct = count / len(df) * 100
        print(f"  {sentiment}: {count} ({pct:.1f}%)")

    # AI-extracted issues summary
    all_issues = [issue for issues in df['issues'] for issue in issues]
    if all_issues:
        issue_counts = Counter(all_issues).most_common(10)
        print(f"\nTop Product Issues (AI-detected):")
        for issue, count in issue_counts:
            print(f"  - {issue}: {count} mentions")

    # AI-extracted themes summary
    all_themes = [theme for themes in df['themes'] for theme in themes]
    if all_themes:
        theme_counts = Counter(all_themes).most_common(8)
        print(f"\nReview Themes:")
        for theme, count in theme_counts:
            print(f"  - {theme}: {count}")

    # Review age analysis
    date_col = None
    for col in ['date', 'review_date', 'created_at', 'timestamp']:
        if col in df.columns:
            date_col = col
            break

    if date_col:
        df['review_date'] = pd.to_datetime(df[date_col], errors='coerce', utc=True)
        df['review_age_days'] = (pd.Timestamp.now(tz='UTC') - df['review_date']).dt.days
        df['is_recent'] = df['review_age_days'] <= 30

        recent_count = df['is_recent'].sum()
        print(f"\nRecent reviews (last 30 days): {recent_count}")

        recent_negative = df[df['is_recent'] & (df['sentiment'] == 'Negative')]
        if len(recent_negative) > 0:
            print(f"  Recent NEGATIVE reviews: {len(recent_negative)}")

    # Verified purchase analysis
    if 'verified_purchase' in df.columns:
        verified_count = df['verified_purchase'].sum()
        print(f"\nVerified purchases: {verified_count} ({verified_count/len(df)*100:.1f}%)")

    # Store summary metrics for downstream blocks
    summary = {
        'total_reviews': len(df),
        'avg_rating': df['rating'].mean() if 'rating' in df.columns else None,
        'negative_pct': (df['sentiment'] == 'Negative').mean() * 100 if 'sentiment' in df.columns else None,
        'ai_powered': use_ai,
        'models_used': GEMINI_MODELS if use_ai else [],
        'top_issues': all_issues[:10] if all_issues else [],
    }

    df.attrs['review_summary'] = summary

    print("\n" + "=" * 50)
    print("SUMMARY")
    print(f"  Analysis method: {'Gemini AI (multi-model rotation)' if use_ai else 'Rating-based (fallback)'}")
    if use_ai:
        print(f"  Models: {' → '.join(GEMINI_MODELS)}")
    print(f"  Total reviews: {summary['total_reviews']}")
    if summary['avg_rating']:
        print(f"  Average rating: {summary['avg_rating']:.2f}")
    if summary['negative_pct'] is not None:
        print(f"  Negative reviews: {summary['negative_pct']:.1f}%")
    print("=" * 50)

    return df


@test
def test_output(output, *args) -> None:
    """Validate AI-powered review analysis quality."""
    assert output is not None, 'Output is undefined'
    assert len(output) > 0, 'No reviews after analysis'

    # AI analysis columns must exist
    assert 'sentiment' in output.columns, 'Sentiment column missing -- Gemini analysis failed'
    assert 'issues' in output.columns, 'Issues column missing -- Gemini analysis failed'
    assert 'themes' in output.columns, 'Themes column missing -- Gemini analysis failed'
    assert 'ai_summary' in output.columns, 'AI summary column missing -- Gemini analysis failed'

    # Sentiment values must be valid categories
    valid_sentiments = {'Positive', 'Neutral', 'Negative', 'Unknown'}
    actual_sentiments = set(output['sentiment'].dropna().unique())
    invalid = actual_sentiments - valid_sentiments
    assert len(invalid) == 0, f'Invalid sentiment values found: {invalid}'

    # AI coverage: at least 50% of reviews should have real AI analysis (not "Unknown")
    analyzed = (output['sentiment'] != 'Unknown').sum()
    coverage = analyzed / len(output) * 100
    assert coverage >= 50, f'AI analysis coverage too low: {coverage:.0f}% (expected >= 50%)'
    print(f'AI analysis coverage: {coverage:.0f}% ({analyzed}/{len(output)} reviews)')
