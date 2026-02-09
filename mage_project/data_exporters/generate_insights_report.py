"""
Generate Insights Report: Summary of product and review analysis.

Creates a report with:
- Product summary
- Review sentiment analysis
- Alerts for concerning trends
- Sends to Slack if configured
"""

import os
import json
import requests
import pandas as pd
from datetime import datetime

if 'data_exporter' not in dir():
    from mage_ai.data_preparation.decorators import data_exporter
if 'test' not in dir():
    from mage_ai.data_preparation.decorators import test


def send_slack_report(webhook_url: str, report: dict) -> bool:
    """Send report to Slack."""
    blocks = [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": "Amazon Product Intelligence Report"
            }
        },
        {
            "type": "section",
            "fields": [
                {"type": "mrkdwn", "text": f"*Products Analyzed:*\n{report.get('product_count', 'N/A')}"},
                {"type": "mrkdwn", "text": f"*Reviews Analyzed:*\n{report.get('review_count', 'N/A')}"},
                {"type": "mrkdwn", "text": f"*Avg Rating:*\n{report.get('avg_rating', 'N/A')}"},
                {"type": "mrkdwn", "text": f"*Negative Reviews:*\n{report.get('negative_pct', 'N/A')}%"}
            ]
        }
    ]

    # Add alerts if any
    alerts = report.get('alerts', [])
    if alerts:
        alert_text = "*Alerts:*\n"
        for alert in alerts:
            alert_text += f"• {alert}\n"

        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": alert_text}
        })

    # Add top negative keywords
    top_keywords = report.get('top_negative_keywords', [])
    if top_keywords:
        kw_text = "*Top Negative Keywords:*\n"
        for kw, count in top_keywords[:5]:
            kw_text += f"• '{kw}': {count} mentions\n"

        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": kw_text}
        })

    blocks.append({
        "type": "context",
        "elements": [
            {"type": "mrkdwn", "text": f"Generated at {datetime.now().strftime('%Y-%m-%d %H:%M')} | Mage AI + Bright Data"}
        ]
    })

    try:
        response = requests.post(webhook_url, json={"blocks": blocks}, timeout=10)
        return response.status_code == 200
    except Exception as e:
        print(f"Slack notification failed: {e}")
        return False


@data_exporter
def export_data(data: pd.DataFrame, *args, **kwargs) -> None:
    """Generate and export insights report."""

    print("=" * 60)
    print("AMAZON PRODUCT INTELLIGENCE REPORT")
    print("=" * 60)

    # Get summary from DataFrame attrs (set by analyze_reviews)
    review_summary = getattr(data, 'attrs', {}).get('review_summary', {})

    # Build report
    report = {
        'timestamp': datetime.now().isoformat(),
        'review_count': len(data),
        'avg_rating': round(data['rating'].mean(), 2) if 'rating' in data.columns else None,
        'negative_pct': round((data['sentiment'] == 'Negative').mean() * 100, 1) if 'sentiment' in data.columns else None,
        'alerts': [],
        'top_negative_keywords': []
    }

    # Check for alerts
    alert_threshold = kwargs.get('negative_review_alert_pct', 20)

    if report['negative_pct'] and report['negative_pct'] > alert_threshold:
        report['alerts'].append(
            f"High negative review rate: {report['negative_pct']}% (threshold: {alert_threshold}%)"
        )

    # Recent negative reviews
    if 'is_recent' in data.columns and 'sentiment' in data.columns:
        recent_negative = len(data[(data['is_recent'] == True) & (data['sentiment'] == 'Negative')])
        if recent_negative > 3:
            report['alerts'].append(f"{recent_negative} negative reviews in the last 30 days")

    # Top negative keywords
    if 'negative_keywords' in data.columns:
        from collections import Counter
        all_keywords = []
        for kws in data['negative_keywords']:
            if isinstance(kws, list):
                all_keywords.extend(kws)
        report['top_negative_keywords'] = Counter(all_keywords).most_common(10)

    # Print report
    print(f"\nReviews Analyzed: {report['review_count']}")
    print(f"Average Rating: {report['avg_rating']}")
    print(f"Negative Reviews: {report['negative_pct']}%")

    if report['alerts']:
        print(f"\n⚠️  ALERTS:")
        for alert in report['alerts']:
            print(f"  - {alert}")

    if report['top_negative_keywords']:
        print(f"\nTop Negative Keywords:")
        for kw, count in report['top_negative_keywords'][:5]:
            print(f"  - '{kw}': {count}")

    # Save report to file
    output_dir = '/home/src/mage_project/output'
    os.makedirs(output_dir, exist_ok=True)

    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    report_file = os.path.join(output_dir, f'intelligence_report_{timestamp}.json')

    with open(report_file, 'w') as f:
        json.dump(report, f, indent=2, default=str)

    print(f"\nReport saved to: {report_file}")

    # Send to Slack if configured
    slack_url = os.getenv('SLACK_WEBHOOK_URL')
    if slack_url:
        print("\nSending to Slack...")
        if send_slack_report(slack_url, report):
            print("Slack notification sent!")
        else:
            print("Slack notification failed")

    print("=" * 60)


@test
def test_output(*args, **kwargs) -> None:
    pass
