"""
Export Alerts: Send notifications for price changes and data quality issues.

Supports multiple notification channels:
- Console logging (always)
- File export (always)
- Slack webhook (if configured)
- Generic webhook (if configured)
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


def send_slack_notification(webhook_url: str, message: dict) -> bool:
    """Send notification to Slack."""
    try:
        response = requests.post(
            webhook_url,
            json=message,
            headers={'Content-Type': 'application/json'},
            timeout=10
        )
        return response.status_code == 200
    except Exception as e:
        print(f"Slack notification failed: {e}")
        return False


def send_webhook_notification(webhook_url: str, payload: dict) -> bool:
    """Send notification to generic webhook."""
    try:
        response = requests.post(
            webhook_url,
            json=payload,
            headers={'Content-Type': 'application/json'},
            timeout=10
        )
        return response.status_code in [200, 201, 202]
    except Exception as e:
        print(f"Webhook notification failed: {e}")
        return False


def format_slack_message(data: pd.DataFrame, alerts: dict) -> dict:
    """Format alert data for Slack."""
    total_products = len(data)
    price_drops = alerts.get('price_drops', 0)
    price_increases = alerts.get('price_increases', 0)

    # Calculate stats
    avg_price = data['best_price'].mean() if 'best_price' in data.columns else 0
    avg_rating = data['rating'].mean() if 'rating' in data.columns else 0

    # Build message blocks
    blocks = [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": "Amazon Price Tracker Update"
            }
        },
        {
            "type": "section",
            "fields": [
                {"type": "mrkdwn", "text": f"*Products Scraped:*\n{total_products}"},
                {"type": "mrkdwn", "text": f"*Average Price:*\n${avg_price:.2f}"},
                {"type": "mrkdwn", "text": f"*Price Drops:*\n{price_drops}"},
                {"type": "mrkdwn", "text": f"*Price Increases:*\n{price_increases}"}
            ]
        }
    ]

    # Add top alerts if any
    top_alerts = alerts.get('top_alerts', [])
    if top_alerts:
        alert_text = "*Top Price Drops:*\n"
        for alert in top_alerts[:5]:
            alert_text += f"• {alert['title'][:30]}... ${alert['old_price']:.2f} → ${alert['new_price']:.2f} ({alert['change_pct']:.1f}%)\n"

        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": alert_text}
        })

    blocks.append({
        "type": "context",
        "elements": [
            {"type": "mrkdwn", "text": f"Scraped at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | Powered by Mage AI + Bright Data"}
        ]
    })

    return {"blocks": blocks}


@data_exporter
def export_data(data: pd.DataFrame, *args, **kwargs) -> None:
    """
    Export alerts and send notifications.

    Environment variables:
    - SLACK_WEBHOOK_URL: Slack incoming webhook URL
    - ALERT_WEBHOOK_URL: Generic webhook URL for alerts

    Pipeline variables:
    - alert_on_any_change: Send alert even if no significant changes (default: False)
    """
    if len(data) == 0:
        print("No data to analyze for alerts")
        return

    print("=" * 60)
    print("ALERT EXPORT")
    print("=" * 60)

    # Get price alerts from transformer
    alerts = getattr(data, 'attrs', {}).get('price_alerts', {
        'total_alerts': 0,
        'price_drops': 0,
        'price_increases': 0,
        'top_alerts': []
    })

    # Also check the dataframe directly
    if 'price_alert' in data.columns:
        alert_rows = data[data['price_alert'] == True]
        if len(alert_rows) > alerts.get('total_alerts', 0):
            alerts['total_alerts'] = len(alert_rows)
            alerts['price_drops'] = len(alert_rows[alert_rows['alert_type'] == 'PRICE_DROP'])
            alerts['price_increases'] = len(alert_rows[alert_rows['alert_type'] == 'PRICE_INCREASE'])

    total_alerts = alerts.get('total_alerts', 0)
    alert_on_any = kwargs.get('alert_on_any_change', False)

    # Build summary
    summary = {
        'timestamp': datetime.now().isoformat(),
        'total_products': len(data),
        'total_alerts': total_alerts,
        'price_drops': alerts.get('price_drops', 0),
        'price_increases': alerts.get('price_increases', 0),
        'avg_price': float(data['best_price'].mean()) if 'best_price' in data.columns else None,
        'avg_rating': float(data['rating'].mean()) if 'rating' in data.columns else None,
        'keywords': data['search_keyword'].unique().tolist() if 'search_keyword' in data.columns else [],
        'top_alerts': alerts.get('top_alerts', [])
    }

    # 1. Always write to file
    output_dir = '/home/src/mage_project/output'
    os.makedirs(output_dir, exist_ok=True)

    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    alert_file = os.path.join(output_dir, f'alerts_{timestamp}.json')

    with open(alert_file, 'w') as f:
        json.dump(summary, f, indent=2, default=str)

    print(f"Alert summary saved to: {alert_file}")

    # 2. Console summary
    print(f"\nSummary:")
    print(f"  Products: {summary['total_products']}")
    print(f"  Price Drops: {summary['price_drops']}")
    print(f"  Price Increases: {summary['price_increases']}")

    if summary['top_alerts']:
        print(f"\nTop Alerts:")
        for alert in summary['top_alerts'][:5]:
            print(f"  - {alert['title'][:35]}... ({alert['change_pct']:.1f}%)")

    # 3. Slack notification (if configured and alerts exist)
    slack_url = os.getenv('SLACK_WEBHOOK_URL')
    if slack_url and (total_alerts > 0 or alert_on_any):
        print(f"\nSending Slack notification...")
        slack_message = format_slack_message(data, alerts)
        if send_slack_notification(slack_url, slack_message):
            print("  Slack notification sent!")
        else:
            print("  Slack notification failed")
    elif slack_url:
        print("\nNo significant price changes - skipping Slack notification")
    else:
        print("\nSlack not configured (set SLACK_WEBHOOK_URL to enable)")

    # 4. Generic webhook (if configured)
    webhook_url = os.getenv('ALERT_WEBHOOK_URL')
    if webhook_url and (total_alerts > 0 or alert_on_any):
        print(f"\nSending webhook notification...")
        if send_webhook_notification(webhook_url, summary):
            print("  Webhook notification sent!")
        else:
            print("  Webhook notification failed")

    print("=" * 60)


@test
def test_output(*args, **kwargs) -> None:
    """Alert exporter doesn't return data."""
    pass
