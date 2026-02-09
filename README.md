# Mage AI + Bright Data: Enterprise Web Scraping Pipelines

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Mage AI](https://img.shields.io/badge/Mage%20AI-Pipeline-purple)](https://www.mage.ai/)
[![Bright Data](https://img.shields.io/badge/Bright%20Data-Web%20Scraping-blue)](https://brightdata.com/)

Production-ready data pipelines demonstrating enterprise-grade web scraping by combining **Mage AI** (data pipeline orchestration) with **Bright Data** (web scraping infrastructure).

## What This Demo Shows

This isn't just a "hello world" - it's a **real enterprise demo** showcasing:

| Feature | What It Does |
|---------|--------------|
| **Price Change Detection** | Compares new prices against historical data, flags significant changes |
| **Data Quality Gates** | Conditional blocks that stop pipeline if data quality is poor |
| **Alert Notifications** | Slack/webhook notifications for price drops and issues |
| **Multiple Data Sources** | Amazon API + Web Unlocker for custom sites |
| **Historical Tracking** | PostgreSQL storage with timestamps for trend analysis |
| **Configurable Pipelines** | Change keywords, thresholds via variables (no code changes) |

## Pipelines

### 1. Amazon Product Discovery
Scrapes Amazon products using Bright Data's Web Scraper API.

```
┌─────────────┐    ┌──────────────┐    ┌─────────────┐    ┌──────────────┐
│ Data Loader │───▶│ Transformer  │───▶│ Price Change│───▶│ Data Quality │
│ (Bright     │    │ (Clean &     │    │ Detection   │    │ Check        │
│  Data API)  │    │  Enrich)     │    │             │    │ (Conditional)│
└─────────────┘    └──────────────┘    └─────────────┘    └──────┬───────┘
                                                                  │
                            ┌─────────────────────────────────────┼─────────────────┐
                            │                                     │                 │
                            ▼                                     ▼                 ▼
                     ┌─────────────┐                       ┌─────────────┐   ┌─────────────┐
                     │ PostgreSQL  │                       │ CSV Export  │   │   Alerts    │
                     │ Export      │                       │             │   │ (Slack/     │
                     └─────────────┘                       └─────────────┘   │  Webhook)   │
                                                                             └─────────────┘
```

**Features:**
- Searches by keyword, returns 50+ data fields per product
- Calculates discounts, price tiers, rating categories
- Detects price changes vs last scrape (alerts on >10% change)
- Data quality gate blocks export if data is poor
- Exports to PostgreSQL + CSV + sends alerts

### 2. Custom Web Scraper
Scrapes ANY website using Bright Data's Web Unlocker.

```
┌─────────────────┐    ┌─────────────┐
│ Web Unlocker    │───▶│ CSV Export  │
│ (Any URL)       │    │             │
└─────────────────┘    └─────────────┘
```

**Features:**
- Scrapes any URL (not just Amazon)
- Automatic CAPTCHA solving
- Anti-bot bypass
- Custom data extraction

## Quick Start

### Prerequisites
- Docker & Docker Compose
- [Bright Data account](https://brightdata.com) with API token

### Setup

```bash
# Clone repository
git clone https://github.com/luminati-io/mage-brightdata-demo.git
cd mage-brightdata-demo

# Configure environment
cp .env.example .env
# Edit .env and add your BRIGHT_DATA_API_TOKEN

# Start services
docker-compose up -d

# Open Mage AI
open http://localhost:6789
```

### Run the Pipeline

1. Navigate to **Pipelines** → **amazon_product_discovery**
2. Click **Run pipeline once**
3. Watch products flow through all stages

## Project Structure

```
mage-brightdata-demo/
├── docker-compose.yml              # Mage AI + PostgreSQL
├── .env.example                    # Environment template
├── blog-post.md                    # Detailed tutorial
│
└── mage_project/
    ├── data_loaders/
    │   ├── amazon_product_discovery.py   # Bright Data Amazon API
    │   └── web_unlocker_scraper.py       # Bright Data Web Unlocker
    │
    ├── transformers/
    │   ├── process_amazon_products.py    # Data cleaning & enrichment
    │   └── detect_price_changes.py       # Price change detection ⭐
    │
    ├── conditionals/
    │   └── check_data_quality.py         # Data quality gate ⭐
    │
    ├── data_exporters/
    │   ├── export_amazon_to_postgres.py  # PostgreSQL export
    │   ├── export_amazon_to_csv.py       # CSV export
    │   └── export_alerts.py              # Slack/webhook alerts ⭐
    │
    └── pipelines/
        ├── amazon_product_discovery/     # Main pipeline
        └── custom_web_scraper/           # Web Unlocker pipeline
```

## Configuration

### Pipeline Variables

Configure without code changes:

```yaml
variables:
  # What to scrape
  keywords:
    - laptop stand
    - mechanical keyboard
  limit_per_keyword: 25

  # Price change alerts (percentage)
  price_change_threshold: 10

  # Data quality thresholds
  min_products: 10
  min_price_rate: 0.5    # 50% must have prices
  min_rating_rate: 0.5   # 50% must have ratings
```

### Environment Variables

| Variable | Required | Description |
|----------|----------|-------------|
| `BRIGHT_DATA_API_TOKEN` | Yes | Web Scraper API token |
| `BRIGHT_DATA_CUSTOMER_ID` | For Web Unlocker | Customer ID (brd-customer-xxx) |
| `BRIGHT_DATA_ZONE_PASSWORD` | For Web Unlocker | Zone password |
| `SLACK_WEBHOOK_URL` | Optional | Slack notifications |
| `ALERT_WEBHOOK_URL` | Optional | Generic webhook |

## Mage AI Features Demonstrated

| Feature | How It's Used |
|---------|---------------|
| **Visual Pipeline Editor** | Connect blocks visually |
| **Pipeline Variables** | Configure keywords, thresholds without code |
| **Conditional Blocks** | Data quality gates - stop if data is bad |
| **Multiple Transformers** | Chain data processing steps |
| **Multiple Exporters** | Parallel export to Postgres + CSV + Alerts |
| **Scheduled Triggers** | Automated daily/hourly runs |

## Bright Data Features Demonstrated

| Feature | How It's Used |
|---------|---------------|
| **Web Scraper API** | Structured Amazon product data |
| **Web Unlocker** | Custom site scraping with anti-bot bypass |
| **Async/Polling API** | Handle long-running scrapes |
| **72M+ Residential IPs** | Reliable data collection |

## Enterprise Use Cases

| Use Case | How This Demo Helps |
|----------|---------------------|
| **Price Monitoring** | Daily scrapes + price change detection + alerts |
| **Competitor Analysis** | Track competitor products, prices, ratings |
| **Market Research** | Historical data in PostgreSQL for trend analysis |
| **Inventory Intelligence** | Monitor stock levels, seller changes |

## Sample Output

The pipeline produces enriched data with price change detection:

| Field | Example |
|-------|---------|
| `title` | BESIGN Laptop Stand |
| `best_price` | 25.99 |
| `previous_price` | 29.99 |
| `price_change_pct` | -13.3% |
| `price_alert` | True |
| `alert_type` | PRICE_DROP |
| `price_tier` | Mid-Range ($25-50) |

## Tutorial

For a detailed step-by-step guide, see **[blog-post.md](blog-post.md)**.

Covers:
- What is Mage AI (features, comparison to Airflow)
- What is Bright Data (products, how it works)
- Building each block from scratch
- Enterprise patterns and scaling

## Tech Stack

- **[Mage AI](https://www.mage.ai/)** - Open-source data pipeline tool
- **[Bright Data](https://brightdata.com/)** - Web data platform
- **[PostgreSQL](https://www.postgresql.org/)** - Historical data storage
- **[Docker](https://www.docker.com/)** - Containerization

## Resources

- [Mage AI Documentation](https://docs.mage.ai)
- [Bright Data Documentation](https://docs.brightdata.com)
- [Amazon Web Scraper API](https://brightdata.com/products/web-scraper/amazon)
- [Web Unlocker](https://brightdata.com/products/web-unlocker)

## License

MIT License - see [LICENSE](LICENSE) for details.

---

Built with [Mage AI](https://www.mage.ai/) and [Bright Data](https://brightdata.com/)
