# Mage AI + Bright Data: Amazon Product Scraping Pipeline

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Mage AI](https://img.shields.io/badge/Mage%20AI-Pipeline-purple)](https://www.mage.ai/)
[![Bright Data](https://img.shields.io/badge/Bright%20Data-Web%20Scraping-blue)](https://brightdata.com/)

A production-ready data pipeline demonstrating enterprise-grade web scraping by combining **Mage AI** (data pipeline orchestration) with **Bright Data** (web scraping infrastructure).

<!-- SCREENSHOT: Add pipeline visualization here -->
![Pipeline Overview](screenshots/complete-pipeline.png)

## What This Demo Does

- Scrapes Amazon products by keyword using Bright Data's Web Scraper API
- Transforms and enriches data (price tiers, discounts, ratings)
- Exports to PostgreSQL for historical tracking
- Exports to CSV for data sharing
- Runs on a schedule for automated price monitoring

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
3. Watch products flow through the pipeline

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                         MAGE AI                                  │
│                                                                  │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────────┐  │
│  │ Data Loader  │───▶│ Transformer  │───▶│  Data Exporters  │  │
│  │              │    │              │    │                  │  │
│  │ Bright Data  │    │ Clean &      │    │ • PostgreSQL     │  │
│  │ Amazon API   │    │ Enrich       │    │ • CSV Files      │  │
│  └──────────────┘    └──────────────┘    └──────────────────┘  │
│         │                                                       │
└─────────┼───────────────────────────────────────────────────────┘
          │
          ▼
┌─────────────────────────────────────────────────────────────────┐
│                       BRIGHT DATA                                │
│                                                                  │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────────┐  │
│  │ Web Scraper  │───▶│   72M+ IPs   │───▶│     Amazon       │  │
│  │     API      │    │   Proxies    │    │    Products      │  │
│  └──────────────┘    └──────────────┘    └──────────────────┘  │
│                                                                  │
│  • Structured JSON output    • Auto CAPTCHA solving             │
│  • 99.99% success rate       • No IP blocks                     │
└─────────────────────────────────────────────────────────────────┘
```

## Project Structure

```
mage-brightdata-demo/
├── docker-compose.yml              # Mage AI + PostgreSQL services
├── .env.example                    # Environment template
├── requirements.txt                # Python dependencies
├── blog-post.md                    # Tutorial blog post
│
└── mage_project/
    ├── data_loaders/
    │   └── amazon_product_discovery.py   # Bright Data API integration
    │
    ├── transformers/
    │   └── process_amazon_products.py    # Data cleaning & enrichment
    │
    ├── data_exporters/
    │   ├── export_amazon_to_postgres.py  # PostgreSQL export
    │   └── export_amazon_to_csv.py       # CSV export
    │
    ├── pipelines/
    │   └── amazon_product_discovery/     # Main pipeline config
    │
    └── utils/
        └── bright_data_client.py         # Reusable BD client
```

## Configuration

### Pipeline Variables

Configure what to scrape without code changes:

```yaml
# In pipeline metadata.yaml or Mage UI
variables:
  keywords:
    - laptop stand
    - mechanical keyboard
    - monitor light
  limit_per_keyword: 25
```

### Environment Variables

| Variable | Required | Description |
|----------|----------|-------------|
| `BRIGHT_DATA_API_TOKEN` | Yes | Your Bright Data API token |
| `POSTGRES_HOST` | No | Database host (default: `postgres`) |
| `POSTGRES_USER` | No | Database user (default: `mage`) |
| `POSTGRES_PASSWORD` | No | Database password (default: `mage_password`) |
| `POSTGRES_DB` | No | Database name (default: `scraped_data`) |

## Features

### Mage AI Capabilities Used

| Feature | How It's Used |
|---------|---------------|
| **Visual Pipeline Editor** | Connect blocks visually |
| **Pipeline Variables** | Configure keywords without code |
| **Scheduled Triggers** | Automated daily/hourly runs |
| **Data Quality Tests** | Validate data at each step |
| **Multiple Exporters** | Parallel export to Postgres + CSV |

### Bright Data Capabilities Used

| Feature | How It's Used |
|---------|---------------|
| **Amazon Web Scraper API** | Structured product data extraction |
| **Async/Polling API** | Handle long-running scrapes |
| **Auto-retry & CAPTCHA** | Reliable data collection |

## Sample Output

The pipeline produces enriched product data:

| Field | Description | Example |
|-------|-------------|---------|
| `title` | Product name | "BESIGN Laptop Stand" |
| `brand` | Brand name | "BESIGN" |
| `asin` | Amazon product ID | "B08N5WRWNW" |
| `best_price` | Current price | 25.99 |
| `discount_percent` | Calculated discount | 15.0 |
| `price_tier` | Derived category | "Mid-Range ($25-50)" |
| `rating` | Customer rating | 4.7 |
| `rating_category` | Derived category | "Excellent" |
| `reviews_count` | Number of reviews | 45234 |
| `search_keyword` | Source keyword | "laptop stand" |
| `scraped_at` | Timestamp | 2024-01-15 10:30:00 |

## Enterprise Use Cases

| Use Case | Description |
|----------|-------------|
| **Price Monitoring** | Track competitor prices daily, alert on changes |
| **Market Research** | Analyze pricing trends, popular brands, gaps |
| **Inventory Intelligence** | Monitor stock levels, seller changes |
| **Review Analysis** | Track rating changes, identify quality issues |

## Scaling for Production

### Higher Volume

```yaml
# Increase products per keyword
variables:
  keywords:
    - keyword1
    - keyword2
    # Add more keywords...
  limit_per_keyword: 100  # Up to 1000
```

### More Frequent Updates

```yaml
# Schedule trigger: every 6 hours
triggers:
  - name: price_tracking
    schedule_type: interval
    interval: 21600  # seconds
```

### Multiple Categories

Create separate pipelines for different product categories, each with their own schedule and keywords.

## Tutorial

For a detailed step-by-step guide on building this pipeline, see **[blog-post.md](blog-post.md)**.

The tutorial covers:
- What is Mage AI and why use it
- What is Bright Data and how it works
- Building each pipeline block from scratch
- Configuring schedules and variables
- Enterprise scaling patterns

## Tech Stack

- **[Mage AI](https://www.mage.ai/)** - Open-source data pipeline tool
- **[Bright Data](https://brightdata.com/)** - Web data platform
- **[PostgreSQL](https://www.postgresql.org/)** - Database for historical data
- **[Docker](https://www.docker.com/)** - Containerization

## Resources

- [Mage AI Documentation](https://docs.mage.ai)
- [Mage AI GitHub](https://github.com/mage-ai/mage-ai)
- [Bright Data Documentation](https://docs.brightdata.com)
- [Amazon Web Scraper API](https://brightdata.com/products/web-scraper/amazon)

## License

MIT License - see [LICENSE](LICENSE) for details.

---

Built with [Mage AI](https://www.mage.ai/) and [Bright Data](https://brightdata.com/)
