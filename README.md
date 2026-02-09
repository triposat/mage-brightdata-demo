# Mage AI + Bright Data: Amazon Product Intelligence

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Mage AI](https://img.shields.io/badge/Mage%20AI-Pipeline-purple)](https://www.mage.ai/)
[![Bright Data](https://img.shields.io/badge/Bright%20Data-Web%20Scraping-blue)](https://brightdata.com/)

A production-ready pipeline that combines **multiple Bright Data APIs** with **Mage AI orchestration** to build an Amazon product intelligence system.

## What This Demo Shows

**The key insight:** This demo uses **TWO different Bright Data APIs** in one pipeline, orchestrated by Mage AI.

| Stage | Bright Data API | What It Does |
|-------|-----------------|--------------|
| 1 | Amazon Products API | Discover products by keyword |
| 2 | Amazon Reviews API | Collect reviews for top products |

**Mage AI orchestrates:**
- Passing data between API calls
- Transforming and enriching data
- Analyzing review sentiment
- Storing to PostgreSQL
- Generating insights + Slack alerts

## Pipeline Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           MAGE AI PIPELINE                                   │
│                                                                              │
│  ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────────────┐  │
│  │ Bright Data     │    │ Process &       │    │ Bright Data             │  │
│  │ Products API    │───▶│ Enrich          │───▶│ Reviews API             │  │
│  │                 │    │                 │    │ (top 5 products)        │  │
│  │ "keyboard" →    │    │ + price tiers   │    │                         │  │
│  │ 15 products     │    │ + discounts     │    │ → 50+ reviews           │  │
│  └─────────────────┘    └─────────────────┘    └───────────┬─────────────┘  │
│                                │                           │                 │
│                                │                           ▼                 │
│                                │                  ┌─────────────────────┐   │
│                                │                  │ Analyze Reviews     │   │
│                                │                  │ + sentiment         │   │
│                                │                  │ + negative keywords │   │
│                                │                  │ + trends            │   │
│                                │                  └───────────┬─────────┘   │
│                                │                              │              │
│                                ▼                              ▼              │
│                    ┌─────────────────────┐      ┌─────────────────────────┐ │
│                    │ PostgreSQL          │      │ Intelligence Report     │ │
│                    │ (products table)    │      │ + Slack Alert           │ │
│                    └─────────────────────┘      └─────────────────────────┘ │
│                                                                              │
└──────────────────────────────────────────────────────────────────────────────┘
```

## Why This Combination Makes Sense

| Without Mage AI | With Mage AI |
|-----------------|--------------|
| Call Products API manually | Automated daily |
| Call Reviews API separately | Chained automatically |
| No data transformation | Price tiers, sentiment analysis |
| No historical tracking | PostgreSQL storage |
| No alerts | Slack notifications |

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

# Optional: Add Slack webhook for alerts
# SLACK_WEBHOOK_URL=https://hooks.slack.com/services/...

# Start services
docker-compose up -d

# Open Mage AI
open http://localhost:6789
```

### Run the Pipeline

1. Navigate to **Pipelines** → **amazon_product_intelligence**
2. Click **Run pipeline once**
3. Watch the stages execute:
   - Products discovered
   - Data transformed
   - Reviews collected
   - Sentiment analyzed
   - Report generated

## Pipeline Variables

Configure without changing code:

```yaml
variables:
  # What to search
  keywords:
    - mechanical keyboard
  limit_per_keyword: 15

  # Review collection
  top_n_products: 5       # Get reviews for top N products
  sort_by: reviews_count  # Sort by this field

  # Alerts
  negative_review_alert_pct: 20  # Alert if >20% negative
```

## Sample Output

### Products Discovered
```
15 products for "mechanical keyboard"
├── Logitech G Pro X     | $149.99 | 4.7★ | 12,456 reviews
├── Keychron K2          | $89.00  | 4.5★ | 8,234 reviews
├── RK Royal Kludge      | $42.99  | 4.4★ | 45,678 reviews
└── ...
```

### Review Analysis
```
Reviews Analyzed: 127
Average Rating: 4.3
Negative Reviews: 12%

Top Negative Keywords:
  - 'stopped working': 8 mentions
  - 'keys stuck': 5 mentions
  - 'cheap plastic': 4 mentions
```

### Slack Alert
> **Amazon Product Intelligence Report**
> - Products Analyzed: 15
> - Reviews Analyzed: 127
> - Avg Rating: 4.3
> - Negative Reviews: 12%

## Project Structure

```
mage-brightdata-demo/
├── docker-compose.yml
├── .env.example
│
└── mage_project/
    ├── data_loaders/
    │   ├── amazon_product_discovery.py   # Products API
    │   └── amazon_reviews_collector.py   # Reviews API
    │
    ├── transformers/
    │   ├── process_amazon_products.py    # Product enrichment
    │   └── analyze_reviews.py            # Sentiment analysis
    │
    ├── data_exporters/
    │   ├── export_amazon_to_postgres.py  # Store products
    │   ├── export_reviews_to_postgres.py # Store reviews
    │   └── generate_insights_report.py   # Report + Slack
    │
    └── pipelines/
        └── amazon_product_intelligence/  # Main pipeline
```

## Bright Data APIs Used

| API | Dataset ID | Purpose |
|-----|------------|---------|
| Amazon Products - Discover by keyword | `gd_l7q7dkf244hwjntr0` | Find products |
| Amazon Reviews - Collect by URL | `gd_le8e811kzy4ggddlq` | Get reviews |

## Enterprise Use Cases

| Use Case | How This Pipeline Helps |
|----------|-------------------------|
| **Product Monitoring** | Track your products' reviews over time |
| **Competitor Analysis** | Monitor competitor product sentiment |
| **Quality Alerts** | Get notified when negative reviews spike |
| **Market Research** | Understand what customers complain about |

## Scheduling

Set up automated daily runs:

1. Go to **Triggers** in Mage AI
2. Create **Schedule** trigger
3. Set to run daily at 6 AM
4. Enable

Now you get daily intelligence automatically.

## Tech Stack

- **[Mage AI](https://www.mage.ai/)** - Pipeline orchestration
- **[Bright Data](https://brightdata.com/)** - Web scraping APIs
- **[PostgreSQL](https://www.postgresql.org/)** - Data storage
- **[Docker](https://www.docker.com/)** - Containerization

## Resources

- [Mage AI Documentation](https://docs.mage.ai)
- [Bright Data Web Scraper API](https://brightdata.com/products/web-scraper)
- [Blog Post Tutorial](blog-post.md)

## License

MIT License - see [LICENSE](LICENSE) for details.

---

Built with [Mage AI](https://www.mage.ai/) and [Bright Data](https://brightdata.com/)
