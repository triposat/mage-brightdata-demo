# Mage AI + Bright Data: Amazon Product Intelligence

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Mage AI](https://img.shields.io/badge/Mage%20AI-Pipeline-purple)](https://www.mage.ai/)
[![Bright Data](https://img.shields.io/badge/Bright%20Data-Web%20Scraping-blue)](https://brightdata.com/)

A production-ready pipeline that chains **two Bright Data Scrapers APIs** with **Mage AI**, **Google Gemini AI**, and **PostgreSQL** to build Amazon product intelligence -- from product discovery to AI-powered review analysis, visualized on a live **Streamlit dashboard**.

## Quick Start

```bash
git clone https://github.com/brightdata/mage-brightdata-demo.git
cd mage-brightdata-demo
cp .env.example .env
# Add your BRIGHT_DATA_API_TOKEN and GEMINI_API_KEY to .env
docker-compose up -d
```

Open `http://localhost:6789`, run the pipeline, then see results at `http://localhost:8501`.

**Prerequisites:** Docker, a [Bright Data](https://brightdata.com) API token, and a [Gemini API key](https://aistudio.google.com/apikey) (free tier works).

## How It Works

The pipeline has 6 blocks across two parallel branches:

| Block | Type | What It Does |
|-------|------|--------------|
| `discover_products` | Data Loader | Bright Data Products API -- discovers products by keyword |
| `process_products` | Transformer | Enriches data with price tiers, discounts, rating categories |
| `export_products_to_db` | Data Exporter | Stores products in PostgreSQL + CSV backup |
| `collect_reviews` | Data Loader | Bright Data Reviews API -- collects reviews for top products |
| `analyze_reviews` | Transformer | Gemini AI sentiment analysis, issue & theme extraction |
| `export_reviews_to_db` | Data Exporter | Stores analyzed reviews in PostgreSQL + CSV backup |

After processing products, the pipeline **branches**: the left branch exports products to PostgreSQL immediately, while the right branch simultaneously collects reviews, runs AI analysis, and exports results. If review collection fails, product data is already safe.

## Sample Output

```
20 products for "laptop stand" + "wireless earbuds"
├── Wireless Earbuds, Bluetooth 5.4  | $19.83 | 4.5★ | 52,455 reviews
├── Nulaxy Aluminum Laptop Stand     | $13.99 | 4.8★ | 36,081 reviews
├── Raycon Everyday Classic Earbuds  | $79.99 | 4.3★ | 29,199 reviews
├── BESIGN LS03 Laptop Stand         | $16.99 | 4.8★ | 22,785 reviews
└── ... (16 more)

Sentiment (Gemini AI): 85% Positive, 9% Neutral, 6% Negative
Top Themes: value for money (53), sound quality (50), battery life (29), comfort (25)
```

## Pipeline Variables

Configure without changing code:

```yaml
variables:
  keywords:
    - laptop stand
    - wireless earbuds
  limit_per_keyword: 10
  top_n_products: 4
  reviews_per_product: 25
  sort_by: reviews_count
```

## Project Structure

```
mage-brightdata-demo/
├── docker-compose.yml          # Mage AI + PostgreSQL + Dashboard
├── dashboard.py                # Streamlit dashboard
├── .env.example                # Environment variable template
├── requirements.txt            # Python dependencies
├── blog-post.md                # Step-by-step tutorial
└── mage_project/
    ├── data_loaders/
    │   ├── amazon_product_discovery.py    # Bright Data Products API
    │   └── amazon_reviews_collector.py    # Bright Data Reviews API
    ├── transformers/
    │   ├── process_amazon_products.py     # Product enrichment
    │   └── analyze_reviews.py             # Gemini AI (3-model rotation)
    ├── data_exporters/
    │   ├── export_products_to_db.py       # Products → PostgreSQL + CSV
    │   └── export_reviews_to_db.py        # Reviews → PostgreSQL + CSV
    └── pipelines/
        └── amazon_product_intelligence/   # Pipeline config
```

## Bright Data APIs Used

| API | Dataset ID | Purpose |
|-----|------------|---------|
| [Amazon Products](https://brightdata.com/products/web-scraper/amazon) | `gd_l7q7dkf244hwjntr0` | Discover products by keyword |
| [Amazon Reviews](https://brightdata.com/products/web-scraper/amazon) | `gd_le8e811kzy4ggddlq` | Collect reviews by product URL |

## Resources

- **[Blog Post Tutorial](blog-post.md)** -- Full walkthrough of how this pipeline works
- [Bright Data Scrapers APIs](https://brightdata.com/products/web-scraper)
- [Mage AI Documentation](https://docs.mage.ai)

## License

MIT -- see [LICENSE](LICENSE) for details.
