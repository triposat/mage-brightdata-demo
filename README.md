# Mage AI + Bright Data: Enterprise Web Scraping Pipeline

A production-ready data pipeline demonstrating how to combine **Mage AI** (scalable data orchestration) with **Bright Data** (web scraping infrastructure) for enterprise-grade web data collection.

## Overview

This project showcases:
- Building modular ETL pipelines with Mage AI
- Web scraping at scale using Bright Data's proxy infrastructure
- Data transformation and cleaning workflows
- Exporting to PostgreSQL and CSV

### Architecture

```
┌─────────────────┐     ┌──────────────┐     ┌─────────────────┐
│   Bright Data   │────▶│   Mage AI    │────▶│   PostgreSQL    │
│  (Web Scraper)  │     │  (Pipeline)  │     │   (Storage)     │
└─────────────────┘     └──────────────┘     └─────────────────┘
        │                      │
        │                      ▼
        │               ┌──────────────┐
        └──────────────▶│  CSV Export  │
                        └──────────────┘
```

## Why This Stack?

### Mage AI
- **Open-source** data pipeline tool with visual interface
- **Modular blocks** for data loading, transformation, and export
- **AI-powered** code generation and self-healing pipelines
- **Scalable** - handles batch and streaming workloads
- **Enterprise-ready** with Mage Pro for production deployments

### Bright Data
- **150M+ residential IPs** for reliable web access
- **Automatic CAPTCHA solving** and anti-bot bypass
- **Browser API** for JavaScript-heavy sites
- **99.9% uptime** with enterprise SLA
- **Compliance-ready** data collection

## Quick Start

### Prerequisites
- Docker & Docker Compose
- Bright Data account ([Sign up](https://brightdata.com))

### Setup

1. **Clone the repository**
   ```bash
   git clone https://github.com/YOUR_USERNAME/mage-brightdata-demo.git
   cd mage-brightdata-demo
   ```

2. **Configure environment**
   ```bash
   cp .env.example .env
   # Edit .env with your Bright Data API key
   ```

3. **Start the services**
   ```bash
   docker-compose up -d
   ```

4. **Access Mage UI**
   Open http://localhost:6789 in your browser

## Project Structure

```
mage-brightdata-demo/
├── docker-compose.yml          # Docker services configuration
├── requirements.txt            # Python dependencies
├── .env.example               # Environment template
│
└── mage_project/
    ├── data_loaders/
    │   ├── bright_data_scraper.py    # Generic URL scraper
    │   └── ecommerce_scraper.py      # E-commerce product scraper
    │
    ├── transformers/
    │   ├── clean_product_data.py     # Data cleaning & normalization
    │   └── aggregate_stats.py        # Statistical aggregations
    │
    ├── data_exporters/
    │   ├── export_to_postgres.py     # PostgreSQL export
    │   └── export_to_csv.py          # CSV file export
    │
    ├── pipelines/
    │   └── ecommerce_scraping/       # Main pipeline
    │
    ├── utils/
    │   └── bright_data_client.py     # Reusable BD client
    │
    ├── io_config.yaml               # IO configuration
    └── metadata.yaml                # Project metadata
```

## Pipeline Overview

### E-commerce Scraping Pipeline

```
[Scrape Products] ──▶ [Clean Data] ──▶ [Export to PostgreSQL]
                            │
                            ▼
                    [Aggregate Stats] ──▶ [Export to CSV]
```

**Stages:**

1. **Data Loader**: Fetches product pages via Bright Data proxy
2. **Transformer**: Cleans prices, ratings, and text fields
3. **Aggregator**: Computes statistics by domain
4. **Exporters**: Saves to PostgreSQL and CSV

## Configuration

### Pipeline Variables

Configure in Mage UI or `metadata.yaml`:

| Variable | Description | Default |
|----------|-------------|---------|
| `urls` | List of URLs to scrape | `[]` |
| `table_name` | PostgreSQL table name | `products` |
| `output_dir` | CSV output directory | `/home/src/mage_project/output` |

### Environment Variables

| Variable | Required | Description |
|----------|----------|-------------|
| `BRIGHT_DATA_API_KEY` | Yes | Your Bright Data API key |
| `BRIGHT_DATA_ZONE` | No | Proxy zone (default: `web_unlocker`) |
| `POSTGRES_*` | No | Database connection settings |

## Usage Examples

### Scrape Custom URLs

```python
# In Mage UI, set pipeline variables:
{
    "urls": [
        "https://amazon.com/dp/B08N5WRWNW",
        "https://amazon.com/dp/B09V3KXJPB"
    ]
}
```

### Schedule Pipeline

In Mage UI:
1. Go to **Triggers**
2. Create new **Schedule Trigger**
3. Set cron expression (e.g., `0 */6 * * *` for every 6 hours)

### Custom Scraper

Extend the base scraper for specific sites:

```python
# data_loaders/my_custom_scraper.py
from utils.bright_data_client import BrightDataClient

@data_loader
def load_data(*args, **kwargs):
    client = BrightDataClient()
    urls = kwargs.get('urls', [])

    results = []
    for url in urls:
        response = client.fetch_url(url)
        # Custom parsing logic here
        results.append(parse_my_site(response.text))

    return pd.DataFrame(results)
```

## Enterprise Use Cases

| Use Case | Description |
|----------|-------------|
| **Price Monitoring** | Track competitor prices across e-commerce sites |
| **Market Research** | Aggregate product listings and trends |
| **Lead Generation** | Collect business contact information |
| **Content Aggregation** | Build news/article databases |
| **SEO Monitoring** | Track search rankings and SERP data |

## Scaling for Production

### With Mage Pro

- Multi-environment orchestration
- Role-based access control
- Advanced monitoring and alerting
- VPC/on-prem deployment options

### With Bright Data

- Increase concurrent requests with higher-tier plans
- Use dedicated IPs for consistent sessions
- Enable residential proxies for sensitive targets

## Resources

- [Mage AI Documentation](https://docs.mage.ai)
- [Mage AI GitHub](https://github.com/mage-ai/mage-ai)
- [Bright Data Documentation](https://docs.brightdata.com)
- [Bright Data Python SDK](https://github.com/brightdata/sdk-python)

## License

MIT License - see [LICENSE](LICENSE) for details.

---

Built with Mage AI and Bright Data
