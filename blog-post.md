# Building Enterprise-Grade Web Scraping Pipelines with Mage AI and Bright Data

Web scraping at scale is hard. Between IP blocks, CAPTCHAs, rate limits, and anti-bot measures, getting reliable data from websites like Amazon requires serious infrastructure. And once you have the data, you need pipelines to transform, store, and analyze it.

In this tutorial, I'll show you how to combine **Mage AI** (a modern data pipeline tool) with **Bright Data** (enterprise web scraping infrastructure) to build a production-ready Amazon product scraping system.

## Why This Combination?

### Bright Data Handles the Hard Scraping Problems

- **Residential proxies** from 72M+ real IPs worldwide
- **Built-in CAPTCHA solving** and anti-bot bypass
- **Pre-built Amazon scraper API** that returns structured JSON
- **99.99% uptime** with automatic retries

### Mage AI Handles the Data Pipeline Problems

- **Visual pipeline editor** - no complex DAG files
- **Built-in scheduling** - run pipelines hourly, daily, or on-demand
- **Data quality tests** - catch issues before they hit production
- **Multiple export targets** - PostgreSQL, CSV, S3, and more

Together, you get reliable scraping + reliable data pipelines = production-ready system.

## Architecture Overview

```
┌─────────────────┐     ┌──────────────────┐     ┌─────────────────┐
│   Mage AI       │────▶│   Bright Data    │────▶│    Amazon       │
│   Pipeline      │     │   Scraper API    │     │    Products     │
└────────┬────────┘     └──────────────────┘     └─────────────────┘
         │
         ▼
┌─────────────────┐
│   Transform     │
│   & Clean       │
└────────┬────────┘
         │
    ┌────┴────┐
    ▼         ▼
┌───────┐ ┌───────┐
│Postgres│ │  CSV  │
│  DB    │ │ Files │
└───────┘ └───────┘
```

The pipeline flow:
1. **Data Loader** - Calls Bright Data's Amazon API with search keywords
2. **Transformer** - Cleans data, calculates discounts, adds price tiers
3. **Data Exporters** - Saves to PostgreSQL (historical tracking) and CSV (sharing)

## Prerequisites

- Docker and Docker Compose installed
- Bright Data account with API token ([sign up here](https://brightdata.com))
- Basic Python knowledge

## Step 1: Project Setup

Clone the repository and set up your environment:

```bash
git clone https://github.com/triposat/mage-brightdata-demo.git
cd mage-brightdata-demo

# Copy environment template
cp .env.example .env
```

Edit `.env` with your Bright Data credentials:

```env
BRIGHT_DATA_API_TOKEN=your_api_token_here
```

## Step 2: Start the Services

```bash
docker-compose up -d
```

This starts:
- **Mage AI** on http://localhost:6789
- **PostgreSQL** for data storage

## Step 3: Understanding the Pipeline

### Data Loader: Amazon Product Discovery

The data loader (`amazon_product_discovery.py`) uses Bright Data's Amazon Web Scraper API:

```python
@data_loader
def load_data(*args, **kwargs):
    api_token = os.getenv('BRIGHT_DATA_API_TOKEN')

    # Get configurable keywords from pipeline variables
    keywords = kwargs.get('keywords', ['laptop stand', 'mechanical keyboard'])
    limit = kwargs.get('limit_per_keyword', 25)

    all_products = []

    for keyword in keywords:
        # Trigger Bright Data scraper
        response = requests.post(
            'https://api.brightdata.com/datasets/v3/scrape',
            headers={'Authorization': f'Bearer {api_token}'},
            params={'dataset_id': 'gd_l7q7dkf244hwjntr0'},  # Amazon dataset
            json=[{'keyword': keyword, 'num_of_results': limit}]
        )

        snapshot_id = response.json()['snapshot_id']

        # Poll for results (async API pattern)
        products = poll_for_results(snapshot_id, api_token)
        all_products.extend(products)

    return pd.DataFrame(all_products)
```

Key points:
- **Async API pattern** - Bright Data returns a `snapshot_id`, you poll until results are ready
- **Pipeline variables** - Keywords and limits are configurable without code changes
- **Structured response** - Returns clean JSON with title, price, rating, reviews, etc.

### Transformer: Data Processing

The transformer (`process_amazon_products.py`) enriches the raw data:

```python
@transformer
def transform(data: pd.DataFrame, *args, **kwargs):
    # Calculate discount percentage
    data['discount_percent'] = (
        (data['initial_price'] - data['final_price']) / data['initial_price'] * 100
    ).round(1)

    # Create price tiers for analysis
    def get_price_tier(price):
        if price < 25: return 'Budget'
        elif price < 50: return 'Mid-Range'
        elif price < 100: return 'Premium'
        else: return 'Luxury'

    data['price_tier'] = data['best_price'].apply(get_price_tier)

    # Categorize ratings
    data['rating_category'] = pd.cut(
        data['rating'],
        bins=[0, 3, 4, 4.5, 5],
        labels=['Poor', 'Average', 'Good', 'Excellent']
    )

    return data
```

This adds:
- **Discount calculations** - Compare initial vs final price
- **Price tiers** - Segment products for analysis
- **Rating categories** - Group by quality level

### Data Exporters

**PostgreSQL Export** - For historical tracking and querying:

```python
@data_exporter
def export_data(data: pd.DataFrame, *args, **kwargs):
    engine = create_engine(connection_string)

    data['scraped_at'] = pd.Timestamp.now()  # Track when scraped

    data.to_sql(
        name='amazon_products',
        con=engine,
        if_exists='append',  # Keep historical data
        index=False
    )
```

**CSV Export** - For sharing and backups:

```python
@data_exporter
def export_data(data: pd.DataFrame, *args, **kwargs):
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = f"amazon_products_{timestamp}.csv"

    data.to_csv(f'/output/{filename}', index=False)
```

## Step 4: Running the Pipeline

1. Open Mage AI at http://localhost:6789
2. Navigate to **Pipelines** → **amazon_product_discovery**
3. Click **Run pipeline once**

You'll see the pipeline execute:
- Data Loader fetches products from Bright Data
- Transformer processes and enriches data
- Exporters save to PostgreSQL and CSV

## Step 5: Setting Up Scheduled Runs

For automated price tracking:

1. Go to **Triggers** in the pipeline
2. Click **Create trigger**
3. Select **Schedule** type
4. Set frequency (e.g., daily at 6 AM)
5. Save and enable

Now your pipeline runs automatically, building a historical database of product prices.

## Step 6: Configuring Pipeline Variables

Change what products to track without modifying code:

1. Open the pipeline's **metadata.yaml**
2. Edit the variables section:

```yaml
variables:
  keywords:
    - laptop stand
    - mechanical keyboard
    - monitor light
    - usb hub
  limit_per_keyword: 50
```

3. Re-run the pipeline with new keywords

## Real Results

Running with 3 keywords and 25 products each, we scraped **75+ Amazon products** in under 2 minutes:

| Metric | Value |
|--------|-------|
| Products scraped | 75 |
| Success rate | 100% |
| Avg price | $45.32 |
| Avg rating | 4.3 stars |
| Avg discount | 12% |

Sample data:

| Product | Price | Rating | Reviews |
|---------|-------|--------|---------|
| BESIGN Laptop Stand | $25.99 | 4.7 | 45,234 |
| Keychron K2 Keyboard | $89.00 | 4.5 | 12,456 |
| BenQ Monitor Light | $109.00 | 4.6 | 8,932 |

## Enterprise Use Cases

This architecture scales for real business applications:

### 1. Competitive Price Monitoring
- Track competitor prices daily
- Alert when prices drop below threshold
- Historical price charts for negotiation

### 2. Market Research
- Analyze pricing trends by category
- Identify gaps in the market
- Track new product launches

### 3. Inventory Intelligence
- Monitor stock availability
- Track seller changes
- Predict stockouts from review velocity

### 4. Review Analysis
- Track rating changes over time
- Identify products losing quality
- Find rising stars before competitors

## Why Bright Data for Enterprise Scraping?

Having built scrapers from scratch before, I can tell you the hidden costs:

| DIY Scraping | Bright Data |
|--------------|-------------|
| Build proxy rotation | Built-in 72M+ IPs |
| Solve CAPTCHAs manually | Auto CAPTCHA solving |
| Handle blocks/bans | 99.99% success rate |
| Parse HTML changes | Structured JSON API |
| Maintain infrastructure | Fully managed |

For enterprise use cases where reliability matters, Bright Data handles the infrastructure so you can focus on the data.

## Why Mage AI for Data Pipelines?

Compared to traditional tools like Airflow:

| Airflow | Mage AI |
|---------|---------|
| Complex DAG Python files | Visual editor |
| Separate scheduler setup | Built-in scheduling |
| Manual testing | Integrated data quality |
| Steep learning curve | Start in minutes |

Mage AI gets you from zero to production pipeline faster.

## Conclusion

Combining Bright Data's scraping infrastructure with Mage AI's pipeline orchestration gives you:

- **Reliable data collection** - No more blocked requests or broken scrapers
- **Automated pipelines** - Set it and forget it scheduling
- **Historical tracking** - Build datasets over time
- **Enterprise scale** - Handle thousands of products across categories

The full code is available on GitHub: [github.com/triposat/mage-brightdata-demo](https://github.com/triposat/mage-brightdata-demo)

## Resources

- [Bright Data Documentation](https://docs.brightdata.com)
- [Mage AI Documentation](https://docs.mage.ai)
- [Amazon Web Scraper API](https://brightdata.com/products/web-scraper/amazon)

---

*Have questions? Find me on [GitHub](https://github.com/triposat) or open an issue on the repo.*
