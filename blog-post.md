# Building an Amazon Product Intelligence Pipeline with Mage AI and Bright Data

Building an Amazon scraper that works in production is a different problem from one that works on your laptop. Anti-bot detection evolves weekly. CAPTCHAs appear mid-session. Your IPs get blocked. And the HTML you parsed yesterday changes overnight because Amazon ran an A/B test on product page layouts. Even if you solve all of that, you still need scheduling, retries, error handling, data storage, and a way to actually *see* what you collected.

Bright Data's [Scrapers APIs](https://brightdata.com/products/web-scraper/amazon) skip the scraping layer entirely. You send a keyword or a product URL, you get back structured JSON -- titles, prices, ratings, reviews, seller info -- parsed, normalized, and delivered via API. No proxy infrastructure, no HTML parsing, no anti-bot cat-and-mouse. When Amazon changes their site, Bright Data updates their parsers. Your code doesn't change.

In this tutorial, we'll chain two of these APIs together inside a [Mage AI](https://github.com/mage-ai/mage-ai) pipeline: one to discover products by keyword, another to collect reviews for the top results. Then we'll run Google Gemini over the reviews for sentiment analysis, issue extraction, and theme tagging. Everything lands in PostgreSQL and feeds a live Streamlit dashboard where you can filter the data and ask questions in plain English.

By the end, you'll have a running pipeline you can point at any Amazon product category and get structured, analyzed product data in minutes.

## Table of Contents

- [What We're Building](#what-were-building)
- [Quick Start](#quick-start)
- [How the Pipeline Works](#how-the-pipeline-works)
- [Real Results and Dashboard](#real-results-and-dashboard)
- [Going Further](#going-further)
- [Conclusion](#conclusion)

---

## What We're Building

The pipeline has 6 blocks across two parallel branches:

<!-- SCREENSHOT: Complete pipeline with all 6 blocks connected -->
![Complete Pipeline](screenshots/complete-pipeline.png)
*The branching pipeline in Mage AI -- left branch exports products immediately, right branch collects and analyzes reviews*

Here's the flow:

1. The **Bright Data Products API** discovers products by keyword and returns structured data -- title, price, rating, ASIN, seller info
2. A **transformer** enriches the data with price tiers, discount percentages, and rating categories
3. The pipeline **branches**: the left side exports products to PostgreSQL immediately, while the right side simultaneously collects reviews
4. The **Bright Data Reviews API** takes the top products (by review count) and fetches their actual customer reviews
5. **Google Gemini** analyzes each review for sentiment, issues, and themes
6. Analyzed reviews export to PostgreSQL

**Why not scrape it yourself?** You could build a scraper with Scrapy or Playwright and a proxy network. It would work -- until Amazon updates their page layout, or your IPs get blocked, or CAPTCHAs start appearing on every third request. Bright Data handles anti-bot detection, IP rotation, CAPTCHA solving, and data parsing behind a single API endpoint. You send `{"keyword": "laptop stand"}`, you get back structured JSON. It's hard to justify maintaining your own scraping infrastructure when the API costs a few cents per run.

**Why a pipeline tool instead of a Python script?** Because this pipeline branches -- after processing products, one branch exports to the database while the other simultaneously collects reviews. If review collection fails, your product data is already safe. Mage AI also handles automatic retries, data quality tests that gate every step, and pipeline variables you can change without touching code.

---

## Quick Start

Clone the repo, add your API keys, and run it -- then we'll walk through the interesting parts.

### Prerequisites

- **Docker and Docker Compose** ([get Docker](https://docs.docker.com/get-docker/))
- A **Bright Data** account with API token
- A **Google Gemini** API key (free tier works)
- Basic familiarity with Python and Docker -- no scraping experience needed, that's the point

### Step 1: Clone and Configure

```bash
git clone https://github.com/brightdata/mage-brightdata-demo.git
cd mage-brightdata-demo
cp .env.example .env
```

Edit `.env` with your API keys:

```env
BRIGHT_DATA_API_TOKEN=your_api_token_here
GEMINI_API_KEY=your_gemini_api_key_here
```

**Getting your Bright Data API token:** Sign up at [brightdata.com](https://brightdata.com) (free trial available), then go to [Account Settings](https://brightdata.com/cp/setting/users) and create an API key. The pipeline uses two [Scrapers APIs](https://brightdata.com/pricing/web-scraper) -- pay per record, no plan commitment. A typical run of this demo (20 products + ~100 reviews) costs under a dollar, and the free trial includes credits to run it multiple times.

**Getting your Gemini API key:** Go to [Google AI Studio](https://aistudio.google.com/apikey), sign in, click **Create API Key**. Free tier, no credit card required. The pipeline works without it too -- it falls back to rating-based sentiment -- but you'll miss the AI-powered issue extraction and themes.

### Step 2: Start the Services

```bash
docker-compose up -d
```

This starts three containers:

| Service | URL | Purpose |
|---------|-----|---------|
| **Mage AI** | `http://localhost:6789` | Pipeline builder and orchestration |
| **Streamlit Dashboard** | `http://localhost:8501` | Live data visualization + chat |
| **PostgreSQL** | `localhost:5432` | Data storage |

First run pulls images (~2-3 GB) and installs dependencies -- about 3-5 minutes on a decent connection. After that, starts are near-instant.

<!-- SCREENSHOT: Docker containers running -->
![Docker Services Running](screenshots/docker-services.png)
*All three services running*

### Step 3: Run the Pipeline

Open `http://localhost:6789`, click into the `amazon_product_intelligence` pipeline, then click **Run pipeline once**.

<!-- SCREENSHOT: Mage AI home page -->
![Mage AI Home](screenshots/mage-home.png)
*The Mage AI dashboard*

The pipeline takes about 10-12 minutes end to end. Most of that time is the Bright Data APIs collecting data from Amazon -- the enrichment, AI analysis, and database exports are near-instant. When all 6 blocks turn green, open `http://localhost:8501` to see the dashboard.

<!-- SCREENSHOT: Pipeline completed successfully -->
![Pipeline Complete](screenshots/pipeline-complete.png)
*All 6 blocks green -- pipeline complete*

---

## How the Pipeline Works

Now that you've seen it run, let's walk through the code. The complete source is in `mage_project/` in the [GitHub repo](https://github.com/brightdata/mage-brightdata-demo) -- the snippets below are simplified for readability. We'll focus on the Bright Data integrations and the Gemini analysis, since the enrichment and export blocks are straightforward data engineering.

### Discovering Products with the Bright Data Products API

We send keywords to the Amazon Products API and get back structured data. The API uses an async pattern: trigger the collection, get a snapshot ID, poll until results are ready.

```python
DATASET_ID = "gd_l7q7dkf244hwjntr0"  # Amazon Products
API_BASE = "https://api.brightdata.com/datasets/v3"

# Trigger the collection
response = requests.post(
    f"{API_BASE}/scrape",
    headers={"Authorization": f"Bearer {api_token}",
             "Content-Type": "application/json"},
    params={"dataset_id": DATASET_ID,
            "discover_by": "keyword",
            "limit_per_input": kwargs.get('limit_per_keyword', 10)},
    json={"input": [{"keyword": kw} for kw in keywords]}
)
snapshot_id = response.json()["snapshot_id"]

# Poll until results are ready
data = requests.get(
    f"{API_BASE}/snapshot/{snapshot_id}",
    headers={"Authorization": f"Bearer {api_token}"},
    params={"format": "json"}
).json()
```

Here's what comes back -- one object per product, structured and ready to use:

```json
{
  "title": "BESIGN LS03 Aluminum Laptop Stand",
  "asin": "B07YFY5MM8",
  "url": "https://www.amazon.com/dp/B07YFY5MM8",
  "initial_price": 19.99,
  "final_price": 16.99,
  "currency": "USD",
  "rating": 4.8,
  "reviews_count": 22776,
  "seller_name": "BESIGN",
  "categories": ["Office Products", "Office & School Supplies"],
  "image_url": "https://m.media-amazon.com/images/I/..."
}
```

No HTML to parse. No selectors to maintain. The `kwargs.get('keywords')` call pulls from Mage AI pipeline variables, so you can change search terms from the UI without editing code.

### Chaining a Second API for Reviews

The review collector receives the processed products from the upstream block, sorts by review count, picks the top N, and feeds their Amazon URLs into a second Bright Data API:

```python
REVIEWS_DATASET_ID = "gd_le8e811kzy4ggddlq"  # Amazon Reviews

# Top products from upstream (passed automatically by Mage AI)
top_products = data.sort_values('reviews_count', ascending=False).head(top_n)
product_urls = top_products['url'].dropna().tolist()

# Feed URLs into the Reviews API
response = requests.post(
    f"{API_BASE}/scrape",
    headers={"Authorization": f"Bearer {api_token}",
             "Content-Type": "application/json"},
    params={"dataset_id": REVIEWS_DATASET_ID},
    json={"input": [{"url": url} for url in product_urls]}
)
# Same async poll pattern as products...
```

The first API discovered *what* to look at. The second goes deeper on the best candidates. Mage AI passes the upstream block's output automatically -- no manual wiring.

Both API blocks have retry configuration: if a call fails, Mage AI retries 3 times with a 30-second delay. Every block also has a `@test` function that runs after execution -- if it fails, downstream blocks don't run, so bad data never reaches PostgreSQL.

### AI-Powered Review Analysis with Gemini

Instead of keyword matching (which would flag "not cheap, great quality!" as negative because of the word "cheap"), we use Gemini to understand context. The block processes reviews in batches with a 3-model rotation to stay within free tier limits:

```python
GEMINI_MODELS = ["gemini-2.5-flash-lite", "gemini-2.5-flash", "gemini-2.5-pro"]

prompt = f"""Analyze these reviews. For EACH, return JSON with:
- "sentiment": "Positive", "Neutral", or "Negative"
- "issues": specific product issues mentioned
- "themes": 1-3 topic tags
- "summary": one-sentence summary
Return ONLY JSON.\n\n{reviews_text}"""

for model in models:
    try:
        response = client.models.generate_content(model=model, contents=prompt)
        return json.loads(response.text.strip())
    except Exception as e:
        if '429' in str(e):
            continue  # Rate limited -- rotate to next model
```

When flash-lite (15 requests/min) hits its limit, the code falls back to flash (10/min), then pro (5/min). Gemini returns sentiment based on actual text meaning (not just star count), specific issues like "wobbles on uneven surfaces" or "hinge loosens over time," themes like "build quality" or "value for money," and a one-sentence summary.

If the Gemini key isn't configured, the pipeline still works -- it falls back to rating-based sentiment (4-5 stars = Positive, 3 = Neutral, 1-2 = Negative). You get the data either way.

The remaining blocks -- a transformer for price tiers and discount calculations, and two database exporters with atomic upsert logic -- are standard data engineering. They're in the [repo](https://github.com/brightdata/mage-brightdata-demo) if you want to dig in.

---

## Real Results and Dashboard

Here's what the pipeline produces with the default keywords -- "laptop stand" and "wireless earbuds."

**20 products discovered** across both keywords, ranging from $9.99 to $79.99. You get a mix of budget and mid-range products across two different categories -- the dashboard charts look much more interesting with this variety than with a single keyword.

**100 reviews analyzed by Gemini** across the top 4 products by review count. This is where two keywords pay off: laptop stands tend to be uniformly well-reviewed, but wireless earbuds surface real issues. The sentiment split lands around 85% Positive, 9% Neutral, and 6% Negative -- not a wall of green. The top themes Gemini extracts tell the story: "value for money" (53 mentions), "sound quality" (50), "battery life" (29), "comfort" (25), "connectivity" (19). These are actionable insights that star ratings alone don't give you.

**What the pipeline adds to your raw data:**

| Field | Example | Added By |
|-------|---------|----------|
| `best_price` | $16.99 | Transformer (calculated) |
| `discount_percent` | 15.0% | Transformer (calculated) |
| `price_tier` | Budget (<$25) | Transformer (enriched) |
| `rating_category` | Excellent (4.5-5) | Transformer (enriched) |
| `sentiment` | Negative | Gemini AI |
| `issues` | ["Bluetooth drops connection frequently"] | Gemini AI |
| `themes` | ["connectivity", "battery life"] | Gemini AI |
| `ai_summary` | "Battery lasts only 2 hours despite claims of 8" | Gemini AI |

### The Dashboard

Open `http://localhost:8501` for the Streamlit dashboard. It reads from PostgreSQL and auto-refreshes every 60 seconds.

<!-- SCREENSHOT: Streamlit dashboard showing product intelligence -->
![Dashboard](screenshots/dashboard.png)
*Product intelligence dashboard -- price comparison, sentiment breakdown, AI-detected issues, and theme analysis*

The charts cover price comparison across products, sentiment distribution (color-coded), AI-detected issues ranked by frequency, and a theme treemap showing what customers talk about most. Sidebar filters let you slice by price tier, rating, or sentiment, and every table is sortable.

The dashboard also has a **Chat with Your Data** feature. Ask questions in plain English and Gemini answers using your actual scraped data as context:

> **You:** "Which product has the best value for money?"
>
> **Gemini:** "Based on the data, the BESIGN LS03 Aluminum Laptop Stand offers the best value -- it has the highest review count (22,776) with a 4.8 rating at just $16.99. Its positive sentiment rate is 96%, making it the top-rated product in the dataset."

<!-- SCREENSHOT: Chat with Your Data feature -->
![Chat Feature](screenshots/chat-with-data.png)
*Ask questions about your scraped data in plain English*

---

## Going Further

### Pipeline Variables

Everything is configurable without touching code:

| Variable | What It Controls | Default |
|----------|-----------------|---------|
| `keywords` | Amazon search terms | `["laptop stand", "wireless earbuds"]` |
| `limit_per_keyword` | Products per keyword from Bright Data | `10` |
| `top_n_products` | How many top products get reviews collected | `4` |
| `reviews_per_product` | Max reviews per product | `25` |
| `sort_by` | How to rank products for review selection | `reviews_count` |

Change `keywords` to `["phone case", "USB-C hub"]` and you've got a completely different dataset -- no code changes.

<!-- SCREENSHOT: Pipeline variables configuration -->
![Pipeline Variables](screenshots/pipeline-variables.png)
*Pipeline variables in the Mage AI UI*

### Scheduling

For automated intelligence, go to **Triggers** in the Mage AI sidebar, click **+ New trigger > Schedule**, and set a frequency (`@daily` for midnight UTC, hourly, or custom cron). Each scheduled run updates existing products while preserving historical data, so you build a time series that tracks price changes, rating shifts, and sentiment trends.

### Enterprise Use Cases

**Competitive Price Monitoring** -- Run daily across multiple keywords to build a historical price database. Track how competitor pricing shifts week over week.

**Product Quality Monitoring** -- Track your own product reviews. The AI sentiment analysis catches negative spikes early, before they affect your listing:

```sql
-- Find products with rising negative sentiment this week
SELECT asin, product_name,
    AVG(CASE WHEN sentiment = 'Negative' THEN 1 ELSE 0 END) as negative_rate
FROM amazon_reviews
WHERE scraped_at > NOW() - INTERVAL '7 days'
GROUP BY asin, product_name
HAVING AVG(CASE WHEN sentiment = 'Negative' THEN 1 ELSE 0 END) > 0.2;
```

**Market Research** -- AI-extracted issues reveal pain points that star ratings miss: "wobbles on uneven surfaces" (stability), "hinge loosens over time" (durability), "scratches laptop bottom" (materials). Use these to position your product against specific competitor weaknesses.

---

## Conclusion

We started with a problem: getting structured, reliable Amazon data without building and maintaining scrapers. The Bright Data APIs solved that -- two API calls replace what would otherwise be weeks of scraper development and an ongoing maintenance burden. Mage AI gave us a way to chain those calls into a production pipeline with branching execution, retries, and scheduling. And Gemini added a layer of analysis that star ratings alone can't provide.

**Get started:**

1. [Sign up for Bright Data](https://brightdata.com) (free trial) and grab your API token from [Account Settings](https://brightdata.com/cp/setting/users)
2. Clone: `git clone https://github.com/brightdata/mage-brightdata-demo.git`
3. Add your keys to `.env` and run `docker-compose up -d`
4. Run the pipeline at `http://localhost:6789`
5. See the dashboard at `http://localhost:8501`

The [Scrapers APIs](https://brightdata.com/products/web-scraper/amazon) work with any Amazon marketplace and support dozens of other e-commerce sites. Swap the dataset IDs and you can monitor any product category on any supported platform.

### Resources

- [Demo Repository](https://github.com/brightdata/mage-brightdata-demo)
- [Bright Data Scrapers APIs](https://brightdata.com/products/web-scraper/amazon)
- [Bright Data Pricing](https://brightdata.com/pricing/web-scraper)
- [Mage AI Documentation](https://docs.mage.ai)
- [Mage AI GitHub](https://github.com/mage-ai/mage-ai)

---

*Have questions? Open an issue on the [GitHub repo](https://github.com/brightdata/mage-brightdata-demo).*
