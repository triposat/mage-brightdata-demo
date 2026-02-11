"""
Amazon Product Intelligence Dashboard
Built with Streamlit + Plotly

Visualizes output from the Mage AI + Bright Data + Gemini AI pipeline.
Reads from PostgreSQL (if available) or CSV files from pipeline output.
"""

import os
import glob
import json
import pandas as pd
import streamlit as st
import plotly.express as px
from collections import Counter

# ── Page Config ──────────────────────────────────────────────────────────────

st.set_page_config(
    page_title="Amazon Product Intelligence",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ── Custom CSS ───────────────────────────────────────────────────────────────

st.markdown("""
<style>
    .main-header {
        font-size: 2rem;
        font-weight: 700;
        margin-bottom: 0;
    }
    .sub-header {
        font-size: 1rem;
        color: #888;
        margin-top: 0;
    }
    .stMetric > div {
        background: #0e1117;
        border-radius: 10px;
        padding: 12px 16px;
        border: 1px solid #262730;
    }
</style>
""", unsafe_allow_html=True)


# ── Data Loading ─────────────────────────────────────────────────────────────

@st.cache_data(ttl=60)
def load_from_postgres():
    """Try loading data from PostgreSQL."""
    try:
        from sqlalchemy import create_engine

        host = os.getenv('POSTGRES_HOST', 'postgres')
        port = os.getenv('POSTGRES_PORT', '5432')
        user = os.getenv('POSTGRES_USER', 'mage')
        password = os.getenv('POSTGRES_PASSWORD', 'mage_password')
        database = os.getenv('POSTGRES_DB', 'scraped_data')

        engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{database}")

        products = None
        reviews = None

        try:
            products = pd.read_sql("SELECT * FROM amazon_products ORDER BY scraped_at DESC", engine)
        except Exception:
            pass

        try:
            reviews = pd.read_sql("SELECT * FROM amazon_reviews ORDER BY scraped_at DESC", engine)
        except Exception:
            pass

        engine.dispose()
        return products, reviews
    except Exception:
        return None, None


@st.cache_data(ttl=60)
def load_from_csv():
    """Load data from CSV files in the output directory."""
    output_dir = os.getenv('OUTPUT_DIR', '/home/src/mage_project/output')

    # Also check local paths for development
    if not os.path.exists(output_dir):
        output_dir = os.path.join(os.path.dirname(__file__), 'mage_project', 'output')

    if not os.path.exists(output_dir):
        return None, None

    csv_files = sorted(glob.glob(os.path.join(output_dir, '*.csv')), reverse=True)

    if not csv_files:
        return None, None

    products = None
    reviews = None

    for f in csv_files:
        try:
            df = pd.read_csv(f)
            # Detect if it's product data or review data
            if 'sentiment' in df.columns or 'ai_summary' in df.columns:
                if reviews is None:
                    reviews = df
            elif 'best_price' in df.columns or 'price_tier' in df.columns:
                if products is None:
                    products = df
            else:
                # Default: treat as product data
                if products is None:
                    products = df
        except Exception:
            continue

    return products, reviews


def load_data():
    """Load data from best available source."""
    # Try PostgreSQL first
    products, reviews = load_from_postgres()

    has_pg_products = products is not None and len(products) > 0
    has_pg_reviews = reviews is not None and len(reviews) > 0

    if has_pg_products or has_pg_reviews:
        return products, reviews, "PostgreSQL"

    # Fallback to CSV
    products, reviews = load_from_csv()

    if products is not None or reviews is not None:
        return products, reviews, "CSV Files"

    return None, None, None


# ── Helper Functions ─────────────────────────────────────────────────────────

def safe_json_parse(val):
    """Parse JSON strings or return as-is if already parsed."""
    if isinstance(val, list):
        return val
    if isinstance(val, str):
        try:
            return json.loads(val.replace("'", '"'))
        except (json.JSONDecodeError, ValueError):
            return []
    return []


def flatten_list_column(series):
    """Flatten a column of lists into a single list."""
    all_items = []
    for val in series.dropna():
        parsed = safe_json_parse(val)
        if isinstance(parsed, list):
            all_items.extend(parsed)
    return all_items


# ── Main App ─────────────────────────────────────────────────────────────────

def main():
    # Header
    st.markdown('<p class="main-header">Amazon Product Intelligence</p>', unsafe_allow_html=True)
    st.markdown(
        '<p class="sub-header">Powered by Mage AI + Bright Data + Gemini AI</p>',
        unsafe_allow_html=True
    )
    st.markdown("---")

    # Load data
    products, reviews, source = load_data()

    if products is None and reviews is None:
        st.warning("No data found. Run the Mage AI pipeline first to generate data.")
        st.markdown("""
        ### Getting Started
        1. Open Mage AI at `http://localhost:6789`
        2. Navigate to **Pipelines** → **amazon_product_intelligence**
        3. Click **Run pipeline once**
        4. Come back here to see the results
        """)
        return

    st.caption(f"Data source: {source}")

    # ── Sidebar Filters ──────────────────────────────────────────────────

    with st.sidebar:
        st.header("Filters")

        if products is not None and len(products) > 0:
            # Keyword filter
            if 'search_keyword' in products.columns:
                keywords = ['All'] + sorted(products['search_keyword'].dropna().unique().tolist())
                selected_keyword = st.selectbox("Search Keyword", keywords)
                if selected_keyword != 'All':
                    products = products[products['search_keyword'] == selected_keyword]

            # Price tier filter
            if 'price_tier' in products.columns:
                tiers = ['All'] + sorted(products['price_tier'].dropna().unique().tolist())
                selected_tier = st.selectbox("Price Tier", tiers)
                if selected_tier != 'All':
                    products = products[products['price_tier'] == selected_tier]

            # Rating filter
            if 'rating' in products.columns:
                products['rating'] = pd.to_numeric(products['rating'], errors='coerce')
                min_rating = st.slider("Minimum Rating", 0.0, 5.0, 0.0, 0.5)
                products = products[products['rating'] >= min_rating]

        if reviews is not None and len(reviews) > 0:
            st.markdown("---")
            st.subheader("Review Filters")

            if 'sentiment' in reviews.columns:
                sentiments = ['All'] + sorted(reviews['sentiment'].dropna().unique().tolist())
                selected_sentiment = st.selectbox("Sentiment", sentiments)
                if selected_sentiment != 'All':
                    reviews = reviews[reviews['sentiment'] == selected_sentiment]

        st.markdown("---")
        if st.button("Refresh Data"):
            st.cache_data.clear()
            st.rerun()

    # ── Product Section ──────────────────────────────────────────────────

    if products is not None and len(products) > 0:

        # KPI Metrics Row
        col1, col2, col3, col4 = st.columns(4)

        with col1:
            st.metric("Products", len(products))

        with col2:
            if 'best_price' in products.columns:
                products['best_price'] = pd.to_numeric(products['best_price'], errors='coerce')
                avg_price = products['best_price'].mean()
                st.metric("Avg Price", f"${avg_price:.2f}")
            elif 'final_price' in products.columns:
                avg_price = pd.to_numeric(products['final_price'], errors='coerce').mean()
                st.metric("Avg Price", f"${avg_price:.2f}")

        with col3:
            if 'rating' in products.columns:
                avg_rating = products['rating'].mean()
                st.metric("Avg Rating", f"{avg_rating:.1f} / 5.0")

        with col4:
            if 'discount_percent' in products.columns:
                products['discount_percent'] = pd.to_numeric(
                    products['discount_percent'], errors='coerce'
                )
                avg_discount = products['discount_percent'].mean()
                st.metric("Avg Discount", f"{avg_discount:.1f}%")

        st.markdown("---")

        # Charts Row 1
        chart_col1, chart_col2 = st.columns(2)

        with chart_col1:
            # Price Comparison
            if 'best_price' in products.columns and 'title' in products.columns:
                st.subheader("Price Comparison")
                price_df = products.dropna(subset=['best_price']).copy()
                price_df['short_title'] = price_df['title'].str[:30] + '...'
                price_df = price_df.sort_values('best_price', ascending=True)
                fig = px.bar(
                    price_df,
                    x='best_price',
                    y='short_title',
                    orientation='h',
                    color_discrete_sequence=['#636EFA'],
                    labels={'best_price': 'Price ($)', 'short_title': ''}
                )
                fig.update_layout(
                    showlegend=False,
                    height=350,
                    margin=dict(l=20, r=20, t=20, b=40),
                    xaxis_title="Price ($)",
                    yaxis_title=""
                )
                st.plotly_chart(fig, width="stretch")

        with chart_col2:
            # Price Tier Breakdown
            if 'price_tier' in products.columns:
                st.subheader("Price Tiers")
                tier_counts = products['price_tier'].value_counts().reset_index()
                tier_counts.columns = ['tier', 'count']
                fig = px.pie(
                    tier_counts,
                    values='count',
                    names='tier',
                    color_discrete_sequence=px.colors.qualitative.Set2,
                    hole=0.4
                )
                fig.update_layout(
                    height=350,
                    margin=dict(l=20, r=20, t=20, b=40)
                )
                st.plotly_chart(fig, width="stretch")

        # Charts Row 2
        chart_col3, chart_col4 = st.columns(2)

        with chart_col3:
            # Rating Comparison
            if 'rating' in products.columns and 'title' in products.columns:
                st.subheader("Rating Comparison")
                rating_df = products.dropna(subset=['rating']).copy()
                rating_df['short_title'] = rating_df['title'].str[:30] + '...'
                rating_df = rating_df.sort_values('rating', ascending=True)
                fig = px.bar(
                    rating_df,
                    x='rating',
                    y='short_title',
                    orientation='h',
                    color='rating',
                    color_continuous_scale=['#EF553B', '#FFA15A', '#00CC96'],
                    labels={'rating': 'Rating', 'short_title': ''}
                )
                fig.update_layout(
                    showlegend=False,
                    height=350,
                    margin=dict(l=20, r=20, t=20, b=40),
                    xaxis_title="Rating",
                    yaxis_title="",
                    coloraxis_showscale=False
                )
                st.plotly_chart(fig, width="stretch")

        with chart_col4:
            # Price vs Rating scatter
            if 'best_price' in products.columns and 'rating' in products.columns:
                st.subheader("Price vs Rating")
                scatter_df = products.dropna(subset=['best_price', 'rating']).copy()
                size_col = 'reviews_count' if 'reviews_count' in scatter_df.columns else None
                fig = px.scatter(
                    scatter_df,
                    x='best_price',
                    y='rating',
                    size=size_col,
                    hover_name='title' if 'title' in scatter_df.columns else None,
                    color_discrete_sequence=['#636EFA'],
                    labels={'best_price': 'Price ($)', 'rating': 'Rating'}
                )
                fig.update_layout(
                    height=350,
                    margin=dict(l=20, r=20, t=20, b=40),
                    xaxis_title="Price ($)",
                    yaxis_title="Rating"
                )
                st.plotly_chart(fig, width="stretch")

        # Product Table
        st.subheader("Product Details")

        display_cols = [
            c for c in [
                'title', 'brand', 'best_price', 'rating', 'reviews_count',
                'price_tier', 'rating_category', 'popularity', 'discount_percent',
                'search_keyword'
            ] if c in products.columns
        ]

        if display_cols:
            st.dataframe(
                products[display_cols].sort_values(
                    'reviews_count' if 'reviews_count' in display_cols else display_cols[0],
                    ascending=False
                ),
                width="stretch",
                height=400
            )

    # ── Review Section ───────────────────────────────────────────────────

    if reviews is not None and len(reviews) > 0:
        st.markdown("---")
        st.header("Review Intelligence (Gemini AI)")

        # Review KPIs
        r_col1, r_col2, r_col3, r_col4 = st.columns(4)

        with r_col1:
            st.metric("Reviews Analyzed", len(reviews))

        with r_col2:
            if 'rating' in reviews.columns:
                reviews['rating'] = pd.to_numeric(reviews['rating'], errors='coerce')
                st.metric("Avg Rating", f"{reviews['rating'].mean():.1f} / 5.0")

        with r_col3:
            if 'sentiment' in reviews.columns:
                neg_pct = (reviews['sentiment'] == 'Negative').mean() * 100
                st.metric("Negative Reviews", f"{neg_pct:.1f}%")

        with r_col4:
            if 'sentiment' in reviews.columns:
                pos_pct = (reviews['sentiment'] == 'Positive').mean() * 100
                st.metric("Positive Reviews", f"{pos_pct:.1f}%")

        st.markdown("---")

        # Sentiment + Issues Row
        sent_col, issues_col = st.columns(2)

        with sent_col:
            # Sentiment Breakdown
            if 'sentiment' in reviews.columns:
                st.subheader("Sentiment Breakdown")
                sent_counts = reviews['sentiment'].value_counts().reset_index()
                sent_counts.columns = ['sentiment', 'count']

                color_map = {
                    'Positive': '#00CC96',
                    'Neutral': '#FFA15A',
                    'Negative': '#EF553B',
                    'Unknown': '#999999'
                }

                fig = px.bar(
                    sent_counts,
                    x='sentiment',
                    y='count',
                    color='sentiment',
                    color_discrete_map=color_map
                )
                fig.update_layout(
                    showlegend=False,
                    height=350,
                    margin=dict(l=20, r=20, t=20, b=40),
                    xaxis_title="",
                    yaxis_title="Count"
                )
                st.plotly_chart(fig, width="stretch")

        with issues_col:
            # Top AI-Detected Issues
            if 'issues' in reviews.columns:
                st.subheader("Top Product Issues (AI-Detected)")

                all_issues = flatten_list_column(reviews['issues'])

                if all_issues:
                    issue_counts = Counter(all_issues).most_common(10)
                    issue_df = pd.DataFrame(issue_counts, columns=['issue', 'mentions'])

                    fig = px.bar(
                        issue_df,
                        x='mentions',
                        y='issue',
                        orientation='h',
                        color_discrete_sequence=['#EF553B']
                    )
                    fig.update_layout(
                        showlegend=False,
                        height=350,
                        margin=dict(l=20, r=20, t=20, b=40),
                        xaxis_title="Mentions",
                        yaxis_title=""
                    )
                    st.plotly_chart(fig, width="stretch")
                else:
                    st.info("No product issues detected in reviews.")

        # Themes
        if 'themes' in reviews.columns:
            st.subheader("Review Themes")

            all_themes = flatten_list_column(reviews['themes'])

            if all_themes:
                theme_counts = Counter(all_themes).most_common(15)
                theme_df = pd.DataFrame(theme_counts, columns=['theme', 'count'])

                fig = px.treemap(
                    theme_df,
                    path=['theme'],
                    values='count',
                    color='count',
                    color_continuous_scale='Teal'
                )
                fig.update_layout(
                    height=350,
                    margin=dict(l=20, r=20, t=20, b=20)
                )
                st.plotly_chart(fig, width="stretch")

        # AI Summaries
        if 'ai_summary' in reviews.columns:
            st.subheader("AI Review Summaries")

            summaries = reviews[reviews['ai_summary'].notna() & (reviews['ai_summary'] != '')]

            if len(summaries) > 0:
                # Show grouped by sentiment
                for sentiment in ['Negative', 'Neutral', 'Positive']:
                    sent_reviews = summaries[summaries['sentiment'] == sentiment] if 'sentiment' in summaries.columns else summaries.head(0)
                    if len(sent_reviews) > 0:
                        label = sentiment
                        if sentiment == 'Negative':
                            label = "Negative Reviews"
                        elif sentiment == 'Positive':
                            label = "Positive Reviews"
                        else:
                            label = "Neutral Reviews"

                        with st.expander(f"{label} ({len(sent_reviews)})", expanded=(sentiment == 'Negative')):
                            for _, row in sent_reviews.head(10).iterrows():
                                rating = row.get('rating', 'N/A')
                                summary = row.get('ai_summary', '')
                                issues = safe_json_parse(row.get('issues', []))
                                issue_str = f" | Issues: {', '.join(issues)}" if issues else ""
                                st.markdown(f"- **{rating}/5** -- {summary}{issue_str}")
            else:
                st.info("No AI summaries available. Make sure GEMINI_API_KEY is set.")

        # Full Review Table
        st.subheader("Review Data")

        review_display = [
            c for c in [
                'rating', 'sentiment', 'ai_summary', 'issues', 'themes',
                'review_text', 'text', 'body',
                'is_verified', 'verified_purchase',
                'review_posted_date', 'date'
            ] if c in reviews.columns
        ]

        if review_display:
            st.dataframe(
                reviews[review_display].head(100),
                width="stretch",
                height=400
            )

    # ── Chat with Your Data ─────────────────────────────────────────────

    st.markdown("---")
    st.header("Chat with Your Data")

    gemini_key = os.getenv('GEMINI_API_KEY')

    if not gemini_key:
        st.info("Set GEMINI_API_KEY in .env to enable the chat feature.")
    else:
        st.caption(
            "Ask questions about your scraped products and reviews. "
            "Gemini AI answers using your actual data as context."
        )

        # Build data context string (keep it concise to fit in prompt)
        context_parts = []

        if products is not None and len(products) > 0:
            # Only include useful columns in context (not all 90+ Bright Data fields)
            summary_cols = [
                c for c in ['title', 'brand', 'best_price', 'rating', 'reviews_count',
                             'price_tier', 'discount_percent', 'search_keyword', 'popularity']
                if c in products.columns
            ]
            if summary_cols:
                prod_summary = products[summary_cols].describe(include='all').to_string()
                prod_sample = products[summary_cols].head(20).to_string(index=False)
            else:
                prod_summary = products.describe().to_string()
                prod_sample = products.head(20).to_string(index=False)
            context_parts.append(
                f"PRODUCT DATA ({len(products)} products):\n"
                f"Statistics:\n{prod_summary}\n\n"
                f"All products:\n{prod_sample}"
            )

        if reviews is not None and len(reviews) > 0:
            # Sentiment distribution
            sent_dist = reviews['sentiment'].value_counts().to_string() if 'sentiment' in reviews.columns else "N/A"
            # Issues
            all_issues = flatten_list_column(reviews.get('issues', pd.Series())) if 'issues' in reviews.columns else []
            top_issues = Counter(all_issues).most_common(15) if all_issues else []
            issues_str = "\n".join(f"  - {issue}: {count}" for issue, count in top_issues)
            # Themes
            all_themes = flatten_list_column(reviews.get('themes', pd.Series())) if 'themes' in reviews.columns else []
            top_themes = Counter(all_themes).most_common(15) if all_themes else []
            themes_str = "\n".join(f"  - {theme}: {count}" for theme, count in top_themes)
            # AI summaries sample
            summaries_sample = ""
            if 'ai_summary' in reviews.columns:
                valid_summaries = reviews[reviews['ai_summary'].notna() & (reviews['ai_summary'] != '')]
                if len(valid_summaries) > 0:
                    sample_cols_r = [
                        c for c in ['rating', 'sentiment', 'ai_summary', 'issues']
                        if c in valid_summaries.columns
                    ]
                    summaries_sample = valid_summaries[sample_cols_r].head(25).to_string(index=False)

            context_parts.append(
                f"REVIEW DATA ({len(reviews)} reviews):\n"
                f"Sentiment distribution:\n{sent_dist}\n\n"
                f"Top product issues (AI-detected):\n{issues_str}\n\n"
                f"Top review themes:\n{themes_str}\n\n"
                f"Sample reviews with AI analysis:\n{summaries_sample}"
            )

        data_context = "\n\n---\n\n".join(context_parts)

        # Chat history in session state
        if "chat_messages" not in st.session_state:
            st.session_state.chat_messages = []

        # Display chat history
        for msg in st.session_state.chat_messages:
            with st.chat_message(msg["role"]):
                st.markdown(msg["content"])

        # Check if a pending question needs answering (from suggestion buttons)
        pending_question = st.session_state.get("pending_question", None)

        # Chat input
        user_question = st.chat_input("Ask about your products or reviews...")

        # Use pending question if no new input
        if not user_question and pending_question:
            user_question = pending_question
            st.session_state.pending_question = None

        if user_question:
            # Show user message
            if not pending_question:
                st.session_state.chat_messages.append({"role": "user", "content": user_question})
            with st.chat_message("user"):
                st.markdown(user_question)

            # Build prompt
            prompt = f"""You are an Amazon product intelligence analyst. Answer the user's question
based ONLY on the data provided below. Be specific, cite numbers, and give actionable insights.
If the data doesn't contain enough information to answer, say so.

{data_context}

User question: {user_question}"""

            # Call Gemini
            with st.chat_message("assistant"):
                with st.spinner("Analyzing your data..."):
                    try:
                        from google import genai

                        client = genai.Client(api_key=gemini_key)
                        chat_models = [
                            "gemini-2.5-flash-lite",
                            "gemini-2.5-flash",
                            "gemini-2.5-pro",
                        ]
                        response = None
                        for model in chat_models:
                            try:
                                response = client.models.generate_content(
                                    model=model,
                                    contents=prompt
                                )
                                break
                            except Exception as model_err:
                                if '429' in str(model_err):
                                    continue
                                raise model_err
                        if response is None:
                            raise Exception("All Gemini models rate limited. Try again in a minute.")
                        answer = response.text
                        st.markdown(answer)
                        st.session_state.chat_messages.append(
                            {"role": "assistant", "content": answer}
                        )
                    except Exception as e:
                        error_msg = f"Error calling Gemini: {str(e)}"
                        st.error(error_msg)
                        st.session_state.chat_messages.append(
                            {"role": "assistant", "content": error_msg}
                        )

        # Suggested questions
        if not st.session_state.chat_messages:
            st.markdown("**Try asking:**")
            suggestions = [
                "Which product has the best value for money?",
                "What are the most common complaints?",
                "Compare the top 3 products by rating",
            ]
            cols = st.columns(len(suggestions))
            for i, suggestion in enumerate(suggestions):
                with cols[i]:
                    if st.button(suggestion, key=f"suggestion_{i}"):
                        st.session_state.chat_messages.append(
                            {"role": "user", "content": suggestion}
                        )
                        st.session_state.pending_question = suggestion
                        st.rerun()

    # ── Footer ───────────────────────────────────────────────────────────

    st.markdown("---")
    st.caption("Built with Mage AI + Bright Data + Gemini AI + Streamlit")


if __name__ == "__main__":
    main()
