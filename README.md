# DagDemo — Minimal Dagster Workshop

A minimal Dagster demo showing RSS feeds, legislative data, and LLM calls.
No Docker or database server required — uses Dagster's built-in SQLite storage.

## Quick Start

```bash
# Install dependencies
uv sync

# Start the Dagster UI
uv run dagster dev
```

Open http://localhost:3000 and materialize assets.

## Assets

| Asset | Description |
|-------|-------------|
| `sltrib_feed_xml` | Fetches SLTrib RSS feed |
| `sltrib_articles` | Parses RSS into structured articles |
| `sltrib_full_content` | Fetches full article content |
| `ut_legislators` | Fetches Utah legislators |
| `ut_bills` | Fetches Utah bills |
| `ai_enriched_articles` | LLM summarization (requires Cloudflare API) |

## LLM Enrichment

To enable AI summarization, set these environment variables:

```bash
export CF_ACCOUNT_ID=your_account_id
export CF_API_TOKEN=your_api_token
```

Get a free Cloudflare Workers AI token at https://dash.cloudflare.com.

## Project Structure

```
src/dagdemo/
├── __init__.py          # Dagster Definitions
├── sltrib.py            # RSS feed pipeline
├── legislation.py       # Utah GLEN API ingestion
└── llm_enrichment.py    # Cloudflare AI enrichment
```
