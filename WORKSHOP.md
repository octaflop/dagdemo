# DagDemo Workshop Guide

A progressive, hands-on tour of Dagster using real-world data: RSS feeds, legislative APIs, and LLM calls. No Docker, no database server — just `uv` and Python.

## Prerequisites

- Python 3.12+ (pinned to 3.13 in this repo)
- `uv` installed (`curl -LsSf https://astral.sh/uv/install.sh | sh`)
- A free Cloudflare account (optional, for the LLM section)

## Setup

```bash
uv sync
```

That's it. No containers, no services to start.

---

## Exploration 1: Your First Asset (5 min)

**Goal:** Understand what a Dagster asset is and how to run it.

Open `src/dagdemo/sltrib.py` and look at the simplest asset:

```python
@asset(
    name="sltrib_feed_xml",
    group_name="sltrib",
    description="Fetches the Salt Lake Tribune RSS feed.",
    retry_policy=DEFAULT_RETRY_POLICY,
)
def sltrib_feed_xml(context: AssetExecutionContext) -> str:
    context.log.info(f"Downloading feed from {SLTRIB_RSS_URL}")
    session = requests.Session()
    session.headers.update(DEFAULT_HEADERS)
    response = session.get(SLTRIB_RSS_URL, timeout=30)
    response.raise_for_status()
    context.log.info(f"Downloaded {len(response.text)} bytes")
    return response.text
```

Key concepts:
- `@asset` decorator turns a function into a managed data asset
- The return value is stored and passed to downstream assets
- `context.log` writes to the Dagster UI and console
- `retry_policy` handles transient failures automatically

**Try it:**

```bash
# Start the UI
uv run dagster dev

# Or materialize a single asset from CLI
uv run dagster asset materialize -a sltrib_feed_xml
```

Open http://localhost:3000 — you'll see the asset graph, run history, and output metadata.

---

## Exploration 2: Building a Pipeline (10 min)

**Goal:** See how assets chain together into a dependency graph.

The SLTrib pipeline has 3 stages:

```
sltrib_feed_xml → sltrib_articles → sltrib_full_content
```

Each stage transforms data:

| Asset | Input | Output | What it does |
|-------|-------|--------|--------------|
| `sltrib_feed_xml` | None | `str` (raw XML) | Downloads RSS feed |
| `sltrib_articles` | XML string | `list[dict]` | Parses XML into structured articles |
| `sltrib_full_content` | `list[dict]` | `list[dict]` | Fetches full article text from URLs |

**Key pattern — `AssetIn`:**

```python
@asset(
    name="sltrib_articles",
    ins={"feed_xml_input": AssetIn(key="sltrib_feed_xml")},
)
def sltrib_articles(context: AssetExecutionContext, feed_xml_input: str):
    # feed_xml_input is the return value of sltrib_feed_xml
```

Dagster infers the dependency from `AssetIn(key="...")`. When you materialize `sltrib_full_content`, Dagster automatically runs `sltrib_feed_xml` and `sltrib_articles` first if they're stale.

**Try it:**

```bash
# Materialize the full pipeline
uv run dagster asset materialize -a sltrib_full_content
```

In the UI, click on `sltrib_full_content` → "Materialize" → check "Include upstream assets".

**Key pattern — `MaterializeResult`:**

Instead of returning raw data, you can return `MaterializeResult` with metadata that shows up in the UI:

```python
return MaterializeResult(
    value=articles_data,          # The actual data
    metadata={
        "article_count": len(parsed_articles),
        "preview": MetadataValue.md("## Preview\n..."),
    },
)
```

---

## Exploration 3: External API Data (10 min)

**Goal:** Fetch structured data from a public API and model it with Pydantic.

The `legislation.py` module pulls data from Utah's GLEN API — no auth required:

```python
LEGISLATORS_URL = "https://le.utah.gov/data/legislators.json"
BILLS_URL = "https://le.utah.gov/data/bills.json"
```

**Key pattern — Pydantic models for API responses:**

```python
class Legislator(BaseModel):
    id: str
    name: str
    house: str = ""
    district: int | None = None
    party: str = ""

    @classmethod
    def from_api(cls, data: dict) -> "Legislator":
        # Maps messy API field names to clean model fields
        return cls(
            id=str(data.get("id", "")),
            name=data.get("formatName", data.get("name", "")),
            ...
        )
```

This pattern is valuable because:
- API field names are often inconsistent (`formatName` vs `name`)
- Pydantic validates types at construction time
- `model_dump()` gives you clean JSON-serializable output

**Try it:**

```bash
uv run dagster asset materialize -a ut_legislators
uv run dagster asset materialize -a ut_bills
```

The `ut_legislators` and `ut_bills` assets are independent — they can run in parallel.

---

## Exploration 4: LLM Enrichment (15 min)

**Goal:** Add AI-powered summarization and analysis to a data pipeline.

The `llm_enrichment.py` module adds a `ai_enriched_articles` asset that depends on `sltrib_full_content` and calls Cloudflare Workers AI:

```
sltrib_full_content → ai_enriched_articles
```

**Key pattern — ConfigurableResource:**

```python
class CloudflareAIResource(ConfigurableResource):
    account_id: str = ""
    api_token: str = ""
    analysis_model: str = "@cf/meta/llama-3.2-3b-instruct"

    @property
    def enabled(self) -> bool:
        acct = self.account_id or os.environ.get("CF_ACCOUNT_ID", "")
        token = self.api_token or os.environ.get("CF_API_TOKEN", "")
        return bool(acct and token)
```

Resources are injected into assets by type name:

```python
@asset(...)
def ai_enriched_articles(
    context: AssetExecutionContext,
    cloudflare_ai: CloudflareAIResource,  # ← injected from Definitions
    articles_input: list[dict],
):
    if not cloudflare_ai.enabled:
        context.log.info("Cloudflare AI not configured. Skipping.")
        return MaterializeResult(value=articles_input, metadata={"skipped": True})
    # ... do LLM calls
```

**Key pattern — Graceful degradation:**

The asset works without API keys — it just passes data through. This is important for workshops where not everyone has credentials.

**To enable LLM calls:**

```bash
export CF_ACCOUNT_ID=your_account_id
export CF_API_TOKEN=your_api_token
```

Get credentials at https://dash.cloudflare.com → Workers & Pages → Workers AI. The free tier is generous.

**Try it:**

```bash
# Without API keys — passes through
uv run dagster asset materialize -a ai_enriched_articles

# With API keys — generates summaries and analysis
CF_ACCOUNT_ID=xxx CF_API_TOKEN=xxx uv run dagster asset materialize -a ai_enriched_articles
```

---

## Exploration 5: Wiring It All Together (5 min)

**Goal:** Understand how `Definitions` connects assets and resources.

`src/dagdemo/__init__.py` is the entry point:

```python
from dagster import Definitions

defs = Definitions(
    assets=[*sltrib_assets, *legislation_assets, *llm_enrichment_assets],
    resources={
        "cloudflare_ai": CloudflareAIResource(
            account_id=os.environ.get("CF_ACCOUNT_ID", ""),
            api_token=os.environ.get("CF_API_TOKEN", ""),
        ),
    },
)
```

Key points:
- `assets=` lists all assets Dagster should know about
- `resources=` provides shared objects (API clients, DB connections, etc.)
- Resource names match the parameter name in asset functions (`cloudflare_ai`)
- Dagster uses SQLite by default — no database setup needed

---

## Exercises

### Exercise 1: Add a New RSS Source (15 min)

Add a second news source (e.g., NPR: `https://feeds.npr.org/1001/rss.xml`).

Hints:
- Copy the 3-asset pattern from `sltrib.py`
- You'll need a different RSS item parser (NPR uses different XML structure)
- Register the new assets in `Definitions`

### Exercise 2: Add an Asset Check (10 min)

Add a check that validates articles have content:

```python
from dagster import asset_check, AssetCheckResult, AssetCheckSeverity

@asset_check(asset="sltrib_articles")
def check_articles_have_content(articles_input: list[dict]) -> AssetCheckResult:
    empty = [a for a in articles_input if not a.get("content") and not a.get("description")]
    return AssetCheckResult(
        passed=len(empty) < len(articles_input) * 0.5,
        metadata={"empty_count": len(empty)},
    )
```

### Exercise 3: Add a Schedule (5 min)

Make the SLTrib pipeline run every 2 hours:

```python
from dagster import ScheduleDefinition, define_asset_job

sltrib_job = define_asset_job("sltrib_job", selection=["sltrib_feed_xml", "sltrib_articles", "sltrib_full_content"])

sltrib_schedule = ScheduleDefinition(
    job=sltrib_job,
    cron_schedule="0 */2 * * *",
)
```

Add `schedules=[sltrib_schedule]` to `Definitions`.

### Exercise 4: Filter Bills by Party (10 min)

Add a downstream asset that filters legislators by party and counts them:

```python
@asset(ins={"legislators": AssetIn(key="ut_legislators")})
def party_counts(legislators: list[dict]) -> dict:
    from collections import Counter
    parties = Counter(leg["party"] for leg in legislators)
    return dict(parties)
```

---

## Project Structure

```
src/dagdemo/
├── __init__.py          # Definitions — wires assets + resources
├── sltrib.py            # RSS feed pipeline (3 assets)
├── legislation.py       # Utah GLEN API (2 assets)
└── llm_enrichment.py    # Cloudflare AI enrichment (1 asset + resource)
```

## Asset Dependency Graph

```
sltrib_feed_xml → sltrib_articles → sltrib_full_content → ai_enriched_articles

ut_legislators  (independent)
ut_bills        (independent)
```

## Key Dagster Concepts Covered

| Concept | Where |
|---------|-------|
| `@asset` | All modules |
| `AssetIn` | `sltrib.py`, `llm_enrichment.py` |
| `MaterializeResult` | All assets |
| `MetadataValue` | All assets |
| `ConfigurableResource` | `llm_enrichment.py` |
| `RetryPolicy` | `sltrib.py`, `legislation.py` |
| `Definitions` | `__init__.py` |
| Asset groups | All modules (`group_name=`) |
