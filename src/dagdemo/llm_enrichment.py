import json
import os

import httpx
from dagster import (
    ConfigurableResource,
    asset,
    AssetExecutionContext,
    AssetIn,
    MaterializeResult,
    MetadataValue,
)

from dagdemo.sltrib import Article


class CloudflareAIResource(ConfigurableResource):
    account_id: str = ""
    api_token: str = ""
    timeout: int = 120
    analysis_model: str = "@cf/meta/llama-3.2-3b-instruct"

    @property
    def enabled(self) -> bool:
        acct = self.account_id or os.environ.get("CF_ACCOUNT_ID", "")
        token = self.api_token or os.environ.get("CF_API_TOKEN", "")
        return bool(acct and token)

    def _endpoint(self, model: str) -> str:
        account_id = self.account_id or os.environ.get("CF_ACCOUNT_ID", "")
        api_token = self.api_token or os.environ.get("CF_API_TOKEN", "")
        return f"https://api.cloudflare.com/client/v4/accounts/{account_id}/ai/run/{model}"

    def _headers(self) -> dict:
        api_token = self.api_token or os.environ.get("CF_API_TOKEN", "")
        return {
            "Authorization": f"Bearer {api_token}",
            "Content-Type": "application/json",
        }

    def _request(self, model: str, payload: dict) -> dict:
        url = self._endpoint(model)
        with httpx.Client(timeout=self.timeout) as client:
            response = client.post(url, headers=self._headers(), json=payload)
            response.raise_for_status()
            return response.json()

    def _extract_text(self, result: dict) -> str:
        inner = result.get("result", {})
        if isinstance(inner, str):
            return inner
        if isinstance(inner, dict):
            return inner.get("response", "")
        return result.get("response", "")

    def summarize(self, text: str, max_length: int = 250) -> str:
        if not text or len(text.strip()) < 50:
            return ""
        clean_text = " ".join(text.split())[:3500]
        prompt = f"""You are a professional news anchor. Write a 2-3 sentence broadcast-style summary of this article.

Requirements:
- Open with the key fact (who, what, where)
- Use active voice and short sentences
- Sound natural when read aloud
- Do NOT begin with "In a...", "According to...", or "The article..."
- Write the summary directly — no preamble

Article:
{clean_text}

Summary:"""
        try:
            result = self._request(
                self.analysis_model,
                {
                    "messages": [{"role": "user", "content": prompt}],
                    "max_tokens": max_length,
                },
            )
            response = self._extract_text(result)
            return response.strip().strip('"').strip()
        except Exception:
            return ""

    def analyze(self, title: str, text: str) -> dict:
        if not text:
            return {
                "categories": [],
                "sentiment": "neutral",
                "tone": "neutral",
            }

        prompt = f"""Analyze this news article and return a JSON object with:
- categories: list of 1-3 topics from [politics, business, technology, science, health, sports, entertainment, world, environment, crime]
- sentiment: one of [positive, negative, neutral, mixed]
- tone: one of [urgent, somber, upbeat, serious, neutral]

Title: {title}
Content: {text[:3000]}

Return ONLY valid JSON, no explanation."""

        try:
            result = self._request(
                self.analysis_model,
                {
                    "messages": [{"role": "user", "content": prompt}],
                    "max_tokens": 500,
                },
            )
            response_text = self._extract_text(result) or "{}"

            if "```json" in response_text:
                response_text = response_text.split("```json")[1].split("```")[0]
            elif "```" in response_text:
                response_text = response_text.split("```")[1].split("```")[0]

            data = json.loads(response_text.strip())
            if not isinstance(data, dict):
                return {"categories": [], "sentiment": "neutral", "tone": "neutral"}
            return {
                "categories": data.get("categories", [])[:3],
                "sentiment": data.get("sentiment", "neutral"),
                "tone": data.get("tone", "neutral"),
            }
        except Exception:
            return {"categories": [], "sentiment": "neutral", "tone": "neutral"}


@asset(
    name="ai_enriched_articles",
    group_name="enrichment",
    description="Enriches SLTrib articles with AI summaries and analysis.",
    ins={"articles_input": AssetIn(key="sltrib_full_content")},
)
def ai_enriched_articles(
    context: AssetExecutionContext,
    cloudflare_ai: CloudflareAIResource,
    articles_input: list[dict],
) -> MaterializeResult:
    if not cloudflare_ai.enabled:
        context.log.info("Cloudflare AI not configured (set CF_ACCOUNT_ID and CF_API_TOKEN). Skipping enrichment.")
        return MaterializeResult(
            value=articles_input,
            metadata={"skipped": True, "reason": "Cloudflare AI not configured"},
        )

    enriched: list[dict] = []
    summaries_count = 0
    analysis_count = 0

    for article_dict in articles_input:
        article = Article.from_dict(article_dict)
        enrichment: dict = {}

        text = article.full_content or article.content
        if text and cloudflare_ai.enabled:
            summary = cloudflare_ai.summarize(text)
            if summary:
                enrichment["summary"] = summary
                summaries_count += 1
                context.log.info(f"Summarized: {article.title[:60]}")

            analysis = cloudflare_ai.analyze(article.title, text)
            if analysis:
                enrichment["analysis"] = analysis
                analysis_count += 1

        article_dict["ai_enrichment"] = enrichment
        enriched.append(article_dict)

    context.log.info(f"Enriched {len(enriched)} articles: {summaries_count} summaries, {analysis_count} analyses")

    metadata = {
        "article_count": len(enriched),
        "summaries_generated": summaries_count,
        "analyses_generated": analysis_count,
    }

    if enriched:
        first = enriched[0]
        ai = first.get("ai_enrichment", {})
        preview_lines = [
            f"## {first.get('title', 'Unknown')}",
            "",
        ]
        if ai.get("summary"):
            preview_lines.extend([
                "**AI Summary:**",
                f"> {ai['summary']}",
                "",
            ])
        if ai.get("analysis"):
            a = ai["analysis"]
            preview_lines.extend([
                f"**Categories:** {', '.join(a.get('categories', []))}",
                f"**Sentiment:** {a.get('sentiment', 'neutral')}",
                f"**Tone:** {a.get('tone', 'neutral')}",
            ])
        metadata["preview"] = MetadataValue.md("\n".join(preview_lines))

    return MaterializeResult(value=enriched, metadata=metadata)


llm_enrichment_assets = [ai_enriched_articles]
