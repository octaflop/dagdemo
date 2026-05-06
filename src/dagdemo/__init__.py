import os

from dagster import Definitions

from dagdemo.sltrib import sltrib_assets
from dagdemo.legislation import legislation_assets
from dagdemo.llm_enrichment import CloudflareAIResource, llm_enrichment_assets

defs = Definitions(
    assets=[*sltrib_assets, *legislation_assets, *llm_enrichment_assets],
    resources={
        "cloudflare_ai": CloudflareAIResource(
            account_id=os.environ.get("CF_ACCOUNT_ID", ""),
            api_token=os.environ.get("CF_API_TOKEN", ""),
        ),
    },
)
