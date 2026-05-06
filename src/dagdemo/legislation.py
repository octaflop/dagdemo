import requests
from datetime import datetime
from pydantic import BaseModel

from dagster import (
    asset,
    AssetExecutionContext,
    MaterializeResult,
    MetadataValue,
    RetryPolicy,
    Backoff,
    Jitter,
)

LEGISLATORS_URL = "https://le.utah.gov/data/legislators.json"
BILLS_URL = "https://le.utah.gov/data/bills.json"

DEFAULT_RETRY_POLICY = RetryPolicy(
    max_retries=3,
    delay=30,
    backoff=Backoff.EXPONENTIAL,
    jitter=Jitter.PLUS_MINUS,
)


class Legislator(BaseModel):
    id: str
    name: str
    house: str = ""
    district: int | None = None
    party: str = ""
    address: str = ""
    phone: str = ""
    email: str = ""
    service_start: str = ""

    @classmethod
    def from_api(cls, data: dict) -> "Legislator":
        phone = (
            data.get("cell", "")
            or data.get("workPhone", "")
            or data.get("homePhone", "")
        )
        district = data.get("district")
        if district is not None:
            try:
                district = int(district)
            except (ValueError, TypeError):
                district = None
        return cls(
            id=str(data.get("id", "")),
            name=data.get("formatName", data.get("name", "")),
            house=data.get("house", ""),
            district=district,
            party=data.get("party", ""),
            address=data.get("address", ""),
            phone=phone,
            email=data.get("email", ""),
            service_start=data.get("serviceStart", ""),
        )


class Bill(BaseModel):
    id: str
    session: str = ""
    title: str = ""
    sponsor: str = ""
    floor_sponsor: str = ""
    status: str = ""
    last_action: str = ""
    last_action_date: datetime | None = None
    last_action_owner: str = ""
    highlighted_provisions: str = ""
    general_provisions: str = ""
    tracking_id: str = ""

    @classmethod
    def from_api(cls, data: dict, session: str = "") -> "Bill":
        last_action_date = None
        raw_date = data.get("lastActionDate", "")
        if raw_date:
            try:
                last_action_date = datetime.strptime(str(raw_date).strip(), "%m/%d/%Y")
            except (ValueError, TypeError):
                try:
                    last_action_date = datetime.fromisoformat(
                        str(raw_date).replace("Z", "+00:00")
                    )
                except (ValueError, TypeError):
                    pass

        bill_id = data.get("billNumber", data.get("bill", data.get("id", "")))
        sponsor = data.get("primeSponsorName", data.get("sponsor", ""))
        floor_sponsor = data.get("floorSponsorName", data.get("floorSponsor", ""))
        status = data.get("billStatus", data.get("lastAction", data.get("status", "")))

        return cls(
            id=bill_id,
            session=session or data.get("sessionID", data.get("session", "")),
            title=data.get("shortTitle", data.get("title", "")),
            sponsor=sponsor,
            floor_sponsor=floor_sponsor,
            status=status,
            last_action=data.get("lastAction", ""),
            last_action_date=last_action_date,
            last_action_owner=data.get("lastActionOwner", ""),
            highlighted_provisions=data.get("highlightedProvisions", ""),
            general_provisions=data.get("generalProvisions", ""),
            tracking_id=data.get("trackingID", data.get("trackingId", "")),
        )


@asset(
    name="ut_legislators",
    group_name="legislation",
    description="Fetches Utah legislators from the GLEN API.",
    retry_policy=DEFAULT_RETRY_POLICY,
)
def ut_legislators(context: AssetExecutionContext) -> MaterializeResult:
    context.log.info(f"Fetching legislators from {LEGISLATORS_URL}")
    response = requests.get(LEGISLATORS_URL, timeout=60)
    response.raise_for_status()
    data = response.json()

    items = data.get("legislators", []) if isinstance(data, dict) else data
    legislators = [Legislator.from_api(item) for item in items if isinstance(item, dict)]

    context.log.info(f"Fetched {len(legislators)} legislators")

    metadata = {
        "count": len(legislators),
    }

    if legislators:
        names_md = "\n".join(f"- {leg.name} ({leg.party}) - District {leg.district}" for leg in legislators[:20])
        if len(legislators) > 20:
            names_md += f"\n- ... and {len(legislators) - 20} more"
        metadata["preview"] = MetadataValue.md(names_md)

    return MaterializeResult(
        value=[leg.model_dump() for leg in legislators],
        metadata=metadata,
    )


@asset(
    name="ut_bills",
    group_name="legislation",
    description="Fetches Utah bills from the GLEN API.",
    retry_policy=DEFAULT_RETRY_POLICY,
)
def ut_bills(context: AssetExecutionContext) -> MaterializeResult:
    context.log.info(f"Fetching bills from {BILLS_URL}")
    response = requests.get(BILLS_URL, timeout=60)
    response.raise_for_status()
    data = response.json()

    items = data.get("bills", []) if isinstance(data, dict) else data
    bills = [Bill.from_api(item) for item in items if isinstance(item, dict)]

    context.log.info(f"Fetched {len(bills)} bills")

    metadata = {
        "count": len(bills),
    }

    if bills:
        bills_md = "\n".join(f"- {b.id}: {b.title[:80]}" for b in bills[:20])
        if len(bills) > 20:
            bills_md += f"\n- ... and {len(bills) - 20} more"
        metadata["preview"] = MetadataValue.md(bills_md)

    return MaterializeResult(
        value=[b.model_dump(mode="json") for b in bills],
        metadata=metadata,
    )


legislation_assets = [ut_legislators, ut_bills]
