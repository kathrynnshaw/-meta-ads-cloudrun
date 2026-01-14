import os
import json
import sys
import requests
from datetime import date, timedelta
from typing import Dict, List, Any, Optional

META_API_VERSION = os.getenv("META_API_VERSION", "v20.0")


def require_env(name: str) -> str:
    v = os.getenv(name)
    if not v:
        raise RuntimeError(f"Missing required env var: {name}")
    return v


def iso_date(s: str) -> str:
    # light validation: ensure YYYY-MM-DD-ish
    if len(s) != 10 or s[4] != "-" or s[7] != "-":
        raise ValueError(f"Expected date in YYYY-MM-DD format, got: {s}")
    return s


def compute_window() -> (str, str):
    """
    Returns (since, until) as strings YYYY-MM-DD.

    Priority:
    1) META_SINCE + META_UNTIL if provided
    2) LOOKBACK_DAYS ending yesterday
    """
    since_env = os.getenv("META_SINCE")
    until_env = os.getenv("META_UNTIL")

    if since_env and until_env:
        return iso_date(since_env), iso_date(until_env)

    lookback_days = int(os.getenv("LOOKBACK_DAYS", "14"))
    since = (date.today() - timedelta(days=lookback_days)).isoformat()
    until = (date.today() - timedelta(days=1)).isoformat()  # yesterday
    return since, until


def meta_get(url: str, params: Dict[str, Any], timeout: int = 60) -> Dict[str, Any]:
    """
    GET wrapper that prints Meta error body on failure.
    """
    r = requests.get(url, params=params, timeout=timeout)

    if r.status_code >= 400:
        # Meta returns useful JSON with error.message, error.code, error.error_subcode etc.
        print("Meta request failed.")
        print("URL:", r.url)
        print("Status:", r.status_code)
        try:
            print("Body:", json.dumps(r.json(), indent=2))
        except Exception:
            print("Body (text):", r.text)
        r.raise_for_status()

    return r.json()


def fetch_campaign_daily(
    access_token: str,
    ad_account_id: str,
    since: str,
    until: str,
    breakdown_publisher_platform: bool = True,
) -> List[Dict[str, Any]]:
    """
    Fetch daily campaign insights for an ad account for [since, until].

    Notes:
    - endpoint MUST include act_
    - breakdowns are NOT included in fields
    - time_increment=1 returns daily rows
    """
    base_url = f"https://graph.facebook.com/{META_API_VERSION}/act_{ad_account_id}/insights"

    fields = [
        "date_start",
        "date_stop",
        "account_id",
        "campaign_id",
        "campaign_name",
        "objective",
        "reach",
        "impressions",
        "spend",
        "clicks",
        "inline_link_clicks",
        "ctr",
        "cpc",
        "cpm",
    ]

    params: Dict[str, Any] = {
        "access_token": access_token,
        "level": "campaign",
        "time_increment": 1,
        "time_range": json.dumps({"since": since, "until": until}),
        "fields": ",".join(fields),
        "limit": 5000,
    }

    # Optional breakdown
    if breakdown_publisher_platform:
        params["breakdowns"] = "publisher_platform"

    rows: List[Dict[str, Any]] = []

    url = base_url
    next_params: Optional[Dict[str, Any]] = params

    while True:
        payload = meta_get(url, next_params or {})
        rows.extend(payload.get("data", []))

        next_url = payload.get("paging", {}).get("next")
        if not next_url:
            break

        # Meta's "next" already contains all query params in the URL, so we clear params.
        url = next_url
        next_params = {}

    return rows


def normalise_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Keep output consistent and future BigQuery-friendly.
    Meta returns numeric fields as strings sometimes.
    We'll leave as-is for now, but add a load timestamp.
    """
    load_ts = date.today().isoformat()
    out = []
    for r in rows:
        r2 = dict(r)
        r2["load_date"] = load_ts
        out.append(r2)
    return out


def main() -> None:
    token = require_env("META_ACCESS_TOKEN")
    ad_account_id = require_env("META_AD_ACCOUNT_ID")

    since, until = compute_window()

    print(f"Fetching Meta Insights for act_{ad_account_id} from {since} to {until} (daily, campaign level)...")

    rows = fetch_campaign_daily(
        access_token=token,
        ad_account_id=ad_account_id,
        since=since,
        until=until,
        breakdown_publisher_platform=True,
    )

    rows = normalise_rows(rows)

    print(f"Fetched {len(rows)} rows.")

    # Print JSON lines so it's easy to pipe/inspect; later we’ll write to BigQuery.
    # (Cloud Run logs will show a sample; keep it short.)
    sample_n = min(3, len(rows))
    for i in range(sample_n):
        print("SAMPLE_ROW:", json.dumps(rows[i])[:2000])

    # If you want: write full output to stdout (not recommended for large pulls)
    # for r in rows:
    #     print(json.dumps(r))


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("Job failed:", repr(e), file=sys.stderr)
        raise