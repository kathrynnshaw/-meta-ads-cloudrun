import os
import json
import requests
from datetime import date, timedelta

META_API_VERSION = os.getenv("META_API_VERSION", "v20.0")

def require_env(name: str) -> str:
    v = os.getenv(name)
    if not v:
        raise RuntimeError(f"Missing required env var: {name}")
    return v

def fetch_insights(access_token: str, ad_account_id: str, since: str, until: str):
    url = f"https://graph.facebook.com/{META_API_VERSION}/act_{ad_account_id}/insights"
    params = {
        "access_token": access_token,
        "time_increment": 1,
        "level": "campaign",
        "breakdowns": "publisher_platform",
        "time_range": json.dumps({"since": since, "until": until}),
        "fields": ",".join([
            "date_start","date_stop",
            "account_id",
            "campaign_id","campaign_name","objective",
            "publisher_platform",
            "reach","impressions","spend",
            "clicks","inline_link_clicks",
            "ctr","cpc","cpm",
        ]),
        "limit": 5000,
    }

    rows = []
    while True:
        r = requests.get(url, params=params, timeout=60)
        r.raise_for_status()
        payload = r.json()
        rows.extend(payload.get("data", []))

        next_url = payload.get("paging", {}).get("next")
        if not next_url:
            break
        url = next_url
        params = {}  # next_url already contains params

    return rows

def main():
    token = require_env("META_ACCESS_TOKEN")
    ad_account_id = require_env("META_AD_ACCOUNT_ID")
    lookback_days = int(os.getenv("LOOKBACK_DAYS", "14"))

    since = (date.today() - timedelta(days=lookback_days)).isoformat()
    until = (date.today() - timedelta(days=1)).isoformat()

    rows = fetch_insights(token, ad_account_id, since, until)
    print(f"Fetched {len(rows)} rows for {since} → {until}")

if __name__ == "__main__":
    main()