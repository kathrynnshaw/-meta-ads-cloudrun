import os
import json
import sys
import requests
from datetime import date, datetime, timedelta, timezone
from typing import Dict, List, Any, Optional

from google.cloud import bigquery

META_API_VERSION = os.getenv("META_API_VERSION", "v20.0")


def require_env(name: str) -> str:
    v = os.getenv(name)
    if not v:
        raise RuntimeError(f"Missing required env var: {name}")
    return v


def iso_date(s: str) -> str:
    if len(s) != 10 or s[4] != "-" or s[7] != "-":
        raise ValueError(f"Expected date in YYYY-MM-DD format, got: {s}")
    return s


def compute_window() -> (str, str):
    since_env = os.getenv("META_SINCE")
    until_env = os.getenv("META_UNTIL")
    if since_env and until_env:
        return iso_date(since_env), iso_date(until_env)

    lookback_days = int(os.getenv("LOOKBACK_DAYS", "14"))
    since = (date.today() - timedelta(days=lookback_days)).isoformat()
    until = (date.today() - timedelta(days=1)).isoformat()
    return since, until


def meta_get(url: str, params: Dict[str, Any], timeout: int = 60) -> Dict[str, Any]:
    r = requests.get(url, params=params, timeout=timeout)
    if r.status_code >= 400:
        print("Meta request failed.")
        print("URL:", r.url)
        print("Status:", r.status_code)
        try:
            print("Body:", json.dumps(r.json(), indent=2)[:5000])
        except Exception:
            print("Body (text):", r.text[:5000])
        r.raise_for_status()
    return r.json()


def fetch_campaign_daily(
    access_token: str,
    ad_account_id: str,
    since: str,
    until: str,
    breakdown_publisher_platform: bool = True,
) -> List[Dict[str, Any]]:
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

        url = next_url
        next_params = {}  # next URL already contains params

    return rows


def to_int(v: Any) -> Optional[int]:
    if v is None or v == "":
        return None
    try:
        return int(float(v))
    except Exception:
        return None


def to_float(v: Any) -> Optional[float]:
    if v is None or v == "":
        return None
    try:
        return float(v)
    except Exception:
        return None


def to_str(v: Any) -> Optional[str]:
    if v is None:
        return None
    s = str(v)
    return s if s != "" else None


def build_bq_rows(rows: List[Dict[str, Any]], load_ts: datetime) -> List[Dict[str, Any]]:
    load_date = load_ts.date().isoformat()
    out: List[Dict[str, Any]] = []

    for r in rows:
        out.append({
            "load_timestamp": load_ts.isoformat(),
            "load_date": load_date,
            "date_start": r.get("date_start"),
            "date_stop": r.get("date_stop"),
            "account_id": to_str(r.get("account_id")),
            "campaign_id": to_str(r.get("campaign_id")),
            "campaign_name": to_str(r.get("campaign_name")),
            "objective": to_str(r.get("objective")),
            "publisher_platform": to_str(r.get("publisher_platform")),
            "reach": to_int(r.get("reach")),
            "impressions": to_int(r.get("impressions")),
            "clicks": to_int(r.get("clicks")),
            "spend": to_str(r.get("spend")),  # NUMERIC accepts string
            "ctr": to_float(r.get("ctr")),
            "cpc": to_str(r.get("cpc")),      # NUMERIC accepts string
            "cpm": to_str(r.get("cpm")),      # NUMERIC accepts string
            "meta_row_json": json.dumps(r),
        })

    return out


def insert_into_bigquery(project: str, dataset: str, table: str, rows: List[Dict[str, Any]]) -> None:
    client = bigquery.Client(project=project)
    table_id = f"{project}.{dataset}.{table}"

    # Insert in chunks to avoid payload limits
    chunk_size = 500
    total = 0
    for i in range(0, len(rows), chunk_size):
        chunk = rows[i:i + chunk_size]
        errors = client.insert_rows_json(table_id, chunk)
        if errors:
            # errors is a list of row-level errors
            print("BigQuery insert errors (first 5):", json.dumps(errors[:5], indent=2)[:5000], file=sys.stderr)
            raise RuntimeError("BigQuery insert failed (see logs for details).")
        total += len(chunk)

    print(f"Inserted {total} rows into {table_id}")


def main() -> None:
    token = require_env("META_ACCESS_TOKEN")
    ad_account_id = require_env("META_AD_ACCOUNT_ID")

    bq_project = require_env("BQ_PROJECT")
    bq_dataset = require_env("BQ_DATASET")
    bq_table = require_env("BQ_TABLE_RAW")

    since, until = compute_window()
    print(f"Fetching Meta Insights for act_{ad_account_id} from {since} to {until} (daily, campaign level)...")

    rows = fetch_campaign_daily(
        access_token=token,
        ad_account_id=ad_account_id,
        since=since,
        until=until,
        breakdown_publisher_platform=True,
    )

    print(f"Fetched {len(rows)} rows.")
    load_ts = datetime.now(timezone.utc)

    bq_rows = build_bq_rows(rows, load_ts)
    if bq_rows:
        insert_into_bigquery(bq_project, bq_dataset, bq_table, bq_rows)
    else:
        print("No rows to insert (nothing returned from Meta).")


if __name__ == "__main__":
    main()