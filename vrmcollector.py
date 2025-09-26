"""Victron VRM -> InfluxDB minimal collector

Usage:
  - configure the variables either via environment variables or edit the CONFIG section below
  - run with python3 vrmcollector.py

This script:
  - fetches installation stats from the Victron VRM API
  - extracts the latest State-of-Charge (SoC) value (if present)
  - writes a point to InfluxDB using influxdb-client

Notes:
  - Do NOT hardcode tokens in source. Use environment variables or a secrets manager.
  - This is a minimal example. Extend metrics, tagging, retries and rate-limiting as needed.
"""

import os
import sys
import time
import logging
from typing import Optional

import requests
from requests.exceptions import RequestException

from influxdb_client import InfluxDBClient, Point, WriteOptions
from influxdb_client.client.write_api import SYNCHRONOUS

# -----------------------
# CONFIG (prefer env vars)
# -----------------------
VRM_API = os.getenv("VRM_API", "https://vrmapi.victronenergy.com/v2")
TOKEN = os.getenv("VRM_TOKEN")  # required
SITE_ID = os.getenv("VRM_SITE_ID")  # required

INFLUX_URL = os.getenv("INFLUX_URL", "http://localhost:8086")
INFLUX_TOKEN = os.getenv("INFLUX_TOKEN")
INFLUX_ORG = os.getenv("INFLUX_ORG", "-")
INFLUX_BUCKET = os.getenv("INFLUX_BUCKET")

POLL_INTERVAL = int(os.getenv("POLL_INTERVAL_SECONDS", "60"))

# -----------------------
# Basic validation
# -----------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

missing = [name for name, val in (
    ("VRM_TOKEN", TOKEN),
    ("VRM_SITE_ID", SITE_ID),
    ("INFLUX_TOKEN", INFLUX_TOKEN),
    ("INFLUX_BUCKET", INFLUX_BUCKET),
) if not val]
if missing:
    logger.error("Missing required config: %s", ", ".join(missing))
    logger.error("Set them via environment variables and try again.")
    sys.exit(2)

headers = {"X-Authorization": f"Bearer {TOKEN}"}

# Create Influx client
influx = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
# Use synchronous writes for simplicity here
write_api = influx.write_api(write_options=WriteOptions(batch_size=1))

# -----------------------
# Helpers
# -----------------------
def fetch_installations(site_id: str, timeout: int = 10) -> Optional[dict]:
    """Fetch installation list from VRM API. Returns parsed JSON or None on failure."""
    url = f"{VRM_API}/installations/"
    try:
        r = requests.get(url, headers=headers, timeout=timeout)
        r.raise_for_status()
        return r.json()
    except RequestException as e:
        logger.warning("Error fetching VRM installations: %s", e)
        return None


def extract_latest_soc(stats: dict) -> Optional[float]:
    """Extract the latest State-of-Charge value from stats if present.

    The expected structure (example):
      { "records": { "soc": { "y": [ ... ] } } }

    Returns float or None.
    """
    try:
        soc_series = stats.get("records", {}).get("soc", {})
        y = soc_series.get("y")
        if not y:
            return None
        # assume last element is latest
        return float(y[-1])
    except Exception:
        return None


# -----------------------
# Main loop
# -----------------------
def main() -> int:
    logger.info("Starting VRM -> InfluxDB collector; polling every %ss", POLL_INTERVAL)

    while True:
        stats = fetch_stats(SITE_ID)
        if stats is None:
            logger.info("Fetch failed, retrying after %s seconds", POLL_INTERVAL)
            time.sleep(POLL_INTERVAL)
            continue

        soc = extract_latest_soc(stats)
        if soc is None:
            logger.info("No SoC value found in response; full response keys: %s", list(stats.keys()))
            time.sleep(POLL_INTERVAL)
            continue

        point = Point("battery").field("soc", soc)
        # Optionally add tags (e.g., site id)
        point.tag("site_id", SITE_ID)

        try:
            write_api.write(bucket=INFLUX_BUCKET, record=point)
            logger.info("Wrote SoC=%.2f to InfluxDB bucket=%s", soc, INFLUX_BUCKET)
        except Exception as e:
            logger.exception("Failed to write point to InfluxDB: %s", e)

        time.sleep(POLL_INTERVAL)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())