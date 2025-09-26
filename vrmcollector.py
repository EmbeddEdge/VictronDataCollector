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

from influxdb import InfluxDBClient

# -----------------------
# CONFIG (prefer env vars)
# -----------------------
VRM_API = os.getenv("VRM_API", "https://vrmapi.victronenergy.com/v2")
TOKEN = os.getenv("VRM_TOKEN")  # required
USER_ID = os.getenv("VRM_USER_ID")  # required
#SITE_ID = os.getenv("VRM_SITE_ID")  # required

INFLUX_HOST = os.getenv("INFLUX_HOST", "localhost")
INFLUX_PORT = int(os.getenv("INFLUX_PORT", "8086"))
INFLUX_DB = os.getenv("INFLUX_DB", "vrm")
#INFLUX_USER = os.getenv("INFLUX_USER")
#INFLUX_PASSWORD = os.getenv("INFLUX_PASSWORD")

POLL_INTERVAL = int(os.getenv("POLL_INTERVAL_SECONDS", "60"))

# -----------------------
# Basic validation
# -----------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

missing = [name for name, val in (
    ("VRM_TOKEN", TOKEN),
    ("VRM_USER_ID", USER_ID),
    #("VRM_SITE_ID", SITE_ID),
    ("INFLUX_DB", INFLUX_DB),
) if not val]
if missing:
    logger.error("Missing required config: %s", ", ".join(missing))
    logger.error("Set them via environment variables and try again.")
    sys.exit(2)

headers = {"X-Authorization": f"Token {TOKEN}"}

# Create InfluxDB 1.x client
try:
    influx = InfluxDBClient(host=INFLUX_HOST, port=INFLUX_PORT,
                            #username=INFLUX_USER, password=INFLUX_PASSWORD,
                            database=INFLUX_DB)
except Exception as e:
    logger.exception("Failed to create InfluxDB client: %s", e)
    sys.exit(2)

# -----------------------
# Helpers
# -----------------------
def fetch_installations(timeout: int = 10) -> Optional[dict]: # type: ignore
    """Fetch installation list from VRM API. Returns parsed JSON or None on failure."""
    url = f"{VRM_API}/users/{USER_ID}/installations/"
    try:
        r = requests.get(url, headers=headers, timeout=timeout)
        r.raise_for_status()
        return r.json()
    except RequestException as e:
        logger.warning("Error fetching VRM installations: %s", e)
        return None


#def extract_latest_soc(stats: dict) -> Optional[float]:
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
        installs = fetch_installations()
        if installs is None:
            logger.info("Fetch failed, retrying after %s seconds", POLL_INTERVAL)
            time.sleep(POLL_INTERVAL)
            continue

        #soc = extract_latest_soc(installs)
        #if soc is None:
        #    logger.info("No SoC value found in response; full response keys: %s", list(installs.keys()))
        #    time.sleep(POLL_INTERVAL)
        #    continue

        # The VRM /installations response contains a top-level `records` array
        records = installs.get("records") if isinstance(installs, dict) else None
        if not records:
            logger.info("No installation records found in VRM response; keys=%s", list(installs.keys()) if isinstance(installs, dict) else type(installs))
            time.sleep(POLL_INTERVAL)
            continue

        points = []
        for rec in records:
            # rec is an object like { "idSite": 12345, "name": "My Site", "identifier": "...", ... }
            site_id = rec.get("idSite")
            name = rec.get("name")
            identifier = rec.get("identifier")
            # measurement: vrm_installation, tag the numeric id and identifier, store the name as a string field
            pt = {
                "measurement": "vrm_installation",
                "tags": {
                    "idSite": str(site_id) if site_id is not None else "unknown",
                    "identifier": identifier or "",
                },
                "fields": {
                    "name": name or "",
                }
            }
            points.append(pt)

        try:
            ok = influx.write_points(points)
            if not ok:
                logger.warning("InfluxDB client reported write failure for %d points", len(points))
            else:
                logger.info("Wrote %d installation points to InfluxDB database=%s", len(points), INFLUX_DB)
        except Exception as e:
            logger.exception("Failed to write installation points to InfluxDB: %s", e)

        time.sleep(POLL_INTERVAL)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())