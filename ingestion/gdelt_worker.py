"""
gdelt_worker.py — GDELT 2.0 real-time ingestion worker for God_View.

Polls the GDELT "last update" manifest every 15 minutes, downloads the
latest event-database CSV export, filters for rows with valid
ActionGeo_Lat / ActionGeo_Long, converts them to JSON records, and
publishes each record to the ``raw-events`` Kafka topic.

Usage:
    python gdelt_worker.py                  # runs one fetch cycle then exits
    python gdelt_worker.py --loop           # polls every 15 minutes
    KAFKA_BOOTSTRAP=kafka:29092 python gdelt_worker.py --loop  # inside Docker
"""

from __future__ import annotations

import argparse
import io
import logging
import os
import sys
import time
import zipfile
from datetime import datetime, timezone
from typing import Optional

import pandas as pd
import requests

from base_producer import GodViewProducer

# --------------- Configuration ---------------
GDELT_LAST_UPDATE_URL = "http://data.gdeltproject.org/gdeltv2/lastupdate.txt"
KAFKA_TOPIC = os.getenv("GDELT_KAFKA_TOPIC", "raw-events")
POLL_INTERVAL_S = int(os.getenv("GDELT_POLL_INTERVAL", "900"))  # 15 minutes
REQUEST_TIMEOUT_S = int(os.getenv("GDELT_REQUEST_TIMEOUT", "60"))

# Full ordered column list for the GDELT 2.0 Event Database (61 fields).
# The raw CSVs are headerless and tab-delimited.
GDELT_COLUMNS = [
    "GLOBALEVENTID", "SQLDATE", "MonthYear", "Year", "FractionDate",
    "Actor1Code", "Actor1Name", "Actor1CountryCode", "Actor1KnownGroupCode",
    "Actor1EthnicCode", "Actor1Religion1Code", "Actor1Religion2Code",
    "Actor1Type1Code", "Actor1Type2Code", "Actor1Type3Code",
    "Actor2Code", "Actor2Name", "Actor2CountryCode", "Actor2KnownGroupCode",
    "Actor2EthnicCode", "Actor2Religion1Code", "Actor2Religion2Code",
    "Actor2Type1Code", "Actor2Type2Code", "Actor2Type3Code",
    "IsRootEvent", "EventCode", "EventBaseCode", "EventRootCode",
    "QuadClass", "GoldsteinScale", "NumMentions", "NumSources",
    "NumArticles", "AvgTone",
    "Actor1Geo_Type", "Actor1Geo_FullName", "Actor1Geo_CountryCode",
    "Actor1Geo_ADM1Code", "Actor1Geo_ADM2Code",
    "Actor1Geo_Lat", "Actor1Geo_Long", "Actor1Geo_FeatureID",
    "Actor2Geo_Type", "Actor2Geo_FullName", "Actor2Geo_CountryCode",
    "Actor2Geo_ADM1Code", "Actor2Geo_ADM2Code",
    "Actor2Geo_Lat", "Actor2Geo_Long", "Actor2Geo_FeatureID",
    "ActionGeo_Type", "ActionGeo_FullName", "ActionGeo_CountryCode",
    "ActionGeo_ADM1Code", "ActionGeo_ADM2Code",
    "ActionGeo_Lat", "ActionGeo_Long", "ActionGeo_FeatureID",
    "DATEADDED", "SOURCEURL",
]

# Columns we keep in the published JSON (avoids noisy 61-field records).
PUBLISH_COLUMNS = [
    "GLOBALEVENTID", "SQLDATE", "Actor1Name", "Actor1CountryCode",
    "Actor2Name", "Actor2CountryCode",
    "EventCode", "EventBaseCode", "EventRootCode", "QuadClass",
    "GoldsteinScale", "NumMentions", "NumSources", "NumArticles", "AvgTone",
    "ActionGeo_FullName", "ActionGeo_CountryCode",
    "ActionGeo_Lat", "ActionGeo_Long",
    "DATEADDED", "SOURCEURL",
]

# --------------- Logging ---------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  [%(levelname)s]  %(name)s  %(message)s",
)
logger = logging.getLogger(__name__)


# ------------------------------------------------------------------
# GDELT download helpers
# ------------------------------------------------------------------

def _fetch_latest_export_url() -> Optional[str]:
    """Read ``lastupdate.txt`` and return the URL of the event-database CSV.

    The manifest contains three lines (events, mentions, GKG). The event
    export line contains ``export`` in its filename.
    """
    try:
        resp = requests.get(GDELT_LAST_UPDATE_URL, timeout=REQUEST_TIMEOUT_S)
        resp.raise_for_status()
    except requests.RequestException as exc:
        logger.error("Failed to fetch GDELT lastupdate manifest: %s", exc)
        return None

    for line in resp.text.strip().splitlines():
        parts = line.split()
        if len(parts) >= 3:
            url = parts[-1]
            if "export" in url.lower():
                return url

    logger.warning("Could not locate export URL in lastupdate manifest.")
    return None


def _download_and_parse(url: str) -> Optional[pd.DataFrame]:
    """Download a ZIP-compressed, tab-delimited GDELT CSV and return a DataFrame."""
    logger.info("Downloading %s …", url)
    try:
        resp = requests.get(url, timeout=REQUEST_TIMEOUT_S)
        resp.raise_for_status()
    except requests.RequestException as exc:
        logger.error("Download failed: %s", exc)
        return None

    try:
        with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
            csv_name = zf.namelist()[0]
            with zf.open(csv_name) as csv_file:
                df = pd.read_csv(
                    csv_file,
                    sep="\t",
                    header=None,
                    names=GDELT_COLUMNS,
                    dtype=str,       # keep everything as string initially
                    encoding="utf-8",
                    on_bad_lines="skip",
                )
    except (zipfile.BadZipFile, KeyError, pd.errors.ParserError) as exc:
        logger.error("Failed to parse CSV from archive: %s", exc)
        return None

    logger.info("Parsed %d raw rows from %s", len(df), csv_name)
    return df


def _filter_geolocated(df: pd.DataFrame) -> pd.DataFrame:
    """Keep only rows with valid (non-empty, numeric) ActionGeo_Lat/Long."""
    df = df.copy()

    # Convert lat/long to numeric; invalid → NaN
    df["ActionGeo_Lat"] = pd.to_numeric(df["ActionGeo_Lat"], errors="coerce")
    df["ActionGeo_Long"] = pd.to_numeric(df["ActionGeo_Long"], errors="coerce")

    geo_df = df.dropna(subset=["ActionGeo_Lat", "ActionGeo_Long"])

    # Sanity-check coordinate ranges
    geo_df = geo_df[
        (geo_df["ActionGeo_Lat"].between(-90, 90))
        & (geo_df["ActionGeo_Long"].between(-180, 180))
    ]

    logger.info(
        "Filtered to %d geolocated rows (dropped %d without valid coords).",
        len(geo_df),
        len(df) - len(geo_df),
    )
    return geo_df


# ------------------------------------------------------------------
# Kafka publishing
# ------------------------------------------------------------------

def _publish_records(df: pd.DataFrame, producer: GodViewProducer) -> int:
    """Convert DataFrame rows to JSON dicts and publish to Kafka.

    Returns the number of messages published.
    """
    # Trim to the subset of useful columns
    cols = [c for c in PUBLISH_COLUMNS if c in df.columns]
    subset = df[cols]

    published = 0
    for _, row in subset.iterrows():
        record = row.to_dict()

        # Inject metadata
        record["_source"] = "gdelt"
        record["_ingested_at"] = datetime.now(timezone.utc).isoformat()

        # Use the event's country code as partition key for locality
        key = str(record.get("ActionGeo_CountryCode", "UNKNOWN"))

        try:
            producer.publish(KAFKA_TOPIC, record, key=key)
            published += 1
        except Exception:
            logger.exception("Failed to publish record %s", record.get("GLOBALEVENTID"))

    # Flush to ensure all messages are delivered
    producer.flush()
    logger.info("Published %d records to topic '%s'.", published, KAFKA_TOPIC)
    return published


# ------------------------------------------------------------------
# Main loop
# ------------------------------------------------------------------

def run_once(producer: GodViewProducer) -> int:
    """Execute a single fetch → parse → filter → publish cycle."""
    url = _fetch_latest_export_url()
    if url is None:
        return 0

    df = _download_and_parse(url)
    if df is None or df.empty:
        return 0

    geo_df = _filter_geolocated(df)
    if geo_df.empty:
        logger.warning("No geolocated events in this batch — skipping.")
        return 0

    return _publish_records(geo_df, producer)


def main() -> None:
    parser = argparse.ArgumentParser(description="GDELT → Kafka ingestion worker")
    parser.add_argument(
        "--loop",
        action="store_true",
        help=f"Poll continuously every {POLL_INTERVAL_S}s (default: run once)",
    )
    args = parser.parse_args()

    with GodViewProducer() as producer:
        if args.loop:
            logger.info(
                "Starting continuous GDELT ingestion (poll every %ds) …",
                POLL_INTERVAL_S,
            )
            last_url: Optional[str] = None
            while True:
                url = _fetch_latest_export_url()
                if url and url != last_url:
                    last_url = url
                    df = _download_and_parse(url)
                    if df is not None and not df.empty:
                        geo_df = _filter_geolocated(df)
                        if not geo_df.empty:
                            _publish_records(geo_df, producer)
                else:
                    logger.info("No new export — sleeping %ds …", POLL_INTERVAL_S)

                time.sleep(POLL_INTERVAL_S)
        else:
            count = run_once(producer)
            if count == 0:
                logger.warning("No records published.")
                sys.exit(1)


if __name__ == "__main__":
    main()
