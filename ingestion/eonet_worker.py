"""
eonet_worker.py — Async NASA EONET ingestion worker for God_View.

Queries the NASA EONET v3 GeoJSON endpoint for active wildfires and
severe storms, normalises each feature into a flat JSON record, and
publishes it to the ``raw-events`` Kafka topic.

Uses ``aiohttp`` for non-blocking HTTP and ``asyncio`` for the event
loop.  Kafka publishing is done via the synchronous ``GodViewProducer``
(called from an executor so the event loop stays free).

Usage:
    python eonet_worker.py                  # single fetch cycle
    python eonet_worker.py --loop           # poll every 15 minutes
    NASA_API_KEY=DEMO_KEY python eonet_worker.py --loop
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import sys
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import aiohttp

from base_producer import GodViewProducer

# --------------- Configuration ---------------
NASA_API_KEY = os.getenv("NASA_API_KEY", "DEMO_KEY")
EONET_BASE_URL = "https://eonet.gsfc.nasa.gov/api/v3/events/geojson"
EONET_CATEGORIES = os.getenv("EONET_CATEGORIES", "wildfires,severeStorms")
EONET_STATUS = os.getenv("EONET_STATUS", "open")  # "open", "closed", or "all"
EONET_LIMIT = int(os.getenv("EONET_LIMIT", "300"))

KAFKA_TOPIC = os.getenv("EONET_KAFKA_TOPIC", "raw-events")
POLL_INTERVAL_S = int(os.getenv("EONET_POLL_INTERVAL", "900"))  # 15 minutes
REQUEST_TIMEOUT_S = int(os.getenv("EONET_REQUEST_TIMEOUT", "30"))

# --------------- Logging ---------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  [%(levelname)s]  %(name)s  %(message)s",
)
logger = logging.getLogger(__name__)

# Thread pool for offloading sync Kafka calls from the async loop
_executor = ThreadPoolExecutor(max_workers=4)


# ------------------------------------------------------------------
# EONET fetch
# ------------------------------------------------------------------

async def fetch_eonet_events(
    session: aiohttp.ClientSession,
) -> Optional[List[Dict[str, Any]]]:
    """Query the EONET GeoJSON endpoint and return the features list."""
    params = {
        "api_key": NASA_API_KEY,
        "category": EONET_CATEGORIES,
        "status": EONET_STATUS,
        "limit": EONET_LIMIT,
    }

    try:
        async with session.get(
            EONET_BASE_URL,
            params=params,
            timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT_S),
        ) as resp:
            resp.raise_for_status()
            data = await resp.json(content_type=None)
    except aiohttp.ClientError as exc:
        logger.error("EONET request failed: %s", exc)
        return None
    except asyncio.TimeoutError:
        logger.error("EONET request timed out after %ds.", REQUEST_TIMEOUT_S)
        return None

    features = data.get("features", [])
    logger.info("Fetched %d features from EONET.", len(features))
    return features


# ------------------------------------------------------------------
# Normalisation
# ------------------------------------------------------------------

def _normalise_feature(feature: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Flatten a GeoJSON Feature into a publishable JSON record.

    Returns ``None`` if the feature lacks usable coordinates.
    """
    props = feature.get("properties", {})
    geometry = feature.get("geometry", {})
    geo_type = geometry.get("type")
    coordinates = geometry.get("coordinates")

    if not coordinates:
        return None

    # Extract lat/lon depending on geometry type
    if geo_type == "Point":
        lon, lat = coordinates[0], coordinates[1]
    elif geo_type == "Polygon":
        # Use the centroid of the first ring as a representative point
        ring = coordinates[0]
        lon = sum(c[0] for c in ring) / len(ring)
        lat = sum(c[1] for c in ring) / len(ring)
    else:
        logger.debug("Unsupported geometry type: %s", geo_type)
        return None

    # Determine category label
    categories = props.get("categories", [])
    category_ids = [c.get("id", "") for c in categories]
    category_titles = [c.get("title", "") for c in categories]

    # Map EONET categories to a simplified event_type
    if "wildfires" in category_ids:
        event_type = "wildfire"
    elif "severeStorms" in category_ids:
        event_type = "severe_storm"
    else:
        event_type = category_ids[0] if category_ids else "unknown"

    record: Dict[str, Any] = {
        "event_id": props.get("id"),
        "title": props.get("title"),
        "description": props.get("description"),
        "event_type": event_type,
        "categories": category_titles,
        "date": props.get("date"),
        "closed": props.get("closed"),
        "magnitude_value": props.get("magnitudeValue"),
        "magnitude_unit": props.get("magnitudeUnit"),
        "latitude": round(lat, 6),
        "longitude": round(lon, 6),
        "geometry_type": geo_type,
        "sources": [
            {"id": s.get("id"), "url": s.get("url")}
            for s in props.get("sources", [])
        ],
        "link": props.get("link"),
        "_source": "eonet",
        "_ingested_at": datetime.now(timezone.utc).isoformat(),
    }
    return record


def normalise_features(
    features: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Normalise a list of GeoJSON features, dropping those without coords."""
    records = []
    for feat in features:
        rec = _normalise_feature(feat)
        if rec is not None:
            records.append(rec)

    logger.info(
        "Normalised %d / %d features (dropped %d without coords).",
        len(records),
        len(features),
        len(features) - len(records),
    )
    return records


# ------------------------------------------------------------------
# Kafka publishing (sync, called from executor)
# ------------------------------------------------------------------

def _publish_batch(
    records: List[Dict[str, Any]],
    producer: GodViewProducer,
) -> int:
    """Publish normalised records to Kafka. Returns count published."""
    published = 0
    for rec in records:
        key = rec.get("event_type", "unknown")
        try:
            producer.publish(KAFKA_TOPIC, rec, key=key)
            published += 1
        except Exception:
            logger.exception(
                "Failed to publish EONET event %s", rec.get("event_id")
            )

    producer.flush()
    logger.info("Published %d EONET records to '%s'.", published, KAFKA_TOPIC)
    return published


# ------------------------------------------------------------------
# Orchestration
# ------------------------------------------------------------------

async def run_once(
    session: aiohttp.ClientSession,
    producer: GodViewProducer,
    loop: asyncio.AbstractEventLoop,
) -> int:
    """Execute a single fetch → normalise → publish cycle."""
    features = await fetch_eonet_events(session)
    if not features:
        return 0

    records = normalise_features(features)
    if not records:
        logger.warning("No publishable EONET records in this batch.")
        return 0

    # Offload blocking Kafka calls to a thread
    count = await loop.run_in_executor(
        _executor, _publish_batch, records, producer
    )
    return count


async def run_loop(
    session: aiohttp.ClientSession,
    producer: GodViewProducer,
    loop: asyncio.AbstractEventLoop,
) -> None:
    """Poll EONET continuously at POLL_INTERVAL_S."""
    logger.info(
        "Starting continuous EONET ingestion (poll every %ds) …",
        POLL_INTERVAL_S,
    )
    while True:
        await run_once(session, producer, loop)
        logger.info("Sleeping %ds until next poll …", POLL_INTERVAL_S)
        await asyncio.sleep(POLL_INTERVAL_S)


async def async_main(continuous: bool) -> None:
    loop = asyncio.get_running_loop()

    async with aiohttp.ClientSession() as session:
        with GodViewProducer() as producer:
            if continuous:
                await run_loop(session, producer, loop)
            else:
                count = await run_once(session, producer, loop)
                if count == 0:
                    logger.warning("No records published.")
                    sys.exit(1)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="NASA EONET → Kafka ingestion worker"
    )
    parser.add_argument(
        "--loop",
        action="store_true",
        help=f"Poll continuously every {POLL_INTERVAL_S}s (default: run once)",
    )
    args = parser.parse_args()

    asyncio.run(async_main(continuous=args.loop))


if __name__ == "__main__":
    main()
