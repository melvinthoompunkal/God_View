"""
db_writer.py — Kafka → PostgreSQL persistence worker for God_View.

Consumes messages from the ``raw-events`` Kafka topic using
``GodViewConsumer``, maps each payload to a ``GeospatialEvent`` ORM
instance, and inserts it into PostgreSQL via SQLAlchemy.

Handles both GDELT and EONET payloads by inspecting the ``_source``
field and normalising field names accordingly.

Usage:
    python db_writer.py                          # default: raw-events
    python db_writer.py --topic raw-events       # explicit topic
    python db_writer.py --batch-size 100         # flush every 100 rows
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from sqlalchemy.exc import IntegrityError, SQLAlchemyError

from base_consumer import GodViewConsumer, ParsedMessage
from db import SessionLocal
from models import GeospatialEvent

# --------------- Configuration ---------------
DEFAULT_TOPIC = os.getenv("DBWRITER_TOPIC", "raw-events")
DEFAULT_GROUP = os.getenv("DBWRITER_GROUP", "db-writer-group")
BATCH_SIZE = int(os.getenv("DBWRITER_BATCH_SIZE", "50"))

# --------------- Logging ---------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  [%(levelname)s]  %(name)s  %(message)s",
)
logger = logging.getLogger(__name__)


# ------------------------------------------------------------------
# Payload → ORM mapping
# ------------------------------------------------------------------

def _safe_float(val: Any) -> Optional[float]:
    """Convert a value to float, returning None on failure."""
    if val is None:
        return None
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


def _safe_str(val: Any, max_len: Optional[int] = None) -> Optional[str]:
    """Convert a value to a trimmed string, returning None for empties."""
    if val is None:
        return None
    s = str(val).strip()
    if not s:
        return None
    if max_len:
        s = s[:max_len]
    return s


def _parse_event_ts(payload: Dict[str, Any]) -> Optional[datetime]:
    """Try to extract an event timestamp from various payload formats."""
    # EONET payloads use 'date', GDELT uses 'SQLDATE' (YYYYMMDD)
    for key in ("date", "exchange_ts"):
        raw = payload.get(key)
        if raw:
            try:
                return datetime.fromisoformat(str(raw))
            except (ValueError, TypeError):
                pass

    sqldate = payload.get("SQLDATE")
    if sqldate:
        try:
            return datetime.strptime(str(sqldate), "%Y%m%d").replace(
                tzinfo=timezone.utc
            )
        except (ValueError, TypeError):
            pass

    return None


def _make_wkt_point(lon: float, lat: float) -> str:
    """Build a WKT POINT string for PostGIS."""
    return f"SRID=4326;POINT({lon} {lat})"


def payload_to_event(payload: Dict[str, Any]) -> Optional[GeospatialEvent]:
    """Map a raw-events Kafka payload to a GeospatialEvent ORM instance.

    Returns ``None`` if the payload lacks usable lat/lon.
    """
    source = _safe_str(payload.get("_source"), 64) or "unknown"

    # --- Resolve latitude / longitude ---
    # GDELT uses ActionGeo_Lat/Long; EONET uses latitude/longitude
    lat = _safe_float(
        payload.get("latitude")
        or payload.get("ActionGeo_Lat")
    )
    lon = _safe_float(
        payload.get("longitude")
        or payload.get("ActionGeo_Long")
    )

    if lat is None or lon is None:
        return None

    # Sanity-check ranges
    if not (-90 <= lat <= 90) or not (-180 <= lon <= 180):
        logger.debug("Out-of-range coords lat=%s lon=%s — skipping.", lat, lon)
        return None

    # --- Map common fields ---
    event = GeospatialEvent(
        source=source,
        event_type=_safe_str(
            payload.get("event_type")
            or payload.get("EventCode"),
            128,
        ),
        title=_safe_str(payload.get("title")),
        description=_safe_str(payload.get("description")),
        latitude=lat,
        longitude=lon,
        location=_make_wkt_point(lon, lat),
        country_code=_safe_str(
            payload.get("country_code")
            or payload.get("ActionGeo_CountryCode"),
            8,
        ),
        region_name=_safe_str(
            payload.get("region_name")
            or payload.get("ActionGeo_FullName"),
            256,
        ),
        severity=_safe_float(payload.get("severity")),
        goldstein_scale=_safe_float(payload.get("GoldsteinScale")),
        avg_tone=_safe_float(payload.get("AvgTone")),
        magnitude_value=_safe_float(payload.get("magnitude_value")),
        magnitude_unit=_safe_str(payload.get("magnitude_unit"), 32),
        raw_payload=payload,
        source_url=_safe_str(
            payload.get("source_url")
            or payload.get("SOURCEURL")
            or payload.get("link"),
        ),
        event_ts=_parse_event_ts(payload),
    )
    return event


# ------------------------------------------------------------------
# Batch writer
# ------------------------------------------------------------------

class BatchWriter:
    """Accumulates ORM instances and flushes them in batches."""

    def __init__(self, batch_size: int = BATCH_SIZE) -> None:
        self._batch_size = batch_size
        self._buffer: list[GeospatialEvent] = []
        self._total_written = 0
        self._total_skipped = 0

    def add(self, event: Optional[GeospatialEvent]) -> None:
        if event is None:
            self._total_skipped += 1
            return
        self._buffer.append(event)
        if len(self._buffer) >= self._batch_size:
            self.flush()

    def flush(self) -> None:
        if not self._buffer:
            return

        session = SessionLocal()
        try:
            session.add_all(self._buffer)
            session.commit()
            count = len(self._buffer)
            self._total_written += count
            logger.info(
                "Flushed %d events to DB  (total written: %d, skipped: %d)",
                count,
                self._total_written,
                self._total_skipped,
            )
        except IntegrityError as exc:
            session.rollback()
            logger.warning(
                "Integrity error during batch insert — falling back to "
                "row-by-row: %s",
                exc.orig,
            )
            self._insert_one_by_one(session)
        except SQLAlchemyError:
            session.rollback()
            logger.exception("Database error during batch flush.")
        finally:
            session.close()
            self._buffer = []

    def _insert_one_by_one(self, session) -> None:
        """Fallback: insert each event individually so one bad row
        doesn't block the rest of the batch."""
        inserted = 0
        for event in self._buffer:
            try:
                session.add(event)
                session.commit()
                inserted += 1
            except SQLAlchemyError:
                session.rollback()
                logger.debug(
                    "Skipped duplicate/bad row source=%s lat=%s lon=%s",
                    event.source,
                    event.latitude,
                    event.longitude,
                )
        self._total_written += inserted
        logger.info("Row-by-row fallback: inserted %d / %d", inserted, len(self._buffer))

    @property
    def stats(self) -> Dict[str, int]:
        return {
            "total_written": self._total_written,
            "total_skipped": self._total_skipped,
            "buffer_pending": len(self._buffer),
        }


# ------------------------------------------------------------------
# Main loop
# ------------------------------------------------------------------

def run(topic: str, group_id: str, batch_size: int) -> None:
    """Consume raw-events and persist them to PostgreSQL."""
    writer = BatchWriter(batch_size=batch_size)

    logger.info(
        "Starting db_writer  topic=%s  group=%s  batch_size=%d",
        topic,
        group_id,
        batch_size,
    )

    with GodViewConsumer(topic=topic, group_id=group_id) as consumer:
        try:
            for message in consumer.stream():
                event = payload_to_event(message.value)
                writer.add(event)
        except KeyboardInterrupt:
            logger.info("Interrupted — flushing remaining buffer …")
        finally:
            writer.flush()
            logger.info("Shutdown complete. %s", writer.stats)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="raw-events → PostgreSQL persistence worker"
    )
    parser.add_argument(
        "--topic",
        default=DEFAULT_TOPIC,
        help=f"Kafka topic to consume (default: {DEFAULT_TOPIC})",
    )
    parser.add_argument(
        "--group-id",
        default=DEFAULT_GROUP,
        help=f"Consumer group ID (default: {DEFAULT_GROUP})",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=BATCH_SIZE,
        help=f"Rows to buffer before flushing to DB (default: {BATCH_SIZE})",
    )
    args = parser.parse_args()

    run(topic=args.topic, group_id=args.group_id, batch_size=args.batch_size)


if __name__ == "__main__":
    main()
