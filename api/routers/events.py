"""
routers/events.py — Events API router for God_View.

Endpoints
---------
GET /api/events/history
    Query geospatial events from PostgreSQL within a time range.
    Returns results formatted as a GeoJSON FeatureCollection.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query
from geoalchemy2.shape import to_shape
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from dependencies import get_db

# Import the ORM model — lives in ml-workers/ but is shared across the
# project.  The model's parent package is added to sys.path by
# dependencies.py at import time.
from models import GeospatialEvent

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/events", tags=["events"])


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------

def _event_to_feature(event: GeospatialEvent) -> Dict[str, Any]:
    """Convert a GeospatialEvent ORM instance to a GeoJSON Feature dict.

    Uses GeoAlchemy2's ``to_shape()`` to extract the PostGIS Point
    as a Shapely geometry, then serialises it into GeoJSON coordinates.
    """
    # Extract lon/lat from the PostGIS column
    try:
        point = to_shape(event.location)
        coordinates = [point.x, point.y]
    except Exception:
        # Fallback to the plain float columns
        coordinates = [event.longitude, event.latitude]

    properties: Dict[str, Any] = {
        "id": str(event.id),
        "source": event.source,
        "event_type": event.event_type,
        "title": event.title,
        "description": event.description,
        "country_code": event.country_code,
        "region_name": event.region_name,
        "severity": event.severity,
        "goldstein_scale": event.goldstein_scale,
        "avg_tone": event.avg_tone,
        "magnitude_value": event.magnitude_value,
        "magnitude_unit": event.magnitude_unit,
        "source_url": event.source_url,
        "event_ts": event.event_ts.isoformat() if event.event_ts else None,
        "ingested_at": event.ingested_at.isoformat() if event.ingested_at else None,
    }

    return {
        "type": "Feature",
        "geometry": {
            "type": "Point",
            "coordinates": coordinates,
        },
        "properties": properties,
    }


def _build_feature_collection(
    features: List[Dict[str, Any]],
    *,
    total: int,
    start: str,
    end: str,
) -> Dict[str, Any]:
    """Wrap a list of GeoJSON Features in a FeatureCollection envelope."""
    return {
        "type": "FeatureCollection",
        "metadata": {
            "total": total,
            "returned": len(features),
            "time_range": {"start": start, "end": end},
        },
        "features": features,
    }


# ------------------------------------------------------------------
# Endpoint
# ------------------------------------------------------------------

@router.get(
    "/history",
    summary="Query historical events as GeoJSON",
    response_description="GeoJSON FeatureCollection of matching events",
)
async def get_events_history(
    start: Optional[datetime] = Query(
        default=None,
        description=(
            "Start of the time range (ISO-8601). "
            "Defaults to 24 hours ago if omitted."
        ),
        examples=["2026-08-01T00:00:00Z"],
    ),
    end: Optional[datetime] = Query(
        default=None,
        description=(
            "End of the time range (ISO-8601). "
            "Defaults to now if omitted."
        ),
        examples=["2026-08-07T00:00:00Z"],
    ),
    source: Optional[str] = Query(
        default=None,
        description="Filter by event source (e.g. 'gdelt', 'eonet').",
    ),
    event_type: Optional[str] = Query(
        default=None,
        description="Filter by event type (e.g. 'wildfire', 'severe_storm').",
    ),
    country: Optional[str] = Query(
        default=None,
        description="Filter by ISO country code (e.g. 'US', 'UA').",
    ),
    limit: int = Query(
        default=500,
        ge=1,
        le=5000,
        description="Maximum number of features to return.",
    ),
    offset: int = Query(
        default=0,
        ge=0,
        description="Number of records to skip (for pagination).",
    ),
    db: AsyncSession = Depends(get_db),
):
    """Return geospatial events within **[start, end]** as a GeoJSON
    ``FeatureCollection``.

    The time range filter applies to the ``event_ts`` column (when the
    event actually occurred).  Results are ordered newest-first.
    """
    # Default time window: last 24 hours
    now = datetime.now(timezone.utc)
    if end is None:
        end = now
    if start is None:
        start = end - timedelta(hours=24)

    # Ensure start < end
    if start >= end:
        raise HTTPException(
            status_code=422,
            detail="'start' must be earlier than 'end'.",
        )

    # Build query
    stmt = (
        select(GeospatialEvent)
        .where(GeospatialEvent.event_ts >= start)
        .where(GeospatialEvent.event_ts <= end)
    )

    if source:
        stmt = stmt.where(GeospatialEvent.source == source)
    if event_type:
        stmt = stmt.where(GeospatialEvent.event_type == event_type)
    if country:
        stmt = stmt.where(GeospatialEvent.country_code == country)

    stmt = stmt.order_by(GeospatialEvent.event_ts.desc())

    # Execute with limit / offset
    paginated = stmt.offset(offset).limit(limit)
    result = await db.execute(paginated)
    events = result.scalars().all()

    # Convert to GeoJSON
    features = [_event_to_feature(e) for e in events]

    logger.info(
        "GET /api/events/history  start=%s  end=%s  returned=%d",
        start.isoformat(),
        end.isoformat(),
        len(features),
    )

    return _build_feature_collection(
        features,
        total=len(features),
        start=start.isoformat(),
        end=end.isoformat(),
    )
