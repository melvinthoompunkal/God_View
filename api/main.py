"""
main.py — FastAPI application entry point for God_View.

Provides:
  • CORS middleware allowing all origins (local frontend dev)
  • /health endpoint returning 200 OK with uptime metadata
  • Lifespan hook for future startup/shutdown logic (DB pools, Redis, etc.)

Usage:
    uvicorn main:app --reload --port 8000
    # or
    python main.py
"""

from __future__ import annotations

import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from dependencies import close_cache, close_db, init_cache, init_db
from routers.events import router as events_router

# --------------- Lifespan ---------------

_start_time: float = 0.0


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup / shutdown lifecycle hook.

    Initialises shared resources (Redis pool, etc.) on startup and
    tears them down gracefully on shutdown.
    """
    global _start_time
    _start_time = time.monotonic()

    # — startup —
    await init_cache()
    await init_db()

    yield

    # — shutdown —
    await close_db()
    await close_cache()


# --------------- Application ---------------

app = FastAPI(
    title="God_View API",
    description="Backend API for the God_View geospatial intelligence platform.",
    version="0.1.0",
    lifespan=lifespan,
)

# --------------- Routers ---------------
app.include_router(events_router)

# --------------- CORS Middleware ---------------
# Allow all origins so the local frontend (Vite / Next dev server) can
# connect without cross-origin errors.  Tighten these settings before
# deploying to production.

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# --------------- Health Check ---------------

@app.get(
    "/health",
    tags=["ops"],
    summary="Health check",
    response_description="Service health status",
)
async def health_check():
    """Return 200 OK with basic service metadata.

    Useful for Docker ``HEALTHCHECK``, load-balancer probes, and the
    frontend connection indicator.
    """
    uptime_s = round(time.monotonic() - _start_time, 2) if _start_time else 0
    return {
        "status": "ok",
        "service": "god_view_api",
        "version": app.version,
        "uptime_seconds": uptime_s,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


# --------------- Local dev runner ---------------

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
    )
