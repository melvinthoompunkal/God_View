"""
dependencies.py — Shared FastAPI dependencies for God_View API.

Provides:
  • An application-scoped async Redis connection pool (managed via lifespan)
  • ``get_cache()`` — a FastAPI dependency that yields a ``redis.asyncio.Redis``
    client for reading/writing cached data
  • An application-scoped async SQLAlchemy engine (managed via lifespan)
  • ``get_db()`` — a FastAPI dependency that yields an ``AsyncSession``
    for database queries

Usage in a route::

    from dependencies import get_cache, get_db
    from redis.asyncio import Redis
    from sqlalchemy.ext.asyncio import AsyncSession

    @router.get("/items/{item_id}")
    async def get_item(
        item_id: str,
        cache: Redis = Depends(get_cache),
        db: AsyncSession = Depends(get_db),
    ):
        ...
"""

from __future__ import annotations

import logging
import os
import sys
from typing import AsyncGenerator, Optional

import redis.asyncio as aioredis
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

# --------------- sys.path: shared models ---------------
# The ORM models (GeospatialEvent, SystemAlert) live in ml-workers/.
# Add that directory so ``from models import ...`` works from the API
# package without duplicating the model definitions.

_ml_workers_dir = os.path.join(os.path.dirname(__file__), os.pardir, "ml-workers")
_ml_workers_dir = os.path.abspath(_ml_workers_dir)
if _ml_workers_dir not in sys.path:
    sys.path.insert(0, _ml_workers_dir)

# --------------- Configuration ---------------
# Matches the env-var conventions used across the project.

# Redis
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_DB = int(os.getenv("REDIS_DB", "0"))
REDIS_PASSWORD: Optional[str] = os.getenv("REDIS_PASSWORD", None)
REDIS_MAX_CONNECTIONS = int(os.getenv("REDIS_MAX_CONNECTIONS", "20"))

# PostgreSQL (async driver: asyncpg)
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql+asyncpg://godview:changeme@localhost:5432/godview_db",
)

# --------------- Logging ---------------
logger = logging.getLogger(__name__)

# --------------- Module-level singletons ---------------
_pool: Optional[aioredis.ConnectionPool] = None
_async_engine: Optional[AsyncEngine] = None
_async_session_factory: Optional[async_sessionmaker[AsyncSession]] = None


# ================================================================
# Redis lifecycle + dependency
# ================================================================

async def init_cache() -> None:
    """Create the application-wide Redis connection pool.

    Call once during FastAPI startup (inside the ``lifespan`` context
    manager).
    """
    global _pool

    _pool = aioredis.ConnectionPool.from_url(
        f"redis://{REDIS_HOST}:{REDIS_PORT}/{REDIS_DB}",
        password=REDIS_PASSWORD,
        max_connections=REDIS_MAX_CONNECTIONS,
        decode_responses=True,
        socket_connect_timeout=5,
        retry_on_timeout=True,
    )

    # Verify connectivity with a quick PING
    client = aioredis.Redis(connection_pool=_pool)
    try:
        await client.ping()
        logger.info(
            "Redis pool initialised  host=%s  port=%d  db=%d  max_conn=%d",
            REDIS_HOST,
            REDIS_PORT,
            REDIS_DB,
            REDIS_MAX_CONNECTIONS,
        )
    except aioredis.RedisError as exc:
        logger.error("Redis PING failed — cache will be unavailable: %s", exc)
        raise
    finally:
        await client.aclose()


async def close_cache() -> None:
    """Drain and close the Redis connection pool.

    Call once during FastAPI shutdown.
    """
    global _pool
    if _pool is not None:
        await _pool.disconnect()
        _pool = None
        logger.info("Redis pool closed.")


async def get_cache() -> AsyncGenerator[aioredis.Redis, None]:
    """Yield an async Redis client backed by the shared connection pool.

    Inject via ``Depends(get_cache)``.
    """
    if _pool is None:
        raise RuntimeError(
            "Redis pool is not initialised. "
            "Ensure init_cache() is called in the application lifespan."
        )

    client = aioredis.Redis(connection_pool=_pool)
    try:
        yield client
    finally:
        await client.aclose()


# ================================================================
# PostgreSQL (async SQLAlchemy) lifecycle + dependency
# ================================================================

async def init_db() -> None:
    """Create the async SQLAlchemy engine and session factory.

    Call once during FastAPI startup.
    """
    global _async_engine, _async_session_factory

    _async_engine = create_async_engine(
        DATABASE_URL,
        echo=False,
        pool_pre_ping=True,
        pool_size=10,
        max_overflow=20,
    )

    _async_session_factory = async_sessionmaker(
        bind=_async_engine,
        class_=AsyncSession,
        expire_on_commit=False,
    )

    # Quick connectivity check
    async with _async_engine.connect() as conn:
        await conn.execute(
            __import__("sqlalchemy").text("SELECT 1")
        )

    logger.info("Async SQLAlchemy engine initialised  url=%s", DATABASE_URL)


async def close_db() -> None:
    """Dispose of the async engine's connection pool.

    Call once during FastAPI shutdown.
    """
    global _async_engine, _async_session_factory
    if _async_engine is not None:
        await _async_engine.dispose()
        _async_engine = None
        _async_session_factory = None
        logger.info("Async SQLAlchemy engine disposed.")


async def get_db() -> AsyncGenerator[AsyncSession, None]:
    """Yield an async SQLAlchemy session for the duration of a request.

    Inject via ``Depends(get_db)``::

        @app.get("/example")
        async def example(db: AsyncSession = Depends(get_db)):
            result = await db.execute(select(SomeModel))
            ...

    The session is automatically closed when the request completes.
    """
    if _async_session_factory is None:
        raise RuntimeError(
            "Database is not initialised. "
            "Ensure init_db() is called in the application lifespan."
        )

    async with _async_session_factory() as session:
        try:
            yield session
        except Exception:
            await session.rollback()
            raise
        finally:
            await session.close()

