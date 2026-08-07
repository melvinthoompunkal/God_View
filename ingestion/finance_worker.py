"""
finance_worker.py — Finnhub WebSocket ingestion worker for God_View.

Connects to the free Finnhub WebSocket endpoint for real-time stock
trade data, normalises each tick into a standardised JSON schema, and
publishes it to the ``market-data`` Kafka topic.

Subscribes to a configurable list of symbols (default: SPY, AAPL).

Usage:
    FINNHUB_API_KEY=<your_key> python finance_worker.py
    FINNHUB_SYMBOLS=SPY,AAPL,MSFT,TSLA python finance_worker.py
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import signal
import sys
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import websockets
import websockets.exceptions

from base_producer import GodViewProducer

# --------------- Configuration ---------------
FINNHUB_API_KEY = os.getenv("FINNHUB_API_KEY", "")
FINNHUB_WS_URL = f"wss://ws.finnhub.io?token={FINNHUB_API_KEY}"

SYMBOLS: List[str] = [
    s.strip().upper()
    for s in os.getenv("FINNHUB_SYMBOLS", "SPY,AAPL").split(",")
    if s.strip()
]

KAFKA_TOPIC = os.getenv("FINANCE_KAFKA_TOPIC", "market-data")
RECONNECT_DELAY_S = int(os.getenv("FINNHUB_RECONNECT_DELAY", "5"))
MAX_RECONNECT_DELAY_S = int(os.getenv("FINNHUB_MAX_RECONNECT_DELAY", "120"))

# --------------- Logging ---------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  [%(levelname)s]  %(name)s  %(message)s",
)
logger = logging.getLogger(__name__)

# Thread pool for offloading sync Kafka calls
_executor = ThreadPoolExecutor(max_workers=4)


# ------------------------------------------------------------------
# Normalisation
# ------------------------------------------------------------------

def _normalise_trade(tick: Dict[str, Any]) -> Dict[str, Any]:
    """Convert a raw Finnhub trade tick into a standardised JSON record.

    Raw tick format from Finnhub:
        {
            "s": "AAPL",      // symbol
            "p": 187.50,      // last price
            "t": 161823456789 // UNIX milliseconds
            "v": 100,         // volume
            "c": [...]        // trade conditions
        }

    Standardised output:
        {
            "symbol":          "AAPL",
            "price":           187.50,
            "volume":          100,
            "trade_conditions": [...],
            "exchange_ts":     "2025-06-01T14:30:00.789000+00:00",
            "exchange_ts_ms":  1618234567890,
            "_source":         "finnhub",
            "_ingested_at":    "2025-06-01T14:30:01.123456+00:00"
        }
    """
    ts_ms = tick.get("t", 0)
    exchange_dt = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc)

    return {
        "symbol": tick.get("s"),
        "price": tick.get("p"),
        "volume": tick.get("v"),
        "trade_conditions": tick.get("c", []),
        "exchange_ts": exchange_dt.isoformat(),
        "exchange_ts_ms": ts_ms,
        "_source": "finnhub",
        "_ingested_at": datetime.now(timezone.utc).isoformat(),
    }


# ------------------------------------------------------------------
# Kafka publishing (sync, run in executor)
# ------------------------------------------------------------------

def _publish_trades(
    records: List[Dict[str, Any]],
    producer: GodViewProducer,
) -> int:
    """Publish a batch of normalised trade records to Kafka."""
    published = 0
    for rec in records:
        key = rec.get("symbol", "UNKNOWN")
        try:
            producer.publish(KAFKA_TOPIC, rec, key=key)
            published += 1
        except Exception:
            logger.exception("Failed to publish trade for %s", key)

    producer.flush()
    return published


# ------------------------------------------------------------------
# WebSocket client
# ------------------------------------------------------------------

async def _subscribe(ws, symbols: List[str]) -> None:
    """Send subscription messages for each symbol."""
    for sym in symbols:
        msg = json.dumps({"type": "subscribe", "symbol": sym})
        await ws.send(msg)
        logger.info("Subscribed to %s", sym)


async def _listen(
    ws,
    producer: GodViewProducer,
    loop: asyncio.AbstractEventLoop,
) -> None:
    """Read trade messages, normalise, and publish to Kafka."""
    total_published = 0
    batch: List[Dict[str, Any]] = []
    batch_size = 50  # flush to Kafka in batches for throughput

    async for raw_msg in ws:
        try:
            message = json.loads(raw_msg)
        except json.JSONDecodeError:
            logger.warning("Non-JSON message received — skipping.")
            continue

        msg_type = message.get("type")

        if msg_type == "ping":
            # Finnhub sends heartbeat pings — just log and continue
            logger.debug("Heartbeat ping received.")
            continue

        if msg_type != "trade":
            logger.debug("Ignoring message type: %s", msg_type)
            continue

        trades = message.get("data", [])
        for tick in trades:
            record = _normalise_trade(tick)
            batch.append(record)

        # Flush batch when it reaches threshold
        if len(batch) >= batch_size:
            count = await loop.run_in_executor(
                _executor, _publish_trades, batch, producer
            )
            total_published += count
            if total_published % 1000 < batch_size:
                logger.info(
                    "Total ticks published so far: %d", total_published
                )
            batch = []

    # Flush remaining
    if batch:
        count = await loop.run_in_executor(
            _executor, _publish_trades, batch, producer
        )
        total_published += count

    logger.info("WebSocket closed — published %d ticks total.", total_published)


async def _connect_with_backoff(
    producer: GodViewProducer,
    loop: asyncio.AbstractEventLoop,
) -> None:
    """Connect to Finnhub with exponential back-off on failures."""
    delay = RECONNECT_DELAY_S

    while True:
        try:
            logger.info("Connecting to Finnhub WebSocket …")
            async with websockets.connect(
                FINNHUB_WS_URL,
                ping_interval=30,
                ping_timeout=10,
                close_timeout=5,
            ) as ws:
                logger.info("WebSocket connected.")
                delay = RECONNECT_DELAY_S  # reset back-off on success

                await _subscribe(ws, SYMBOLS)
                await _listen(ws, producer, loop)

        except websockets.exceptions.ConnectionClosed as exc:
            logger.warning("WebSocket closed: code=%s reason=%s", exc.code, exc.reason)
        except websockets.exceptions.WebSocketException as exc:
            logger.error("WebSocket error: %s", exc)
        except OSError as exc:
            logger.error("Network error: %s", exc)

        logger.info("Reconnecting in %ds …", delay)
        await asyncio.sleep(delay)
        delay = min(delay * 2, MAX_RECONNECT_DELAY_S)


# ------------------------------------------------------------------
# Main
# ------------------------------------------------------------------

async def async_main() -> None:
    loop = asyncio.get_running_loop()

    # Graceful shutdown on SIGINT / SIGTERM
    stop_event = asyncio.Event()

    def _shutdown(sig: signal.Signals) -> None:
        logger.info("Received %s — shutting down …", sig.name)
        stop_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _shutdown, sig)
        except NotImplementedError:
            # Windows doesn't support add_signal_handler
            pass

    with GodViewProducer() as producer:
        ws_task = asyncio.create_task(
            _connect_with_backoff(producer, loop)
        )

        # Wait until a stop signal or the WS task completes
        done, _ = await asyncio.wait(
            [ws_task, asyncio.create_task(stop_event.wait())],
            return_when=asyncio.FIRST_COMPLETED,
        )

        if not ws_task.done():
            ws_task.cancel()
            try:
                await ws_task
            except asyncio.CancelledError:
                pass

    logger.info("Finance worker stopped.")


def main() -> None:
    if not FINNHUB_API_KEY:
        logger.error(
            "FINNHUB_API_KEY is not set. "
            "Get a free key at https://finnhub.io and export it."
        )
        sys.exit(1)

    logger.info("Symbols: %s", ", ".join(SYMBOLS))
    asyncio.run(async_main())


if __name__ == "__main__":
    main()
