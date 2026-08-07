"""
nlp_worker.py — Headline sentiment enrichment worker for God_View.

Consumes raw events from the ``raw-events`` Kafka topic, extracts news
headlines / titles from the payload, classifies each via the Hugging Face
Inference API (facebook/bart-large-mnli zero-shot classification), and
stores the enriched record in Redis for fast frontend retrieval.

Sentiment labels:
    • Bullish   — positive market / geopolitical signal
    • Bearish   — negative market / geopolitical signal
    • Escalation — conflict / disaster intensity increasing

Usage:
    HF_API_TOKEN=hf_xxxxx python nlp_worker.py
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import signal
import sys
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import redis
import requests

# Project-local imports
from base_consumer import GodViewConsumer

# --------------- Configuration ---------------

# Kafka
CONSUME_TOPIC = os.getenv("NLP_CONSUME_TOPIC", "raw-events")
CONSUMER_GROUP = os.getenv("NLP_GROUP_ID", "godview-nlp-enrichment")

# Hugging Face Inference API
HF_API_TOKEN = os.getenv("HF_API_TOKEN", "")
HF_MODEL = os.getenv("HF_MODEL", "facebook/bart-large-mnli")
HF_API_URL = (
    os.getenv("HF_API_URL")
    or f"https://api-inference.huggingface.co/models/{HF_MODEL}"
)
HF_TIMEOUT_S = int(os.getenv("HF_TIMEOUT_S", "15"))
HF_MAX_RETRIES = int(os.getenv("HF_MAX_RETRIES", "3"))
HF_RETRY_DELAY_S = float(os.getenv("HF_RETRY_DELAY_S", "2.0"))

# Zero-shot candidate labels
CANDIDATE_LABELS: List[str] = ["Bullish", "Bearish", "Escalation"]

# Redis
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_DB = int(os.getenv("REDIS_DB", "0"))
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD", None)
REDIS_KEY_PREFIX = os.getenv("NLP_REDIS_PREFIX", "godview:nlp:")
REDIS_TTL_S = int(os.getenv("NLP_REDIS_TTL_S", "3600"))  # 1 hour default

# Fields in raw-events payloads that may contain headlines
HEADLINE_FIELDS = ["title", "description"]

# --------------- Logging ---------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  [%(levelname)s]  %(name)s  %(message)s",
)
logger = logging.getLogger(__name__)


# ------------------------------------------------------------------
# Redis Client
# ------------------------------------------------------------------

def _connect_redis() -> redis.Redis:
    """Create and verify a Redis connection."""
    client = redis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        db=REDIS_DB,
        password=REDIS_PASSWORD,
        decode_responses=True,
        socket_connect_timeout=5,
        retry_on_timeout=True,
    )
    # Verify connectivity
    client.ping()
    logger.info(
        "Redis connected  host=%s  port=%d  db=%d",
        REDIS_HOST,
        REDIS_PORT,
        REDIS_DB,
    )
    return client


# ------------------------------------------------------------------
# Hugging Face Inference API
# ------------------------------------------------------------------

def _classify_headline(
    text: str,
    session: requests.Session,
) -> Optional[Dict[str, Any]]:
    """Send *text* to HF zero-shot classification and return results.

    Returns a dict like::

        {
            "label": "Bullish",
            "score": 0.87,
            "scores": {"Bullish": 0.87, "Bearish": 0.09, "Escalation": 0.04}
        }

    Returns ``None`` on unrecoverable failure.
    """
    payload = {
        "inputs": text,
        "parameters": {
            "candidate_labels": CANDIDATE_LABELS,
        },
    }

    headers: Dict[str, str] = {"Content-Type": "application/json"}
    if HF_API_TOKEN:
        headers["Authorization"] = f"Bearer {HF_API_TOKEN}"

    last_error: Optional[Exception] = None

    for attempt in range(1, HF_MAX_RETRIES + 1):
        try:
            resp = session.post(
                HF_API_URL,
                json=payload,
                headers=headers,
                timeout=HF_TIMEOUT_S,
            )

            # HF returns 503 while the model is loading — back off and retry
            if resp.status_code == 503:
                body = resp.json()
                wait = body.get("estimated_time", HF_RETRY_DELAY_S)
                logger.info(
                    "Model loading (attempt %d/%d) — retrying in %.1fs …",
                    attempt,
                    HF_MAX_RETRIES,
                    wait,
                )
                time.sleep(wait)
                continue

            resp.raise_for_status()
            result = resp.json()

            # Standard HF zero-shot response structure
            labels = result.get("labels", [])
            scores = result.get("scores", [])

            if not labels or not scores:
                logger.warning("Unexpected HF response shape: %s", result)
                return None

            score_map = dict(zip(labels, scores))
            return {
                "label": labels[0],
                "score": round(scores[0], 4),
                "scores": {k: round(v, 4) for k, v in score_map.items()},
            }

        except requests.exceptions.Timeout:
            logger.warning(
                "HF API timeout (attempt %d/%d).", attempt, HF_MAX_RETRIES
            )
            last_error = TimeoutError("HF API timeout")
        except requests.exceptions.RequestException as exc:
            logger.error(
                "HF API error (attempt %d/%d): %s", attempt, HF_MAX_RETRIES, exc
            )
            last_error = exc

        time.sleep(HF_RETRY_DELAY_S * attempt)

    logger.error("HF classification failed after %d attempts: %s", HF_MAX_RETRIES, last_error)
    return None


# ------------------------------------------------------------------
# Headline Extraction
# ------------------------------------------------------------------

def _extract_headline(event: Dict[str, Any]) -> Optional[str]:
    """Return the first non-empty headline-like field from *event*.

    Checks ``title`` and ``description`` in order, returning the first
    string that is long enough to classify meaningfully.
    """
    for field in HEADLINE_FIELDS:
        text = event.get(field)
        if isinstance(text, str) and len(text.strip()) >= 10:
            return text.strip()
    return None


# ------------------------------------------------------------------
# Redis Storage
# ------------------------------------------------------------------

def _store_enriched(
    event: Dict[str, Any],
    redis_client: redis.Redis,
) -> str:
    """Write the enriched event to Redis and return the key used.

    Storage strategy:
        • Each enriched event is stored as a Redis Hash under a
          deterministic key derived from its content.
        • A sorted set ``godview:nlp:latest`` keeps an index ordered
          by ingestion timestamp so the frontend can paginate.
    """
    # Deterministic key: hash the headline + source + event id (if present)
    raw_id = (
        f"{event.get('headline', '')}"
        f"::{event.get('_source', '')}"
        f"::{event.get('event_id', event.get('GLOBALEVENTID', ''))}"
    )
    short_hash = hashlib.sha256(raw_id.encode()).hexdigest()[:16]
    key = f"{REDIS_KEY_PREFIX}{short_hash}"

    # Flatten for Redis HSET (values must be strings)
    flat: Dict[str, str] = {}
    for k, v in event.items():
        if isinstance(v, (dict, list)):
            flat[k] = json.dumps(v, default=str)
        elif v is not None:
            flat[k] = str(v)

    pipe = redis_client.pipeline(transaction=False)
    pipe.hset(key, mapping=flat)
    pipe.expire(key, REDIS_TTL_S)

    # Add to the "latest" sorted set (score = epoch seconds)
    ts = datetime.now(timezone.utc).timestamp()
    index_key = f"{REDIS_KEY_PREFIX}latest"
    pipe.zadd(index_key, {key: ts})

    # Trim the sorted set to keep the most recent 5 000 entries
    pipe.zremrangebyrank(index_key, 0, -5001)

    pipe.execute()
    return key


# ------------------------------------------------------------------
# Worker
# ------------------------------------------------------------------

class NlpWorker:
    """Consumes raw-events, classifies headlines, writes to Redis."""

    def __init__(self) -> None:
        self._processed: int = 0
        self._classified: int = 0
        self._skipped: int = 0

    def run(self) -> None:
        logger.info(
            "Starting NlpWorker  topic=%s  group=%s  model=%s  redis=%s:%d",
            CONSUME_TOPIC,
            CONSUMER_GROUP,
            HF_MODEL,
            REDIS_HOST,
            REDIS_PORT,
        )

        if not HF_API_TOKEN:
            logger.warning(
                "HF_API_TOKEN is not set — requests may be rate-limited. "
                "Get a token at https://huggingface.co/settings/tokens"
            )

        redis_client = _connect_redis()
        http_session = requests.Session()

        try:
            with GodViewConsumer(topic=CONSUME_TOPIC, group_id=CONSUMER_GROUP) as consumer:
                for message in consumer.stream():
                    event = message.value
                    self._processed += 1

                    # Skip deserialization errors surfaced by base_consumer
                    if "_error" in event:
                        self._skipped += 1
                        continue

                    # Extract headline text
                    headline = _extract_headline(event)
                    if headline is None:
                        self._skipped += 1
                        continue

                    # Classify via HF Inference API
                    result = _classify_headline(headline, http_session)
                    if result is None:
                        logger.debug(
                            "Classification failed for headline: %.80s…", headline
                        )
                        self._skipped += 1
                        continue

                    # Enrich the original event
                    enriched = {
                        **event,
                        "headline": headline,
                        "sentiment_label": result["label"],
                        "sentiment_score": result["score"],
                        "sentiment_scores": result["scores"],
                        "_enriched_at": datetime.now(timezone.utc).isoformat(),
                        "_enriched_by": "nlp_worker",
                    }

                    # Write to Redis
                    redis_key = _store_enriched(enriched, redis_client)
                    self._classified += 1

                    logger.info(
                        "✓ %s  [%.2f]  %.60s…  → %s",
                        result["label"],
                        result["score"],
                        headline,
                        redis_key,
                    )

                    # Periodic stats
                    if self._processed % 100 == 0:
                        logger.info(
                            "Stats  processed=%d  classified=%d  skipped=%d",
                            self._processed,
                            self._classified,
                            self._skipped,
                        )

        except KeyboardInterrupt:
            logger.info("Interrupted — shutting down.")
        finally:
            http_session.close()
            redis_client.close()

        logger.info(
            "NlpWorker stopped — processed=%d  classified=%d  skipped=%d",
            self._processed,
            self._classified,
            self._skipped,
        )


# ------------------------------------------------------------------
# Entry Point
# ------------------------------------------------------------------

def main() -> None:
    worker = NlpWorker()
    worker.run()


if __name__ == "__main__":
    main()
