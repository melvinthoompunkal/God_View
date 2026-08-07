"""
base_producer.py — Reusable Kafka producer for God_View ingestion pipeline.

Provides a `GodViewProducer` class that wraps `kafka-python`'s KafkaProducer
with:
  • Automatic JSON serialization of Python dicts
  • Configurable retries and connection back-off
  • Delivery callbacks (success / failure) with structured logging
  • Graceful shutdown via `close()` or context-manager protocol

Usage:
    from base_producer import GodViewProducer

    with GodViewProducer() as producer:
        producer.publish("raw-events", {"sensor_id": 42, "value": 3.14})
"""

from __future__ import annotations

import json
import logging
import os
import uuid
from typing import Any, Dict, Optional

from kafka import KafkaProducer
from kafka.errors import KafkaError, KafkaTimeoutError, NoBrokersAvailable

# --------------- Configuration ---------------
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
CLIENT_ID = os.getenv("KAFKA_CLIENT_ID", "godview-producer")
RETRIES = int(os.getenv("KAFKA_RETRIES", "5"))
ACKS = os.getenv("KAFKA_ACKS", "all")  # "all" for strongest durability
LINGER_MS = int(os.getenv("KAFKA_LINGER_MS", "10"))
BATCH_SIZE = int(os.getenv("KAFKA_BATCH_SIZE", "16384"))
MAX_BLOCK_MS = int(os.getenv("KAFKA_MAX_BLOCK_MS", "60000"))
REQUEST_TIMEOUT_MS = int(os.getenv("KAFKA_REQUEST_TIMEOUT_MS", "30000"))

# --------------- Logging ---------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  [%(levelname)s]  %(name)s  %(message)s",
)
logger = logging.getLogger(__name__)


class GodViewProducer:
    """Thread-safe Kafka producer with JSON serialization and delivery logging."""

    def __init__(
        self,
        bootstrap_servers: str = BOOTSTRAP_SERVERS,
        client_id: str = CLIENT_ID,
        retries: int = RETRIES,
        acks: str = ACKS,
        linger_ms: int = LINGER_MS,
        batch_size: int = BATCH_SIZE,
        max_block_ms: int = MAX_BLOCK_MS,
        request_timeout_ms: int = REQUEST_TIMEOUT_MS,
    ) -> None:
        self._bootstrap_servers = bootstrap_servers
        self._producer: Optional[KafkaProducer] = None

        try:
            self._producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                client_id=client_id,
                retries=retries,
                acks=acks,
                linger_ms=linger_ms,
                batch_size=batch_size,
                max_block_ms=max_block_ms,
                request_timeout_ms=request_timeout_ms,
                key_serializer=lambda k: k.encode("utf-8") if isinstance(k, str) else k,
                value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
            )
            logger.info(
                "KafkaProducer initialized  (bootstrap=%s, acks=%s, retries=%d)",
                bootstrap_servers,
                acks,
                retries,
            )
        except NoBrokersAvailable:
            logger.error(
                "No Kafka brokers available at %s — producer NOT started.",
                bootstrap_servers,
            )
            raise
        except KafkaError as exc:
            logger.error("Failed to create KafkaProducer: %s", exc)
            raise

    # ---- Context-manager protocol ----
    def __enter__(self) -> "GodViewProducer":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:  # noqa: ANN001
        self.close()

    # ---- Public API ----
    def publish(
        self,
        topic: str,
        payload: Dict[str, Any],
        key: Optional[str] = None,
        headers: Optional[list] = None,
    ) -> None:
        """Serialize *payload* as JSON and send it to *topic*.

        Parameters
        ----------
        topic:
            Kafka topic name (e.g. ``"raw-events"``).
        payload:
            Python dict to be JSON-serialized as the message value.
        key:
            Optional partition key (string). Messages with the same key
            are guaranteed to land in the same partition.
        headers:
            Optional list of ``(header_key, header_value)`` tuples.
        """
        if self._producer is None:
            raise RuntimeError("Producer is not initialised or has been closed.")

        if not isinstance(payload, dict):
            raise TypeError(
                f"payload must be a dict, got {type(payload).__name__}"
            )

        # Generate a correlation ID for tracing
        correlation_id = uuid.uuid4().hex[:12]

        try:
            future = self._producer.send(
                topic,
                key=key,
                value=payload,
                headers=headers,
            )

            # Register async callbacks
            future.add_callback(self._on_success, topic, correlation_id)
            future.add_errback(self._on_error, topic, correlation_id)

            logger.debug(
                "Message enqueued  topic=%s  key=%s  cid=%s",
                topic,
                key,
                correlation_id,
            )

        except KafkaTimeoutError:
            logger.error(
                "Timeout enqueuing message  topic=%s  cid=%s — buffer may be full.",
                topic,
                correlation_id,
            )
            raise
        except KafkaError as exc:
            logger.error(
                "Kafka error while publishing  topic=%s  cid=%s  error=%s",
                topic,
                correlation_id,
                exc,
            )
            raise

    def flush(self, timeout: Optional[float] = None) -> None:
        """Block until all buffered messages have been delivered."""
        if self._producer is None:
            return
        self._producer.flush(timeout=timeout)
        logger.debug("Producer buffer flushed.")

    def close(self) -> None:
        """Flush remaining messages and close the underlying producer."""
        if self._producer is not None:
            logger.info("Shutting down KafkaProducer …")
            self._producer.flush()
            self._producer.close()
            self._producer = None
            logger.info("KafkaProducer closed.")

    # ---- Delivery callbacks ----
    @staticmethod
    def _on_success(record_metadata, topic: str, correlation_id: str) -> None:  # noqa: ANN001
        logger.info(
            "✓ Delivered  topic=%s  partition=%d  offset=%d  cid=%s",
            topic,
            record_metadata.partition,
            record_metadata.offset,
            correlation_id,
        )

    @staticmethod
    def _on_error(exc: Exception, topic: str, correlation_id: str) -> None:
        logger.error(
            "✗ Delivery failed  topic=%s  cid=%s  error=%s",
            topic,
            correlation_id,
            exc,
        )
