"""
base_consumer.py — Reusable Kafka consumer for God_View ML workers.

Provides a `GodViewConsumer` class that wraps `kafka-python`'s KafkaConsumer
with:
  • Automatic JSON deserialization of incoming messages
  • A generator method `stream()` that continuously yields parsed messages
  • Configurable consumer group, offsets, and polling behaviour
  • Graceful shutdown via `close()` or context-manager protocol
  • Robust error handling for deserialization and broker failures

Usage:
    from base_consumer import GodViewConsumer

    with GodViewConsumer(topic="raw-events", group_id="ml-pipeline") as consumer:
        for message in consumer.stream():
            print(message.value)
"""

from __future__ import annotations

import json
import logging
import os
import signal
import sys
from dataclasses import dataclass
from typing import Any, Dict, Generator, Optional

from kafka import KafkaConsumer
from kafka.errors import KafkaError, NoBrokersAvailable

# --------------- Configuration ---------------
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
DEFAULT_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "godview-workers")
AUTO_OFFSET_RESET = os.getenv("KAFKA_AUTO_OFFSET_RESET", "earliest")
ENABLE_AUTO_COMMIT = os.getenv("KAFKA_AUTO_COMMIT", "true").lower() == "true"
AUTO_COMMIT_INTERVAL_MS = int(os.getenv("KAFKA_AUTO_COMMIT_INTERVAL_MS", "5000"))
POLL_TIMEOUT_MS = int(os.getenv("KAFKA_POLL_TIMEOUT_MS", "1000"))
MAX_POLL_RECORDS = int(os.getenv("KAFKA_MAX_POLL_RECORDS", "500"))
SESSION_TIMEOUT_MS = int(os.getenv("KAFKA_SESSION_TIMEOUT_MS", "30000"))

# --------------- Logging ---------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  [%(levelname)s]  %(name)s  %(message)s",
)
logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ParsedMessage:
    """Lightweight wrapper around a consumed Kafka record."""

    topic: str
    partition: int
    offset: int
    key: Optional[str]
    value: Dict[str, Any]
    timestamp: int  # epoch ms
    headers: list


class GodViewConsumer:
    """Kafka consumer with JSON deserialization and a streaming generator."""

    def __init__(
        self,
        topic: str,
        group_id: str = DEFAULT_GROUP_ID,
        bootstrap_servers: str = BOOTSTRAP_SERVERS,
        auto_offset_reset: str = AUTO_OFFSET_RESET,
        enable_auto_commit: bool = ENABLE_AUTO_COMMIT,
        auto_commit_interval_ms: int = AUTO_COMMIT_INTERVAL_MS,
        poll_timeout_ms: int = POLL_TIMEOUT_MS,
        max_poll_records: int = MAX_POLL_RECORDS,
        session_timeout_ms: int = SESSION_TIMEOUT_MS,
    ) -> None:
        self._topic = topic
        self._running = True
        self._consumer: Optional[KafkaConsumer] = None

        # Register signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

        try:
            self._consumer = KafkaConsumer(
                topic,
                bootstrap_servers=bootstrap_servers,
                group_id=group_id,
                auto_offset_reset=auto_offset_reset,
                enable_auto_commit=enable_auto_commit,
                auto_commit_interval_ms=auto_commit_interval_ms,
                max_poll_records=max_poll_records,
                session_timeout_ms=session_timeout_ms,
                key_deserializer=self._decode_key,
                value_deserializer=self._decode_value,
                consumer_timeout_ms=poll_timeout_ms,
            )
            logger.info(
                "KafkaConsumer initialized  topic=%s  group=%s  bootstrap=%s",
                topic,
                group_id,
                bootstrap_servers,
            )
        except NoBrokersAvailable:
            logger.error(
                "No Kafka brokers available at %s — consumer NOT started.",
                bootstrap_servers,
            )
            raise
        except KafkaError as exc:
            logger.error("Failed to create KafkaConsumer: %s", exc)
            raise

    # ---- Context-manager protocol ----
    def __enter__(self) -> "GodViewConsumer":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:  # noqa: ANN001
        self.close()

    # ---- Deserializers ----
    @staticmethod
    def _decode_key(raw: Optional[bytes]) -> Optional[str]:
        if raw is None:
            return None
        try:
            return raw.decode("utf-8")
        except (UnicodeDecodeError, AttributeError):
            logger.warning("Could not decode message key — returning raw bytes.")
            return raw  # type: ignore[return-value]

    @staticmethod
    def _decode_value(raw: bytes) -> Dict[str, Any]:
        try:
            return json.loads(raw.decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            logger.error("Failed to deserialize message value: %s", exc)
            # Return a wrapper so downstream can still inspect the raw bytes
            return {"_raw": raw.decode("utf-8", errors="replace"), "_error": str(exc)}

    # ---- Streaming generator ----
    def stream(self) -> Generator[ParsedMessage, None, None]:
        """Continuously yield parsed Kafka messages until shutdown.

        Yields
        ------
        ParsedMessage
            A dataclass containing the deserialized message payload
            along with metadata (topic, partition, offset, key, timestamp).

        Example
        -------
        >>> with GodViewConsumer("raw-events") as c:
        ...     for msg in c.stream():
        ...         process(msg.value)
        """
        if self._consumer is None:
            raise RuntimeError("Consumer is not initialised or has been closed.")

        logger.info("Streaming from topic '%s' — press Ctrl+C to stop.", self._topic)
        messages_consumed = 0

        while self._running:
            try:
                # poll() returns within consumer_timeout_ms if no records
                for record in self._consumer:
                    if not self._running:
                        break

                    messages_consumed += 1
                    parsed = ParsedMessage(
                        topic=record.topic,
                        partition=record.partition,
                        offset=record.offset,
                        key=record.key,
                        value=record.value,
                        timestamp=record.timestamp,
                        headers=record.headers or [],
                    )

                    if messages_consumed % 1000 == 0:
                        logger.info(
                            "Consumed %d messages so far  (latest offset=%d, partition=%d)",
                            messages_consumed,
                            record.offset,
                            record.partition,
                        )

                    yield parsed

            except StopIteration:
                # consumer_timeout_ms elapsed with no records — loop again
                continue
            except KafkaError as exc:
                logger.error("Kafka error during consumption: %s", exc)
                if not self._running:
                    break
                continue

        logger.info(
            "Stream ended — consumed %d total messages from '%s'.",
            messages_consumed,
            self._topic,
        )

    # ---- Shutdown helpers ----
    def _signal_handler(self, signum: int, frame: Any) -> None:  # noqa: ANN401
        sig_name = signal.Signals(signum).name
        logger.info("Received %s — initiating graceful shutdown …", sig_name)
        self._running = False

    def close(self) -> None:
        """Commit offsets and close the underlying consumer."""
        self._running = False
        if self._consumer is not None:
            logger.info("Closing KafkaConsumer …")
            try:
                self._consumer.close(autocommit=True)
            except KafkaError as exc:
                logger.error("Error closing consumer: %s", exc)
            finally:
                self._consumer = None
                logger.info("KafkaConsumer closed.")
