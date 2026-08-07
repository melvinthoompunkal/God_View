"""
init_topics.py — Bootstrap Kafka topics for God_View.

Creates the following topics (idempotently) on first run:
  • raw-events   — raw inbound event stream
  • market-data  — market / pricing feeds
  • ml-alerts    — alerts emitted by ML workers

Each topic is created with 3 partitions and a replication factor of 1
(suitable for a single-broker dev environment).

Usage:
    python init_topics.py                           # default: localhost:9092
    KAFKA_BOOTSTRAP=kafka:29092 python init_topics.py  # inside Docker network
"""

import os
import sys
import time
import logging

from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError, NoBrokersAvailable

# --------------- Configuration ---------------
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
NUM_PARTITIONS = 3
REPLICATION_FACTOR = 1
MAX_RETRIES = 10
RETRY_INTERVAL_S = 5

TOPICS = [
    "raw-events",
    "market-data",
    "ml-alerts",
]

# --------------- Logging ---------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  [%(levelname)s]  %(message)s",
)
logger = logging.getLogger(__name__)


def _connect(retries: int = MAX_RETRIES) -> KafkaAdminClient:
    """Attempt to connect to Kafka, retrying on NoBrokersAvailable."""
    for attempt in range(1, retries + 1):
        try:
            client = KafkaAdminClient(
                bootstrap_servers=BOOTSTRAP_SERVERS,
                client_id="godview-topic-init",
            )
            logger.info("Connected to Kafka at %s", BOOTSTRAP_SERVERS)
            return client
        except NoBrokersAvailable:
            logger.warning(
                "Kafka not ready (attempt %d/%d) — retrying in %ds …",
                attempt,
                retries,
                RETRY_INTERVAL_S,
            )
            time.sleep(RETRY_INTERVAL_S)

    logger.error("Could not connect to Kafka after %d attempts. Exiting.", retries)
    sys.exit(1)


def create_topics(admin: KafkaAdminClient) -> None:
    """Create topics that do not already exist."""
    existing = set(admin.list_topics())
    topics_to_create = [t for t in TOPICS if t not in existing]

    if not topics_to_create:
        logger.info("All topics already exist — nothing to do.")
        return

    new_topics = [
        NewTopic(
            name=name,
            num_partitions=NUM_PARTITIONS,
            replication_factor=REPLICATION_FACTOR,
        )
        for name in topics_to_create
    ]

    try:
        admin.create_topics(new_topics=new_topics, validate_only=False)
        for t in topics_to_create:
            logger.info("✓ Created topic: %s  (partitions=%d)", t, NUM_PARTITIONS)
    except TopicAlreadyExistsError:
        logger.info("Topics already exist (race condition) — safe to continue.")


def main() -> None:
    logger.info("Bootstrapping Kafka topics …")
    admin = _connect()
    create_topics(admin)
    admin.close()
    logger.info("Done.")


if __name__ == "__main__":
    main()
