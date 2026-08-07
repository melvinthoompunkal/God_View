"""
anomaly_worker.py — Real-time market anomaly detector for God_View.

Consumes normalised trade ticks from the ``market-data`` Kafka topic,
maintains a rolling 1-hour Pandas DataFrame of price changes per symbol,
and fits a Scikit-Learn IsolationForest on the fly. When an anomaly is
detected the worker publishes a structured JSON alert to ``ml-alerts``.

Usage:
    python anomaly_worker.py
    ANOMALY_CONTAMINATION=0.02 python anomaly_worker.py
"""

from __future__ import annotations

import json
import logging
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd
from sklearn.ensemble import IsolationForest

# Project-local imports (same package)
from base_consumer import GodViewConsumer

# Producer lives in the sibling ingestion/ package — adjust sys.path
# so the import works both inside Docker and during local development.
_ingestion_dir = os.path.join(os.path.dirname(__file__), os.pardir, "ingestion")
if _ingestion_dir not in sys.path:
    sys.path.insert(0, os.path.abspath(_ingestion_dir))

from base_producer import GodViewProducer  # noqa: E402

# --------------- Configuration ---------------
CONSUME_TOPIC = os.getenv("ANOMALY_CONSUME_TOPIC", "market-data")
ALERT_TOPIC = os.getenv("ANOMALY_ALERT_TOPIC", "ml-alerts")
CONSUMER_GROUP = os.getenv("ANOMALY_GROUP_ID", "godview-anomaly-detector")

# Rolling window duration
WINDOW_HOURS = float(os.getenv("ANOMALY_WINDOW_HOURS", "1"))

# IsolationForest hyper-parameters
CONTAMINATION = float(os.getenv("ANOMALY_CONTAMINATION", "0.01"))
N_ESTIMATORS = int(os.getenv("ANOMALY_N_ESTIMATORS", "100"))
RANDOM_STATE = int(os.getenv("ANOMALY_RANDOM_STATE", "42"))

# Minimum observations required before fitting the model
MIN_SAMPLES = int(os.getenv("ANOMALY_MIN_SAMPLES", "30"))

# How often (in seconds) to re-fit the IsolationForest
REFIT_INTERVAL_S = int(os.getenv("ANOMALY_REFIT_INTERVAL_S", "60"))

# --------------- Logging ---------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  [%(levelname)s]  %(name)s  %(message)s",
)
logger = logging.getLogger(__name__)


# ------------------------------------------------------------------
# Feature Engineering Helpers
# ------------------------------------------------------------------

def _build_feature_row(tick: Dict[str, Any]) -> Dict[str, Any]:
    """Extract numeric features from a normalised market-data tick.

    Expected schema (produced by finance_worker.py):
        {
            "symbol":          "AAPL",
            "price":           187.50,
            "volume":          100,
            "exchange_ts_ms":  1618234567890,
            ...
        }

    Returns a dict suitable for appending to the rolling DataFrame.
    """
    return {
        "ts": pd.Timestamp(
            tick.get("exchange_ts_ms", int(time.time() * 1000)),
            unit="ms",
            tz="UTC",
        ),
        "symbol": tick.get("symbol", "UNKNOWN"),
        "price": float(tick.get("price", 0.0)),
        "volume": float(tick.get("volume", 0.0)),
    }


def _compute_features(df: pd.DataFrame) -> pd.DataFrame:
    """Compute per-symbol derived features on the rolling window.

    Adds:
        pct_change     — tick-over-tick price change (%)
        log_volume     — log1p of volume
        z_price        — z-score of price within the symbol's window
        z_volume       — z-score of volume within the symbol's window
    """
    df = df.copy()

    # Percent change within each symbol group
    df["pct_change"] = df.groupby("symbol")["price"].pct_change().fillna(0.0)

    # Log volume (handles zero safely)
    df["log_volume"] = np.log1p(df["volume"])

    # Z-scores within each symbol group
    for col, z_col in [("price", "z_price"), ("volume", "z_volume")]:
        group_mean = df.groupby("symbol")[col].transform("mean")
        group_std = df.groupby("symbol")[col].transform("std").replace(0, 1)
        df[z_col] = (df[col] - group_mean) / group_std

    return df


# Feature columns passed to IsolationForest
FEATURE_COLS = ["pct_change", "log_volume", "z_price", "z_volume"]


# ------------------------------------------------------------------
# Alert Construction
# ------------------------------------------------------------------

def _build_alert(row: pd.Series, anomaly_score: float) -> Dict[str, Any]:
    """Build a JSON-serializable alert payload."""
    return {
        "alert_type": "market_anomaly",
        "severity": "high" if anomaly_score < -0.6 else "medium",
        "symbol": row.get("symbol", "UNKNOWN"),
        "price": float(row.get("price", 0.0)),
        "volume": float(row.get("volume", 0.0)),
        "pct_change": float(row.get("pct_change", 0.0)),
        "anomaly_score": float(anomaly_score),
        "detected_at": datetime.now(timezone.utc).isoformat(),
        "exchange_ts": row["ts"].isoformat() if pd.notna(row.get("ts")) else None,
        "_source": "anomaly_worker",
    }


# ------------------------------------------------------------------
# Core Worker Loop
# ------------------------------------------------------------------

class AnomalyWorker:
    """Consumes market data, detects anomalies, publishes alerts."""

    def __init__(self) -> None:
        self._window = pd.DataFrame(columns=["ts", "symbol", "price", "volume"])
        self._model: Optional[IsolationForest] = None
        self._last_fit_time: float = 0.0
        self._anomalies_detected: int = 0
        self._ticks_processed: int = 0

    # ---- Rolling window management ----
    def _append_tick(self, tick: Dict[str, Any]) -> None:
        """Add a tick and prune rows older than the rolling window."""
        row = _build_feature_row(tick)
        new_row = pd.DataFrame([row])
        self._window = pd.concat([self._window, new_row], ignore_index=True)
        self._ticks_processed += 1

        # Prune rows older than the window
        cutoff = pd.Timestamp.now(tz="UTC") - timedelta(hours=WINDOW_HOURS)
        self._window = self._window[self._window["ts"] >= cutoff].reset_index(drop=True)

    # ---- Model fitting ----
    def _maybe_refit(self) -> None:
        """Re-fit the IsolationForest if enough time has elapsed."""
        now = time.monotonic()
        if (now - self._last_fit_time) < REFIT_INTERVAL_S:
            return

        if len(self._window) < MIN_SAMPLES:
            logger.debug(
                "Skipping refit — only %d samples (need %d).",
                len(self._window),
                MIN_SAMPLES,
            )
            return

        features_df = _compute_features(self._window)
        X = features_df[FEATURE_COLS].replace([np.inf, -np.inf], np.nan).fillna(0.0)

        self._model = IsolationForest(
            n_estimators=N_ESTIMATORS,
            contamination=CONTAMINATION,
            random_state=RANDOM_STATE,
            n_jobs=-1,
        )
        self._model.fit(X)
        self._last_fit_time = now

        logger.info(
            "IsolationForest re-fitted on %d samples  (window=%.1fh, contamination=%.3f)",
            len(X),
            WINDOW_HOURS,
            CONTAMINATION,
        )

    # ---- Anomaly scoring ----
    def _score_latest(self) -> List[Dict[str, Any]]:
        """Score the most recently appended row and return alerts if anomalous."""
        if self._model is None or len(self._window) < MIN_SAMPLES:
            return []

        features_df = _compute_features(self._window)
        latest_row = features_df.iloc[-1:]
        X_latest = latest_row[FEATURE_COLS].replace([np.inf, -np.inf], np.nan).fillna(0.0)

        prediction = self._model.predict(X_latest)[0]         # 1 = normal, -1 = anomaly
        score = self._model.decision_function(X_latest)[0]    # lower = more anomalous

        if prediction == -1:
            self._anomalies_detected += 1
            alert = _build_alert(features_df.iloc[-1], score)
            logger.warning(
                "🚨 Anomaly detected  symbol=%s  price=%.4f  pct_change=%.4f%%  score=%.4f",
                alert["symbol"],
                alert["price"],
                alert["pct_change"] * 100,
                score,
            )
            return [alert]

        return []

    # ---- Main entry point ----
    def run(self) -> None:
        """Consume from market-data, detect anomalies, publish to ml-alerts."""
        logger.info(
            "Starting AnomalyWorker  consume=%s  alerts=%s  group=%s  "
            "window=%.1fh  contamination=%.3f  refit_every=%ds",
            CONSUME_TOPIC,
            ALERT_TOPIC,
            CONSUMER_GROUP,
            WINDOW_HOURS,
            CONTAMINATION,
            REFIT_INTERVAL_S,
        )

        with (
            GodViewConsumer(topic=CONSUME_TOPIC, group_id=CONSUMER_GROUP) as consumer,
            GodViewProducer() as producer,
        ):
            for message in consumer.stream():
                tick = message.value

                # Skip deserialization errors surfaced by base_consumer
                if "_error" in tick:
                    logger.debug("Skipping malformed message: %s", tick.get("_error"))
                    continue

                # 1. Append to rolling window
                self._append_tick(tick)

                # 2. Periodically re-fit the model
                self._maybe_refit()

                # 3. Score the latest tick
                alerts = self._score_latest()

                # 4. Publish any alerts
                for alert in alerts:
                    producer.publish(
                        ALERT_TOPIC,
                        alert,
                        key=alert.get("symbol", "UNKNOWN"),
                    )

                # Periodic stats
                if self._ticks_processed % 500 == 0:
                    logger.info(
                        "Stats  ticks=%d  window_size=%d  anomalies=%d",
                        self._ticks_processed,
                        len(self._window),
                        self._anomalies_detected,
                    )

            producer.flush()

        logger.info(
            "AnomalyWorker stopped — processed %d ticks, detected %d anomalies.",
            self._ticks_processed,
            self._anomalies_detected,
        )


# ------------------------------------------------------------------
# Entry Point
# ------------------------------------------------------------------

def main() -> None:
    worker = AnomalyWorker()
    worker.run()


if __name__ == "__main__":
    main()
