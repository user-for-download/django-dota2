# infrastructure/broker.py
# ============================================================================
"""
Initialises the global FastStream Redis broker used for pub/sub in the Dota project.

This module provides reusable components for connecting to and interacting with
the message broker. It is designed to be safely imported by both the ASGI web
application and standalone worker processes.
"""

from __future__ import annotations

import asyncio
import os
from contextlib import asynccontextmanager
from functools import cache
from typing import Final
from urllib.parse import urlparse

import structlog
from faststream import FastStream
from faststream.redis import RedisBroker
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import TimeoutError as RedisTimeoutError

from common.messaging.reliable import ReliableBrokerPublisher

# ─────────────────────────────────────────── Logging
log = structlog.get_logger(__name__).bind(comp="RedisBroker")

# ─────────────────────────────────────────── Django Integration
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings.local")

try:
    import django

    django.setup()
    from django.conf import settings
except Exception as e:
    log.exception("Failed to setup Django", error=str(e))
    raise


# ─────────────────────────────────────────── Configuration
def _get_redis_url() -> str:
    """Get Redis URL with proper validation and fallback logic."""
    url = os.getenv(
        "FASTSTREAM_REDIS_URL",
        getattr(settings, "FASTSTREAM_REDIS_URL", "redis://127.0.0.1:6379/2"),
    )
    parsed = urlparse(url)
    if not parsed.hostname:
        msg = f"Invalid FASTSTREAM_REDIS_URL: {url!r}"
        raise ValueError(msg)
    return url


def _get_batch_size() -> int:
    """Get batch size with validation."""
    batch_size = int(
        os.getenv("BATCH_SIZE", getattr(settings, "BATCH_SIZE", 100)),
    )
    if batch_size <= 0:
        msg = f"BATCH_SIZE must be positive, got: {batch_size}"
        raise ValueError(msg)
    return batch_size


# Configuration constants
FASTSTREAM_REDIS_URL: Final[str] = _get_redis_url().rstrip("/")
BATCH_SIZE: Final[int] = _get_batch_size()
CONNECTION_TIMEOUT: Final[int] = int(os.getenv("REDIS_CONNECTION_TIMEOUT", "10"))
MAX_RETRIES: Final[int] = int(os.getenv("REDIS_MAX_RETRIES", "3"))
RETRY_DELAY_S: Final[float] = float(os.getenv("REDIS_RETRY_DELAY", "1.0"))

log.info(
    "Initialising FastStream broker configuration",
    redis_url=FASTSTREAM_REDIS_URL.replace(urlparse(FASTSTREAM_REDIS_URL).password or "", "***"),
    batch_size=BATCH_SIZE,
)

# ─────────────────────────────────────────── FastStream Objects
broker: Final[RedisBroker] = RedisBroker(
    FASTSTREAM_REDIS_URL,
    logger=log,
    socket_connect_timeout=CONNECTION_TIMEOUT,
    retry_on_timeout=True,
    health_check_interval=30.0,
)

app: Final[FastStream] = FastStream(broker, logger=log)
reliable_publisher: Final[ReliableBrokerPublisher] = ReliableBrokerPublisher(broker)

# ─────────────────────────────────────────── Connection Management
_broker_lock: asyncio.Lock = asyncio.Lock()
_connection_status: dict[str, bool] = {"connected": False}


class BrokerConnectionError(Exception):
    """Raised when broker connection fails after retries."""


async def _connect_with_retry() -> None:
    """Connect to broker with retry logic."""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            # The broker's connect method handles timeout and retries internally
            await broker.connect()
            _connection_status["connected"] = True
            log.info("Broker connected successfully", attempt=attempt)
            return
        except (RedisConnectionError, RedisTimeoutError) as e:
            log.warning(
                "Broker connection failed, retrying...",
                attempt=attempt,
                max_retries=MAX_RETRIES,
                error=str(e),
            )
            if attempt == MAX_RETRIES:
                msg = f"Failed to connect after {MAX_RETRIES} attempts: {e}"
                raise BrokerConnectionError(msg) from e
            await asyncio.sleep(RETRY_DELAY_S * attempt)


async def ensure_broker_connected() -> None:
    """Ensure broker connection is established idempotently."""
    async with _broker_lock:
        if not _connection_status["connected"]:
            await _connect_with_retry()


async def shutdown_broker() -> None:
    """Gracefully shutdown the broker and clean up resources."""
    log.info("Shutting down broker")
    async with _broker_lock:
        if _connection_status["connected"]:
            try:
                if hasattr(reliable_publisher, "stop"):
                    await reliable_publisher.stop()
                await broker.close()
                _connection_status["connected"] = False
                log.info("Broker shutdown complete")
            except Exception as e:
                log.exception("Error during broker shutdown", error=str(e))


@asynccontextmanager
async def broker_context():
    """Async context manager for broker lifecycle management."""
    try:
        await ensure_broker_connected()
        yield
    finally:
        await shutdown_broker()


# ─────────────────────────────────────────── Convenience Utilities
@cache
def get_broker() -> RedisBroker:
    return broker


def get_publisher() -> ReliableBrokerPublisher:
    return reliable_publisher


def is_connected() -> bool:
    return _connection_status["connected"]


# ─────────────────────────────────────────── Export Public API
__all__ = [
    "BATCH_SIZE",
    "BrokerConnectionError",
    "app",
    "broker",
    "broker_context",
    "ensure_broker_connected",
    "get_broker",
    "get_publisher",
    "is_connected",
    "reliable_publisher",
    "shutdown_broker",
]
