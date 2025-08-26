"""
Lock-free "already processed ID" helper backed by Redis SET operations.
Optimized for Django 5.2.3 with connection pooling and retry logic.
"""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Final

import structlog
from django.conf import settings
from redis.asyncio import ConnectionPool, Redis
from redis.exceptions import RedisError

from common.cache_utils import get_redis_client

if TYPE_CHECKING:
    from collections.abc import Iterable

log = structlog.get_logger(__name__)

# Constants
DEFAULT_KEY_TTL_SEC: Final[int] = 60 * 60 * 24 * 3  # 3 days
BATCH_SIZE: Final[int] = 1000  # Max IDs per operation
MAX_RETRIES: Final[int] = 3
RETRY_DELAY: Final[float] = 0.1

# Connection pool for better performance
_connection_pool: ConnectionPool | None = None


def get_connection_pool() -> ConnectionPool:
    """Get or create Redis connection pool."""
    global _connection_pool
    if _connection_pool is None:
        redis_url = getattr(settings, "MESSAGING_REDIS_URL", "redis://localhost:6379/1")
        _connection_pool = ConnectionPool.from_url(
            redis_url,
            decode_responses=True,
            max_connections=50,
            socket_keepalive=True,
            socket_keepalive_options={
                1: 1,  # TCP_KEEPIDLE
                2: 1,  # TCP_KEEPINTVL
                3: 3,  # TCP_KEEPCNT
            },
        )
    return _connection_pool



@asynccontextmanager
async def lifespan_redis():
    """ASGI lifespan context manager for Redis."""
    client = await get_redis_client()
    try:
        yield client
    finally:
        await client.aclose(close_connection_pool=True)
        global _connection_pool
        _connection_pool = None


class RedisProcessedIDChecker:
    """
    Optimized ID checker with batching, retry logic, and better error handling.
    """

    def __init__(
        self,
        client: Redis | None = None,
        key: str = "processed_ids",
        *,
        ttl: int = DEFAULT_KEY_TTL_SEC,
        batch_size: int = BATCH_SIZE,
    ) -> None:
        self._client = client
        self._key = key
        self._ttl = ttl
        self._batch_size = batch_size
        self._owns_client = client is None

    async def __aenter__(self):
        """Async context manager entry."""
        if self._owns_client:
            self._client = await get_redis_client()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        if self._owns_client and self._client:
            await self._client.aclose()

    async def filter_processed(self, ids: set[int]) -> set[int]:
        """
        Filter out already processed IDs with optimized batching.

        Returns set of new (unprocessed) IDs.
        """
        if not ids:
            return set()

        if not self._client:
            msg = "Redis client not initialized"
            raise RuntimeError(msg)

        # Process in batches for large sets
        new_ids = set()
        id_list = list(ids)

        for i in range(0, len(id_list), self._batch_size):
            batch = id_list[i:i + self._batch_size]
            batch_new = await self._process_batch(batch)
            new_ids.update(batch_new)

        return new_ids

    async def mark_processed(self, ids: Iterable[int]) -> None:
        """Mark IDs as processed with batching."""
        if not ids:
            return

        if not self._client:
            msg = "Redis client not initialized"
            raise RuntimeError(msg)

        id_list = list(ids)

        # Process in batches
        for i in range(0, len(id_list), self._batch_size):
            batch = id_list[i:i + self._batch_size]
            await self._mark_batch_processed(batch)

    async def get_processed_count(self) -> int:
        """Get total number of processed IDs."""
        if not self._client:
            msg = "Redis client not initialized"
            raise RuntimeError(msg)

        return await self._retry_operation(
            self._client.scard, self._key,
        )

    async def clear_processed(self) -> bool:
        """Clear all processed IDs."""
        if not self._client:
            msg = "Redis client not initialized"
            raise RuntimeError(msg)

        result = await self._retry_operation(
            self._client.delete, self._key,
        )
        return bool(result)

    # Private methods

    async def _process_batch(self, batch: list[int]) -> set[int]:
        """Process a single batch of IDs."""
        str_ids = [str(i) for i in batch]

        # Check which IDs are already processed
        flags = await self._retry_operation(
            self._client.smismember, self._key, str_ids,
        )

        # Find new IDs
        new_ids = {
            id_val for id_val, is_processed
            in zip(batch, flags, strict=True)
            if not is_processed
        }

        # Mark new IDs as processed
        if new_ids:
            await self._mark_batch_processed(list(new_ids))

        return new_ids

    async def _mark_batch_processed(self, batch: list[int]) -> None:
        """Mark a batch of IDs as processed."""
        if not batch:
            return

        str_ids = [str(i) for i in batch]

        async def _pipeline_ops():
            async with self._client.pipeline(transaction=False) as pipe:
                await pipe.sadd(self._key, *str_ids)
                await pipe.expire(self._key, self._ttl)
                await pipe.execute()

        await self._retry_operation(_pipeline_ops)

    async def _retry_operation(self, operation, *args, **kwargs):
        """Execute operation with retry logic."""
        last_error = None

        for attempt in range(MAX_RETRIES):
            try:
                return await operation(*args, **kwargs)
            except RedisError as e:
                last_error = e
                if attempt < MAX_RETRIES - 1:
                    await asyncio.sleep(RETRY_DELAY * (attempt + 1))
                    log.warning(
                        "Redis operation failed, retrying",
                        attempt=attempt + 1,
                        error=str(e),
                    )
                else:
                    log.exception(
                        "Redis operation failed after retries",
                        attempts=MAX_RETRIES,
                        error=str(e),
                    )

        raise last_error


# Convenience functions

async def filter_new_ids(ids: set[int], key: str = "processed_ids") -> set[int]:
    """Convenience function to filter new IDs."""
    async with RedisProcessedIDChecker(key=key) as checker:
        return await checker.filter_processed(ids)


async def mark_ids_processed(ids: Iterable[int], key: str = "processed_ids") -> None:
    """Convenience function to mark IDs as processed."""
    async with RedisProcessedIDChecker(key=key) as checker:
        await checker.mark_processed(ids)
