# utils/cache_ext.py  (or wherever this lives)
from __future__ import annotations

import asyncio
import hashlib
import os
import re
import urllib.parse
import uuid
from contextlib import asynccontextmanager
from functools import cache as memoize_cache
from typing import (
    TYPE_CHECKING,
    Any,
    TypeVar,
    cast,
)

import orjson
import redis.asyncio as aioredis
import structlog
from django.conf import settings
from django.core.cache import cache

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Awaitable, Callable

T = TypeVar("T")

log = structlog.get_logger(__name__).bind(component="CacheUtils")


# ===================================================================
# 0.  Core helpers
# ===================================================================
def _dumps(obj: Any) -> bytes:
    return orjson.dumps(obj)


def _loads(raw: bytes) -> Any:
    return orjson.loads(raw)


# ===================================================================
# 1.  Cache-key builder
# ===================================================================
def build_cache_key(prefix: str, **params: Any) -> str:
    """
    Builds a stable, URL-safe cache key from a prefix and query parameters.
    Falls back to an MD5 hash when the resulting key would exceed 250 bytes.
    """
    HASH_SALT = "dj-cache-salt"
    MEMCACHED_MAX = 250

    if not params:
        return prefix

    query = urllib.parse.urlencode(sorted(params.items()), doseq=True)
    key = f"{prefix}:{query}"

    if len(key) <= MEMCACHED_MAX:
        return key

    digest = hashlib.md5(f"{query}:{HASH_SALT}".encode(), usedforsecurity=False).hexdigest()
    cut = MEMCACHED_MAX - len(digest) - 1
    return f"{prefix[:cut]}:{digest}"


# ===================================================================
# 2.  Shared Redis client
# ===================================================================
def _redis_location() -> str:
    try:
        location = settings.CACHES["default"]["LOCATION"]
        if isinstance(location, list | tuple):
            return location[0] if location else "redis://127.0.0.1:6379/1"
        if isinstance(location, str):
            return re.split(r"[,;]", location)[0].strip()
        msg = f"Unsupported CACHES LOCATION type: {type(location)}"
        raise TypeError(msg)
    except (KeyError, AttributeError):
        return os.getenv("REDIS_URL", "redis://127.0.0.1:6379/1")


@memoize_cache
def get_redis_client() -> aioredis.Redis:
    return aioredis.from_url(_redis_location(), decode_responses=True)


# ===================================================================
# 3.  Distributed lock
# ===================================================================
@asynccontextmanager
async def redis_lock(
    key: str,
    *,
    timeout: int = 10,
    retry_delay: float = 0.05,
) -> AsyncIterator[None]:
    token = str(uuid.uuid4())
    redis = get_redis_client()
    lock_key = f"lock:{key}"

    try:
        while not await redis.set(lock_key, token, nx=True, ex=timeout):
            await asyncio.sleep(retry_delay)
        yield
    finally:
        script = """
        if redis.call("GET", KEYS[1]) == ARGV[1] then
            return redis.call("DEL", KEYS[1])
        else
            return 0
        end
        """
        await redis.eval(script, 1, lock_key, token)


# ===================================================================
# 4.  get-or-set with double-checked locking
# ===================================================================
async def aget_or_set(
    key: str,
    producer: Callable[[], T | Awaitable[T]],
    *,
    ttl: int = 300,
    lock_timeout: int = 30,
) -> T:
    # 1ᵗʰ check
    raw = await cache.aget(key)
    if raw is not None:
        return cast("T", _loads(raw))

    # lock
    async with redis_lock(key, timeout=lock_timeout):
        # 2ⁿᵈ check
        raw = await cache.aget(key)
        if raw is not None:
            return cast("T", _loads(raw))

        # produce
        result: T | Awaitable[T] = producer()
        if asyncio.iscoroutine(result):  # robust coroutine check
            result = await cast("Awaitable[T]", result)
        value = cast("T", result)

        try:
            data = _dumps(value)
            await cache.aset(key, data, timeout=ttl)
            log.debug("Cache set", key=key, size_kb=f"{len(data) / 1024:.1f}")
        except Exception:
            log.exception("Failed to cache key=%s", key)

        return value


# ===================================================================
# 5.  Misc async helpers
# ===================================================================
async def adelete(key: str) -> int:
    return await cache.adelete(key)


async def adelete_pattern(pattern: str, *, chunk_size: int = 1_000) -> int:
    redis = get_redis_client()
    deleted = 0
    async for key in redis.scan_iter(match=pattern, count=chunk_size):
        deleted += await redis.unlink(key)
    return deleted


# ===================================================================
# 6.  JSON convenience wrappers
# ===================================================================
async def aget_json[T](key: str, default: T | None = None) -> T | None:
    raw = await cache.aget(key)
    if raw is None:
        return default
    try:
        return cast("T", _loads(raw))
    except (orjson.JSONDecodeError, ValueError, TypeError):
        log.warning("Corrupt JSON in cache – deleting key=%s", key)
        await cache.adelete(key)
        return default


async def aset_json(key: str, value: Any, ttl: int | None = None) -> None:
    await cache.aset(key, _dumps(value), timeout=ttl)


def get_json[T](key: str, default: T | None = None) -> T | None:
    raw = cache.get(key)
    if raw is None:
        return default
    try:
        return cast("T", _loads(raw))
    except (orjson.JSONDecodeError, ValueError, TypeError):
        log.warning("Corrupt JSON in cache – deleting key=%s", key)
        cache.delete(key)
        return default


def set_json(key: str, value: Any, ttl: int | None = None) -> None:
    cache.set(key, _dumps(value), timeout=ttl)
