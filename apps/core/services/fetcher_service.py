"""
Generic service layer for orchestrating data fetching, caching and status monitoring.
Optimized for Django 5.2.3 with improved async patterns.
"""

from __future__ import annotations

import asyncio
import time
from datetime import UTC, datetime
from enum import Enum, auto
from typing import TYPE_CHECKING, Any, Final, TypeVar, cast
import redis.asyncio as aioredis
import structlog
from django.conf import settings
from django.core.cache import caches
from pydantic import ValidationError

from apps.core.conf import CACHE_PREFIXES, BaseFetcherConfig
from apps.core.datatype import CachePayload, FetcherResult, StatsPayload
from apps.core.services.protocols import AsyncFetcherProtocol
from apps.leagues.conf import LeagueFetcherConfig
from apps.leagues.services.league_fetcher import LeagueFetcher
from apps.matches.conf import MatchFetcherConfig
from apps.matches.services.match_fetcher import MatchFetcher
from apps.players.conf import PlayerFetcherConfig
from apps.players.services.player_fetcher import PlayerFetcher
from apps.teams.conf import TeamFetcherConfig
from apps.teams.services.team_fetcher import TeamFetcher
from common.cache_utils import adelete_pattern, aget_json, aset_json, get_redis_client

if TYPE_CHECKING:
    from apps.core.datatype import UpsertResult

log = structlog.get_logger(__name__).bind(comp="FetcherService")

ConfigT = TypeVar("ConfigT", bound=BaseFetcherConfig)
FetcherT = TypeVar("FetcherT", bound=AsyncFetcherProtocol)


class FetcherType(Enum):
    """Enumeration of available fetcher types."""

    TEAM = auto()
    LEAGUE = auto()
    PLAYER = auto()
    MATCH = auto()

    @property
    def cache_prefix(self) -> str:
        """Get cache prefix for this fetcher type."""
        return self.name.lower()

    @property
    def api_path_prefix(self) -> str:
        """Get API path prefix for this fetcher type."""
        return f"/api/v1/{self.cache_prefix}s"

    @property
    def status_cache_key(self) -> str:
        """Get status cache key for this fetcher type."""
        return f"{CACHE_PREFIXES['fetcher_status']}{self.cache_prefix}"

    @property
    def stats_cache_key(self) -> str:
        """Get stats cache key for this fetcher type."""
        return f"{CACHE_PREFIXES['fetcher_stats']}{self.cache_prefix}"

    def __str__(self) -> str:
        return self.cache_prefix


# Configuration and fetcher mappings
CFG_MAP: Final[dict[FetcherType, type[BaseFetcherConfig]]] = {
    FetcherType.TEAM: TeamFetcherConfig,
    FetcherType.PLAYER: PlayerFetcherConfig,
    FetcherType.LEAGUE: LeagueFetcherConfig,
    FetcherType.MATCH: MatchFetcherConfig,
}

FETCHER_MAP: Final[dict[FetcherType, type[AsyncFetcherProtocol]]] = {
    FetcherType.TEAM: cast("type[AsyncFetcherProtocol]", TeamFetcher),
    FetcherType.PLAYER: cast("type[AsyncFetcherProtocol]", PlayerFetcher),
    FetcherType.LEAGUE: cast("type[AsyncFetcherProtocol]", LeagueFetcher),
    FetcherType.MATCH: cast("type[AsyncFetcherProtocol]", MatchFetcher),
}


class FetcherService[FetcherT: AsyncFetcherProtocol, ConfigT: BaseFetcherConfig]:
    """
    Generic service for orchestrating data fetching and caching.

    Improvements:
    - Better error handling with context managers
    - Optimized cache operations
    - Cleaner async patterns
    """

    def __init__(
        self,
        *,
        fetcher_type: FetcherType,
        fetcher_cls: type[FetcherT] | None = None,
        cache_timeout_seconds: int | None = None,
    ) -> None:
        self.fetcher_type: Final = fetcher_type
        self.fetcher_cls: type[FetcherT] = fetcher_cls or FETCHER_MAP[fetcher_type]

        self.cache_timeout: int = cache_timeout_seconds or getattr(
            settings,
            f"{fetcher_type.name}_CACHE_TIMEOUT",
            600,
        )

        # Use default cache
        self._cache = caches["default"]

        # Pre-compute cache keys
        self._status_key = self.fetcher_type.status_cache_key
        self._stats_key = self.fetcher_type.stats_cache_key

    async def fetch_and_cache(
        self,
        cfg: ConfigT | None = None,
        *,
        force_refresh: bool = False,
    ) -> FetcherResult:
        """
        Main orchestration entry point with improved error handling.
        """
        # Check cache first
        if not force_refresh:
            if cached := await self._get_cached_status():
                return FetcherResult(source="cache", status="ok", **cached)

        cfg = cfg or self._get_default_config()

        start_time = time.perf_counter()

        try:
            upsert_result = await self._execute_fetch(cfg)
        except ValidationError as e:
            log.warning("Configuration validation failed",
                        prefix=self.fetcher_type,
                        errors=e.errors())
            return self._error_result("validation_error", str(e))
        except TimeoutError:
            log.exception("Fetcher timeout", prefix=self.fetcher_type)
            return self._error_result("timeout", "Operation timed out")
        except Exception as e:
            log.exception("Fetcher failed", prefix=self.fetcher_type)
            return self._error_result("error", str(e))

        # Process results
        duration_ms = round((time.perf_counter() - start_time) * 1000, 1)

        cache_payload = CachePayload(
            created=upsert_result["created"],
            updated=upsert_result["updated"],
            skipped=upsert_result["skipped"],
            timestamp=datetime.now(UTC).isoformat(),
            processing_ms=duration_ms,
            cfg=cfg.model_dump(),
        )

        # Post-fetch operations
        await self._post_fetch_operations(upsert_result, cache_payload, duration_ms)

        log.info("Fetch completed",
                 prefix=self.fetcher_type,
                 duration_ms=duration_ms,
                 **upsert_result)

        return FetcherResult(source="fresh", status="ok", **cache_payload)

    async def get_stats(self) -> StatsPayload | None:
        """Get aggregated statistics for this fetcher."""
        return await aget_json(self._stats_key)

    async def clear_cache(self) -> int:
        """Clear all cache entries for this fetcher type."""
        pattern = f"{self.fetcher_type.api_path_prefix}*"
        deleted = await adelete_pattern(pattern)

        # Also clear status and stats
        await self._cache.adelete(self._status_key)
        await self._cache.adelete(self._stats_key)

        log.info("Cache cleared",
                 prefix=self.fetcher_type,
                 keys_deleted=deleted + 2)

        return deleted + 2

    # Private methods

    async def _execute_fetch(self, cfg: ConfigT) -> UpsertResult:
        """Execute the fetch operation with proper timeout."""
        timeout = self.cache_timeout + 5  # Grace period

        async with asyncio.timeout(timeout):
            async with self.fetcher_cls(cfg) as fetcher:
                return await fetcher.run()

    async def _get_cached_status(self) -> CachePayload | None:
        """Get cached status if available."""
        cached: CachePayload | None = await aget_json(self._status_key)
        if cached:
            log.debug("Cache hit", prefix=self.fetcher_type)
        return cached

    def _get_default_config(self) -> ConfigT:
        """Get default configuration for this fetcher type."""
        cfg_cls = CFG_MAP[self.fetcher_type]
        cfg = cast("ConfigT", cfg_cls())
        cfg.check()
        return cfg

    def _error_result(self, status: str, message: str) -> FetcherResult:
        """Create an error result."""
        return FetcherResult(
            source="error",
            status=status,
            message=message,
            created=0,
            updated=0,
            skipped=0,
        )

    async def _post_fetch_operations(
        self,
        upsert_result: UpsertResult,
        cache_payload: CachePayload,
        duration_ms: float,
    ) -> None:
        """Handle post-fetch operations like caching and stats update."""
        try:
            # Cache the status
            await aset_json(self._status_key, cache_payload, ttl=self.cache_timeout)

            # Invalidate API cache if data changed
            if upsert_result["created"] or upsert_result["updated"]:
                await self._invalidate_api_cache()

            # Update statistics
            await self._update_stats(upsert_result, duration_ms)

        except Exception:
            log.exception("Post-fetch operations failed", prefix=self.fetcher_type)

    async def _invalidate_api_cache(self) -> None:
        """Invalidate API cache entries for this fetcher type."""
        pattern = f"{self.fetcher_type.api_path_prefix}*"
        deleted = await adelete_pattern(pattern)
        log.info("API cache invalidated",
                 prefix=self.fetcher_type,
                 pattern=pattern,
                 keys_deleted=deleted)

    async def _update_stats(self, upsert: UpsertResult, duration_ms: float) -> None:
        """Update aggregated statistics atomically."""
        is_redis_cache = "redis" in self._cache.__class__.__module__

        if is_redis_cache:
            try:
                redis_client = get_redis_client()
                await self._update_stats_atomic(redis_client, upsert, duration_ms)
            except Exception:
                log.warning(
                    "Atomic Redis stats update failed, using fallback.",
                    prefix=self.fetcher_type,
                    exc_info=True,
                )
                await self._update_stats_fallback(upsert, duration_ms)
        else:
            # If the cache is not Redis (e.g., in-memory), use the fallback
            await self._update_stats_fallback(upsert, duration_ms)

    # --- THIS METHOD IS NOW CORRECT because it receives an aioredis client ---
    async def _update_stats_atomic(
        self, redis_client: aioredis.Redis, upsert: UpsertResult, duration_ms: float
    ) -> None:
        """Update stats using Redis atomic operations."""
        # This code is correct for an aioredis client
        async with redis_client.pipeline() as pipe:
            await pipe.hincrby(self._stats_key, "total_fetches", 1)
            await pipe.hincrbyfloat(self._stats_key, "proc_sum_ms", duration_ms)
            await pipe.hincrby(self._stats_key, "created_sum", upsert["created"])
            await pipe.hincrby(self._stats_key, "updated_sum", upsert["updated"])
            await pipe.expire(self._stats_key, 86_400)
            results = await pipe.execute() # This will now work correctly

        # Calculate and store average
        total_fetches = int(results[0])
        proc_sum_ms = float(results[1])
        avg_ms = round(proc_sum_ms / total_fetches, 2) if total_fetches else 0

        # It's better to use the async client directly here too
        await redis_client.set(f"{self._stats_key}:avg_ms", avg_ms, ex=86_400)


    async def _update_stats_fallback(self, upsert: UpsertResult, duration_ms: float) -> None:
        """Fallback stats update for non-Redis backends."""
        stats: StatsPayload | None = await aget_json(self._stats_key)

        if stats is None:
            stats = StatsPayload(
                total_fetches=0,
                proc_sum_ms=0.0,
                created_sum=0,
                updated_sum=0,
                avg_ms=0.0,
            )

        stats["total_fetches"] += 1
        stats["proc_sum_ms"] += duration_ms
        stats["created_sum"] += upsert["created"]
        stats["updated_sum"] += upsert["updated"]
        stats["avg_ms"] = round(stats["proc_sum_ms"] / stats["total_fetches"], 2)

        await aset_json(self._stats_key, stats, ttl=86_400)


# Concrete service implementations
class TeamFetcherService(FetcherService[TeamFetcher, TeamFetcherConfig]):
    """Service for fetching team data."""

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(fetcher_type=FetcherType.TEAM, **kwargs)


class PlayerFetcherService(FetcherService[PlayerFetcher, PlayerFetcherConfig]):
    """Service for fetching player data."""

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(fetcher_type=FetcherType.PLAYER, **kwargs)


class LeagueFetcherService(FetcherService[LeagueFetcher, LeagueFetcherConfig]):
    """Service for fetching league data."""

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(fetcher_type=FetcherType.LEAGUE, **kwargs)


class MatchFetcherService(FetcherService[MatchFetcher, MatchFetcherConfig]):
    """Service for fetching match data."""

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(fetcher_type=FetcherType.MATCH, **kwargs)
