# apps/core/services/dota_data_handler.py
# ==============================================================================
"""
SQL ➜ HTTP fetcher that validates and chunks data returned by a Dota “Explorer”
endpoint.  Refactored for Python 3.12, httpx, and realistic User-Agent rotation.
"""

from __future__ import annotations

import asyncio
import random
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Self

import httpx
import structlog
from django.conf import settings
from pydantic import BaseModel, ValidationError

from apps.core.conf import USER_AGENTS, PassthroughModel, SQLGen
from apps.leagues.conf import LeagueValidator
from apps.matches.conf import MatchValidator
from apps.players.conf import PlayerValidator
from apps.teams.conf import TeamValidator
from common.iterables_utils import chunked

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

# ─────────────────────────────── constants ────────────────────────────────
log = structlog.get_logger(__name__).bind(component="DotaDataHandler")


DATA_VALIDATORS: Mapping[str, type[BaseModel]] = {
    "teams": TeamValidator,
    "leagues": LeagueValidator,
    "players": PlayerValidator,
    "matches": MatchValidator,
}


# ─────────────────────────────── dataclasses ────────────────────────────────
@dataclass(slots=True, frozen=True)
class RetryConfig:
    """Config for exponential-backoff retries."""

    max_retries: int = 3
    base_delay_s: float = 1.0
    max_delay_s: float = 30.0
    jitter_factor: float = 0.5

    def backoff(self, attempt: int) -> float:
        base = min(self.base_delay_s * (2**attempt), self.max_delay_s)
        jitter = base * self.jitter_factor * random.uniform(-1, 1)
        return max(0.1, base + jitter)


@dataclass(slots=True, frozen=True)
class DotaApiConfig:
    explorer_url: str
    timeout_s: int
    retry: RetryConfig = field(default_factory=RetryConfig)

    @classmethod
    def from_settings(cls) -> Self:
        cfg = settings.DOTA_API_CONFIG
        return cls(cfg.EXPLORER_URL, cfg.TIMEOUT_S, RetryConfig(**cfg.RETRY_CONFIG))


# ─────────────────────────────── main handler ────────────────────────────────
class DotaDataHandler:
    """
    1. Build SQL with supplied generators.
    2. HTTP-GET Explorer endpoint.
    3. Validate rows (pydantic).
    4. Chunk rows for async persistence.
    """

    def __init__(
        self,
        *,
        query_generators: Mapping[str, SQLGen],
        session: httpx.AsyncClient | None = None,
        config: DotaApiConfig | None = None,
    ) -> None:
        self._query_generators = query_generators
        self._config = config or DotaApiConfig.from_settings()
        self._external_session = session
        self._session: httpx.AsyncClient | None = None

    # ------------------------------------------------------- context manager --
    async def __aenter__(self) -> Self:
        default_headers = {"User-Agent": random.choice(USER_AGENTS)}
        self._session = self._external_session or httpx.AsyncClient(
            timeout=self._config.timeout_s,
            follow_redirects=True,
            headers=default_headers,
        )
        return self

    async def __aexit__(self, *exc) -> None:
        if not self._external_session and self._session:
            await self._session.aclose()

    # ------------------------------------------------------- public API -------
    async def fetch_and_chunk(
        self,
        data_type: str,
        *,
        chunk_size: int = 1_000,
    ) -> list[list[dict[str, Any]]]:
        sql = await self._generate_sql(data_type)
        payload = await self._request({"sql": sql})

        rows: list[dict[str, Any]] = payload.get("rows", [])
        if not rows:
            log.warning("Explorer API returned 0 rows", dtype=data_type)
            return []

        validated = self._validate_rows(data_type, rows)
        return list(chunked(validated, chunk_size))

    # ------------------------------------------------------- internals --------
    async def _request(self, params: Mapping[str, Any]) -> dict[str, Any]:
        assert self._session, "Session not initialised"

        for attempt in range(self._config.retry.max_retries + 1):
            try:
                # Rotate UA on every attempt for extra entropy
                headers = {"User-Agent": random.choice(USER_AGENTS)}
                resp = await self._session.get(
                    self._config.explorer_url,
                    params=params,
                    headers=headers,
                )
                resp.raise_for_status()
                return resp.json()
            except (httpx.TimeoutException, httpx.HTTPError) as exc:
                if attempt >= self._config.retry.max_retries:
                    msg = "Explorer request failed"
                    raise RuntimeError(msg) from exc
                delay = self._config.retry.backoff(attempt)
                log.warning(
                    "Explorer request failed, retrying",
                    attempt=attempt + 1,
                    delay=f"{delay:.1f}s",
                    err=str(exc),
                )
                await asyncio.sleep(delay)

        # Should never reach here
        msg = "Unreachable retry loop exit"
        raise RuntimeError(msg)

    async def _generate_sql(self, dtype: str) -> str:
        gen = self._query_generators.get(dtype)
        if not gen:
            msg = f"No SQL generator registered for '{dtype}'"
            raise ValueError(msg)
        return await gen() if asyncio.iscoroutinefunction(gen) else gen()

    def _validate_rows(self, dtype: str, rows: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
        Validator = DATA_VALIDATORS.get(dtype, PassthroughModel)

        valid_rows: list[dict[str, Any]] = []
        invalid_count = 0
        for idx, row in enumerate(rows):
            try:
                model = Validator.model_validate(row)
                valid_rows.append(model.model_dump(by_alias=True))
            except ValidationError as e:
                invalid_count += 1
                if invalid_count <= 5:
                    log.warning(
                        "Row validation failed",
                        dtype=dtype,
                        idx=idx,
                        err=e.errors(),
                    )

        if invalid_count:
            ratio = len(valid_rows) / len(rows)
            log.warning(
                "Validation finished with errors",
                dtype=dtype,
                valid=len(valid_rows),
                invalid=invalid_count,
                ratio=f"{ratio:.2%}",
            )
            if ratio < 0.5:
                msg = f">50 % of '{dtype}' rows failed validation (valid ratio={ratio:.2%})"
                raise RuntimeError(
                    msg,
                )

        return valid_rows
