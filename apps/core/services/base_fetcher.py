# /home/ubuntu/dota/apps/core/services/base_fetcher.py
# ================================================================================
"""
Abstract async fetcher with concurrency, persistence & match-ID publishing.
Optimised for Python 3.12 / Django 5 async ORM.
"""

from __future__ import annotations

import asyncio
import itertools
import os
import random
from abc import ABC, abstractmethod
from collections import Counter
from typing import TYPE_CHECKING, Any, Protocol, Self, TypeVar

import httpx
import structlog
from django.db import connection

from apps.core.conf import DEFAULT_TIMEOUT_S, USER_AGENTS, BaseFetcherConfig
from common.messaging.batching import schedule_matches_for_processing
from common.parsers_utils import parse_match_ids_from_rows

if TYPE_CHECKING:
    from collections.abc import Iterable, Sequence

    from apps.core.datatype import UpsertResult

module_log = structlog.get_logger(__name__).bind(component="BaseFetcher")

CfgT = TypeVar("CfgT", bound=BaseFetcherConfig)
HdlT = TypeVar("HdlT", bound="HandlerProtocol")


class FetcherError(RuntimeError): ...


class HandlerProtocol(Protocol):
    async def upsert_async(self, rows: Sequence[dict[str, Any]], **kw) -> UpsertResult: ...


class BaseFetcher[CfgT: BaseFetcherConfig, HdlT: "HandlerProtocol"](ABC):
    """
    Template class for concrete fetchers.
    """

    def __init__(
        self,
        cfg: CfgT | None = None,
        *,
        handler: HdlT | None = None,
        session: httpx.AsyncClient | None = None,
    ) -> None:
        self.cfg: CfgT = cfg or self._default_config()
        self.handler: HdlT = handler or self._default_handler()
        self.log = module_log.bind(fetcher=self.__class__.__name__)
        self._session = session
        self._session_created = False

    # ------------------------------------------------------------- context
    async def __aenter__(self) -> Self:
        self.cfg.check()
        await self._validate_specific()
        if self._session is None:
            headers = {"User-Agent": random.choice(USER_AGENTS)}
            self._session = httpx.AsyncClient(timeout=DEFAULT_TIMEOUT_S, headers=headers)
            self._session_created = True
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        if self._session_created and self._session:
            await self._session.aclose()

    # ------------------------------------------------------------- abstract
    @abstractmethod
    def _default_config(self) -> CfgT: ...

    @abstractmethod
    def _default_handler(self) -> HdlT: ...

    @abstractmethod
    def _fetcher_type(self) -> str: ...

    @abstractmethod
    async def _fetch_primary_source(self) -> list[list[dict[str, Any]]]: ...

    @abstractmethod
    def _fallback_url(self) -> str | None: ...

    @abstractmethod
    def _count_query(self) -> str: ...

    async def _validate_specific(self) -> None: ...

    # ------------------------------------------------------------- main run
    async def run(self) -> UpsertResult:
        self.log.debug("run() start", cfg=self.cfg.model_dump(mode="json"))
        try:
            chunks = await self._fetch_primary_source()
            if chunks:
                return await self._persist_chunks(chunks, publish=not self.cfg.skip_matches)

            self.log.warning("primary source empty; trying fallback")
            if fb_url := self._fallback_url():
                rows = await self._fetch_fallback(fb_url)
                return (
                    await self._persist_chunks([rows], publish=False)
                    if rows
                    else {"created": 0, "updated": 0, "skipped": 0}
                )

            self.log.info("no data from any source")
            return {"created": 0, "updated": 0, "skipped": 0}
        except Exception as exc:
            self.log.error("critical fetch error", exc_info=True)
            msg = f"Fetch failed for {self._fetcher_type()}: {exc}"
            raise FetcherError(msg) from exc

    # ------------------------------------------------------------- metrics
    async def get_metrics(self) -> dict[str, Any]:
        try:
            async with connection.cursor() as cur:
                await cur.execute(self._count_query())
                (total,) = await cur.fetchone()
            return {f"total_{self._fetcher_type()}": int(total)}
        except Exception:
            self.log.error("metrics retrieval failed", exc_info=True)
            return {"error": "metrics failed"}

    # ------------------------------------------------------------- helpers
    async def _fetch_fallback(self, url: str) -> list[dict[str, Any]]:
        if not self._session:
            msg = "session not ready"
            raise RuntimeError(msg)
        try:
            resp = await self._session.get(url)
            resp.raise_for_status()
            data = resp.json()
            return data if isinstance(data, list) else []
        except httpx.HTTPError as exc:
            self.log.warning("fallback fetch failed", url=url, err=str(exc))
            return []

    async def _persist_chunks(
        self,
        chunks: Sequence[Sequence[dict[str, Any]]],
        *,
        publish: bool,
    ) -> UpsertResult:
        if not chunks or not chunks[0]:
            return {"created": 0, "updated": 0, "skipped": 0}

        queue: asyncio.Queue[Sequence[dict[str, Any]]] = asyncio.Queue(maxsize=len(chunks))
        for chunk in chunks:
            await queue.put(chunk)

        async def worker() -> UpsertResult:
            res: UpsertResult = {"created": 0, "updated": 0, "skipped": 0}
            while not queue.empty():
                batch = await queue.get()
                try:
                    batch_res = await self.handler.upsert_async(batch)
                    res["created"] += batch_res.get("created", 0)
                    res["updated"] += batch_res.get("updated", 0)
                    res["skipped"] += batch_res.get("skipped", 0)
                except Exception:
                    self.log.exception("persistence worker failed")
                finally:
                    queue.task_done()
            return res

        n_workers = min(self.cfg.max_parallel_chunks, len(chunks), (os.cpu_count() or 4) * 2)
        workers = [asyncio.create_task(worker()) for _ in range(n_workers)]
        await queue.join()
        results = await asyncio.gather(*workers)

        total = Counter()
        for r in results:
            total.update(r)

        aggregated = dict(total)
        if publish:
            await self._publish_matches(itertools.chain.from_iterable(chunks))
        self.log.info("persistence complete", **aggregated)
        return aggregated  # type: ignore[return-value]

    async def _publish_matches(self, rows: Iterable[dict[str, Any]]) -> None:
        if self.cfg.skip_matches:
            self.log.info("skip match publishing (cfg)")
            return

        ids = parse_match_ids_from_rows(rows)
        if not ids:
            self.log.warning("no match ids parsed")
            return

        self.log.info("publishing match ids", count=len(ids))
        await schedule_matches_for_processing(list(ids), force=self.cfg.force)
