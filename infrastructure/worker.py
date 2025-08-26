"""
FastStream workers – launch with

    $ python workers.py

or Django’s  `run_workers` management command.
"""

from __future__ import annotations

import asyncio
import os
import signal
import sys
import time
from contextlib import asynccontextmanager, suppress
from dataclasses import dataclass
from datetime import UTC, datetime
from functools import partial
from typing import TYPE_CHECKING, Any, Final

import structlog
from django.core.exceptions import ValidationError as DjangoValidationError

from apps.core.services.fetcher_service import MatchFetcherService
from apps.core.services.processed_ids import RedisProcessedIDChecker
from apps.matches.conf import MatchFetcherConfig
from common.iterables_utils import chunked
from common.messaging.types import MatchBatchPayload
from infrastructure.broker import BATCH_SIZE, app, broker, ensure_broker_connected, shutdown_broker
from infrastructure.queues import QUEUES
from config.log import LOGGING

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Sequence

# ───────────────────────── config knobs ─────────────────────────
log: Final = structlog.get_logger(__name__)

CONCURRENCY_DEFAULT = int(os.getenv("WORKER_CONCURRENCY", "10"))
HEALTH_CHECK_INTERVAL_S = int(os.getenv("HEALTH_CHECK_INTERVAL_S", "30"))
GRACEFUL_SHUTDOWN_TIMEOUT_S = int(os.getenv("GRACEFUL_SHUTDOWN_TIMEOUT_S", "30"))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))
RETRY_DELAY_S = float(os.getenv("RETRY_DELAY_S", "1.0"))


# ───────────────────────── metrics struct ───────────────────────
@dataclass(slots=True)
class WorkerMetrics:
    batches_processed: int = 0
    batches_failed: int = 0
    items_processed: int = 0
    items_failed: int = 0
    total_processing_time: float = 0.0
    average_batch_size: float = 0.0
    last_activity: datetime | None = None
    startup_time: datetime = datetime.now(UTC)

    def record_batch_start(self) -> None:
        self.last_activity = datetime.now(UTC)

    def _update_avg(self, size: int) -> None:
        total = self.batches_processed + self.batches_failed
        if total > 0:
            self.average_batch_size = (self.average_batch_size * (total - 1) + size) / total

    def record_success(self, size: int, dur: float) -> None:
        self.batches_processed += 1
        self.items_processed += size
        self.total_processing_time += dur
        self._update_avg(size)

    def record_failure(self, size: int, failed_items: int) -> None:
        self.batches_failed += 1
        self.items_failed += failed_items
        self._update_avg(size)

    def as_dict(self) -> dict[str, Any]:
        total_batches = self.batches_processed + self.batches_failed
        return {
            "batches_processed": self.batches_processed,
            "batches_failed": self.batches_failed,
            "items_processed": self.items_processed,
            "items_failed": self.items_failed,
            "success_rate_pct": (round(self.batches_processed / total_batches * 100, 2) if total_batches else 100),
            "avg_proc_time_s": (
                round(self.total_processing_time / self.batches_processed, 3) if self.batches_processed else 0
            ),
            "avg_batch_size": round(self.average_batch_size, 1),
            "last_activity": self.last_activity.isoformat() if self.last_activity else None,
            "uptime_s": round((datetime.now(UTC) - self.startup_time).total_seconds(), 1),
        }


worker_metrics = WorkerMetrics()


# ───────────────────────── circuit breaker ─────────────────────
class CircuitBreaker:
    def __init__(self, threshold: int = 5, recovery_s: float = 60.0) -> None:
        self.threshold, self.recovery = threshold, recovery_s
        self.failures = 0
        self.last_failure: float = 0.0
        self.state: str = "closed"
        self._lock = asyncio.Lock()

    @property
    def is_open(self) -> bool:
        return self.state == "open" and (time.time() - self.last_failure) < self.recovery

    async def __aenter__(self):
        async with self._lock:
            if self.is_open:
                msg = f"Circuit breaker is open. Recovery in {self.recovery - (time.time() - self.last_failure):.1f}s"
                raise RuntimeError(msg)
        return self

    async def __aexit__(self, exc_type, exc_val, traceback):
        async with self._lock:
            if exc_type:
                self.failures += 1
                self.last_failure = time.time()
                if self.failures >= self.threshold:
                    self.state = "open"
                    log.warning("Circuit breaker opened", failures=self.failures, threshold=self.threshold)
            else:
                self.failures = 0
                if self.state != "closed":
                    self.state = "closed"
                    log.info("Circuit breaker closed")


circuit_breaker = CircuitBreaker(threshold=int(os.getenv("CB_FAIL_THRESHOLD", "5")))


# ───────────────────────── task processor ──────────────────────
async def run_concurrently(
    *,
    items: Sequence[Any],
    worker_fn,
    item_label: str,
    batch_id: str,
    concurrency: int,
) -> None:
    if not items:
        return

    async with circuit_breaker:
        semaphore = asyncio.Semaphore(max(1, concurrency))
        t0 = time.perf_counter()

        async def guard(item):
            async with semaphore:
                await worker_fn(item)

        try:
            async with asyncio.TaskGroup() as tg:
                for itm in items:
                    tg.create_task(guard(itm))
        except* Exception as eg:
            worker_metrics.record_failure(len(items), len(eg.exceptions))
            log.exception(
                "Concurrent exec failed",
                item=item_label,
                failed=len(eg.exceptions),
                total=len(items),
                batch=batch_id,
                exc_info=eg,
            )
            raise
        else:
            worker_metrics.record_success(len(items), time.perf_counter() - t0)
            log.info(
                "Concurrent exec ok",
                item=item_label,
                count=len(items),
                dur_s=f"{time.perf_counter() - t0:.3f}",
                batch=batch_id,
            )


TIMEOUT_PER_CHUNK_S = int(os.getenv("CHUNK_TIMEOUT_S", "120"))


async def _process_chunk(ids: list[int], *, service: MatchFetcherService, force: bool) -> None:
    global checker
    if not force:
        checker = RedisProcessedIDChecker(key="processed:match_ids")
        new_ids = await checker.filter_processed(set(ids))
        if not new_ids:
            log.debug("All match IDs already processed", ids=ids)
            return
        ids = list(new_ids)

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            async with asyncio.timeout(TIMEOUT_PER_CHUNK_S):
                cfg = MatchFetcherConfig(match_ids=ids)
                await service.fetch_and_cache(cfg, force_refresh=force)
            if not force:
                await checker.mark_processed(ids)
            return
        except DjangoValidationError as e:
            log.exception("Validation error – not retrying", err=str(e), ids=ids)
            raise
        except Exception:
            if attempt >= MAX_RETRIES:
                log.exception("Chunk failed after max retries", ids=ids)
                raise
            backoff = RETRY_DELAY_S * 2 ** (attempt - 1)
            log.warning("Chunk retry", attempt=attempt, sleep=backoff, ids=ids)
            await asyncio.sleep(backoff)


# ───────────────────────── context manager ─────────────────────
@asynccontextmanager
async def _get_match_fetcher_service() -> AsyncIterator[MatchFetcherService]:
    yield MatchFetcherService()


@broker.subscriber(QUEUES.PROCESS_MATCH_BATCH, retry=True)
async def process_match_batch(msg_raw: Any) -> None:
    try:
        if isinstance(msg_raw, str):
            payload = MatchBatchPayload.model_validate_json(msg_raw)
        elif isinstance(msg_raw, dict):
            payload = MatchBatchPayload.model_validate(msg_raw)
        elif isinstance(msg_raw, MatchBatchPayload):
            payload = msg_raw
        else:
            log.error("Invalid message type", type=type(msg_raw), raw=msg_raw)
            return

    except Exception as e:
        log.exception("Failed to parse MatchBatchPayload", exc_info=e)
        return

    worker_metrics.record_batch_start()
    log.info(
        "Batch received",
        batch_id=payload.batch_id,
        match_count=len(payload.match_ids),
        force=payload.force_update,
    )

    if not payload.match_ids:
        worker_metrics.record_success(0, 0)
        log.warning("Empty batch – skipping", batch_id=payload.batch_id)
        return

    chunks = list(chunked(payload.match_ids, BATCH_SIZE))
    try:
        async with _get_match_fetcher_service() as service:
            await run_concurrently(
                items=chunks,
                worker_fn=partial(
                    _process_chunk,
                    service=service,
                    force=payload.force_update,
                ),
                item_label="match-chunk",
                batch_id=payload.batch_id,
                concurrency=payload.concurrency,
            )
    except Exception as exc:
        log.exception(
            "Batch processing failed",
            batch_id=payload.batch_id,
            err=str(exc),
        )
        raise


# ───────────────────────── lifecycle manager ───────────────────
class WorkerManager:
    def __init__(self) -> None:
        self._shutdown_evt = asyncio.Event()
        self._health_task: asyncio.Task | None = None

    async def start(self) -> None:
        log.info("WorkerManager starting")
        await ensure_broker_connected()
        self._install_signals()
        self._health_task = asyncio.create_task(self._health_loop())
        await app.run()

    async def stop(self) -> None:
        if self._shutdown_evt.is_set():
            return
        self._shutdown_evt.set()
        log.warning("Stopping workers…")
        try:
            await asyncio.wait_for(app.stop(), timeout=GRACEFUL_SHUTDOWN_TIMEOUT_S)
        except TimeoutError:
            log.warning("app.stop timed out – forcing shutdown")
        if self._health_task:
            self._health_task.cancel()
            with suppress(asyncio.CancelledError):
                await self._health_task
        await shutdown_broker()
        log.info("Workers stopped")

    def _install_signals(self) -> None:
        if sys.platform != "win32":
            loop = asyncio.get_running_loop()
            for sig in (signal.SIGINT, signal.SIGTERM):
                loop.add_signal_handler(sig, lambda s=sig: asyncio.create_task(self._sig_handler(s)))

    async def _sig_handler(self, sig: signal.Signals) -> None:
        log.warning("OS signal received", sig=sig.name)
        await self.stop()

    async def _health_loop(self) -> None:
        while not self._shutdown_evt.is_set():
            await asyncio.sleep(HEALTH_CHECK_INTERVAL_S)
            if not self._shutdown_evt.is_set():
                log.debug("Worker health", **worker_metrics.as_dict())


# ───────────────────────── CLI entrypoint ───────────────────────
async def main() -> None:
    """Main entry point to start the worker manager."""
    mgr = WorkerManager()
    try:
        await mgr.start()
    except (KeyboardInterrupt, SystemExit):
        log.info("Shutdown initiated by user")
    finally:
        await mgr.stop()


if __name__ == "__main__":
    # This allows running the worker directly for development/debugging.
    # `python -m infrastructure.worker`
    asyncio.run(main())
