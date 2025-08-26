# common/messaging/batching.py
# ===========================================================================
"""
Filter, batch, and publish match-IDs to FastStream (or any broker).

Can be called
    • inside an already-running FastStream worker, or
    • from standalone async contexts (management-commands, tests).

Key steps
---------
1. De-dupe the incoming IDs.
2. Drop IDs that were already processed (unless *force* is True).
3. Slice the remainder into batches and publish them concurrently.
4. Return a `PublishResult` telemetry dict.
"""

from __future__ import annotations

import asyncio
import os
import time
import uuid
from typing import TYPE_CHECKING, Final

import structlog
from django.conf import settings

from apps.core.services.processed_ids import RedisProcessedIDChecker, get_redis_client
from common.iterables_utils import chunked
from common.messaging.types import MatchBatchPayload, PublishResult
from infrastructure.broker import ensure_broker_connected, reliable_publisher
from infrastructure.queues import QUEUES

if TYPE_CHECKING:  # only for type-checking / IDE
    from collections.abc import Sequence

log = structlog.get_logger(__name__).bind(component="MatchBatcher")

# ─── knobs that can be overridden via env or Django settings ────────────────
MAX_PUBLISH_CONCURRENCY: Final[int] = int(
    os.getenv("MAX_PUBLISH_CONCURRENCY", getattr(settings, "MAX_PUBLISH_CONCURRENCY", 10)),
)
BATCH_SIZE: Final[int] = int(
    os.getenv("BATCH_SIZE", getattr(settings, "BATCH_SIZE", 100)),
)
PROCESSED_REDIS_KEY: Final[str] = "processed:match_ids"


# ───────────────────────── internal sender helper ───────────────────────────
async def _send_match_batches(
    match_ids: set[int],
    *,
    force: bool,
    queue_name: str,
) -> PublishResult:
    """
    Slice *match_ids* into batches and publish them concurrently.
    Never raises – any exceptions are swallowed & counted (caller decides).
    """
    t0 = time.perf_counter()
    if not match_ids:
        return PublishResult(queue=queue_name, ids_published=0, batches_created=0, batches_failed=0, duration_s=0.0)

    await ensure_broker_connected()

    batches: list[list[int]] = list(chunked(match_ids, BATCH_SIZE))
    total_batches = len(batches)
    op_id = uuid.uuid4().hex[:8]

    log.info(
        "publishing batches",
        queue=queue_name,
        total_ids=len(match_ids),
        batches=total_batches,
        concurrency=MAX_PUBLISH_CONCURRENCY,
    )

    sem = asyncio.Semaphore(MAX_PUBLISH_CONCURRENCY)
    sent_count = 0
    failed_count = 0

    async def _publish_single(idx: int, batch: list[int]) -> None:
        nonlocal sent_count, failed_count
        payload = MatchBatchPayload(
            match_ids=batch,
            force_update=force,
            batch_id=f"{op_id}-{idx + 1}",
            batch_number=idx + 1,
            total_batches=total_batches,
            timestamp=time.time(),
        )
        try:
            async with sem:
                await asyncio.shield(reliable_publisher.publish(payload, queue=queue_name))
            sent_count += len(batch)
        except Exception:
            failed_count += 1
            log.exception("batch publish failed", batch_no=idx + 1, size=len(batch), op_id=op_id)

    # Use a task group to manage concurrent publishing
    try:
        async with asyncio.TaskGroup() as tg:
            for i, batch in enumerate(batches):
                tg.create_task(_publish_single(i, batch))
    except* Exception as eg:
        log.warning("task group for publishing batches raised exceptions", count=len(eg.exceptions))

    return PublishResult(
        queue=queue_name,
        ids_published=sent_count,
        batches_created=total_batches,
        batches_failed=failed_count,
        duration_s=round(time.perf_counter() - t0, 3),
    )


# ───────────────────────────── public facade ────────────────────────────────
async def schedule_matches_for_processing(
    match_ids: Sequence[int],
    *,
    force: bool = False,
) -> PublishResult:
    """
    Orchestrate publication of *match_ids*; see module docstring.
    """
    queue_name = QUEUES.PROCESS_MATCH_BATCH.value
    unique_ids = set(match_ids)

    log.info("schedule request", queue=queue_name, received=len(match_ids), unique=len(unique_ids), force=force)

    # Filter already processed IDs
    if force:
        ids_to_publish = unique_ids
        already_processed = 0
    else:
        redis_cli = get_redis_client()
        checker = RedisProcessedIDChecker(redis_cli, PROCESSED_REDIS_KEY)
        ids_to_publish = await checker.filter_processed(unique_ids)
        already_processed = len(unique_ids) - len(ids_to_publish)
        if already_processed > 0:
            log.info("filtered already-processed IDs", count=already_processed)

    # Early exit if no new IDs to publish
    if not ids_to_publish:
        result = PublishResult(
            queue=queue_name,
            initial_ids=len(match_ids),
            already_processed=already_processed,
            ids_to_publish=0,
            ids_published=0,
            batches_created=0,
            batches_failed=0,
            duration_s=0.0,
        )
        log.info("nothing to publish", **result)
        return result

    # Publish the batches
    try:
        publish_stats = await _send_match_batches(ids_to_publish, force=force, queue_name=queue_name)
        final_result = PublishResult(
            **publish_stats,
            initial_ids=len(match_ids),
            already_processed=already_processed,
            ids_to_publish=len(ids_to_publish),
        )
        log.info("scheduling complete", **final_result)
        return final_result

    except asyncio.CancelledError:
        log.warning("scheduling cancelled by caller")
        raise
    except Exception:
        log.exception("critical failure during match scheduling")
        # Return a consistent result type on critical failure
        return PublishResult(
            queue=queue_name,
            initial_ids=len(match_ids),
            already_processed=already_processed,
            ids_to_publish=len(ids_to_publish),
            ids_published=0,
            batches_created=0,
            batches_failed=len(list(chunked(ids_to_publish, BATCH_SIZE))),
            duration_s=0.0,
        )
