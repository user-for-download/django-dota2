from __future__ import annotations

import asyncio
import json
import os
import time
from typing import TYPE_CHECKING

import orjson
from asgiref.sync import sync_to_async
from django.db import connection
from django.utils import timezone
from starlette.responses import StreamingResponse

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from starlette.requests import Request

try:
    import psutil
except ImportError:
    psutil = None

# --------------------------------------------------------------------------- tuning
STREAM_ITERATIONS: int = int(os.getenv("STATUS_STREAM_ITER", 20))
STREAM_DELAY_SEC: float = float(os.getenv("STATUS_STREAM_DELAY", 0.5))


# --------------------------------------------------------------------------- helpers
@sync_to_async
def _get_active_connections() -> int:
    with connection.cursor() as cur:
        cur.execute(
            """
            SELECT count(*)
            FROM pg_stat_activity
            WHERE datname = current_database()
            """,
        )
        return cur.fetchone()[0]


def _memory_mb() -> float | None:
    if psutil is None:  # psutil not installed
        return None
    return round(psutil.virtual_memory().used / 1024 / 1024, 2)


# --------------------------------------------------------------------------- stream generator
async def _status_stream_gen() -> AsyncIterator[bytes]:
    """
    Async generator yielding SSE frames.

    Yields at most STREAM_ITERATIONS times, sleeping STREAM_DELAY_SEC
    between iterations.
    """
    start = time.time()
    for i in range(1, STREAM_ITERATIONS + 1):
        try:
            status: dict[str, int | float | str | None] = {
                "iteration": i,
                "timestamp": timezone.now().isoformat(),
                "uptime_seconds": round(time.time() - start, 2),
                "memory_mb": _memory_mb(),
                "active_connections": await _get_active_connections(),
            }
            payload = orjson.dumps(status).decode()
            yield f"id: {i}\nevent: status\ndata: {payload}\n\n".encode()
        except Exception as exc:  # noqa: BLE001
            error = {
                "iteration": i,
                "error": str(exc),
                "timestamp": timezone.now().isoformat(),
            }
            yield f"event: error\ndata: {json.dumps(error)}\n\n".encode()

        await asyncio.sleep(STREAM_DELAY_SEC)

    # closing frame
    yield b'event: complete\ndata: {"status": "stream_complete"}\n\n'


# --------------------------------------------------------------------------- endpoint
async def status_stream(request: Request) -> StreamingResponse:
    """
    GET /status-stream  →  continuous Server-Sent-Events feed.

    Headers:
      Cache-Control: no-cache
      Connection: keep-alive
      Access-Control-Allow-Origin: *
    """
    return StreamingResponse(
        _status_stream_gen(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "Access-Control-Allow-Origin": "*",
        },
    )
