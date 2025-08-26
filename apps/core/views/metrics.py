from __future__ import annotations

import asyncio
import time
from typing import TYPE_CHECKING

from asgiref.sync import sync_to_async
from django.core.cache import cache
from django.db import connection
from django.utils import timezone
from starlette.responses import JSONResponse

if TYPE_CHECKING:
    from starlette.requests import Request

MODULE_START_TIME: float = time.time()


@sync_to_async
def _fetch_db_metrics() -> dict[str, int | list[dict[str, int]]]:
    try:
        with connection.cursor() as cur:
            cur.execute(
                """
                SELECT
                    schemaname,
                    relname AS table_name,
                    COALESCE(n_tup_ins,0) +
                    COALESCE(n_tup_upd,0) +
                    COALESCE(n_tup_del,0) AS total_ops
                FROM pg_stat_user_tables
                WHERE schemaname = 'public'
                ORDER BY total_ops DESC
                LIMIT 10;
                """,
            )
            rows = cur.fetchall()

        return {
            "active_tables": len(rows),
            "top_tables": [{"table": r[1], "operations": r[2]} for r in rows],
        }
    except Exception as exc:
        return {"error": f"Unable to fetch database metrics: {exc}"}


@sync_to_async(thread_sensitive=True)
def _fetch_cache_metrics() -> dict[str, int | float]:
    stats = cache.get("teams:fetch_statistics") or {}
    return {
        "total_fetches": stats.get("total_fetches", 0),
        "average_processing_time": stats.get("average_processing_time", 0),
    }


# --------------------------------------------------------------------------- endpoint
async def metrics_endpoint(request: Request) -> JSONResponse:  # ← 2️⃣ JSONResponse
    """
    Prometheus-style metrics endpoint.
    """
    try:
        db_result, cache_result = await asyncio.gather(
            _fetch_db_metrics(),
            _fetch_cache_metrics(),
        )

        payload = {
            "timestamp": timezone.now().isoformat(),
            "uptime_seconds": time.time() - MODULE_START_TIME,
            "database": db_result,
            "cache": cache_result,
        }
        return JSONResponse(payload)  # ← 2️⃣ JSONResponse
    except Exception as exc:
        return JSONResponse({"error": str(exc)}, status_code=500)
