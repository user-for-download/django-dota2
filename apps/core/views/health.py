# apps/core/views.py

from __future__ import annotations

import asyncio
import time
from typing import TYPE_CHECKING

from infrastructure.broker import broker

# Use to_thread for clarity and compatibility
try:
    from asyncio import to_thread
except ImportError:

    def to_thread(func, /, *args):
        loop = asyncio.get_running_loop()
        return loop.run_in_executor(None, func, *args)


from django.conf import settings
from django.core.cache import cache
from django.db import DatabaseError, connections
from django.utils import timezone
from starlette.responses import JSONResponse

# Import the broker for a real health check

if TYPE_CHECKING:
    from starlette.requests import Request

# --------------------------------------------------------------------------- helpers


# CORRECT: This is now a pure synchronous function.
# We removed @sync_to_async and will call it with to_thread.
def _simple_db_query() -> None:
    """Gets a connection and performs a simple query within the same thread."""
    # By getting the connection object here, we ensure it's created
    # and used in the *same* worker thread.
    db_conn = connections["default"]
    with db_conn.cursor() as cur:
        cur.execute("SELECT 1")
        cur.fetchone()


async def _check_database() -> dict[str, str | float]:
    start = time.perf_counter()
    try:
        # CORRECT: Run the synchronous, thread-safe function in a thread pool.
        await to_thread(_simple_db_query)
        return {
            "status": "healthy",
            "response_time_ms": round((time.perf_counter() - start) * 1000, 2),
        }
    except DatabaseError as exc:
        return {"status": "unhealthy", "error": str(exc)}


# CORRECT: Using to_thread is slightly more explicit for sync functions.
# Your original implementation was also correct and safe.
async def _check_cache() -> dict[str, str]:
    key = f"health:{int(time.time())}"
    try:

        def check_cache_sync():
            cache.set(key, "ok", 10)
            ok = cache.get(key) == "ok"
            cache.delete(key)
            return ok

        success = await to_thread(check_cache_sync)
        if success:
            return {"status": "healthy"}
        msg = "Cache round-trip check failed"
        raise RuntimeError(msg)
    except Exception as exc:
        return {"status": "unhealthy", "error": str(exc)}


async def _check_broker() -> dict[str, str]:
    """ENHANCED: Perform a real check against the message broker."""
    try:
        # FastStream brokers typically have a .ping() method.
        # It's often async, but check your specific broker's implementation.
        # If it were sync, you'd use `await to_thread(broker.ping)`.
        await broker.ping(timeout=3)
        return {"status": "healthy"}
    except Exception as exc:
        return {"status": "unhealthy", "error": str(exc)}


async def _check_application() -> dict[str, str | dict]:
    """
    ENHANCED: Placeholder for app-specific logic with a default healthy status.
    e.g., check connectivity to a critical third-party API.
    """
    try:
        # Add any custom application logic checks here.
        # For now, we assume it's healthy if no exceptions are raised.
        return {"status": "healthy"}
    except Exception as exc:
        return {"status": "unhealthy", "error": str(exc)}


# --------------------------------------------------------------------------- view
async def health_check(request: Request) -> JSONResponse:
    """
    Comprehensive health endpoint.
    • `?check=basic`  → liveness-only.
    """
    start_view = time.perf_counter()
    base_payload = {
        "timestamp": timezone.now().isoformat(),
        "version": getattr(settings, "APP_VERSION", "unknown"),
        "environment": getattr(settings, "ENVIRONMENT", "unknown"),
    }

    # very cheap liveness probe for Kubernetes/ECS
    if request.query_params.get("check") == "basic":
        return JSONResponse({"status": "ok", **base_payload})

    (
        db_result,
        cache_result,
        broker_result,
        app_result,
    ) = await asyncio.gather(
        _check_database(),
        _check_cache(),
        _check_broker(),
        _check_application(),
    )

    checks = {
        "database": db_result,
        "cache": cache_result,
        "message_broker": broker_result,
        "application": app_result,
    }

    # Determine overall status. It's unhealthy if any enabled check is not healthy.
    overall_healthy = all(v["status"] == "healthy" for v in checks.values() if v.get("status") != "disabled")
    status_code = 200 if overall_healthy else 503

    response = {
        "status": "healthy" if overall_healthy else "unhealthy",
        "checks": checks,
        "response_time_ms": round((time.perf_counter() - start_view) * 1000, 2),
        **base_payload,
    }
    return JSONResponse(response, status_code=status_code)
