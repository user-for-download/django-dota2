"""
Optimized Django 5.2 ASGI application with FastStream integration.
"""

from __future__ import annotations

import asyncio
import os
import sys
import time
from contextlib import asynccontextmanager, suppress
from typing import TYPE_CHECKING

import structlog
from asgiref.sync import sync_to_async
from django.conf import settings
from django.core.asgi import get_asgi_application
from django.core.cache import cache
from django.db import connections
from starlette.applications import Starlette
from starlette.middleware import Middleware
from starlette.middleware.cors import CORSMiddleware
from starlette.routing import BaseRoute, Mount, Route


if TYPE_CHECKING:
    from collections.abc import AsyncIterator
    from django.db.backends.base.base import BaseDatabaseWrapper

# --- Set up Django environment
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings.local")
import django
django.setup()

# --- Import endpoints and broker utilities
from apps.core.views import health_check, metrics_endpoint, status_stream
from infrastructure.broker import BrokerConnectionError, ensure_broker_connected, shutdown_broker

# --- Constants
WARMUP_CACHE_KEY = "system:warmup:test"
WARMUP_CACHE_TTL = 30
DB_WARMUP_TIMEOUT = 5.0
CACHE_WARMUP_TIMEOUT = 3.0
SHUTDOWN_TIMEOUT = 10.0

logger = structlog.get_logger(__name__)

django_app = get_asgi_application()

class WarmupError(Exception):
    """Raised when a warmup operation fails."""

async def _test_database_connection() -> None:
    """Test database connectivity with timeout protection."""
    def _db_test() -> None:
        conn: BaseDatabaseWrapper = connections["default"]
        conn.ensure_connection()
        with conn.cursor() as cursor:
            cursor.execute("SELECT 1")
            cursor.fetchone()
    try:
        await asyncio.wait_for(asyncio.to_thread(_db_test), timeout=DB_WARMUP_TIMEOUT)
    except TimeoutError:
        msg = f"Database connection timed out after {DB_WARMUP_TIMEOUT}s"
        raise WarmupError(msg)
    except Exception as e:
        msg = f"Database connection failed: {e}"
        raise WarmupError(msg)

async def _test_cache_system() -> None:
    """Test cache system functionality with timeout protection."""
    test_value = f"warmup_{time.time()}"
    try:
        @sync_to_async
        def cache_test() -> None:
            cache.set(WARMUP_CACHE_KEY, test_value, WARMUP_CACHE_TTL)
            result = cache.get(WARMUP_CACHE_KEY)
            if result != test_value:
                msg = f"Cache verification failed: expected '{test_value}', got '{result}'"
                raise WarmupError(msg)
        await asyncio.wait_for(cache_test(), timeout=CACHE_WARMUP_TIMEOUT)
    except TimeoutError:
        msg = f"Cache operation timed out after {CACHE_WARMUP_TIMEOUT}s"
        raise WarmupError(msg)
    except Exception as e:
        if isinstance(e, WarmupError):
            raise
        logger.debug("Cache error details", error=str(e), error_type=type(e).__name__)
        msg = f"Cache system not available: {type(e).__name__}"
        raise WarmupError(msg)

async def _run_system_checks() -> None:
    """
    Run Django system checks in a separate process and capture output on failure.
    """
    try:
        command = [sys.executable, "manage.py", "check"]
        process = await asyncio.create_subprocess_exec(
            *command,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await process.communicate()
        if process.returncode != 0:
            error_message = stderr.decode().strip()
            logger.error(
                "Django system check command found issues.",
                exit_code=process.returncode,
                error_output=error_message,
            )
            msg = f"Django system check failed. Details:\n{error_message}"
            raise WarmupError(msg)
    except Exception as e:
        if isinstance(e, WarmupError):
            raise
        msg = f"Failed to execute Django system check subprocess: {e!s}"
        raise WarmupError(msg) from e

async def _warm_up_application() -> None:
    """Runs all non-broker warmup checks concurrently."""
    logger.info("Starting application warm-up...")
    start_time = time.monotonic()
    tasks = {
        "system_check": asyncio.create_task(_run_system_checks()),
        "database": asyncio.create_task(_test_database_connection()),
        "cache": asyncio.create_task(_test_cache_system()),
    }
    results = await asyncio.gather(*tasks.values(), return_exceptions=True)
    has_errors = False
    for name, result in zip(tasks.keys(), results, strict=True):
        if isinstance(result, Exception):
            logger.warning(f"Component '{name}' warm-up failed", error=str(result))
            has_errors = True
        else:
            logger.info(f"✓ Component '{name}' warmed up successfully")
    duration = time.monotonic() - start_time
    if has_errors:
        logger.warning("Application warm-up completed with warnings", duration_s=f"{duration:.2f}")
    else:
        logger.info("✅ Application warm-up completed successfully", duration_s=f"{duration:.2f}")

# --- Centralized Lifespan Manager ---
@asynccontextmanager
async def lifespan(app: Starlette) -> AsyncIterator[None]:
    """
    Manages the complete application lifecycle for startup and shutdown.
    Handles warm-up, broker connection via `infrastructure.broker`, and graceful cleanup.
    This is triggered by the ASGI server (e.g., Uvicorn).
    """
    # --- Startup Logic ---
    logger.info("🚀 ASGI application starting up...")
    try:
        await _warm_up_application()
        logger.info("Connecting to message broker...")
        await ensure_broker_connected()
    except (WarmupError, BrokerConnectionError) as e:
        logger.error("Critical error during application startup. Aborting.", error=str(e), exc_info=True)
        raise
    except Exception as e:
        logger.error("An unexpected critical error occurred during startup.", error=str(e), exc_info=True)
        raise
    logger.info("✅ Application startup complete. Ready to serve requests.")
    yield
    # --- Shutdown Logic ---
    logger.info("🛑 ASGI application shutting down...")
    try:
        async with asyncio.timeout(SHUTDOWN_TIMEOUT):
            await shutdown_broker()
            async def close_db_connections():
                with suppress(Exception):
                    await sync_to_async(connections.close_all)()
                    logger.info("✓ Database connections closed")
            await close_db_connections()
    except TimeoutError:
        logger.warning(
            f"Shutdown timed out after {SHUTDOWN_TIMEOUT}s. Forcing exit.",
        )
    except Exception as e:
        logger.exception("Error during shutdown cleanup", error=str(e))
    logger.info("✅ ASGI application shutdown complete.")

# --- Application Factory Functions ---
def create_middleware() -> list[Middleware]:
    """Create middleware stack based on settings."""
    return [
        Middleware(
            CORSMiddleware,
            allow_origins=getattr(settings, "CORS_ALLOWED_ORIGINS", ["*"]),
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"],
        ),
    ]

def create_routes() -> list[BaseRoute]:
    """Create application routes."""
    routes: list[BaseRoute] = [
        Route("/health", endpoint=health_check, methods=["GET", "HEAD"]),
        Route("/metrics", endpoint=metrics_endpoint, methods=["GET"]),
        Route("/status-stream", endpoint=status_stream, methods=["GET"]),
        Mount("/", app=django_app),
    ]
    return routes

# --- Main Application Instance ---
application = Starlette(
    debug=settings.DEBUG,
    routes=create_routes(),
    middleware=create_middleware(),
    lifespan=lifespan,
)
