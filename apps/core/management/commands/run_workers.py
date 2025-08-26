# your_app/management/commands/run_faststream_worker.py

"""
django-admin command: starts the FastStream worker.

Usage:
    python manage.py run_faststream_worker --log-level DEBUG
"""

from __future__ import annotations

import asyncio

import structlog
from django.core.management.base import BaseCommand

from infrastructure.worker import WorkerManager

from config.log import LOGGING

log = structlog.get_logger(__name__)


class Command(BaseCommand):
    help = "Run the FastStream message-broker worker."

    def add_arguments(self, parser) -> None:
        parser.add_argument(
            "--log-level",
            default="INFO",
            help="Set the structlog logging level.",
        )

    def handle(self, *args, **options) -> None:
        level_name: str = options["log_level"].upper()
        log.info("FastStream worker starting...", log_level=level_name)

        manager = WorkerManager()

        try:
            asyncio.run(manager.start())
        except KeyboardInterrupt:
            log.warning("Keyboard interrupt received, forcing exit.")
        except Exception:
            log.exception("Worker crashed unexpectedly.")
            raise

        log.info("FastStream worker exited cleanly.")
