# /home/ubuntu/dota/apps/players/management/commands/fetch_players.py
# ================================================================================
"""
Django management command to fetch and store professional player data.
"""

from __future__ import annotations

import asyncio
import json
from typing import TYPE_CHECKING, Any

import structlog
from django.core.management.base import BaseCommand, CommandError, CommandParser

from apps.core.services.fetcher_service import PlayerFetcherService
from apps.players.conf import (
    FETCH_PLAYERS_CMD_DEFAULT_LIMIT,
    FETCH_PLAYERS_CMD_DEFAULT_MAX_PARALLEL,
    PlayerFetcherConfig,
)

if TYPE_CHECKING:
    from collections.abc import Mapping

log = structlog.get_logger(__name__)


class Command(BaseCommand):
    """Fetches and upserts player data from the primary API source asynchronously."""

    help = "Asynchronously fetches and upserts player data via the FetcherService."

    def add_arguments(self, parser: CommandParser) -> None:
        """Defines the command-line arguments that configure the fetch process."""
        parser.add_argument(
            "--limit",
            type=int,
            default=FETCH_PLAYERS_CMD_DEFAULT_LIMIT,
            help="Maximum number of players to fetch.",
        )
        parser.add_argument("--force", action="store_true", help="Force a refresh, ignoring any fresh cached data.")
        parser.add_argument(
            "--max-parallel",
            type=int,
            default=FETCH_PLAYERS_CMD_DEFAULT_MAX_PARALLEL,
            dest="max_parallel_chunks",
            help="Number of parallel chunks for DB upserts.",
        )
        parser.add_argument("--skip-matches", action="store_true", help="Skip publishing associated match IDs.")
        parser.add_argument("--json", action="store_true", help="Output the result as raw JSON.")

    def handle(self, *args: Any, **options: Any) -> None:
        """Sync entry point that orchestrates the async execution."""
        try:
            asyncio.run(self._handle_async(**options))
        except KeyboardInterrupt:
            self.stderr.write(self.style.WARNING("\nOperation cancelled by user."))
        except CommandError:
            raise
        except Exception as e:
            log.exception("fetch_players command failed unexpectedly.", exc_info=e)
            msg = f"Command failed with an unhandled exception: {e}"
            raise CommandError(msg) from e

    async def _handle_async(self, **options: Any) -> None:
        """The core asynchronous logic of the command."""
        self.stdout.write(self.style.SUCCESS("► Starting player fetch process..."))
        try:
            config = PlayerFetcherConfig(
                limit=options["limit"],
                force=options["force"],
                skip_matches=options["skip_matches"],
                max_parallel_chunks=options["max_parallel_chunks"],
            )
            config.check()
        except ValueError as e:
            msg = f"Invalid configuration: {e}"
            raise CommandError(msg)

        service = PlayerFetcherService()
        result = await service.fetch_and_cache(config, force_refresh=options["force"])

        if options["json"]:
            self.stdout.write(json.dumps(result, indent=2, default=str))
        else:
            self._pretty_print(result)

        if result.get("status") == "error":
            self.stderr.write(self.style.ERROR("✗ Player fetch finished with an error."))
        else:
            self.stdout.write(self.style.SUCCESS("✓ Player fetch completed successfully."))

    def _pretty_print(self, result: Mapping[str, Any]) -> None:
        """Formats and prints a user-friendly summary of the fetch result."""
        style = self.style
        self.stdout.write(style.MIGRATE_HEADING("\n" + "=" * 26 + "\n  FETCH SUMMARY\n" + "=" * 26))
        if "error" in result:
            self.stdout.write(f"  Status  : {style.ERROR('Failed')}")
            self.stdout.write(f"  Error   : {result.get('error', 'Unknown error')}")
        else:
            self.stdout.write(f"  Source  : {result.get('source', 'n/a')}")
            self.stdout.write(f"  Created : {style.SUCCESS(result.get('created', 0))}")
            self.stdout.write(f"  Updated : {style.SUCCESS(result.get('updated', 0))}")
            self.stdout.write(f"  Skipped : {style.WARNING(result.get('skipped', 0))}")
        self.stdout.write(style.MIGRATE_HEADING("=" * 26))
