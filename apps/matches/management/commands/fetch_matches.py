# /home/ubuntu/dota/apps/core/management/commands/process_matches.py
# ================================================================================
import asyncio
import logging

import httpx
import orjson
from django.core.management.base import BaseCommand, CommandError

from apps.matches.models import Match
from common.messaging.batching import schedule_matches_for_processing

logger = logging.getLogger(__name__)

MATCH_IDS_URL = "https://api.opendota.com/api/explorer?sql=SELECT%20STRING_AGG(m.match_id%3A%3ATEXT%2C%20%27%2C%27%20ORDER%20BY%20m.start_time%20DESC)%20AS%20match_ids%20FROM%20matches%20m%20WHERE%20m.start_time%20%3E%3D%20EXTRACT(EPOCH%20FROM%20TIMESTAMP%20%272025-05-22T23%3A36%3A01.602Z%27)%3B%20"


class Command(BaseCommand):
    """
    Asynchronously fetches recent professional match IDs and schedules them for processing.
    """

    help = "Asynchronously fetches recent match IDs and schedules them for processing."

    def add_arguments(self, parser):
        """Adds command-line arguments."""
        parser.add_argument(
            "--match_ids",
            type=str,
            help="A comma-separated string of match IDs to process instead of fetching from the URL.",
        )
        parser.add_argument(
            "--force",
            action="store_true",
            help="Force reprocessing of matches that have already been parsed.",
        )

    def handle(self, *args, **options):
        """Entry point for the command."""
        self.stdout.write(self.style.SUCCESS("► Starting async fetch of match IDs..."))
        try:
            asyncio.run(self.process_matches(options))
        except Exception as e:
            msg = f"An unexpected error occurred: {e}"
            raise CommandError(msg) from e

    async def process_matches(self, options: dict):
        """The core asynchronous logic of the command."""
        match_ids_str = options.get("match_ids")
        force_processing = options.get("force", False)

        all_ids: set[int] = set()
        ids_to_process: set[int] = set()

        if match_ids_str:
            self.stdout.write("Processing match IDs provided via command-line argument.")
            parsed_ids = self.parse_string_to_int_list(match_ids_str)
            if parsed_ids:
                all_ids.update(parsed_ids)
        else:
            self.stdout.write("No match IDs provided, fetching from remote URL...")
            fetched_ids = await self.fetch_and_parse_match_ids()
            if fetched_ids:
                all_ids.update(fetched_ids)

        if not all_ids:
            self.stdout.write(self.style.WARNING("No match IDs found to process. Aborting."))
            return

        if force_processing:
            self.stdout.write(
                self.style.WARNING("Force flag is active. Skipping database check and scheduling all matches.")
            )
        else:
            self.stdout.write("Checking for existing match IDs in the database...")
            existing_ids_qs = Match.objects.filter(match_id__in=all_ids).values_list(
                "match_id", flat=True
            )

            # Asynchronously fetch the IDs into a set for efficient comparison.
            existing_ids = {pk async for pk in existing_ids_qs}

            if existing_ids:
                self.stdout.write(
                    f"Found {len(existing_ids)} existing matches. They will be skipped."
                )
            ids_to_process = all_ids - existing_ids

            if not ids_to_process:
                self.stdout.write(self.style.SUCCESS("No new match IDs found to process. Aborting."))
                return

            self.stdout.write(
                f"Preparing to schedule {len(ids_to_process)} new matches for processing..."
            )

        result = await schedule_matches_for_processing(
            list(ids_to_process), force=force_processing
        )

        result_json = orjson.dumps(result, option=orjson.OPT_INDENT_2).decode()
        self.stdout.write(self.style.SUCCESS("✓ Command finished. Result:"))
        self.stdout.write(result_json)

    async def fetch_and_parse_match_ids(self) -> list[int] | None:
        """Fetches data from the URL and parses the match IDs from it."""
        raw_data = await self.fetch_remote_data()
        if not raw_data:
            self.stderr.write(self.style.ERROR("Failed to fetch data, cannot parse IDs."))
            return None

        return self.parse_ids_from_response(raw_data)

    async def fetch_remote_data(self) -> dict | None:
        """Fetches and decodes the match ID data using aiohttp and orjson."""
        self.stdout.write(f"Fetching data from {MATCH_IDS_URL}...")
        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(MATCH_IDS_URL, timeout=15)
                response.raise_for_status()
                data = orjson.loads(response.content)
            if not isinstance(data, dict):
                msg = "Fetched data is not in the expected dictionary format."
                raise TypeError(msg)
            self.stdout.write(self.style.SUCCESS("Successfully fetched hero data."))
            return data
        except (httpx.HTTPError, orjson.JSONDecodeError, TypeError) as e:
            self.stderr.write(self.style.ERROR(f"Failed to fetch or parse hero data: {e}"))
            return None

    def parse_ids_from_response(self, data: dict) -> list[int] | None:
        """Extracts and parses match IDs from the raw JSON response."""
        try:
            # Safely access nested data
            rows = data.get("rows")
            if not rows or not isinstance(rows, list) or len(rows) == 0:
                self.stderr.write(self.style.WARNING("JSON response is missing 'rows' or it is empty."))
                return None

            match_ids_str = rows[0].get("match_ids")
            if not match_ids_str or not isinstance(match_ids_str, str):
                self.stderr.write(self.style.WARNING("First row in JSON is missing 'match_ids' string."))
                return None

            return self.parse_string_to_int_list(match_ids_str)
        except (KeyError, IndexError, AttributeError) as e:
            self.stderr.write(self.style.ERROR(f"Error parsing JSON structure: {e}"))
            return None

    def parse_string_to_int_list(self, id_string: str) -> list[int]:
        """Converts a comma-separated string of numbers into a list of integers."""
        parsed_ids = []
        for item in id_string.split(","):
            item = item.strip()
            if item.isdigit():
                parsed_ids.append(int(item))
            else:
                self.stderr.write(self.style.WARNING(f"Skipping non-integer value '{item}'."))
        return parsed_ids
