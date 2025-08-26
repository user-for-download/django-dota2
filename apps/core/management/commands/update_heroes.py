# /home/ubuntu/dota/apps/core/management/commands/update_heroes.py
# ================================================================================
import asyncio
import logging

import httpx
import orjson
from asgiref.sync import sync_to_async
from django.core.management.base import BaseCommand, CommandError
from django.db import transaction

from apps.core.models import Hero

logger = logging.getLogger(__name__)

HEROES_URL = "https://raw.githubusercontent.com/odota/dotaconstants/master/build/heroes.json"


class Command(BaseCommand):
    """Asynchronously fetches hero data and updates the local database."""

    help = "Asynchronously fetches hero data and updates the local database."

    def handle(self, *args, **options):
        """Entry point for the command."""
        self.stdout.write(self.style.SUCCESS("► Starting async fetch of hero data..."))
        try:
            asyncio.run(self.process_heroes())
        except Exception as e:
            msg = f"An unexpected error occurred: {e}"
            raise CommandError(msg) from e

    async def process_heroes(self):
        """Main async processing function."""
        heroes_data = await self.fetch_heroes_data()
        if not heroes_data:
            self.stdout.write(self.style.WARNING("No hero data was fetched. Aborting."))
            return

        heroes_to_upsert = self.prepare_hero_instances(heroes_data)
        if not heroes_to_upsert:
            self.stdout.write(self.style.WARNING("No valid hero data to process after parsing."))
            return

        self.stdout.write(f"Preparing to create or update {len(heroes_to_upsert)} heroes...")
        await self._bulk_upsert_heroes_async(heroes_to_upsert)
        self.stdout.write(self.style.SUCCESS("✓ Successfully updated heroes in the database."))

    async def fetch_heroes_data(self) -> dict | None:
        """Fetches and decodes hero data using httpx and orjson."""
        self.stdout.write(f"Fetching data from {HEROES_URL}...")
        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(HEROES_URL, timeout=15)
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

    def prepare_hero_instances(self, heroes_data: dict) -> list[Hero]:
        """Converts raw data dict into a list of Hero model instances."""
        heroes = []
        for data in heroes_data.values():
            if not isinstance(data, dict) or "id" not in data:
                continue
            try:
                heroes.append(
                    Hero(
                        id=data["id"],
                        name=data["name"],
                        localized_name=data["localized_name"],
                        primary_attr=data["primary_attr"],
                        attack_type=data["attack_type"],
                        roles=data.get("roles", []),
                    ),
                )
            except KeyError as e:
                self.stderr.write(self.style.WARNING(f"Skipping hero due to missing key: {e}"))
        return heroes

    @sync_to_async
    def _bulk_upsert_heroes_async(self, heroes: list[Hero]):
        """Wraps the synchronous bulk_create in an async-callable function."""
        with transaction.atomic():
            Hero.objects.bulk_create(
                heroes,
                update_conflicts=True,
                unique_fields=["id"],
                update_fields=["name", "localized_name", "primary_attr", "attack_type", "roles"],
            )
