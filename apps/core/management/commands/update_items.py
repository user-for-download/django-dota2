update_items.py
# ================================================================================
import asyncio
import logging

import httpx
import orjson
from asgiref.sync import sync_to_async
from django.core.management.base import BaseCommand, CommandError
from django.db import transaction

from apps.core.models import Item

logger = logging.getLogger(__name__)

ITEMS_URL = "https://raw.githubusercontent.com/odota/dotaconstants/master/build/items.json"


class Command(BaseCommand):
    """Asynchronously fetches item data and updates the local database."""

    help = "Asynchronously fetches item data and updates the local database."

    def handle(self, *args, **options):
        """Entry point for the command."""
        self.stdout.write(self.style.SUCCESS("► Starting async fetch of item data..."))
        try:
            asyncio.run(self.process_items())
        except Exception as e:
            msg = f"An unexpected error occurred: {e}"
            raise CommandError(msg) from e

    async def process_items(self):
        """Main async processing function."""
        items_data = await self.fetch_items_data()
        if not items_data:
            self.stdout.write(self.style.WARNING("No item data was fetched. Aborting."))
            return

        items_to_upsert = self.prepare_item_instances(items_data)
        if not items_to_upsert:
            self.stdout.write(self.style.WARNING("No valid item data to process after parsing."))
            return

        self.stdout.write(f"Preparing to create or update {len(items_to_upsert)} items...")
        await self._bulk_upsert_items_async(items_to_upsert)
        self.stdout.write(self.style.SUCCESS("✓ Successfully updated items in the database."))

    async def fetch_items_data(self) -> dict | None:
        """Fetches and decodes item data using httpx and orjson."""
        self.stdout.write(f"Fetching data from {ITEMS_URL}...")
        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(ITEMS_URL, timeout=15)
                response.raise_for_status()
                data = orjson.loads(response.content)
            if not isinstance(data, dict):
                msg = "Fetched data is not in the expected dictionary format."
                raise TypeError(msg)
            self.stdout.write(self.style.SUCCESS("Successfully fetched item data."))
            return data
        except (httpx.HTTPError, orjson.JSONDecodeError, TypeError) as e:
            self.stderr.write(self.style.ERROR(f"Failed to fetch or parse item data: {e}"))
            return None

    def prepare_item_instances(self, items_data: dict) -> list[Item]:
        """Converts raw data dict into a list of Item model instances."""
        items = []
        for key, data in items_data.items():
            if not isinstance(data, dict) or "id" not in data:
                continue
            try:
                items.append(
                    Item(
                        id=data["id"],
                        name=key,
                        localized_name=data.get("dname"),
                        cost=data.get("cost"),
                        secret_shop=bool(data.get("secret_shop")),
                        side_shop=bool(data.get("side_shop")),
                        recipe=bool(data.get("recipe")),
                    ),
                )
            except KeyError as e:
                self.stderr.write(self.style.WARNING(f"Skipping item '{key}' due to missing key: {e}"))
        return items

    @sync_to_async
    def _bulk_upsert_items_async(self, items: list[Item]):
        """Wraps the synchronous bulk_create in an async-callable function."""
        with transaction.atomic():
            Item.objects.bulk_create(
                items,
                update_conflicts=True,
                unique_fields=["id"],
                update_fields=["name", "localized_name", "cost", "secret_shop", "side_shop", "recipe"],
            )
