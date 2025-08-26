"""Core data models for fundamental game entities like Heroes, Items, and Cosmetics."""

from django.contrib.postgres.fields import ArrayField
from django.core.validators import MinValueValidator
from django.db import models


class Hero(models.Model):
    """Represents a single hero's definitions and attributes."""

    id = models.IntegerField(primary_key=True)
    name = models.TextField(
        help_text="The hero's internal name, e.g., 'npc_dota_hero_antimage'.",
        db_index=True,  # Add index for lookups
    )
    localized_name = models.TextField(db_index=True)  # Add index
    primary_attr = models.CharField(
        max_length=10,
        choices=[  # Add choices for validation
            ("str", "Strength"),
            ("agi", "Agility"),
            ("int", "Intelligence"),
            ("all", "Universal"),
        ],
        db_index=True,
    )
    attack_type = models.CharField(
        max_length=10,
        choices=[("Melee", "Melee"), ("Ranged", "Ranged")],
        db_index=True,
    )
    roles = ArrayField(
        models.TextField(),
        default=list,
        db_index=True,  # GIN index for array fields
    )

    class Meta:
        db_table = "heroes"
        db_table_comment = "Hero definitions and attributes"  # Django 5.2+
        verbose_name = "Hero"
        verbose_name_plural = "Heroes"
        ordering = ["localized_name"]
        indexes = [
            models.Index(fields=["primary_attr", "attack_type"]),  # Composite index
        ]

    def __str__(self) -> str:
        return self.localized_name


class Item(models.Model):
    """Represents a single in-game item's definitions and properties."""

    id = models.IntegerField(primary_key=True)
    name = models.TextField(
        help_text="The item's internal name, e.g., 'item_blink'.",
        db_index=True,
    )
    cost = models.IntegerField(
        null=True,
        blank=True,
        validators=[MinValueValidator(0)],
        db_index=True,
    )
    secret_shop = models.BooleanField(default=False, db_index=True)
    side_shop = models.BooleanField(default=False, db_index=True)
    recipe = models.BooleanField(default=False, db_index=True)
    localized_name = models.TextField(null=True, blank=True, db_index=True)

    class Meta:
        db_table = "items"
        db_table_comment = "In-game item definitions"
        verbose_name = "Item"
        verbose_name_plural = "Items"
        ordering = ["localized_name"]
        indexes = [
            models.Index(fields=["cost", "recipe"]),
            models.Index(fields=["secret_shop", "side_shop"]),
        ]

    def __str__(self) -> str:
        return self.localized_name or self.name


class Cosmetic(models.Model):
    """Represents a single cosmetic item's metadata."""

    item_id = models.IntegerField(primary_key=True)
    name = models.TextField(null=True, blank=True, db_index=True)
    prefab = models.TextField(null=True, blank=True)
    creation_date = models.DateTimeField(null=True, blank=True, db_index=True)
    image_inventory = models.TextField(null=True, blank=True)
    image_path = models.TextField(null=True, blank=True)
    item_description = models.TextField(null=True, blank=True)
    item_name = models.TextField(null=True, blank=True, db_index=True)
    item_rarity = models.TextField(null=True, blank=True, db_index=True)
    item_type_name = models.TextField(null=True, blank=True, db_index=True)
    used_by_heroes = models.TextField(null=True, blank=True)  # Consider JSONField

    class Meta:
        db_table = "cosmetics"
        db_table_comment = "Cosmetic items metadata"
        verbose_name = "Cosmetic"
        verbose_name_plural = "Cosmetics"
        indexes = [
            models.Index(fields=["item_rarity", "creation_date"]),
            models.Index(fields=["item_type_name"]),
        ]

    def __str__(self) -> str:
        return self.item_name or self.name or f"Cosmetic {self.item_id}"
