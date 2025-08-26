"""
High-performance data parser optimized for OpenDota API responses.
"""

from __future__ import annotations

import logging
import re
from collections import Counter
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Final

log = logging.getLogger(__name__)

__all__ = [
    "DataParser",
    "DataType",
    "ParseError",
    "ParseResult",
    "ParsedItem",
    "parse_match_ids_from_rows",
]


class ParseError(RuntimeError):
    """Raised when a single row cannot be parsed."""


class DataType(str, Enum):
    TEAMS = "teams"
    PLAYERS = "players"
    LEAGUES = "leagues"
    UNKNOWN = "unknown"


# Optimized field mappings
ID_FIELD_MAP: Final[Mapping[DataType, tuple[str, ...]]] = {
    DataType.TEAMS: ("team_id", "id"),
    DataType.PLAYERS: ("account_id", "id"),
    DataType.LEAGUES: ("leagueid", "league_id", "id"),
    DataType.UNKNOWN: ("id",),
}

NAME_FIELDS_MAP: Final[Mapping[DataType, tuple[str, ...]]] = {
    DataType.TEAMS: ("name", "tag", "team_name"),
    DataType.PLAYERS: ("personaname", "pro_name", "name"),
    DataType.LEAGUES: ("name", "league_name"),
    DataType.UNKNOWN: ("name", "personaname", "tag"),
}

PREFIX_MAP: Final[Mapping[DataType, str]] = {
    DataType.TEAMS: "Team",
    DataType.PLAYERS: "Player",
    DataType.LEAGUES: "League",
    DataType.UNKNOWN: "Item",
}

# Compiled regex for better performance
_MATCH_RE: Final[re.Pattern[str]] = re.compile(r"\b\d{9,12}\b")  # Match realistic match IDs
_COMMA_SPLIT_RE: Final[re.Pattern[str]] = re.compile(r"[,\s]+")


def parse_match_ids_from_rows(rows: Iterable[dict[str, Any]]) -> set[int]:
    """
    Parses an iterable of data rows to extract a unique set of all match IDs.

    This utility is robust and can find match IDs from various common keys and formats:
    - A key named 'match_id' or 'id' containing an integer.
    - A key named 'match_ids' containing a comma-separated string of IDs.
    - A key named 'match_ids' containing a list of integers.

    Args:
        rows: An iterable of dictionaries, where each dict is a data row.

    Returns:
        A set of unique integer match IDs found across all rows.
    """
    unique_ids: set[int] = set()

    # Common keys where a single match ID might be found.
    single_id_keys = ("match_id", "id")
    # Common keys where multiple match IDs might be found.
    multiple_id_keys = ("match_ids", "matches")

    for row in rows:
        if not isinstance(row, dict):
            continue

        # 1. Check for single match ID fields.
        for key in single_id_keys:
            if (match_id := row.get(key)) and isinstance(match_id, int):
                unique_ids.add(match_id)
                break  # Found it, no need to check other single_id_keys for this row

        # 2. Check for multiple match ID fields.
        for key in multiple_id_keys:
            if value := row.get(key):
                if isinstance(value, list):
                    for item in value:
                        if isinstance(item, int):
                            unique_ids.add(item)
                elif isinstance(value, str):
                    for part in value.split(","):
                        cleaned_id = part.strip()
                        if cleaned_id.isdigit():
                            unique_ids.add(int(cleaned_id))
                break

    return unique_ids


@dataclass
class ParsedItem:
    item_id: int | str
    name: str
    match_ids: list[int]
    data_type: DataType
    raw_data: Mapping[str, Any] = field(repr=False, compare=False)

    # Additional metadata for better processing
    priority: int = field(default=0, compare=False)
    last_match_time: int | None = field(default=None, compare=False)


@dataclass
class ParseResult:
    data_type: DataType
    unique_match_ids: list[int]
    total_items: int
    valid_items: int
    skipped_items: int
    total_matches: int
    parsed_items: list[ParsedItem]
    statistics: Mapping[str, Any]

    # Performance metrics
    parse_time_ms: float = 0.0
    duplicate_matches_removed: int = 0


class DataParser:
    """
    High-performance parser for OpenDota API responses.
    Optimized for bulk data processing with minimal memory usage.
    """

    def __init__(self, *, strict_validation: bool = False, min_matches: int = 1):
        self.strict_validation = strict_validation
        self.min_matches = min_matches
        self._stats_cache: dict[str, Any] = {}

    def parse_data(
        self,
        payload: Sequence[Mapping[str, Any]] | Mapping[str, Any] | None,
    ) -> ParseResult:
        """
        Parse OpenDota API response with optimized performance.
        """
        import time

        start_time = time.perf_counter()

        if not payload:
            return self._empty_result()

        rows = self._extract_rows(payload)
        if not rows:
            return self._empty_result()

        data_type = self._detect_type(rows)
        parsed_items, stats = self._parse_rows_optimized(rows, data_type)
        unique_matches = self._extract_unique_matches(parsed_items)

        parse_time = (time.perf_counter() - start_time) * 1000

        return ParseResult(
            data_type=data_type,
            unique_match_ids=unique_matches,
            total_items=len(rows),
            valid_items=len(parsed_items),
            skipped_items=len(rows) - len(parsed_items),
            total_matches=sum(len(item.match_ids) for item in parsed_items),
            parsed_items=parsed_items,
            statistics=stats,
            parse_time_ms=round(parse_time, 2),
            duplicate_matches_removed=stats.get("duplicates_removed", 0),
        )

    def _extract_rows(self, payload: Any) -> list[Mapping[str, Any]]:
        """Extract rows from various payload formats."""
        if isinstance(payload, Mapping):
            if "rows" in payload:
                return list(payload["rows"])
            if "data" in payload:
                return list(payload["data"])
            return [payload]
        if isinstance(payload, Sequence):
            return list(payload)
        return []

    def _parse_rows_optimized(
        self,
        rows: list[Mapping[str, Any]],
        data_type: DataType,
    ) -> tuple[list[ParsedItem], dict[str, Any]]:
        """
        Optimized batch parsing with detailed statistics.
        """
        parsed_items: list[ParsedItem] = []
        skipped_reasons: Counter = Counter()
        match_count_distribution: Counter = Counter()

        for idx, row in enumerate(rows):
            try:
                item = self._parse_row_fast(row, data_type)
                if len(item.match_ids) >= self.min_matches:
                    parsed_items.append(item)
                    match_count_distribution[len(item.match_ids)] += 1
                else:
                    skipped_reasons["insufficient_matches"] += 1

            except ParseError as exc:
                skipped_reasons[str(exc)] += 1
                if not self.strict_validation and idx < 5:  # Log first few errors
                    log.debug("Parse error at row %d: %s", idx, exc)

        # Generate statistics
        stats = self._generate_statistics(parsed_items, skipped_reasons, match_count_distribution)

        return parsed_items, stats

    def _parse_row_fast(self, row: Mapping[str, Any], data_type: DataType) -> ParsedItem:
        """
        Fast row parsing with minimal validation.
        """
        # Extract ID using multiple possible field names
        item_id = None
        for id_field in ID_FIELD_MAP[data_type]:
            item_id = row.get(id_field)
            if item_id is not None:
                break

        if item_id is None:
            msg = "missing_id"
            raise ParseError(msg)

        # Extract matches
        match_ids = self._extract_match_ids_fast(row.get("match_ids"))
        if not match_ids:
            msg = "no_matches"
            raise ParseError(msg)

        # Extract name
        name = self._extract_name_fast(row, data_type, item_id)

        # Extract optional metadata
        last_match_time = row.get("last_match_time")
        priority = len(match_ids)  # Use match count as priority

        return ParsedItem(
            item_id=item_id,
            name=name,
            match_ids=match_ids,
            data_type=data_type,
            raw_data=row,
            priority=priority,
            last_match_time=last_match_time,
        )

    def _extract_match_ids_fast(self, source: Any) -> list[int]:
        """
        Ultra-fast match ID extraction with various input formats.
        """
        if source is None:
            return []

        # Handle single integer
        if isinstance(source, int):
            return [source] if source > 0 else []

        # Handle string (CSV or single)
        if isinstance(source, str):
            if not source.strip():
                return []

            # Try simple comma split first (faster)
            if "," in source:
                try:
                    return [int(x.strip()) for x in source.split(",") if x.strip().isdigit()]
                except ValueError:
                    pass

            # Fallback to regex
            matches = _MATCH_RE.findall(source)
            return [int(m) for m in matches]

        # Handle list/iterable
        if isinstance(source, Iterable):
            result: list[int] = []
            for item in source:
                if isinstance(item, int) and item > 0:
                    result.append(item)
                elif isinstance(item, str) and item.isdigit():
                    result.append(int(item))
            return result

        return []

    def _extract_name_fast(self, row: Mapping[str, Any], data_type: DataType, item_id: Any) -> str:
        """Fast name extraction with fallback."""
        for field in NAME_FIELDS_MAP[data_type]:
            value = row.get(field)
            if value and str(value).strip():
                return str(value).strip()

        return f"{PREFIX_MAP[data_type]}_{item_id}"

    def _detect_type(self, rows: Sequence[Mapping[str, Any]]) -> DataType:
        """
        Optimized type detection using field presence scoring.
        """
        if not rows:
            return DataType.UNKNOWN

        # Sample first few rows for type detection
        sample_size = min(5, len(rows))
        field_scores = {
            DataType.TEAMS: 0,
            DataType.PLAYERS: 0,
            DataType.LEAGUES: 0,
        }

        for row in rows[:sample_size]:
            if any(field in row for field in ("team_id", "tag")):
                field_scores[DataType.TEAMS] += 2
            if "name" in row and "tag" in row:
                field_scores[DataType.TEAMS] += 1

            if any(field in row for field in ("account_id", "personaname", "pro_name")):
                field_scores[DataType.PLAYERS] += 2
            if "steamid" in row or "profile" in row:
                field_scores[DataType.PLAYERS] += 1

            if any(field in row for field in ("leagueid", "league_id")):
                field_scores[DataType.LEAGUES] += 2
            if "tier" in row or "tournament" in row:
                field_scores[DataType.LEAGUES] += 1

        # Return type with highest score
        best_type = max(field_scores, key=field_scores.get)
        return best_type if field_scores[best_type] > 0 else DataType.UNKNOWN

    def _extract_unique_matches(self, items: list[ParsedItem]) -> list[int]:
        """
        Extract unique match IDs efficiently using set operations.
        """
        if not items:
            return []

        # Use set for deduplication, then sort
        unique_matches: set[int] = set()
        for item in items:
            unique_matches.update(item.match_ids)

        return sorted(unique_matches)

    def _generate_statistics(
        self,
        items: list[ParsedItem],
        skipped_reasons: Counter,
        match_distribution: Counter,
    ) -> dict[str, Any]:
        """Generate comprehensive parsing statistics."""

        if not items:
            return {"error": "no_valid_items"}

        match_counts = [len(item.match_ids) for item in items]

        # Top items by match count
        top_items = sorted(items, key=lambda x: len(x.match_ids), reverse=True)[:10]

        return {
            "matches": {
                "total": sum(match_counts),
                "avg_per_item": round(sum(match_counts) / len(match_counts), 2),
                "min": min(match_counts),
                "max": max(match_counts),
                "distribution": dict(match_distribution.most_common(20)),
            },
            "items": {
                "valid": len(items),
                "top_contributors": [
                    {
                        "id": item.item_id,
                        "name": item.name[:50],  # Truncate long names
                        "matches": len(item.match_ids),
                        "priority": item.priority,
                    }
                    for item in top_items
                ],
            },
            "parsing": {
                "skipped_reasons": dict(skipped_reasons),
                "success_rate": round(len(items) / (len(items) + sum(skipped_reasons.values())) * 100, 2),
            },
        }

    def _empty_result(self) -> ParseResult:
        """Return empty result structure."""
        return ParseResult(
            data_type=DataType.UNKNOWN,
            unique_match_ids=[],
            total_items=0,
            valid_items=0,
            skipped_items=0,
            total_matches=0,
            parsed_items=[],
            statistics={},
        )
