from datetime import UTC, datetime


def to_unix_timestamp_safe(value: str | datetime | float | None) -> int | None:
    if value is None:
        return None
    if isinstance(value, int | float):
        return int(value)
    if isinstance(value, datetime):
        return int(value.timestamp())
    if isinstance(value, str):
        try:
            if value.endswith("Z"):
                value = value.replace("Z", "+00:00")
            dt = datetime.fromisoformat(value)
            return int(dt.timestamp())
        except ValueError:
            return None
    return None


def to_datetime_aware_safe(value: str | datetime | float | None) -> datetime | None:
    ts = to_unix_timestamp_safe(value)
    if ts is None:
        return None
    return datetime.fromtimestamp(ts, tz=UTC)
