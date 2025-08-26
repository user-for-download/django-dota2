# infrastructure/queues.py
from enum import Enum, auto
from typing import Final


class StrAutoEnum(str, Enum):
    """Enum whose `auto()` values are *str* equal to the lowercase name."""

    def _generate_next_value_(self, start, count, last_values):
        return self.lower()


class Queue(StrAutoEnum):
    PROCESS_MATCH_BATCH = auto()
    PROCESS_TEAM_DATA = auto()
    PROCESS_LEAGUE_DATA = auto()
    PROCESS_PLAYER_DATA = auto()


QUEUES: Final = Queue
