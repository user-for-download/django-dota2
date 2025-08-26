"""
utils/iterables.py
==================

Handy helpers for working with in-memory iterables.  The functions
remain dependency-free and type-annotated, so they can be re-used
across apps without pulling in heavy third-party libs.

Exported symbols
────────────────
• chunked(iterable, size) → Iterator[list[T]]
• flatten(nested_iterable) → list[Any]      (alias: _flatten for b/c)

Both functions are deliberately *lazy* (generators) so that very
large iterables do not allocate memory up-front.
"""

from __future__ import annotations

import itertools
from typing import TYPE_CHECKING, TypeVar

if TYPE_CHECKING:
    from collections.abc import Iterable, Iterator, Sequence

T = TypeVar("T")


# ────────────────────────────────────────────────────────────────────────────
# HELPER UTILITIES
# ────────────────────────────────────────────────────────────────────────────
def chunked[T](iterable: Iterable[T], size: int) -> Iterator[list[T]]:
    if size <= 0:
        msg = "size must be a positive integer"
        raise ValueError(msg)
    it = iter(iterable)
    while chunk := list(itertools.islice(it, size)):
        yield chunk


# ──────────────────────────────────────────────
#  flatten
# ──────────────────────────────────────────────
def flatten[T](nested: Iterable[Sequence[T]]) -> list[T]:
    """
    Fully realise a nested iterable into a single flat list.
    Primarily used for logging / broker payloads where we *do*
    need the materialised list.

    >>> flatten([[1, 2], [3], []])
    [1, 2, 3]
    """
    return list(itertools.chain.from_iterable(nested))
