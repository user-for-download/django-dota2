# /home/ubuntu/dota/apps/core/services/protocols.py
# ================================================================================
# /home/ubuntu/dota/apps/core/services/protocols.py
"""
Defines the structural contracts (Protocols) for services in the core app.

Using protocols allows for flexible, decoupled architectures where concrete
implementations can be swapped without changing the calling code, as long as they
adhere to the defined "shape".
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Protocol, Self

if TYPE_CHECKING:
    from types import TracebackType

    import httpx

    from ..conf import BaseFetcherConfig
    from ..datatype import UpsertResult


class AsyncFetcherProtocol(Protocol):
    """
    Defines the contract for all asynchronous fetcher classes.

    Any class that implements these methods can be used by `FetcherService`.
    This enforces a consistent interface for fetching, resource management
    (via async context manager), and health reporting.
    """

    def __init__(
        self,
        cfg: BaseFetcherConfig | None = None,
        *,
        handler: Any | None = None,
        session: httpx.AsyncClient | None = None,
    ) -> None:
        """Initializes the fetcher with optional config and dependencies."""
        ...

    async def __aenter__(self) -> Self:
        """Enters the asynchronous context, for setting up resources like HTTP sessions."""
        ...

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        """Exits the context, for cleaning up resources."""
        ...

    async def run(self) -> UpsertResult:
        """
        The main execution method.

        Orchestrates the entire fetching and persistence process for a specific
        domain (e.g., teams) and returns a summary of the database operations.
        """
        ...

    async def get_metrics(self) -> dict[str, Any]:
        """
        Provides lightweight operational metrics.

        Typically used for health checks to report statistics like the total
        number of records in the database without performing a full fetch.
        """
        ...
