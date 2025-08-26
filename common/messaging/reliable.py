# common/messaging/reliable.py
# ============================================================================

"""
Framework-agnostic publisher with full-jitter exponential-backoff retries.

Designed for FastStream's RedisBroker but works with any object exposing
`await broker.publish(message, queue=…)`.
"""

from __future__ import annotations

import asyncio
import random
from dataclasses import dataclass
from typing import TypeVar

import structlog
from pydantic import BaseModel

log = structlog.get_logger(__name__).bind(comp="ReliablePublisher")

# TYPE PARAMS -----------------------------------------------------------------
T = TypeVar("T", bound=BaseModel)


# EXCEPTIONS ------------------------------------------------------------------
class BrokerPublishError(RuntimeError):
    """Raised when publishing fails after all retry attempts."""


# CONFIG ----------------------------------------------------------------------
@dataclass(slots=True, frozen=True)
class RetryConfig:
    max_retries: int = 3
    initial_delay_s: float = 0.5
    backoff_factor: float = 2.0
    max_backoff_s: float = 10.0


# PUBLISHER -------------------------------------------------------------------
class ReliableBrokerPublisher[T: BaseModel]:
    """
    Wrap a broker instance to make its `publish` method fault-tolerant.

    Example
    -------
        broker = RedisBroker()
        reliable = ReliableBrokerPublisher(broker)
        await reliable.publish(MyPayload(...), queue="my-q")
    """

    def __init__(self, broker, **retry_kw) -> None:  # broker is duck-typed
        self._broker = broker
        self._cfg = RetryConfig(**retry_kw)

    # ------------------------------------------------------------------ utils
    @staticmethod
    def _random_delay(base: float) -> float:
        """Full-jitter sleep: U(0, base)."""
        return random.uniform(0.0, base)

    # ------------------------------------------------------------------ api
    async def publish(
        self,
        message: T,
        *,
        queue: str,
        **broker_kw,
    ) -> None:
        """
        Publish *message* (Pydantic model) to *queue* using the underlying broker.

        Parameters
        ----------
        message : BaseModel
            The payload to publish.
        queue : str
            Target queue / channel.
        **broker_kw
            Extra kwargs forwarded to `broker.publish`.
        """
        delay = self._cfg.initial_delay_s
        last_exc: Exception | None = None

        for attempt in range(self._cfg.max_retries + 1):
            try:
                # Let the publish call fail if broker is down, then retry.
                await self._broker.publish(message=message, channel=queue, **broker_kw)

                if attempt:  # was a retry
                    log.info("publish succeeded after retries", queue=queue, retries=attempt)
                return

            except asyncio.CancelledError:
                log.warning("publish cancelled by caller", queue=queue)
                raise

            except Exception as exc:  # noqa: BLE001
                last_exc = exc
                if attempt >= self._cfg.max_retries:
                    break

                sleep = self._random_delay(delay)
                log.warning(
                    "publish failed – retry scheduled",
                    queue=queue,
                    err=str(exc),
                    attempt=attempt + 1,
                    next_delay_s=round(sleep, 2),
                )
                await asyncio.sleep(sleep)
                delay = min(delay * self._cfg.backoff_factor, self._cfg.max_backoff_s)

        # ---------------- after loop -> permanent failure ----------------
        log.error(
            "publish failed after max retries",
            queue=queue,
            retries=self._cfg.max_retries,
            exc_info=last_exc,
        )
        msg = f"Failed to publish to '{queue}' after {self._cfg.max_retries} retries."
        raise BrokerPublishError(msg) from last_exc
