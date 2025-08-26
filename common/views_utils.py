# common/views_utils.py
# ======================================================================
from __future__ import annotations

import asyncio
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import (
    TYPE_CHECKING,
    Any,
    TypeVar,
    cast,
)

# Self back-port for < 3.11
try:
    from typing import Self
except ImportError:  # pragma: no cover
    from typing import Self  # type: ignore

import orjson
import structlog
from django.http import (
    Http404,
    HttpRequest,
    HttpResponse,
)
from django.views import View

from .cache_utils import (
    adelete,
    aset_json,
)
from .cache_utils import (
    aget_or_set as _aget_or_set,
)
from .cache_utils import (
    build_cache_key as _build_cache_key,
)

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

T = TypeVar("T")

log = structlog.get_logger(__name__).bind(component="ViewsUtils")


# ------------------------------------------------------------------ orjson helpers
def _orjson_default(obj: Any) -> Any:
    """
    Custom serializer for types orjson doesn't handle.

    If the object implements `to_json()`, that is used; otherwise we raise
    TypeError so orjson can propagate an informative message.
    """
    if hasattr(obj, "to_json"):
        return obj.to_json()
    msg = f"{type(obj).__name__} is not JSON serialisable"
    raise TypeError(msg)


class OrjsonResponse(HttpResponse):
    """
    A high-performance JSON response using `orjson`.

    Data are encoded as UTF-8 bytes; `content_type` is set to
    `application/json` automatically.
    """

    def __init__(self, data: Any, *, status: int = 200, **kw: Any) -> None:
        opts = orjson.OPT_NAIVE_UTC
        content = orjson.dumps(data, default=_orjson_default, option=opts)
        kw.setdefault("content_type", "application/json")
        super().__init__(content=content, status=status, **kw)


# ------------------------------------------------------------------ pagination
@dataclass(slots=True, frozen=True)
class Page:
    """
    Simple value-object for pagination.

    Attributes
    ----------
    number : 1-based page index
    size   : page size in rows
    offset : SQL offset, computed automatically
    """

    number: int
    size: int
    offset: int = field(init=False)

    def __post_init__(self) -> None:
        object.__setattr__(self, "offset", (self.number - 1) * self.size)

    # factory --------------------------------------------------------
    @classmethod
    def from_request(
        cls,
        req: HttpRequest,
        /,
        *,
        max_size: int = 100,
        default_size: int = 50,
    ) -> Self:
        """
        Parse `page` and `page_size` query params into a Page instance,
        applying sane defaults & bounds.
        """
        try:
            page_num = int(req.GET.get("page", 1))
        except (TypeError, ValueError):
            page_num = 1
        page_num = max(page_num, 1)

        try:
            raw_size = int(req.GET.get("page_size", default_size))
        except (TypeError, ValueError):
            raw_size = default_size
        page_size = max(1, min(raw_size, max_size))

        return cls(page_num, page_size)


# ------------------------------------------------------------------ BaseAsyncView
class BaseAsyncView(View):
    """
    Base-class for *async* Django CBVs with

        • Centralised error handling
        • orjson responses
        • First-class async caching helpers
        • `nocache=true`  → bypass cache
        • `reset=true`    → refresh cache
        • `X-Cache-Status` header (HIT | MISS | REFRESH | BYPASS)
    """

    META_CACHE_PARAMS: set[str] = {"reset", "nocache"}

    # ───────────────────────────── dispatch ──────────────────────────
    async def dispatch(self, request: HttpRequest, *args: Any, **kw: Any):  # type: ignore[override]
        self.request = request  # Make request available to all methods

        handler = getattr(self, request.method.lower(), None)
        if handler is None:
            return await self.http_method_not_allowed(request, *args, **kw)

        try:
            response = await handler(request, *args, **kw)
        except Http404 as exc:
            log.info("Resource not found", path=request.path, err=str(exc))
            response = OrjsonResponse({"detail": str(exc) or "Not found."}, status=404)
        except Exception as exc:
            log.exception("Unhandled API error", path=request.path, exc_info=exc)
            response = OrjsonResponse({"detail": "An internal server error occurred."}, status=500)

        # inject cache status header if set by get_cached_data()
        cache_status = getattr(request, "_cache_status", None)
        if cache_status:
            response["X-Cache-Status"] = cache_status
        return response

    async def http_method_not_allowed(self, request: HttpRequest, *a: Any, **k: Any) -> HttpResponse:
        log.warning("Method Not Allowed", method=request.method, path=request.path)
        return OrjsonResponse({"detail": f'Method "{request.method}" not allowed.'}, status=405)

    # ─────────────────────── request-parsing helpers ─────────────────
    @staticmethod
    def get_page(request: HttpRequest) -> Page:
        return Page.from_request(request)

    @staticmethod
    def get_bool_param(request: HttpRequest, key: str, *, default: bool = False) -> bool:
        val = request.GET.get(key)
        if val is None:
            return default
        return val.lower() in {"1", "true", "yes", "on"}

    @staticmethod
    def get_int_param(
        request: HttpRequest,
        key: str,
        /,
        *,
        default: int,
        min_val: int | None = None,
        max_val: int | None = None,
    ) -> int:
        try:
            val = int(request.GET.get(key, default))
        except (TypeError, ValueError):
            return default
        if min_val is not None:
            val = max(val, min_val)
        if max_val is not None:
            val = min(val, max_val)
        return val

    # ───────────────────────── cache-key builder ────────────────────

    def build_cache_key(self, request: HttpRequest, **extra: Any) -> str:
        """
        Build a stable key: path + sorted query params (minus META params)
        + optional extra kw-args (e.g. path variables).

        If the same key appears in both the query-string and `extra`, the
        value from `extra` wins.
        """
        params = request.GET.dict()  # copy – we won't mutate QueryDict
        for mp in self.META_CACHE_PARAMS:  # strip meta flags
            params.pop(mp, None)

        params.update(extra)  # <- merge, extra overwrites
        return _build_cache_key(request.path, **params)

    # ───────────────────────── single-flight proxy ───────────────────
    async def _aget_or_set(
        self,
        key: str,
        producer: Callable[[], T | Awaitable[T]],
        *,
        ttl: int,
    ) -> T:
        return await _aget_or_set(key, producer, ttl=ttl)

    # ──────────────────── public caching convenience ─────────────────
    async def get_cached_data(
        self,
        request: HttpRequest,
        producer: Callable[[], T | Awaitable[T]],
        *,
        ttl: int,
        **cache_key_kwargs: Any,
    ) -> T:
        """
        • `nocache=true`  → run producer, skip read/write (BYPASS)
        • `reset=true`    → delete old entry, produce fresh, store (REFRESH)
        • default         → HIT / MISS via single-flight
        """

        async def produce_and_await() -> T:
            """Helper to robustly call sync or async producer."""
            res = producer()
            if asyncio.iscoroutine(res):
                return await cast("Awaitable[T]", res)
            return cast("T", res)

        cache_key = self.build_cache_key(request, **cache_key_kwargs)
        want_reset = self.get_bool_param(request, "reset")
        want_bypass = self.get_bool_param(request, "nocache")

        if want_bypass:
            log.warning("==nocache==", method=request.method, key=cache_key)
            request._cache_status = "BYPASS"
            return await produce_and_await()

        if want_reset:
            log.warning("==RESET==", method=request.method, key=cache_key)
            await adelete(cache_key)
            data = await produce_and_await()
            await aset_json(cache_key, data, ttl=ttl)
            request._cache_status = "REFRESH"
            return data

        async def _wrapped_producer_for_miss() -> T:
            request._cache_status = "MISS"
            return await produce_and_await()

        data = await self._aget_or_set(cache_key, _wrapped_producer_for_miss, ttl=ttl)

        if not hasattr(request, "_cache_status"):
            request._cache_status = "HIT"

        return data


class BaseAppView(BaseAsyncView, ABC):
    """
    A new universal base class for all application API views.

    It standardizes the entire GET request lifecycle:
    1. It calls a subclass-defined `_get_params` to gather all relevant parameters
       from the request path (kwargs) and query string (GET).
    2. It calls a subclass-defined `_produce_payload` with these clean parameters
       to generate the core data.
    3. It handles caching, using the parameters to build a unique cache key.
    4. It returns a consistent OrjsonResponse.
    """

    CACHE_TTL: int = 300

    @abstractmethod
    def _get_params(self, request: HttpRequest, **kwargs) -> dict[str, Any]:
        """Subclasses must implement this to define the parameters they care about."""
        raise NotImplementedError

    @abstractmethod
    async def _produce_payload(self, params: dict[str, Any]) -> Any:
        """Subclasses must implement this to perform their core data generation."""
        raise NotImplementedError

    async def get(self, request: HttpRequest, **kwargs) -> OrjsonResponse:
        """Universal GET handler that orchestrates the entire process."""
        params = self._get_params(request, **kwargs)

        async def _producer() -> Any:
            return await self._produce_payload(params)

        data = await self.get_cached_data(
            request,
            producer=_producer,
            ttl=self.CACHE_TTL,
            **params,
        )
        return OrjsonResponse(data)
