"""
apps.core.views
---------------

Package initialiser.

Makes the main async endpoints directly importable via:

    from apps.core.views import health_check, metrics_endpoint, status_stream
"""

from __future__ import annotations

import time
from typing import List

from .custom_handler import json_404_handler, json_500_handler
from .health import health_check
from .metrics import metrics_endpoint
from .stream import status_stream

__all__: list[str] = [
    "health_check",
    "metrics_endpoint",
    "status_stream",
]
