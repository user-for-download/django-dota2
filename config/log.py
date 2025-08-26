import logging
import os
import sys

import structlog
from structlog.processors import StackInfoRenderer, TimeStamper, format_exc_info
from structlog.stdlib import add_log_level
from structlog.threadlocal import merge_threadlocal

# Detect environment
DEBUG = os.getenv("DJANGO_ENV", "dev") == "dev"
IS_WORKER = "infrastructure.worker" in sys.modules or "run_workers" in sys.argv

# Shared processors
shared_processors = [
    merge_threadlocal,
    add_log_level,
    TimeStamper(fmt="iso", utc=True),
    StackInfoRenderer(),
    format_exc_info,
]

CONSOLE_RENDERER = structlog.dev.ConsoleRenderer(colors=True, pad_event=0, pad_level=False)

# Choose renderer
# if DEBUG and not IS_WORKER:
#     CONSOLE_RENDERER = structlog.dev.ConsoleRenderer(colors=True, pad_event=0, pad_level=False)
# else:
#     CONSOLE_RENDERER = structlog.processors.JSONRenderer()

# Django LOGGING
LOGGING = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "plain": {
            "()": "structlog.stdlib.ProcessorFormatter",
            "processor": CONSOLE_RENDERER,
            "foreign_pre_chain": shared_processors,
        },
        "json": {
            "()": "structlog.stdlib.ProcessorFormatter",
            "processor": structlog.processors.JSONRenderer(),
            "foreign_pre_chain": shared_processors,
        },
    },
    "handlers": {
        "console": {
            "class": "logging.StreamHandler",
            "formatter": "plain",
        },
    },
    "loggers": {
        "": {
            "handlers": ["console"],
            "level": "INFO",
            "propagate": False,
        },
        "django": {
            "handlers": ["console"],
            "level": "INFO",
            "propagate": False,
        },
        "apps": {
            "handlers": ["console"],
            "level": "DEBUG",
            "propagate": False,
        },
    },
}

structlog.configure(
    processors=[
        *shared_processors,
        structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
    ],
    wrapper_class=structlog.make_filtering_bound_logger(logging.INFO),
    context_class=dict,
    logger_factory=structlog.stdlib.LoggerFactory(),
    cache_logger_on_first_use=True,
)
