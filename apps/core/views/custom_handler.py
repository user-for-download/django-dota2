# apps/core/views.py

from asgiref.sync import async_to_sync

from common.views_utils import OrjsonResponse


# 1. This remains our core async logic.
async def _async_json_404_handler(request, exception):
    """Core async logic for the 404 handler."""
    return OrjsonResponse(
        {"detail": "The requested endpoint was not found."},
        status=404,
    )


async def _async_json_500_handler(request):
    """Core async logic for the 500 handler."""
    return OrjsonResponse(
        {"detail": "An internal server error occurred."},
        status=500,
    )


# --- THE FIX ---
# 2. Create simple, synchronous wrappers for the URLconf.
#    These have clear signatures that the Django check framework can inspect.
def json_404_handler(request, exception):
    """Synchronous wrapper for the async 404 handler."""
    # async_to_sync can also be called as a regular function.
    return async_to_sync(_async_json_404_handler)(request, exception)


def json_500_handler(request):
    """Synchronous wrapper for the async 500 handler."""
    return async_to_sync(_async_json_500_handler)(request)


# --- END FIX ---
