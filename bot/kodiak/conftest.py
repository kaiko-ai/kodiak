from unittest.mock import AsyncMock, patch

import pytest

from kodiak.queries import clear_config_cache


@pytest.fixture(autouse=True)
def _clear_caches() -> None:
    """Clear module-level caches between tests to prevent leakage."""
    clear_config_cache()


@pytest.fixture(autouse=True)
def _mock_debug_history_redis() -> None:  # type: ignore[misc]
    """Prevent debug event recording from hitting a real Redis connection in tests."""
    mock_redis = AsyncMock()
    with patch("kodiak.debug_history.redis_bot", mock_redis):
        yield


@pytest.fixture(autouse=True)
def configure_structlog() -> None:
    """
    Configures cleanly structlog for each test method.
    https://github.com/hynek/structlog/issues/76#issuecomment-240373958
    """
    import structlog

    structlog.reset_defaults()
    structlog.configure(
        processors=[
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.KeyValueRenderer(),
        ],
        wrapper_class=structlog.stdlib.BoundLogger,
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=False,
    )
