# https://slowapi.readthedocs.io/en/latest/#fastapi
import logging

from slowapi import Limiter
from slowapi.util import get_remote_address

from src.config.config import KEEP_LIMITER_DEFAULT_LIMIT, KEEP_USE_LIMITER

logger = logging.getLogger(__name__)
limiter_enabled = KEEP_USE_LIMITER
default_limit = KEEP_LIMITER_DEFAULT_LIMIT

logger.warning(f"Rate limiter is {'enabled' if limiter_enabled else 'disabled'}")

limiter = Limiter(
    key_func=get_remote_address, enabled=limiter_enabled, default_limits=[default_limit]
)
