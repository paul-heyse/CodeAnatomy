"""Feature flags for relspec execution behaviour.

Module-level constants parsed from environment variables control
compatibility and migration behaviour across the relspec subsystem.
"""

from __future__ import annotations

from utils.env_utils import env_bool

USE_GLOBAL_EXTRACT_REGISTRY: bool = env_bool(
    "CODEANATOMY_USE_GLOBAL_EXTRACT_REGISTRY",
    default=False,
    on_invalid="false",
)
"""When ``True``, extract executor dispatch falls back to the global mutable
registry in ``extract_execution_registry`` instead of the immutable
``ExecutionAuthorityContext.extract_executor_map``.  Default ``False``.
"""

__all__ = ["USE_GLOBAL_EXTRACT_REGISTRY"]
