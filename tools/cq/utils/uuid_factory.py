"""UUID generation helpers for CQ (time-ordered defaults).

Prefer UUIDv7 for sortable identifiers. Use secure_token_hex for
security-sensitive tokens.
"""

from __future__ import annotations

import secrets
import threading
import uuid
from collections.abc import Callable
from typing import TYPE_CHECKING, Final, cast

try:
    import uuid6 as uuid6_pkg
except ImportError:  # pragma: no cover - optional dependency
    uuid6_pkg = None

if TYPE_CHECKING:
    from types import ModuleType

UUID7_HEX_LENGTH: Final[int] = 32
_UUID_LOCK: Final[threading.Lock] = threading.Lock()

if uuid6_pkg is None:
    UUID6_MODULE: ModuleType | None = None
else:
    UUID6_MODULE = uuid6_pkg


def uuid7() -> uuid.UUID:
    """Return a time-ordered UUIDv7 (thread-safe, monotone).

    Raises:
        RuntimeError: If the operation cannot be completed.
    """
    with _UUID_LOCK:
        uuid7_func = getattr(uuid, "uuid7", None)
        if callable(uuid7_func):
            return cast("Callable[[], uuid.UUID]", uuid7_func)()
        if UUID6_MODULE is None:
            msg = "uuid7 requires Python 3.14+ or the uuid6 package."
            raise RuntimeError(msg)
        return UUID6_MODULE.uuid7()


def uuid7_str() -> str:
    """Return a UUIDv7 as a string.

    Used by CQ identifiers that prefer a string representation.

    Returns:
    -------
    str
        UUIDv7 string.
    """
    return str(uuid7())


def uuid7_hex() -> str:
    """Return a UUIDv7 as a 32-character hex string.

    Used by CQ artifacts that need compact, sortable identifiers.

    Returns:
    -------
    str
        UUIDv7 hex string.
    """
    return uuid7().hex


def uuid7_suffix(length: int = 12) -> str:
    """Return a short suffix from the UUIDv7 random tail.

    Args:
        length: Description.

    Raises:
        ValueError: If the operation cannot be completed.
    """
    if length <= 0:
        msg = "length must be positive."
        raise ValueError(msg)
    if length > UUID7_HEX_LENGTH:
        msg = "length must not exceed 32."
        raise ValueError(msg)
    return uuid7().hex[-length:]


def secure_token_hex(nbytes: int = 16) -> str:
    """Return a CSPRNG-backed hex token.

    Args:
        nbytes: Description.

    Raises:
        ValueError: If the operation cannot be completed.
    """
    if nbytes <= 0:
        msg = "nbytes must be positive."
        raise ValueError(msg)
    return secrets.token_hex(nbytes)


__all__ = [
    "UUID6_MODULE",
    "UUID7_HEX_LENGTH",
    "secure_token_hex",
    "uuid7",
    "uuid7_hex",
    "uuid7_str",
    "uuid7_suffix",
]
