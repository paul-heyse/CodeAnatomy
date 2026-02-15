"""UUID generation helpers with time-ordered defaults.

Use UUIDv7 for internal, sortable identifiers. Use CSPRNG-backed tokens
(`secure_token_hex`) for security-sensitive or external/public identifiers.
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


def new_uuid7() -> uuid.UUID:
    """Return UUIDv7 from the canonical UUID factory."""
    return uuid7()


def uuid7_str() -> str:
    """Return a UUIDv7 as a string.

    Returns:
    -------
    str
        String representation of UUIDv7.
    """
    return str(uuid7())


def run_id() -> str:
    """Return sortable run identifier string."""
    return uuid7_str()


def uuid7_hex() -> str:
    """Return a UUIDv7 as a 32-character hex string.

    Returns:
    -------
    str
        Hex-encoded UUIDv7 (32 characters).
    """
    return uuid7().hex


def artifact_id_hex() -> str:
    """Return sortable artifact identifier (hex)."""
    return uuid7_hex()


def uuid7_suffix(length: int = 12) -> str:
    """Return a short suffix from the UUIDv7 random tail.

    Args:
        length: Description.

    Returns:
    -------
    str
        Suffix string from the UUIDv7 value.

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


def normalize_legacy_identity(value: uuid.UUID) -> uuid.UUID:
    """Normalize legacy UUIDv1 values to UUIDv6 when conversion is available.

    Returns:
    -------
    uuid.UUID
        The normalized UUID value when conversion succeeds.
    """
    if value.version != 1:
        return value
    if UUID6_MODULE is None:
        return value
    convert = getattr(UUID6_MODULE, "uuid1_to_uuid6", None)
    if not callable(convert):
        return value
    try:
        return cast("Callable[[uuid.UUID], uuid.UUID]", convert)(value)
    except (RuntimeError, TypeError, ValueError):
        return value


def legacy_compatible_event_id(
    *,
    node: int | None = None,
    clock_seq: int | None = None,
) -> uuid.UUID:
    """Return UUIDv6 for legacy-compatibility paths when available."""
    if UUID6_MODULE is None:
        return new_uuid7()
    uuid6_fn = getattr(UUID6_MODULE, "uuid6", None)
    if not callable(uuid6_fn):
        return new_uuid7()
    try:
        return cast(
            "Callable[..., uuid.UUID]",
            uuid6_fn,
        )(node=node, clock_seq=clock_seq)
    except (RuntimeError, TypeError, ValueError):
        return new_uuid7()


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
    "artifact_id_hex",
    "legacy_compatible_event_id",
    "new_uuid7",
    "normalize_legacy_identity",
    "run_id",
    "secure_token_hex",
    "uuid7",
    "uuid7_hex",
    "uuid7_str",
    "uuid7_suffix",
]
