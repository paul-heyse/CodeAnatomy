"""Run-scoped context helpers for OpenTelemetry."""

from __future__ import annotations

from contextvars import ContextVar, Token

_RUN_ID: ContextVar[str | None] = ContextVar("codeanatomy.run_id", default=None)
_QUERY_ID: ContextVar[str | None] = ContextVar("codeanatomy.query_id", default=None)


def get_run_id() -> str | None:
    """Return the current run_id, if set.

    Returns
    -------
    str | None
        Current run identifier or None.
    """
    return _RUN_ID.get()


def set_run_id(run_id: str) -> Token[str | None]:
    """Set the run_id and return the context token.

    Parameters
    ----------
    run_id
        Run identifier to set.

    Returns
    -------
    contextvars.Token[str | None]
        Token used to restore the previous value.
    """
    return _RUN_ID.set(run_id)


def reset_run_id(token: Token[str | None]) -> None:
    """Reset the run_id to the previous value using the token."""
    _RUN_ID.reset(token)


def clear_run_id() -> None:
    """Clear the run_id for the current context."""
    _RUN_ID.set(None)


def get_query_id() -> str | None:
    """Return the current query_id, if set.

    Returns
    -------
    str | None
        Current query identifier or None.
    """
    return _QUERY_ID.get()


def set_query_id(query_id: str) -> Token[str | None]:
    """Set the query_id and return the context token.

    Parameters
    ----------
    query_id
        Query identifier to set.

    Returns
    -------
    contextvars.Token[str | None]
        Token used to restore the previous value.
    """
    return _QUERY_ID.set(query_id)


def reset_query_id(token: Token[str | None]) -> None:
    """Reset the query_id to the previous value using the token."""
    _QUERY_ID.reset(token)


def clear_query_id() -> None:
    """Clear the query_id for the current context."""
    _QUERY_ID.set(None)


__all__ = [
    "clear_query_id",
    "clear_run_id",
    "get_query_id",
    "get_run_id",
    "reset_query_id",
    "reset_run_id",
    "set_query_id",
    "set_run_id",
]
