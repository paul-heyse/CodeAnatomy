"""Plan profiling helpers for DataFusion DataFrames."""

from __future__ import annotations

import contextlib
import io
import os
import re
from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from datafusion.dataframe import DataFrame


_DURATION_RE = re.compile(
    r"\|\s*Duration\s*\|\s*(?P<value>[0-9.]+)\s*(?P<unit>ns|us|ms|s|\u03bcs|\u00b5s)\s*\|"
)
_OUTPUT_ROWS_RE = re.compile(r"\|\s*Output Rows\s*\|\s*(?P<value>[0-9]+)\s*\|")


@dataclass(frozen=True)
class ExplainCapture:
    """Capture explain output and parsed metrics.

    Attributes:
    ----------
    text
        Raw explain output text.
    duration_ms
        Parsed duration in milliseconds, if available.
    output_rows
        Parsed output row count, if available.
    """

    text: str
    duration_ms: float | None = None
    output_rows: int | None = None


def capture_explain(
    df: DataFrame,
    *,
    verbose: bool = False,
    analyze: bool = False,
) -> ExplainCapture | None:
    """Capture DataFrame explain output and parse basic metrics.

    Parameters
    ----------
    df
        DataFusion DataFrame to explain.
    verbose
        Whether to request verbose explain output.
    analyze
        Whether to request EXPLAIN ANALYZE output.

    Returns:
    -------
    ExplainCapture | None
        Captured explain output and metrics, or ``None`` if unavailable.
    """
    if os.environ.get("CODEANATOMY_ENABLE_DF_EXPLAIN", "0") != "1":
        return None
    explain_method = getattr(df, "explain", None)
    if not callable(explain_method):
        return None
    plan_ok = _plan_has_partitions(df)
    if plan_ok is not True:
        return None
    text = _run_explain_text(explain_method, verbose=verbose, analyze=analyze)
    if text is None:
        return None
    return ExplainCapture(
        text=text,
        duration_ms=_parse_duration_ms(text),
        output_rows=_parse_output_rows(text),
    )


def _plan_has_partitions(df: DataFrame) -> bool | None:
    plan_fn = getattr(df, "execution_plan", None)
    if not callable(plan_fn):
        return True
    try:
        plan = plan_fn()
    except (AttributeError, OSError, RuntimeError, TypeError, ValueError):
        return None
    if plan is None:
        return True
    count_fn = getattr(plan, "partition_count", None)
    if not callable(count_fn):
        return True
    try:
        partition_count = count_fn()
    except (RuntimeError, TypeError, ValueError):
        return True
    return partition_count != 0


def _run_explain_text(
    explain_method: Callable[..., object],
    *,
    verbose: bool,
    analyze: bool,
) -> str | None:
    buffer = io.StringIO()
    with contextlib.redirect_stdout(buffer):
        try:
            explain_method(verbose=verbose, analyze=analyze)
        except BaseException as exc:
            if exc.__class__.__name__ == "PanicException":
                return None
            if isinstance(exc, Exception) and str(exc).startswith("DataFusion error:"):
                return None
            raise
    text = buffer.getvalue()
    if not text:
        return None
    return text


def _parse_duration_ms(text: str) -> float | None:
    """Parse duration in milliseconds from explain output.

    Parameters
    ----------
    text
        Explain output text.

    Returns:
    -------
    float | None
        Duration in milliseconds, when available.
    """
    match = _DURATION_RE.search(text)
    if match is None:
        return None
    value = float(match.group("value"))
    unit = match.group("unit")
    if unit == "ns":
        return value / 1_000_000.0
    if unit in {"us", "\u03bcs", "\u00b5s"}:
        return value / 1_000.0
    if unit == "ms":
        return value
    if unit == "s":
        return value * 1_000.0
    return None


def _parse_output_rows(text: str) -> int | None:
    """Parse output row count from explain output.

    Parameters
    ----------
    text
        Explain output text.

    Returns:
    -------
    int | None
        Output row count, when available.
    """
    match = _OUTPUT_ROWS_RE.search(text)
    if match is None:
        return None
    return int(match.group("value"))


__all__ = ["ExplainCapture", "capture_explain"]
