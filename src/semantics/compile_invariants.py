"""Compile-once invariant enforcement for semantic pipeline runs."""

from __future__ import annotations

import threading
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Iterator


@dataclass
class CompileTracker:
    """Track semantic compile invocations within a pipeline run.

    Use as a context manager at orchestration boundaries to enforce
    the single-compile invariant. Increment on each compile call and
    assert the count at context exit.

    Args:
        max_compiles: Maximum allowed compile invocations (default 1).
        label: Descriptive label for diagnostics on invariant violation.
    """

    max_compiles: int = 1
    label: str = "pipeline"
    _count: int = field(default=0, init=False, repr=False)
    _lock: threading.Lock = field(default_factory=threading.Lock, init=False, repr=False)

    def record_compile(self) -> None:
        """Record a semantic compile invocation.

        Raises:
            RuntimeError: When the compile count exceeds ``max_compiles``.
        """
        with self._lock:
            self._count += 1
            if self._count > self.max_compiles:
                msg = (
                    f"Compile invariant violation in {self.label!r}: "
                    f"expected at most {self.max_compiles} compile(s), "
                    f"got {self._count}"
                )
                raise RuntimeError(msg)

    @property
    def compile_count(self) -> int:
        """Return the current compile count."""
        return self._count

    def assert_compile_count(self, expected: int | None = None) -> None:
        """Assert the compile count matches the expected value.

        Args:
            expected: Expected compile count. Defaults to ``max_compiles``.

        Raises:
            RuntimeError: When the actual compile count does not match.
        """
        target = expected if expected is not None else self.max_compiles
        if self._count != target:
            msg = f"Compile count mismatch in {self.label!r}: expected {target}, got {self._count}"
            raise RuntimeError(msg)


# Module-level tracker state using mutable container to avoid global reassignment.
# Pattern consistent with src/obs/otel/heartbeat.py (_STAGE_FALLBACK, etc.).
_tracker_state: dict[str, CompileTracker | None] = {"active": None}
_tracker_lock = threading.Lock()


@contextmanager
def compile_tracking(
    *,
    max_compiles: int = 1,
    label: str = "pipeline",
    strict: bool = False,
) -> Iterator[CompileTracker]:
    """Activate compile tracking for a pipeline run scope.

    Args:
        max_compiles: Maximum allowed compile invocations.
        label: Descriptive label for diagnostics.
        strict: When ``True``, assert exact compile count at context exit.
            When ``False``, only enforce the ceiling via ``record_compile()``.

    Yields:
        Active tracker for the pipeline run scope.
    """
    tracker = CompileTracker(max_compiles=max_compiles, label=label)
    with _tracker_lock:
        previous = _tracker_state["active"]
        _tracker_state["active"] = tracker
    try:
        yield tracker
    finally:
        with _tracker_lock:
            _tracker_state["active"] = previous
        if strict:
            tracker.assert_compile_count()


def get_active_tracker() -> CompileTracker | None:
    """Return the active compile tracker, or ``None`` if not tracking.

    Returns:
        Active tracker when ``compile_tracking()`` is in scope, else ``None``.
    """
    return _tracker_state["active"]


def record_compile_if_tracking() -> None:
    """Record a compile invocation on the active tracker if one exists.

    Call this from ``compile_semantic_program()`` or
    ``build_semantic_execution_context()`` to automatically enforce
    the single-compile invariant when tracking is active.
    """
    tracker = _tracker_state["active"]
    if tracker is not None:
        tracker.record_compile()


__all__ = [
    "CompileTracker",
    "compile_tracking",
    "get_active_tracker",
    "record_compile_if_tracking",
]
