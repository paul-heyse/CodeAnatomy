"""Resolver identity invariant enforcement for semantic pipeline runs.

Enforce that all pipeline subsystems (view registration, scan overrides,
CDF, readiness diagnostics) share a single ``ManifestDatasetResolver``
instance. The check uses ``id()`` (object identity), not value equality,
to detect resolver drift caused by accidental re-creation.
"""

from __future__ import annotations

import threading
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Iterator


@dataclass
class ResolverIdentityTracker:
    """Track resolver identity across pipeline subsystems.

    Record each resolver registration with a label identifying the
    subsystem that registered it. After the pipeline completes, verify
    that all registrations reference the same object instance.

    Parameters
    ----------
    label : str
        Descriptive label for the pipeline scope (used in diagnostics).
    """

    label: str = "pipeline"
    _resolver_ids: list[int] = field(default_factory=list, init=False, repr=False)
    _labels: list[str] = field(default_factory=list, init=False, repr=False)
    _lock: threading.Lock = field(default_factory=threading.Lock, init=False, repr=False)

    def record_resolver(self, resolver: object, *, label: str = "unknown") -> None:
        """Record a resolver instance registration.

        Parameters
        ----------
        resolver : object
            The resolver instance to track. Only ``id(resolver)`` is stored.
        label : str
            Subsystem label for diagnostics (e.g. ``"view_registration"``).
        """
        with self._lock:
            self._resolver_ids.append(id(resolver))
            self._labels.append(label)

    def distinct_resolvers(self) -> int:
        """Return the count of distinct resolver instances recorded.

        Returns:
        -------
        int
            Number of unique ``id()`` values across all registrations.
            Returns ``0`` when no registrations have been recorded.
        """
        with self._lock:
            return len(set(self._resolver_ids))

    def verify_identity(self) -> list[str]:
        """Return a list of identity violation descriptions.

        Returns:
        -------
        list[str]
            Empty when all registrations share the same resolver instance.
            Contains one description per violation when drift is detected.
        """
        with self._lock:
            unique_ids = set(self._resolver_ids)
            if len(unique_ids) <= 1:
                return []

            # Build a mapping from resolver id to the labels that used it.
            id_to_labels: dict[int, list[str]] = {}
            for rid, lbl in zip(self._resolver_ids, self._labels):
                id_to_labels.setdefault(rid, []).append(lbl)

            violations: list[str] = []
            violations.append(
                f"Resolver identity violation in {self.label!r}: "
                f"{len(unique_ids)} distinct resolver instances across "
                f"{len(self._resolver_ids)} registration(s)"
            )
            for rid, labels in sorted(id_to_labels.items()):
                violations.append(f"  resolver id={rid}: subsystems={labels}")
            return violations

    def assert_identity(self) -> None:
        """Raise ``RuntimeError`` when multiple distinct resolvers are detected.

        Raises:
        ------
        RuntimeError
            When more than one distinct resolver instance has been recorded.
        """
        violations = self.verify_identity()
        if violations:
            msg = "\n".join(violations)
            raise RuntimeError(msg)


# ---------------------------------------------------------------------------
# Module-level tracker state
# ---------------------------------------------------------------------------
# Mutable container avoids global reassignment.  Pattern consistent with
# ``compile_invariants.py`` and ``src/obs/otel/heartbeat.py``.
_tracker_state: dict[str, ResolverIdentityTracker | None] = {"active": None}
_tracker_lock = threading.Lock()


@contextmanager
def resolver_identity_tracking(
    *,
    label: str = "pipeline",
    strict: bool = False,
) -> Iterator[ResolverIdentityTracker]:
    """Activate resolver identity tracking for a pipeline run scope.

    Parameters
    ----------
    label : str
        Descriptive label for the pipeline scope.
    strict : bool
        When ``True``, call ``assert_identity()`` at context exit.
        When ``False``, the caller must inspect the tracker manually.

    Yields:
    ------
    ResolverIdentityTracker
        Active tracker for the pipeline run scope.
    """
    tracker = ResolverIdentityTracker(label=label)
    with _tracker_lock:
        previous = _tracker_state["active"]
        _tracker_state["active"] = tracker
    try:
        yield tracker
    finally:
        with _tracker_lock:
            _tracker_state["active"] = previous
        if strict:
            tracker.assert_identity()


def get_active_resolver_tracker() -> ResolverIdentityTracker | None:
    """Return the active resolver identity tracker, or ``None``.

    Returns:
    -------
    ResolverIdentityTracker | None
        Active tracker when ``resolver_identity_tracking()`` is in scope,
        else ``None``.
    """
    return _tracker_state["active"]


def record_resolver_if_tracking(resolver: object, *, label: str = "unknown") -> None:
    """Record a resolver on the active tracker if one exists.

    Call this from subsystem registration points (view registration,
    scan override application, CDF cursor setup, readiness diagnostics)
    to automatically participate in resolver identity tracking when a
    tracker is active. Safe to call unconditionally -- it is a no-op
    when no tracker is active.

    Parameters
    ----------
    resolver : object
        The resolver instance to record.
    label : str
        Subsystem label for diagnostics.
    """
    tracker = _tracker_state["active"]
    if tracker is not None:
        tracker.record_resolver(resolver, label=label)


__all__ = [
    "ResolverIdentityTracker",
    "get_active_resolver_tracker",
    "record_resolver_if_tracking",
    "resolver_identity_tracking",
]
