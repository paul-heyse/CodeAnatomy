"""Tests for resolver identity invariant enforcement."""

from __future__ import annotations

import threading

import pytest

from semantics.resolver_identity import (
    ResolverIdentityTracker,
    get_active_resolver_tracker,
    record_resolver_if_tracking,
    resolver_identity_tracking,
)

# ---------------------------------------------------------------------------
# ResolverIdentityTracker unit tests
# ---------------------------------------------------------------------------


class TestResolverIdentityTracker:
    """Unit tests for the ResolverIdentityTracker dataclass."""

    def test_no_registrations(self) -> None:
        """Verify empty tracker reports zero distinct resolvers."""
        tracker = ResolverIdentityTracker()
        assert tracker.distinct_resolvers() == 0
        assert tracker.verify_identity() == []
        # assert_identity should not raise when empty.
        tracker.assert_identity()

    def test_single_resolver_same_instance(self) -> None:
        """Verify same resolver instance across multiple subsystems passes."""
        resolver = object()
        tracker = ResolverIdentityTracker(label="test-pipeline")
        tracker.record_resolver(resolver, label="view_registration")
        tracker.record_resolver(resolver, label="scan_override")
        tracker.record_resolver(resolver, label="cdf_cursor")

        assert tracker.distinct_resolvers() == 1
        assert tracker.verify_identity() == []
        tracker.assert_identity()

    def test_multiple_distinct_resolvers_detected(self) -> None:
        """Verify distinct resolver instances are detected as violations."""
        resolver_a = object()
        resolver_b = object()
        tracker = ResolverIdentityTracker(label="test-pipeline")
        tracker.record_resolver(resolver_a, label="view_registration")
        tracker.record_resolver(resolver_b, label="scan_override")

        assert tracker.distinct_resolvers() == 2
        violations = tracker.verify_identity()
        assert len(violations) >= 1
        assert "Resolver identity violation" in violations[0]
        assert "test-pipeline" in violations[0]
        assert "2 distinct" in violations[0]

    def test_assert_identity_raises_on_violation(self) -> None:
        """Verify assert_identity raises RuntimeError on drift."""
        resolver_a = object()
        resolver_b = object()
        tracker = ResolverIdentityTracker(label="strict-test")
        tracker.record_resolver(resolver_a, label="subsystem_a")
        tracker.record_resolver(resolver_b, label="subsystem_b")

        with pytest.raises(RuntimeError, match="Resolver identity violation"):
            tracker.assert_identity()

    def test_violation_message_includes_subsystem_labels(self) -> None:
        """Verify violation messages include subsystem label detail."""
        resolver_a = object()
        resolver_b = object()
        tracker = ResolverIdentityTracker(label="label-test")
        tracker.record_resolver(resolver_a, label="alpha")
        tracker.record_resolver(resolver_b, label="beta")

        violations = tracker.verify_identity()
        full_msg = "\n".join(violations)
        assert "alpha" in full_msg
        assert "beta" in full_msg

    def test_default_label(self) -> None:
        """Verify the default label for the tracker is 'pipeline'."""
        tracker = ResolverIdentityTracker()
        assert tracker.label == "pipeline"

    def test_default_subsystem_label(self) -> None:
        """Verify the default subsystem label is 'unknown'."""
        resolver = object()
        tracker = ResolverIdentityTracker()
        tracker.record_resolver(resolver)

        # Record a second distinct resolver to trigger violation output.
        tracker.record_resolver(object())
        violations = tracker.verify_identity()
        full_msg = "\n".join(violations)
        assert "unknown" in full_msg


# ---------------------------------------------------------------------------
# Context manager tests
# ---------------------------------------------------------------------------


def _run_tracking_with_drift(resolver_a: object, resolver_b: object) -> None:
    """Run identity tracking with two distinct resolver instances."""
    with resolver_identity_tracking(strict=True) as tracker:
        tracker.record_resolver(resolver_a, label="a")
        tracker.record_resolver(resolver_b, label="b")


class TestResolverIdentityTracking:
    """Tests for the resolver_identity_tracking context manager."""

    def test_context_manager_activates_tracker(self) -> None:
        """Verify the context manager sets and clears the active tracker."""
        assert get_active_resolver_tracker() is None

        with resolver_identity_tracking(label="cm-test") as tracker:
            assert get_active_resolver_tracker() is tracker
            assert tracker.label == "cm-test"

        assert get_active_resolver_tracker() is None

    def test_strict_mode_passes_on_identity(self) -> None:
        """Verify strict mode exits cleanly when identity is maintained."""
        resolver = object()
        with resolver_identity_tracking(strict=True) as tracker:
            tracker.record_resolver(resolver, label="a")
            tracker.record_resolver(resolver, label="b")
        # No exception means success.

    def test_strict_mode_raises_on_violation(self) -> None:
        """Verify strict mode raises on context exit when drift detected."""
        resolver_a = object()
        resolver_b = object()
        with pytest.raises(RuntimeError, match="Resolver identity violation"):
            _run_tracking_with_drift(resolver_a, resolver_b)

    def test_non_strict_mode_does_not_raise(self) -> None:
        """Verify non-strict mode does not raise even with violations."""
        resolver_a = object()
        resolver_b = object()
        with resolver_identity_tracking(strict=False) as tracker:
            tracker.record_resolver(resolver_a, label="a")
            tracker.record_resolver(resolver_b, label="b")
        # Non-strict should not raise, even though there are violations.
        assert tracker.distinct_resolvers() == 2

    def test_nested_context_restores_previous(self) -> None:
        """Verify nested tracking contexts restore the outer tracker."""
        with resolver_identity_tracking(label="outer") as outer:
            assert get_active_resolver_tracker() is outer

            with resolver_identity_tracking(label="inner") as inner:
                assert get_active_resolver_tracker() is inner

            assert get_active_resolver_tracker() is outer

        assert get_active_resolver_tracker() is None


# ---------------------------------------------------------------------------
# record_resolver_if_tracking tests
# ---------------------------------------------------------------------------


class TestRecordResolverIfTracking:
    """Tests for the record_resolver_if_tracking convenience function."""

    def test_no_op_when_no_tracker(self) -> None:
        """Verify no error when calling without an active tracker."""
        assert get_active_resolver_tracker() is None
        # Should be a silent no-op.
        record_resolver_if_tracking(object(), label="orphan")

    def test_records_when_tracker_active(self) -> None:
        """Verify the resolver is recorded when a tracker is active."""
        resolver = object()
        with resolver_identity_tracking() as tracker:
            record_resolver_if_tracking(resolver, label="via-convenience")
            assert tracker.distinct_resolvers() == 1


# ---------------------------------------------------------------------------
# Thread safety tests
# ---------------------------------------------------------------------------


class TestThreadSafety:
    """Tests for concurrent resolver recording."""

    def test_concurrent_same_resolver(self) -> None:
        """Verify multiple threads recording the same resolver instance."""
        resolver = object()
        tracker = ResolverIdentityTracker(label="threaded")
        barrier = threading.Barrier(4)
        errors: list[str] = []

        def worker(lbl: str) -> None:
            try:
                barrier.wait(timeout=5)
                for _ in range(50):
                    tracker.record_resolver(resolver, label=lbl)
            except (RuntimeError, threading.BrokenBarrierError) as exc:
                errors.append(str(exc))

        threads = [threading.Thread(target=worker, args=(f"thread-{i}",)) for i in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=10)

        assert not errors, f"Thread errors: {errors}"
        assert tracker.distinct_resolvers() == 1
        tracker.assert_identity()

    def test_concurrent_distinct_resolvers(self) -> None:
        """Verify distinct resolvers from different threads are detected."""
        tracker = ResolverIdentityTracker(label="threaded-drift")
        barrier = threading.Barrier(4)
        # Each thread creates its own resolver -- should produce 4 distinct.
        errors: list[str] = []

        def worker(lbl: str) -> None:
            try:
                local_resolver = object()
                barrier.wait(timeout=5)
                for _ in range(10):
                    tracker.record_resolver(local_resolver, label=lbl)
            except (RuntimeError, threading.BrokenBarrierError) as exc:
                errors.append(str(exc))

        threads = [threading.Thread(target=worker, args=(f"thread-{i}",)) for i in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=10)

        assert not errors, f"Thread errors: {errors}"
        assert tracker.distinct_resolvers() == 4
        violations = tracker.verify_identity()
        assert len(violations) >= 1
        assert "4 distinct" in violations[0]
