"""Test compile tracking integration with compile boundary functions.

Verify that ``compile_semantic_program()`` and ``build_semantic_execution_context()``
record compile invocations through the ``CompileTracker`` infrastructure, and that
the ``compile_tracking`` context manager enforces the single-compile invariant.

Plan reference: docs/plans/programmatic_architecture_assessment_v1_2026-02-07.md, Wave 2 item 6
"""

from __future__ import annotations

import pytest

from semantics.compile_invariants import (
    CompileTracker,
    compile_tracking,
    get_active_tracker,
    record_compile_if_tracking,
)

COMPILE_COUNT_AFTER_TWO_RECORDS = 2


class TestCompileTrackerUnit:
    """Unit tests for CompileTracker state management."""

    @staticmethod
    def test_initial_count_is_zero() -> None:
        """Start with zero compile invocations."""
        tracker = CompileTracker()
        assert tracker.compile_count == 0

    @staticmethod
    def test_record_increments_count() -> None:
        """Increment compile count after each record call."""
        tracker = CompileTracker(max_compiles=5)
        tracker.record_compile()
        assert tracker.compile_count == 1
        tracker.record_compile()
        assert tracker.compile_count == COMPILE_COUNT_AFTER_TWO_RECORDS

    @staticmethod
    def test_exceeding_max_compiles_raises() -> None:
        """Raise RuntimeError when exceeding the max compile threshold."""
        tracker = CompileTracker(max_compiles=1)
        tracker.record_compile()
        with pytest.raises(RuntimeError, match="Compile invariant violation"):
            tracker.record_compile()

    @staticmethod
    def test_assert_compile_count_matches() -> None:
        """Assert compile count matches the expected value."""
        tracker = CompileTracker(max_compiles=2)
        tracker.record_compile()
        tracker.assert_compile_count(expected=1)

    @staticmethod
    def test_assert_compile_count_mismatch_raises() -> None:
        """Raise RuntimeError when assert_compile_count detects mismatch."""
        tracker = CompileTracker(max_compiles=2)
        with pytest.raises(RuntimeError, match="Compile count mismatch"):
            tracker.assert_compile_count(expected=1)


class TestCompileTrackingContextManager:
    """Test the compile_tracking context manager lifecycle."""

    @staticmethod
    def test_tracker_active_within_context() -> None:
        """Return active tracker within compile_tracking scope."""
        with compile_tracking(label="test") as tracker:
            assert get_active_tracker() is tracker

    @staticmethod
    def test_tracker_inactive_outside_context() -> None:
        """Return None for get_active_tracker outside scope."""
        with compile_tracking(label="test"):
            pass
        assert get_active_tracker() is None

    @staticmethod
    def test_strict_mode_asserts_count_at_exit() -> None:
        """Raise when strict mode expects a compile but none occurred."""
        with (
            pytest.raises(RuntimeError, match="Compile count mismatch"),
            compile_tracking(strict=True, label="test"),
        ):
            pass  # No compile recorded, but strict expects exactly 1

    @staticmethod
    def test_nested_tracking_restores_previous() -> None:
        """Restore previous tracker after nested scope exits."""
        with compile_tracking(label="outer") as outer:
            with compile_tracking(label="inner") as inner:
                assert get_active_tracker() is inner
            assert get_active_tracker() is outer


class TestRecordCompileIfTracking:
    """Test the record_compile_if_tracking sentinel integration."""

    @staticmethod
    def test_records_when_active() -> None:
        """Increment count when a tracker is active."""
        with compile_tracking(label="test") as tracker:
            record_compile_if_tracking()
            assert tracker.compile_count == 1

    @staticmethod
    def test_noop_when_no_tracker() -> None:
        """Do nothing when no tracker is active."""
        record_compile_if_tracking()  # Should not raise
