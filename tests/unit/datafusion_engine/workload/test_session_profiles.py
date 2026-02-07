"""Test workload session profile configuration."""

from __future__ import annotations

import pytest

from datafusion_engine.workload.classifier import WorkloadClass
from datafusion_engine.workload.session_profiles import (
    WorkloadSessionProfile,
    workload_session_profile,
)


class TestWorkloadSessionProfile:
    """Test WorkloadSessionProfile construction and defaults."""

    def test_default_profile_all_none(self) -> None:
        """Default profile has all None fields."""
        profile = WorkloadSessionProfile()
        assert profile.target_partitions is None
        assert profile.batch_size is None
        assert profile.repartition_aggregations is None
        assert profile.repartition_file_scans is None
        assert profile.sort_spill_reservation_bytes is None
        assert profile.memory_fraction is None

    def test_frozen_immutable(self) -> None:
        """Profile is frozen and immutable."""
        profile = WorkloadSessionProfile(target_partitions=4)
        with pytest.raises(AttributeError):
            profile.target_partitions = 8  # type: ignore[misc]


class TestWorkloadSessionProfileLookup:
    """Test workload_session_profile returns valid profiles."""

    def test_batch_ingest_profile(self) -> None:
        """Batch ingest profile has high resource settings."""
        profile = workload_session_profile(WorkloadClass.BATCH_INGEST)
        assert profile.target_partitions is not None
        assert profile.target_partitions >= 4
        assert profile.batch_size is not None
        assert profile.batch_size >= 8192
        assert profile.memory_fraction is not None
        assert profile.memory_fraction >= 0.5

    def test_interactive_query_profile(self) -> None:
        """Interactive query profile has low-latency settings."""
        profile = workload_session_profile(WorkloadClass.INTERACTIVE_QUERY)
        assert profile.target_partitions is not None
        assert profile.target_partitions <= 4
        assert profile.batch_size is not None
        assert profile.batch_size <= 8192
        assert profile.memory_fraction is not None
        assert profile.memory_fraction <= 0.5

    def test_compile_replay_profile(self) -> None:
        """Compile replay profile has minimal resource settings."""
        profile = workload_session_profile(WorkloadClass.COMPILE_REPLAY)
        assert profile.target_partitions is not None
        assert profile.target_partitions <= 2
        assert profile.memory_fraction is not None
        assert profile.memory_fraction <= 0.3

    def test_incremental_update_profile(self) -> None:
        """Incremental update profile has moderate settings."""
        profile = workload_session_profile(WorkloadClass.INCREMENTAL_UPDATE)
        assert profile.target_partitions is not None
        assert profile.repartition_file_scans is True

    def test_every_class_has_a_profile(self) -> None:
        """Every WorkloadClass produces a non-default profile."""
        for wc in WorkloadClass:
            profile = workload_session_profile(wc)
            assert isinstance(profile, WorkloadSessionProfile)
            # All shipped profiles have target_partitions set
            assert profile.target_partitions is not None

    def test_batch_has_higher_partitions_than_interactive(self) -> None:
        """Batch profile uses more partitions than interactive."""
        batch = workload_session_profile(WorkloadClass.BATCH_INGEST)
        interactive = workload_session_profile(WorkloadClass.INTERACTIVE_QUERY)
        assert batch.target_partitions is not None
        assert interactive.target_partitions is not None
        assert batch.target_partitions > interactive.target_partitions
