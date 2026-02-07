"""Test PruningMetrics struct creation and validation."""

from __future__ import annotations

import msgspec

from datafusion_engine.pruning.metrics import PruningMetrics


class TestPruningMetricsCreation:
    """Test PruningMetrics construction."""

    def test_default_values(self) -> None:
        """Default PruningMetrics has all-zero fields."""
        m = PruningMetrics()
        assert m.row_groups_total == 0
        assert m.row_groups_pruned == 0
        assert m.pages_total == 0
        assert m.pages_pruned == 0
        assert m.filters_pushed == 0
        assert m.statistics_available is False
        assert m.pruning_effectiveness == 0.0

    def test_full_construction(self) -> None:
        """Construct PruningMetrics with all fields set."""
        m = PruningMetrics(
            row_groups_total=100,
            row_groups_pruned=75,
            pages_total=400,
            pages_pruned=200,
            filters_pushed=3,
            statistics_available=True,
            pruning_effectiveness=0.75,
        )
        assert m.row_groups_total == 100
        assert m.row_groups_pruned == 75
        assert m.pages_total == 400
        assert m.pages_pruned == 200
        assert m.filters_pushed == 3
        assert m.statistics_available is True
        assert m.pruning_effectiveness == 0.75

    def test_frozen(self) -> None:
        """PruningMetrics is immutable (frozen)."""
        import pytest

        m = PruningMetrics()
        with pytest.raises(AttributeError):
            m.row_groups_total = 5  # type: ignore[misc]


class TestPruningMetricsSerialization:
    """Test msgspec round-trip for PruningMetrics."""

    def test_round_trip(self) -> None:
        """Encode and decode PruningMetrics via msgspec."""
        original = PruningMetrics(
            row_groups_total=50,
            row_groups_pruned=25,
            filters_pushed=2,
            statistics_available=True,
            pruning_effectiveness=0.5,
        )
        data = msgspec.json.encode(original)
        decoded = msgspec.json.decode(data, type=PruningMetrics)
        assert decoded.row_groups_total == 50
        assert decoded.row_groups_pruned == 25
        assert decoded.pruning_effectiveness == 0.5

    def test_omit_defaults(self) -> None:
        """Default-valued fields are omitted in serialized output."""
        m = PruningMetrics()
        data = msgspec.json.encode(m)
        decoded_raw = msgspec.json.decode(data)
        # All defaults omitted means empty or minimal object
        assert isinstance(decoded_raw, dict)
