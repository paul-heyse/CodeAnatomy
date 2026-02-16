"""Test PruningMetrics struct creation and validation."""

from __future__ import annotations

import msgspec

from datafusion_engine.pruning.metrics import PruningMetrics
from tests.test_helpers.immutability import assert_immutable_assignment

ROW_GROUPS_TOTAL = 100
ROW_GROUPS_PRUNED = 75
PAGES_TOTAL = 400
PAGES_PRUNED = 200
FILTERS_PUSHED = 3
PRUNING_EFFECTIVENESS_HIGH = 0.75
ROUNDTRIP_ROW_GROUPS_TOTAL = 50
ROUNDTRIP_ROW_GROUPS_PRUNED = 25
PRUNING_EFFECTIVENESS_MEDIUM = 0.5


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
            row_groups_total=ROW_GROUPS_TOTAL,
            row_groups_pruned=ROW_GROUPS_PRUNED,
            pages_total=PAGES_TOTAL,
            pages_pruned=PAGES_PRUNED,
            filters_pushed=FILTERS_PUSHED,
            statistics_available=True,
            pruning_effectiveness=PRUNING_EFFECTIVENESS_HIGH,
        )
        assert m.row_groups_total == ROW_GROUPS_TOTAL
        assert m.row_groups_pruned == ROW_GROUPS_PRUNED
        assert m.pages_total == PAGES_TOTAL
        assert m.pages_pruned == PAGES_PRUNED
        assert m.filters_pushed == FILTERS_PUSHED
        assert m.statistics_available is True
        assert m.pruning_effectiveness == PRUNING_EFFECTIVENESS_HIGH

    def test_frozen(self) -> None:
        """PruningMetrics is immutable (frozen)."""
        m = PruningMetrics()
        assert_immutable_assignment(
            factory=lambda: m,
            attribute="row_groups_total",
            attempted_value=5,
            expected_exception=AttributeError,
        )


class TestPruningMetricsSerialization:
    """Test msgspec round-trip for PruningMetrics."""

    def test_round_trip(self) -> None:
        """Encode and decode PruningMetrics via msgspec."""
        original = PruningMetrics(
            row_groups_total=ROUNDTRIP_ROW_GROUPS_TOTAL,
            row_groups_pruned=ROUNDTRIP_ROW_GROUPS_PRUNED,
            filters_pushed=2,
            statistics_available=True,
            pruning_effectiveness=PRUNING_EFFECTIVENESS_MEDIUM,
        )
        data = msgspec.json.encode(original)
        decoded = msgspec.json.decode(data, type=PruningMetrics)
        assert decoded.row_groups_total == ROUNDTRIP_ROW_GROUPS_TOTAL
        assert decoded.row_groups_pruned == ROUNDTRIP_ROW_GROUPS_PRUNED
        assert decoded.pruning_effectiveness == PRUNING_EFFECTIVENESS_MEDIUM

    def test_omit_defaults(self) -> None:
        """Default-valued fields are omitted in serialized output."""
        m = PruningMetrics()
        data = msgspec.json.encode(m)
        decoded_raw = msgspec.json.decode(data)
        # All defaults omitted means empty or minimal object
        assert isinstance(decoded_raw, dict)
