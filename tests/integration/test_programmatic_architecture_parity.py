"""Integration tests for programmatic architecture parity.

Verify that the programmatic architecture changes in Waves 0-2
maintain parity with the prior behavior and correctly compose across
module boundaries.

Test classes cover:
- Cache policy hierarchy resolution
- Entity registry equivalence
- Inferred join keys parity
- Inference confidence flow
- Calibration integration
"""

from __future__ import annotations

import pytest

from datafusion_engine.views.artifacts import CachePolicy
from relspec.calibration_bounds import (
    DEFAULT_CALIBRATION_BOUNDS,
    CalibrationBounds,
    validate_calibration_bounds,
)
from relspec.inference_confidence import (
    InferenceConfidence,
    high_confidence,
    low_confidence,
)
from relspec.policy_calibrator import (
    CalibrationThresholds,
    ExecutionMetricsSummary,
    calibrate_from_execution_metrics,
)
from semantics.entity_registry import ENTITY_DECLARATIONS, generate_table_specs
from semantics.ir import (
    InferredViewProperties,
    ir_cache_hint_to_execution_policy,
)
from semantics.ir_pipeline import _infer_join_keys_from_fields
from semantics.pipeline_build import (
    _resolve_cache_policy_hierarchy,
)
from semantics.registry import SEMANTIC_TABLE_SPECS

HIGH_CONFIDENCE_THRESHOLD = 0.8
LOW_CONFIDENCE_MAX = 0.49
FK_CONFIDENCE = 0.85
CUSTOM_HIGH_FANOUT_THRESHOLD = 5
CUSTOM_ROW_THRESHOLD_SMALL = 50_000
CUSTOM_ROW_THRESHOLD_LARGE = 5_000_000


@pytest.mark.integration
class TestCachePolicyHierarchy:
    """Verify explicit -> compiled cache policy resolution.

    Resolution order (highest priority first):
    1. explicit_policy -- caller-provided per-view overrides
    2. compiled_policy -- topology-derived from CompiledExecutionPolicy
    """

    @staticmethod
    def test_explicit_policy_takes_precedence_over_compiled() -> None:
        """Verify explicit_policy wins over compiled_policy."""
        explicit: dict[str, CachePolicy] = {"cpg_nodes_v1": "none", "cst_refs_norm_v1": "none"}
        compiled: dict[str, CachePolicy] = {
            "cpg_nodes_v1": "delta_output",
            "cst_refs_norm_v1": "delta_staging",
        }

        result = _resolve_cache_policy_hierarchy(
            explicit_policy=explicit,
            compiled_policy=compiled,
        )

        assert result is explicit

    @staticmethod
    def test_compiled_policy_takes_precedence_over_inferred() -> None:
        """Verify compiled_policy wins when explicit_policy is None."""
        compiled: dict[str, CachePolicy] = {
            "cpg_nodes_v1": "delta_output",
            "cst_refs_norm_v1": "none",
        }

        result = _resolve_cache_policy_hierarchy(
            explicit_policy=None,
            compiled_policy=compiled,
        )

        assert result is compiled

    @staticmethod
    def test_empty_mapping_when_both_none() -> None:
        """Verify empty mapping when both explicit and compiled are None."""
        result = _resolve_cache_policy_hierarchy(
            explicit_policy=None,
            compiled_policy=None,
        )

        assert result == {}

    @staticmethod
    def test_explicit_overrides_both_compiled_and_inferred() -> None:
        """Verify explicit_policy overrides compiled_policy and inferred policy."""
        explicit: dict[str, CachePolicy] = {"cpg_nodes_v1": "delta_output"}
        compiled: dict[str, CachePolicy] = {"cpg_nodes_v1": "none"}

        result = _resolve_cache_policy_hierarchy(
            explicit_policy=explicit,
            compiled_policy=compiled,
        )

        # explicit_policy is returned as-is (identity check).
        assert result is explicit
        assert result["cpg_nodes_v1"] == "delta_output"


@pytest.mark.integration
class TestEntityRegistryEquivalence:
    """Verify that SEMANTIC_TABLE_SPECS derives from entity declarations.

    The entity registry cutover replaced a hand-written dict with
    ``generate_table_specs(ENTITY_DECLARATIONS)``.  These tests confirm
    the cutover preserves key parity.
    """

    @staticmethod
    def test_keys_match() -> None:
        """Verify SEMANTIC_TABLE_SPECS keys match generated keys."""
        generated = generate_table_specs(ENTITY_DECLARATIONS)
        assert set(SEMANTIC_TABLE_SPECS.keys()) == set(generated.keys())

    @staticmethod
    def test_spec_count_matches() -> None:
        """Verify the number of specs matches the number of declarations."""
        generated = generate_table_specs(ENTITY_DECLARATIONS)
        assert len(SEMANTIC_TABLE_SPECS) == len(generated)
        assert len(SEMANTIC_TABLE_SPECS) == len(ENTITY_DECLARATIONS)

    @staticmethod
    def test_spec_values_match() -> None:
        """Verify each generated spec matches the registry spec."""
        generated = generate_table_specs(ENTITY_DECLARATIONS)
        for key, expected_spec in generated.items():
            actual_spec = SEMANTIC_TABLE_SPECS[key]
            assert actual_spec.table == expected_spec.table
            assert actual_spec.primary_span == expected_spec.primary_span
            assert actual_spec.entity_id == expected_spec.entity_id
            assert actual_spec.foreign_keys == expected_spec.foreign_keys

    @staticmethod
    def test_all_declarations_produce_specs() -> None:
        """Verify every entity declaration produces a table spec."""
        generated = generate_table_specs(ENTITY_DECLARATIONS)
        for decl in ENTITY_DECLARATIONS:
            assert decl.source_table in generated, (
                f"Declaration {decl.name!r} (source_table={decl.source_table!r}) "
                f"missing from generated specs"
            )


@pytest.mark.integration
class TestInferredJoinKeysParity:
    """Verify join key inference from field sets.

    ``_infer_join_keys_from_fields`` infers equi-join key pairs from
    field presence, prioritizing FILE_IDENTITY > SPAN > SYMBOL groups.
    """

    @staticmethod
    def test_file_identity_fields_produce_keys() -> None:
        """Verify FILE_IDENTITY fields (file_id, path) produce join keys."""
        left = frozenset({"file_id", "path", "bstart", "bend"})
        right = frozenset({"file_id", "path", "symbol"})

        result = _infer_join_keys_from_fields(left, right)

        assert result is not None
        # file_id and path are in FILE_IDENTITY priority group.
        key_names = {pair[0] for pair in result}
        assert "file_id" in key_names
        assert "path" in key_names

    @staticmethod
    def test_empty_field_sets_return_none() -> None:
        """Verify empty field sets produce no join keys."""
        result = _infer_join_keys_from_fields(frozenset(), frozenset())
        assert result is None

    @staticmethod
    def test_disjoint_fields_return_none() -> None:
        """Verify disjoint field sets produce no join keys."""
        left = frozenset({"file_id", "bstart"})
        right = frozenset({"symbol", "qname"})

        result = _infer_join_keys_from_fields(left, right)
        assert result is None

    @staticmethod
    def test_span_fields_produce_keys() -> None:
        """Verify span fields (bstart, bend) produce join keys when shared."""
        left = frozenset({"bstart", "bend"})
        right = frozenset({"bstart", "bend"})

        result = _infer_join_keys_from_fields(left, right)

        assert result is not None
        key_names = {pair[0] for pair in result}
        assert "bstart" in key_names
        assert "bend" in key_names

    @staticmethod
    def test_priority_ordering() -> None:
        """Verify FILE_IDENTITY keys appear before SPAN keys in output."""
        left = frozenset({"file_id", "bstart", "bend", "symbol"})
        right = frozenset({"file_id", "bstart", "bend", "symbol"})

        result = _infer_join_keys_from_fields(left, right)

        assert result is not None
        key_list = [pair[0] for pair in result]
        # file_id (FILE_IDENTITY) should precede bstart (SPAN).
        file_idx = key_list.index("file_id")
        bstart_idx = key_list.index("bstart")
        assert file_idx < bstart_idx


@pytest.mark.integration
class TestInferenceConfidenceFlow:
    """Verify inference confidence types and IR cache hint mapping.

    Confirm that the data structures for inference confidence are
    well-formed and that the IR vocabulary maps correctly to the
    execution layer vocabulary.
    """

    @staticmethod
    def test_inferred_view_properties_has_confidence_field() -> None:
        """Verify InferredViewProperties accepts inference_confidence."""
        conf = high_confidence("test", "value", ("source",))
        props = InferredViewProperties(inference_confidence=conf)

        assert props.inference_confidence is not None
        assert props.inference_confidence.confidence_score >= HIGH_CONFIDENCE_THRESHOLD

    @staticmethod
    def test_high_confidence_clamps_score() -> None:
        """Verify high_confidence clamps score to [0.8, 1.0]."""
        conf = high_confidence("test", "value", ("lineage",), score=0.5)
        assert conf.confidence_score >= HIGH_CONFIDENCE_THRESHOLD

        conf_high = high_confidence("test", "value", ("lineage",), score=1.5)
        assert conf_high.confidence_score <= 1.0

    @staticmethod
    def test_low_confidence_clamps_score() -> None:
        """Verify low_confidence clamps score to [0.0, 0.49]."""
        conf = low_confidence("test", "value", "not enough evidence", ("stats",), score=0.8)
        assert conf.confidence_score <= LOW_CONFIDENCE_MAX

        conf_low = low_confidence("test", "value", "no evidence", (), score=-1.0)
        assert conf_low.confidence_score >= 0.0

    @staticmethod
    def test_low_confidence_has_fallback_reason() -> None:
        """Verify low_confidence populates fallback_reason."""
        reason = "insufficient evidence"
        conf = low_confidence("test", "value", reason, ())
        assert conf.fallback_reason == reason

    @staticmethod
    def test_ir_cache_hint_eager_maps_to_delta_staging() -> None:
        """Verify 'eager' IR hint maps to 'delta_staging'."""
        assert ir_cache_hint_to_execution_policy("eager") == "delta_staging"

    @staticmethod
    def test_ir_cache_hint_lazy_maps_to_none() -> None:
        """Verify 'lazy' IR hint maps to 'none'."""
        assert ir_cache_hint_to_execution_policy("lazy") == "none"

    @staticmethod
    def test_ir_cache_hint_none_returns_none() -> None:
        """Verify None IR hint returns None."""
        assert ir_cache_hint_to_execution_policy(None) is None

    @staticmethod
    def test_inference_confidence_round_trip() -> None:
        """Verify InferenceConfidence fields are accessible after construction."""
        conf = InferenceConfidence(
            confidence_score=FK_CONFIDENCE,
            evidence_sources=("lineage", "stats"),
            decision_type="join_strategy",
            decision_value="span_overlap",
        )
        assert conf.confidence_score == FK_CONFIDENCE
        assert conf.evidence_sources == ("lineage", "stats")
        assert conf.decision_type == "join_strategy"
        assert conf.decision_value == "span_overlap"
        assert conf.fallback_reason is None


@pytest.mark.integration
class TestCalibrationIntegration:
    """Verify closed-loop policy calibration behavior.

    Test that the calibrator produces expected results in each mode
    and that default bounds validate cleanly.
    """

    @staticmethod
    def test_apply_mode_produces_adjusted_thresholds() -> None:
        """Verify 'apply' mode adjusts thresholds from execution metrics."""
        metrics = ExecutionMetricsSummary(
            predicted_cost=100.0,
            actual_cost=200.0,
            observation_count=10,
        )
        thresholds = CalibrationThresholds()
        result = calibrate_from_execution_metrics(
            metrics=metrics,
            current_thresholds=thresholds,
            bounds=DEFAULT_CALIBRATION_BOUNDS,
            mode="apply",
        )

        assert result.mode == "apply"
        # Under-estimation (actual > predicted) should raise thresholds.
        assert result.cost_ratio is not None
        assert result.cost_ratio > 1.0
        assert result.calibration_confidence.confidence_score > 0.0

    @staticmethod
    def test_off_mode_returns_unchanged() -> None:
        """Verify 'off' mode returns thresholds unchanged."""
        metrics = ExecutionMetricsSummary(
            predicted_cost=100.0,
            actual_cost=200.0,
            observation_count=10,
        )
        thresholds = CalibrationThresholds(
            high_fanout_threshold=CUSTOM_HIGH_FANOUT_THRESHOLD,
            small_table_row_threshold=CUSTOM_ROW_THRESHOLD_SMALL,
            large_table_row_threshold=CUSTOM_ROW_THRESHOLD_LARGE,
        )
        result = calibrate_from_execution_metrics(
            metrics=metrics,
            current_thresholds=thresholds,
            bounds=DEFAULT_CALIBRATION_BOUNDS,
            mode="off",
        )

        assert result.mode == "off"
        assert result.adjusted_thresholds.high_fanout_threshold == CUSTOM_HIGH_FANOUT_THRESHOLD
        assert result.adjusted_thresholds.small_table_row_threshold == CUSTOM_ROW_THRESHOLD_SMALL
        assert result.adjusted_thresholds.large_table_row_threshold == CUSTOM_ROW_THRESHOLD_LARGE

    @staticmethod
    def test_observe_mode_computes_without_committing() -> None:
        """Verify 'observe' mode computes adjustments without side effects."""
        metrics = ExecutionMetricsSummary(
            predicted_cost=100.0,
            actual_cost=50.0,
            observation_count=3,
        )
        result = calibrate_from_execution_metrics(
            metrics=metrics,
            current_thresholds=CalibrationThresholds(),
            bounds=DEFAULT_CALIBRATION_BOUNDS,
            mode="observe",
        )

        assert result.mode == "observe"
        assert result.cost_ratio is not None
        assert result.cost_ratio < 1.0

    @staticmethod
    def test_default_calibration_bounds_validate() -> None:
        """Verify DEFAULT_CALIBRATION_BOUNDS passes validation."""
        errors = validate_calibration_bounds(DEFAULT_CALIBRATION_BOUNDS)
        assert errors == []

    @staticmethod
    def test_invalid_bounds_raise_on_calibrate() -> None:
        """Verify invalid bounds cause ValueError on calibrate."""
        bad_bounds = CalibrationBounds(
            min_high_fanout_threshold=10,
            max_high_fanout_threshold=1,
        )
        metrics = ExecutionMetricsSummary(
            predicted_cost=100.0,
            actual_cost=100.0,
            observation_count=1,
        )
        with pytest.raises(ValueError, match="Invalid calibration bounds"):
            calibrate_from_execution_metrics(
                metrics=metrics,
                current_thresholds=CalibrationThresholds(),
                bounds=bad_bounds,
                mode="apply",
            )

    @staticmethod
    def test_high_observation_count_yields_high_confidence() -> None:
        """Verify sufficient observations produce high-confidence calibration."""
        metrics = ExecutionMetricsSummary(
            predicted_cost=100.0,
            actual_cost=110.0,
            observation_count=20,
            mean_duration_ms=50.0,
            mean_row_count=1000.0,
        )
        result = calibrate_from_execution_metrics(
            metrics=metrics,
            current_thresholds=CalibrationThresholds(),
            bounds=DEFAULT_CALIBRATION_BOUNDS,
            mode="apply",
        )

        assert result.calibration_confidence.confidence_score >= HIGH_CONFIDENCE_THRESHOLD


__all__ = [
    "TestCachePolicyHierarchy",
    "TestCalibrationIntegration",
    "TestEntityRegistryEquivalence",
    "TestInferenceConfidenceFlow",
    "TestInferredJoinKeysParity",
]
