"""Integration tests for semantic pipeline.

Tests end-to-end behavior of the semantic pipeline components including:
- SemanticCompiler normalization
- Input validation
- Join strategy inference
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pyarrow as pa
import pytest

from semantics.input_registry import require_semantic_inputs, validate_semantic_inputs
from semantics.joins import JoinStrategyType, infer_join_strategy
from semantics.naming import SEMANTIC_OUTPUT_NAMES, canonical_output_name
from semantics.types import AnnotatedSchema
from semantics.validation import validate_semantic_input_columns

if TYPE_CHECKING:
    from datafusion import SessionContext


@pytest.fixture
def datafusion_session() -> SessionContext:
    """Create a clean DataFusion session for testing.

    Returns
    -------
    SessionContext
        Fresh DataFusion session context.
    """
    from datafusion import SessionContext as DFSessionContext

    return DFSessionContext()


@pytest.mark.integration
class TestSemanticPipelineIntegration:
    """Integration tests for end-to-end semantic pipeline."""

    def test_semantic_input_validation_detects_missing_tables(
        self,
        datafusion_session: SessionContext,
    ) -> None:
        """Verify input validation detects missing tables."""
        result = validate_semantic_inputs(datafusion_session)

        # Without extraction tables, validation should fail
        assert not result.valid
        assert len(result.missing_required) > 0
        # Verify expected required tables are reported missing
        assert "cst_refs" in result.missing_required
        assert "scip_occurrences" in result.missing_required

    def test_semantic_input_validation_resolves_present_tables(
        self,
        datafusion_session: SessionContext,
    ) -> None:
        """Verify input validation resolves tables when present."""
        from tests.test_helpers.arrow_seed import register_arrow_table

        # Register a required table
        test_data = pa.table(
            {"file_id": ["file-1"], "path": ["test.py"], "bstart": [0], "bend": [50]}
        )
        register_arrow_table(datafusion_session, name="cst_refs", value=test_data)

        result = validate_semantic_inputs(datafusion_session)

        # cst_refs should be resolved now
        assert "cst_refs" not in result.missing_required
        assert result.resolved_names.get("cst_refs") == "cst_refs"
        # But still invalid because other required tables are missing
        assert not result.valid

    def test_semantic_input_validation_uses_fallback_dataset_names(
        self,
        datafusion_session: SessionContext,
    ) -> None:
        """Verify fallback dataset names resolve for extract outputs."""
        from tests.test_helpers.arrow_seed import register_arrow_table

        test_data = pa.table({"path": ["test.py"]})
        register_arrow_table(datafusion_session, name="cst_refs_v1", value=test_data)

        result = validate_semantic_inputs(datafusion_session)
        assert result.resolved_names.get("cst_refs") == "cst_refs_v1"

    def test_require_semantic_inputs_raises_when_missing(
        self,
        datafusion_session: SessionContext,
    ) -> None:
        """Verify require_semantic_inputs raises when required inputs are missing."""
        with pytest.raises(ValueError, match="Missing required semantic inputs"):
            require_semantic_inputs(datafusion_session)

    def test_semantic_inputs_validate_when_tables_present(
        self,
        datafusion_session: SessionContext,
    ) -> None:
        """Verify semantic input validation passes with required tables + columns."""
        from tests.test_helpers.arrow_seed import register_arrow_table

        def _table(columns: tuple[str, ...]) -> pa.Table:
            data: dict[str, list[object]] = {}
            for name in columns:
                if name in {
                    "bstart",
                    "bend",
                    "def_bstart",
                    "def_bend",
                    "alias_bstart",
                    "alias_bend",
                    "call_bstart",
                    "call_bend",
                    "start_line",
                    "end_line",
                    "start_char",
                    "end_char",
                    "line_base",
                    "line_no",
                    "line_start_byte",
                }:
                    data[name] = [1]
                else:
                    data[name] = ["x"]
            return pa.table(data)

        register_arrow_table(
            datafusion_session,
            name="cst_refs",
            value=_table(("file_id", "path", "bstart", "bend", "ref_text")),
        )
        register_arrow_table(
            datafusion_session,
            name="cst_defs",
            value=_table(("file_id", "path", "def_bstart", "def_bend")),
        )
        register_arrow_table(
            datafusion_session,
            name="cst_imports",
            value=_table(("file_id", "path", "alias_bstart", "alias_bend")),
        )
        register_arrow_table(
            datafusion_session,
            name="cst_callsites",
            value=_table(("file_id", "path", "call_bstart", "call_bend")),
        )
        register_arrow_table(
            datafusion_session,
            name="cst_call_args",
            value=_table(("file_id", "path", "bstart", "bend", "arg_text")),
        )
        register_arrow_table(
            datafusion_session,
            name="cst_docstrings",
            value=_table(("file_id", "path", "bstart", "bend", "docstring")),
        )
        register_arrow_table(
            datafusion_session,
            name="cst_decorators",
            value=_table(("file_id", "path", "bstart", "bend", "decorator_text")),
        )
        register_arrow_table(
            datafusion_session,
            name="scip_occurrences",
            value=_table(
                (
                    "path",
                    "symbol",
                    "start_line",
                    "end_line",
                    "start_char",
                    "end_char",
                    "line_base",
                    "col_unit",
                )
            ),
        )
        register_arrow_table(
            datafusion_session,
            name="file_line_index_v1",
            value=_table(("path", "line_no", "line_start_byte", "line_text")),
        )

        resolved = require_semantic_inputs(datafusion_session)
        validation = validate_semantic_input_columns(
            datafusion_session,
            input_mapping=resolved,
        )
        assert validation.valid

    def test_canonical_naming_consistency(self) -> None:
        """Verify naming module exports expected canonical names."""
        # Check expected mappings exist
        assert "scip_occurrences_norm" in SEMANTIC_OUTPUT_NAMES
        assert "rel_name_symbol" in SEMANTIC_OUTPUT_NAMES
        assert "cpg_nodes" in SEMANTIC_OUTPUT_NAMES
        assert "cpg_edges" in SEMANTIC_OUTPUT_NAMES

        # Verify _v1 suffix applied
        assert canonical_output_name("scip_occurrences_norm") == "scip_occurrences_norm_v1"
        assert canonical_output_name("rel_name_symbol") == "rel_name_symbol_v1"
        assert canonical_output_name("cpg_nodes") == "cpg_nodes_v1"

        # Verify unknown names pass through unchanged
        assert canonical_output_name("unknown_table") == "unknown_table"

    def test_join_strategy_inference_span_overlap(self) -> None:
        """Verify join inference produces SPAN_OVERLAP for span-capable schemas."""
        # Create schemas with file_id + spans
        left_schema = pa.schema(
            [
                ("file_id", pa.string()),
                ("bstart", pa.int64()),
                ("bend", pa.int64()),
            ]
        )
        right_schema = pa.schema(
            [
                ("file_id", pa.string()),
                ("bstart", pa.int64()),
                ("bend", pa.int64()),
            ]
        )

        left = AnnotatedSchema.from_arrow_schema(left_schema)
        right = AnnotatedSchema.from_arrow_schema(right_schema)

        strategy = infer_join_strategy(left, right)

        assert strategy is not None
        assert strategy.strategy_type == JoinStrategyType.SPAN_OVERLAP

    def test_join_strategy_inference_file_equi_join(self) -> None:
        """Verify join inference falls back to EQUI_JOIN when only file_id present."""
        # Create schemas with file_id only (no spans)
        left_schema = pa.schema(
            [
                ("file_id", pa.string()),
                ("name", pa.string()),
            ]
        )
        right_schema = pa.schema(
            [
                ("file_id", pa.string()),
                ("value", pa.int64()),
            ]
        )

        left = AnnotatedSchema.from_arrow_schema(left_schema)
        right = AnnotatedSchema.from_arrow_schema(right_schema)

        strategy = infer_join_strategy(left, right)

        assert strategy is not None
        assert strategy.strategy_type == JoinStrategyType.EQUI_JOIN

    def test_join_strategy_inference_no_common_keys(self) -> None:
        """Verify join inference returns None when no common join keys."""
        # Create schemas with no common join keys
        left_schema = pa.schema(
            [
                ("name", pa.string()),
                ("value", pa.int64()),
            ]
        )
        right_schema = pa.schema(
            [
                ("other_name", pa.string()),
                ("other_value", pa.int64()),
            ]
        )

        left = AnnotatedSchema.from_arrow_schema(left_schema)
        right = AnnotatedSchema.from_arrow_schema(right_schema)

        strategy = infer_join_strategy(left, right)

        assert strategy is None

    def test_join_strategy_with_hint(self) -> None:
        """Verify join inference respects strategy hints."""
        left_schema = pa.schema(
            [
                ("file_id", pa.string()),
                ("bstart", pa.int64()),
                ("bend", pa.int64()),
            ]
        )
        right_schema = pa.schema(
            [
                ("file_id", pa.string()),
                ("bstart", pa.int64()),
                ("bend", pa.int64()),
            ]
        )

        left = AnnotatedSchema.from_arrow_schema(left_schema)
        right = AnnotatedSchema.from_arrow_schema(right_schema)

        # Request SPAN_CONTAINS instead of default SPAN_OVERLAP
        strategy = infer_join_strategy(left, right, hint=JoinStrategyType.SPAN_CONTAINS)

        assert strategy is not None
        assert strategy.strategy_type == JoinStrategyType.SPAN_CONTAINS

    def test_annotated_schema_from_arrow(self) -> None:
        """Verify AnnotatedSchema correctly annotates Arrow schema."""
        from semantics.types import SemanticType

        schema = pa.schema(
            [
                ("entity_id", pa.string()),
                ("file_id", pa.string()),
                ("bstart", pa.int64()),
                ("bend", pa.int64()),
                ("symbol", pa.string()),
            ]
        )

        annotated = AnnotatedSchema.from_arrow_schema(schema)

        assert len(annotated) == 5
        assert annotated.has_semantic_type(SemanticType.ENTITY_ID)
        assert annotated.has_semantic_type(SemanticType.FILE_ID)
        assert annotated.has_semantic_type(SemanticType.SPAN_START)
        assert annotated.has_semantic_type(SemanticType.SPAN_END)
        assert "entity_id" in annotated
        assert "file_id" in annotated

    def test_annotated_schema_join_key_inference(self) -> None:
        """Verify AnnotatedSchema can infer join keys between schemas."""
        left_schema = pa.schema(
            [
                ("file_id", pa.string()),
                ("bstart", pa.int64()),
            ]
        )
        right_schema = pa.schema(
            [
                ("file_id", pa.string()),
                ("bend", pa.int64()),
            ]
        )

        left = AnnotatedSchema.from_arrow_schema(left_schema)
        right = AnnotatedSchema.from_arrow_schema(right_schema)

        join_pairs = left.infer_join_keys(right)

        # Should find file_id as a common join key
        assert len(join_pairs) > 0
        file_id_pairs = [p for p in join_pairs if p[0] == "file_id" and p[1] == "file_id"]
        assert len(file_id_pairs) == 1
