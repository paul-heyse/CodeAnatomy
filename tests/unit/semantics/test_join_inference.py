"""Unit tests for join key inference in the semantic compiler."""

# ruff: noqa: SLF001

from __future__ import annotations

import pytest
from datafusion import SessionContext

from semantics.compiler import SemanticCompiler, TableInfo
from semantics.exprs import c
from semantics.quality import (
    QualityRelationshipSpec,
    SelectExpr,
    SignalsSpec,
)
from semantics.schema import SemanticSchemaError

EXPECTED_INNER_JOIN_MATCH_COUNT = 2


class TestResolveJoinKeysStatic:
    """Tests for _resolve_join_keys static method via TableInfo objects."""

    def test_explicit_keys_returned_unchanged(self) -> None:
        """Explicit left_on/right_on bypass inference and pass through."""
        ctx = SessionContext()
        left_df = ctx.from_pydict({"file_id": ["f1"], "entity_id": ["e1"]}, name="left_t")
        right_df = ctx.from_pydict({"file_id": ["f1"], "symbol": ["sym"]}, name="right_t")
        left_info = TableInfo.analyze("left_t", left_df)
        right_info = TableInfo.analyze("right_t", right_df)

        left_on, right_on = SemanticCompiler._resolve_join_keys(
            left_info=left_info,
            right_info=right_info,
            left_on=["file_id"],
            right_on=["file_id"],
        )
        assert list(left_on) == ["file_id"]
        assert list(right_on) == ["file_id"]

    def test_infers_file_id_when_both_empty(self) -> None:
        """Infer file_id as join key when left_on and right_on are both empty."""
        ctx = SessionContext()
        left_df = ctx.from_pydict(
            {"file_id": ["f1"], "bstart": [0], "bend": [10]}, name="left_infer"
        )
        right_df = ctx.from_pydict(
            {"file_id": ["f1"], "bstart": [0], "bend": [10]}, name="right_infer"
        )
        left_info = TableInfo.analyze("left_infer", left_df)
        right_info = TableInfo.analyze("right_infer", right_df)

        left_on, right_on = SemanticCompiler._resolve_join_keys(
            left_info=left_info,
            right_info=right_info,
            left_on=[],
            right_on=[],
        )
        # file_id is the only FILE_IDENTITY column shared; bstart/bend are SPAN_POSITION
        assert "file_id" in list(left_on)
        assert "file_id" in list(right_on)
        # Span columns must NOT appear (they are SPAN_POSITION, not FILE_IDENTITY)
        assert "bstart" not in list(left_on)
        assert "bend" not in list(left_on)

    def test_infers_file_id_and_path(self) -> None:
        """Infer both file_id and path when both are present on both sides."""
        ctx = SessionContext()
        left_df = ctx.from_pydict(
            {"file_id": ["f1"], "path": ["/a.py"], "entity_id": ["e1"]}, name="left_both"
        )
        right_df = ctx.from_pydict(
            {"file_id": ["f1"], "path": ["/a.py"], "symbol": ["sym"]}, name="right_both"
        )
        left_info = TableInfo.analyze("left_both", left_df)
        right_info = TableInfo.analyze("right_both", right_df)

        left_on, right_on = SemanticCompiler._resolve_join_keys(
            left_info=left_info,
            right_info=right_info,
            left_on=[],
            right_on=[],
        )
        left_list = list(left_on)
        right_list = list(right_on)
        assert "file_id" in left_list
        assert "path" in left_list
        assert "file_id" in right_list
        assert "path" in right_list

    def test_asymmetric_keys_raises(self) -> None:
        """Error when only one of left_on/right_on is provided."""
        ctx = SessionContext()
        left_df = ctx.from_pydict({"file_id": ["f1"]}, name="left_asym")
        right_df = ctx.from_pydict({"file_id": ["f1"]}, name="right_asym")
        left_info = TableInfo.analyze("left_asym", left_df)
        right_info = TableInfo.analyze("right_asym", right_df)

        with pytest.raises(SemanticSchemaError, match="symmetric"):
            SemanticCompiler._resolve_join_keys(
                left_info=left_info,
                right_info=right_info,
                left_on=["file_id"],
                right_on=[],
            )

    def test_asymmetric_right_only_raises(self) -> None:
        """Error when only right_on is provided."""
        ctx = SessionContext()
        left_df = ctx.from_pydict({"file_id": ["f1"]}, name="left_asym2")
        right_df = ctx.from_pydict({"file_id": ["f1"]}, name="right_asym2")
        left_info = TableInfo.analyze("left_asym2", left_df)
        right_info = TableInfo.analyze("right_asym2", right_df)

        with pytest.raises(SemanticSchemaError, match="symmetric"):
            SemanticCompiler._resolve_join_keys(
                left_info=left_info,
                right_info=right_info,
                left_on=[],
                right_on=["file_id"],
            )

    def test_no_file_identity_columns_raises(self) -> None:
        """Error when schemas share no FILE_IDENTITY columns."""
        ctx = SessionContext()
        left_df = ctx.from_pydict({"x": [1], "y": [2]}, name="left_no_fid")
        right_df = ctx.from_pydict({"a": [1], "b": [2]}, name="right_no_fid")
        left_info = TableInfo.analyze("left_no_fid", left_df)
        right_info = TableInfo.analyze("right_no_fid", right_df)

        with pytest.raises(SemanticSchemaError, match="FILE_IDENTITY"):
            SemanticCompiler._resolve_join_keys(
                left_info=left_info,
                right_info=right_info,
                left_on=[],
                right_on=[],
            )

    def test_only_span_columns_raises(self) -> None:
        """Error when only SPAN_POSITION columns are shared (no FILE_IDENTITY)."""
        ctx = SessionContext()
        left_df = ctx.from_pydict({"bstart": [0], "bend": [10]}, name="left_span_only")
        right_df = ctx.from_pydict({"bstart": [0], "bend": [10]}, name="right_span_only")
        left_info = TableInfo.analyze("left_span_only", left_df)
        right_info = TableInfo.analyze("right_span_only", right_df)

        with pytest.raises(SemanticSchemaError, match="FILE_IDENTITY"):
            SemanticCompiler._resolve_join_keys(
                left_info=left_info,
                right_info=right_info,
                left_on=[],
                right_on=[],
            )


class TestJoinKeyInferenceEndToEnd:
    """Tests for join key inference through compile_relationship_with_quality."""

    def test_inferred_join_produces_correct_result(self) -> None:
        """Compiler infers file_id and produces correct joined output."""
        ctx = SessionContext()
        ctx.from_pydict(
            {
                "file_id": ["f1"],
                "path": ["a.py"],
                "bstart": [0],
                "bend": [10],
                "entity_id": ["e1"],
            },
            name="left_e2e",
        )
        ctx.from_pydict(
            {
                "file_id": ["f1"],
                "symbol": ["sym_a"],
                "bstart": [0],
                "bend": [10],
            },
            name="right_e2e",
        )
        ctx.from_pydict(
            {"file_id": ["f1"], "file_quality_score": [800.0]},
            name="file_quality_v1",
        )
        compiler = SemanticCompiler(ctx=ctx)
        spec = QualityRelationshipSpec(
            name="test_inferred_e2e",
            left_view="left_e2e",
            right_view="right_e2e",
            # left_on and right_on intentionally empty (inference)
            how="inner",
            provider="test",
            origin="test_origin",
            signals=SignalsSpec(base_score=100),
            select_exprs=[
                SelectExpr(c("l__entity_id"), "entity_id"),
                SelectExpr(c("r__symbol"), "symbol"),
            ],
        )
        df = compiler.compile_relationship_with_quality(
            spec,
            file_quality_df=ctx.table("file_quality_v1"),
        )
        result = df.collect()[0]
        entity_ids = result["entity_id"].to_pylist()
        symbols = result["symbol"].to_pylist()
        assert entity_ids == ["e1"]
        assert symbols == ["sym_a"]

    def test_no_common_file_id_raises_through_public_api(self) -> None:
        """Error propagates through public API when no FILE_IDENTITY columns."""
        ctx = SessionContext()
        ctx.from_pydict({"x": [1], "y": [2]}, name="left_no_fileid")
        ctx.from_pydict({"a": [1], "b": [2]}, name="right_no_fileid")
        compiler = SemanticCompiler(ctx=ctx)
        spec = QualityRelationshipSpec(
            name="test_no_keys",
            left_view="left_no_fileid",
            right_view="right_no_fileid",
            how="inner",
        )
        with pytest.raises(SemanticSchemaError, match="FILE_IDENTITY"):
            compiler.compile_relationship_with_quality(spec)

    def test_explicit_keys_still_work(self) -> None:
        """Explicit left_on/right_on bypass inference through public API."""
        ctx = SessionContext()
        ctx.from_pydict(
            {"file_id": ["f1"], "entity_id": ["e1"]},
            name="left_explicit",
        )
        ctx.from_pydict(
            {"file_id": ["f1"], "symbol": ["sym"]},
            name="right_explicit",
        )
        ctx.from_pydict(
            {"file_id": ["f1"], "file_quality_score": [800.0]},
            name="file_quality_v1",
        )
        compiler = SemanticCompiler(ctx=ctx)
        spec = QualityRelationshipSpec(
            name="test_explicit",
            left_view="left_explicit",
            right_view="right_explicit",
            left_on=["file_id"],
            right_on=["file_id"],
            how="inner",
            provider="test",
            origin="test_origin",
            signals=SignalsSpec(base_score=100),
            select_exprs=[
                SelectExpr(c("l__entity_id"), "entity_id"),
                SelectExpr(c("r__symbol"), "symbol"),
            ],
        )
        df = compiler.compile_relationship_with_quality(
            spec,
            file_quality_df=ctx.table("file_quality_v1"),
        )
        result = df.collect()[0]
        assert len(result["entity_id"].to_pylist()) == 1

    def test_multi_row_inferred_join(self) -> None:
        """Inferred join correctly matches on file_id with multiple rows."""
        ctx = SessionContext()
        ctx.from_pydict(
            {
                "file_id": ["f1", "f2"],
                "entity_id": ["e1", "e2"],
            },
            name="left_multi",
        )
        ctx.from_pydict(
            {
                "file_id": ["f1", "f2", "f3"],
                "symbol": ["sym_1", "sym_2", "sym_3"],
            },
            name="right_multi",
        )
        ctx.from_pydict(
            {"file_id": ["f1", "f2", "f3"], "file_quality_score": [800.0, 800.0, 800.0]},
            name="file_quality_v1",
        )
        compiler = SemanticCompiler(ctx=ctx)
        spec = QualityRelationshipSpec(
            name="test_multi_row",
            left_view="left_multi",
            right_view="right_multi",
            how="inner",
            provider="test",
            origin="test_origin",
            signals=SignalsSpec(base_score=100),
            select_exprs=[
                SelectExpr(c("l__entity_id"), "entity_id"),
                SelectExpr(c("r__symbol"), "symbol"),
            ],
        )
        df = compiler.compile_relationship_with_quality(
            spec,
            file_quality_df=ctx.table("file_quality_v1"),
        )
        result = df.collect()[0]
        # Inner join on file_id: f1->sym_1, f2->sym_2 (f3 has no left match)
        assert len(result["entity_id"].to_pylist()) == EXPECTED_INNER_JOIN_MATCH_COUNT


class TestIRInferredKeysParityWithCompiler:
    """Verify IR-level inferred keys match compiler _resolve_join_keys output.

    The IR pipeline's ``_resolve_keys_from_inferred`` must produce keys
    consistent with the compiler's ``_resolve_join_keys`` so that join
    group optimization groups specs the same way the compiler would
    resolve them at runtime.
    """

    def test_file_id_parity(self) -> None:
        """IR inferred keys match compiler resolution for file_id columns."""
        from semantics.ir import InferredViewProperties, SemanticIRView
        from semantics.ir_pipeline import _resolve_keys_from_inferred

        ctx = SessionContext()
        left_df = ctx.from_pydict(
            {"file_id": ["f1"], "bstart": [0], "bend": [10]},
            name="left_parity",
        )
        right_df = ctx.from_pydict(
            {"file_id": ["f1"], "bstart": [0], "bend": [10]},
            name="right_parity",
        )
        left_info = TableInfo.analyze("left_parity", left_df)
        right_info = TableInfo.analyze("right_parity", right_df)

        # Compiler resolution
        compiler_left, compiler_right = SemanticCompiler._resolve_join_keys(
            left_info=left_info,
            right_info=right_info,
            left_on=[],
            right_on=[],
        )

        # IR-level inference: simulate what _infer_join_keys_from_fields produces
        # then verify _resolve_keys_from_inferred extracts the same FILE_IDENTITY keys
        from semantics.ir_pipeline import _infer_join_keys_from_fields

        left_fields = frozenset(left_df.schema().names)
        right_fields = frozenset(right_df.schema().names)
        inferred_keys = _infer_join_keys_from_fields(left_fields, right_fields)
        assert inferred_keys is not None

        view = SemanticIRView(
            name="test_rel",
            kind="relate",
            inputs=("left_parity", "right_parity"),
            outputs=("test_rel",),
            inferred_properties=InferredViewProperties(
                inferred_join_keys=inferred_keys,
            ),
        )
        ir_result = _resolve_keys_from_inferred(view)
        assert ir_result is not None
        ir_left, ir_right = ir_result

        # Parity: both should resolve to the same FILE_IDENTITY keys
        assert set(ir_left) == set(compiler_left)
        assert set(ir_right) == set(compiler_right)

    def test_file_id_and_path_parity(self) -> None:
        """IR inferred keys match compiler for file_id + path columns."""
        from semantics.ir import InferredViewProperties, SemanticIRView
        from semantics.ir_pipeline import (
            _infer_join_keys_from_fields,
            _resolve_keys_from_inferred,
        )

        ctx = SessionContext()
        left_df = ctx.from_pydict(
            {"file_id": ["f1"], "path": ["/a.py"], "entity_id": ["e1"]},
            name="left_parity2",
        )
        right_df = ctx.from_pydict(
            {"file_id": ["f1"], "path": ["/a.py"], "symbol": ["sym"]},
            name="right_parity2",
        )
        left_info = TableInfo.analyze("left_parity2", left_df)
        right_info = TableInfo.analyze("right_parity2", right_df)

        compiler_left, compiler_right = SemanticCompiler._resolve_join_keys(
            left_info=left_info,
            right_info=right_info,
            left_on=[],
            right_on=[],
        )

        left_fields = frozenset(left_df.schema().names)
        right_fields = frozenset(right_df.schema().names)
        inferred_keys = _infer_join_keys_from_fields(left_fields, right_fields)
        assert inferred_keys is not None

        view = SemanticIRView(
            name="test_rel2",
            kind="relate",
            inputs=("left_parity2", "right_parity2"),
            outputs=("test_rel2",),
            inferred_properties=InferredViewProperties(
                inferred_join_keys=inferred_keys,
            ),
        )
        ir_result = _resolve_keys_from_inferred(view)
        assert ir_result is not None
        ir_left, ir_right = ir_result

        assert set(ir_left) == set(compiler_left)
        assert set(ir_right) == set(compiler_right)

    def test_no_file_identity_parity(self) -> None:
        """Both IR and compiler fail gracefully when no FILE_IDENTITY columns."""
        from semantics.ir import InferredViewProperties, SemanticIRView
        from semantics.ir_pipeline import (
            _infer_join_keys_from_fields,
            _resolve_keys_from_inferred,
        )

        ctx = SessionContext()
        left_df = ctx.from_pydict({"x": [1], "y": [2]}, name="left_no_fid2")
        right_df = ctx.from_pydict({"x": [1], "y": [2]}, name="right_no_fid2")
        left_info = TableInfo.analyze("left_no_fid2", left_df)
        right_info = TableInfo.analyze("right_no_fid2", right_df)

        # Compiler raises on no FILE_IDENTITY columns
        with pytest.raises(SemanticSchemaError, match="FILE_IDENTITY"):
            SemanticCompiler._resolve_join_keys(
                left_info=left_info,
                right_info=right_info,
                left_on=[],
                right_on=[],
            )

        # IR-level: _infer_join_keys_from_fields finds common columns
        # but _resolve_keys_from_inferred filters to FILE_IDENTITY only
        left_fields = frozenset(left_df.schema().names)
        right_fields = frozenset(right_df.schema().names)
        inferred_keys = _infer_join_keys_from_fields(left_fields, right_fields)

        if inferred_keys is not None:
            view = SemanticIRView(
                name="test_no_fid",
                kind="relate",
                inputs=("left_no_fid2", "right_no_fid2"),
                outputs=("test_no_fid",),
                inferred_properties=InferredViewProperties(
                    inferred_join_keys=inferred_keys,
                ),
            )
            # IR returns None (graceful degradation) instead of raising
            assert _resolve_keys_from_inferred(view) is None
