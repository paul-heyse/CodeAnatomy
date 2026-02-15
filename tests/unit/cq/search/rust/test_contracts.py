"""Tests for rust contracts (merged from macro_expansion_contracts + module_graph_contracts)."""

from __future__ import annotations

from tools.cq.search.rust.contracts import (
    RustImportEdgeV1,
    RustMacroExpansionRequestV1,
    RustMacroExpansionResultV1,
    RustModuleGraphV1,
    RustModuleNodeV1,
)


class TestMacroExpansionContracts:
    """Tests for macro expansion request/result contracts."""

    def test_request_round_trip(self) -> None:
        """Verify request struct construction and field access."""
        req = RustMacroExpansionRequestV1(
            file_path="src/lib.rs",
            line=10,
            col=4,
            macro_call_id="src/lib.rs:10:4:sql",
        )
        assert req.file_path == "src/lib.rs"
        assert req.macro_call_id == "src/lib.rs:10:4:sql"

    def test_result_defaults(self) -> None:
        """Verify result struct defaults."""
        result = RustMacroExpansionResultV1(macro_call_id="id")
        assert result.applied is False
        assert result.name is None
        assert result.expansion is None
        assert result.source == "rust_analyzer"


class TestModuleGraphContracts:
    """Tests for module graph node/edge/graph contracts."""

    def test_module_node_construction(self) -> None:
        """Verify module node construction with optional file_path."""
        node = RustModuleNodeV1(module_id="module:core", module_name="core")
        assert node.file_path is None

    def test_import_edge_defaults(self) -> None:
        """Verify import edge defaults."""
        edge = RustImportEdgeV1(
            source_module_id="module:core",
            target_path="core::io",
        )
        assert edge.visibility == "private"
        assert edge.is_reexport is False

    def test_module_graph_defaults(self) -> None:
        """Verify module graph empty defaults."""
        graph = RustModuleGraphV1()
        assert graph.modules == ()
        assert graph.edges == ()
        assert graph.metadata == {}
