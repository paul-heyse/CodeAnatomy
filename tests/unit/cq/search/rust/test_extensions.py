"""Tests for rust extensions (merged from macro_expansion_bridge + module_graph_builder)."""

from __future__ import annotations

from tools.cq.search.rust.contracts import RustMacroExpansionRequestV1
from tools.cq.search.rust.extensions import build_module_graph, expand_macro, expand_macros

BATCH_EXPANSION_RESULT_COUNT = 2


class _FakeClient:
    """Fake LSP client for macro expansion tests."""

    @staticmethod
    def request(method: str, payload: dict[str, object]) -> dict[str, object]:
        """Return a canned macro expansion response."""
        assert method == "rust-analyzer/expandMacro"
        assert "textDocument" in payload
        return {
            "result": {
                "name": "sql",
                "expansion": "SELECT 1",
            }
        }


class TestExpandMacro:
    """Tests for expand_macro."""

    @staticmethod
    def test_returns_applied_result() -> None:
        """Verify applied result from a real client."""
        request = RustMacroExpansionRequestV1(
            file_path="src/lib.rs",
            line=9,
            col=4,
            macro_call_id="src/lib.rs:9:4:sql",
        )
        result = expand_macro(_FakeClient(), request)
        assert result.applied is True
        assert result.name == "sql"
        assert result.expansion == "SELECT 1"

    @staticmethod
    def test_fails_open_without_client_request() -> None:
        """Verify graceful degradation when client lacks request method."""
        request = RustMacroExpansionRequestV1(
            file_path="src/lib.rs",
            line=1,
            col=0,
            macro_call_id="id",
        )
        result = expand_macro(object(), request)
        assert result.applied is False
        assert result.name is None


class TestExpandMacros:
    """Tests for expand_macros batch helper."""

    @staticmethod
    def test_batch_expansion() -> None:
        """Verify batch expansion returns tuple of results."""
        requests = (
            RustMacroExpansionRequestV1(file_path="src/lib.rs", line=1, col=0, macro_call_id="id1"),
            RustMacroExpansionRequestV1(file_path="src/lib.rs", line=2, col=0, macro_call_id="id2"),
        )
        results = expand_macros(client=_FakeClient(), requests=requests)
        assert len(results) == BATCH_EXPANSION_RESULT_COUNT
        assert all(r.applied for r in results)


class TestBuildModuleGraph:
    """Tests for build_module_graph."""

    @staticmethod
    def test_normalizes_nodes_and_edges() -> None:
        """Verify deduplication of module nodes and import edges."""
        graph = build_module_graph(
            module_rows=[
                {"module_name": "core", "module_id": "module:core", "file_path": "src/lib.rs"},
                {"module_name": "core", "module_id": "module:core", "file_path": "src/lib.rs"},
            ],
            import_rows=[
                {
                    "source_module_id": "module:core",
                    "target_path": "core::io::read",
                    "visibility": "public",
                    "is_reexport": True,
                },
                {
                    "source_module_id": "module:core",
                    "target_path": "core::io::read",
                    "visibility": "public",
                    "is_reexport": True,
                },
            ],
        )
        modules = graph.get("modules")
        edges = graph.get("edges")
        metadata = graph.get("metadata")

        assert isinstance(modules, (list, tuple))
        assert isinstance(edges, (list, tuple))
        assert isinstance(metadata, dict)
        assert len(modules) == 1
        assert len(edges) == 1
        assert metadata.get("module_count") == 1
        assert metadata.get("edge_count") == 1

    @staticmethod
    def test_empty_inputs() -> None:
        """Verify build_module_graph handles empty inputs gracefully."""
        graph = build_module_graph(module_rows=[], import_rows=[])
        # msgspec.to_builtins omits fields at default values (empty tuples)
        assert graph.get("modules", ()) == ()
        assert graph.get("edges", ()) == ()
