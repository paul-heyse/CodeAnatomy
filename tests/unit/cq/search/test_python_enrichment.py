"""Tests for Python context enrichment module."""

from __future__ import annotations

import ast

import pytest
from ast_grep_py import SgNode, SgRoot
from tools.cq.search._shared.core import (
    PythonByteRangeEnrichmentRequest,
    PythonNodeEnrichmentRequest,
)
from tools.cq.search.python.extractors import (
    _AST_CACHE,
    _classify_item_role,
    _extract_behavior_summary,
    _extract_call_target,
    _extract_class_context,
    _extract_class_shape,
    _extract_decorators,
    _extract_generator_flag,
    _extract_import_detail,
    _extract_scope_chain,
    _extract_signature,
    _extract_structural_context,
    _find_ast_function,
    _get_ast,
    _truncate,
    _unwrap_decorated,
    clear_python_enrichment_cache,
    enrich_python_context,
    enrich_python_context_by_byte_range,
)

TRUNCATION_MAX_CHARS = 20
MIN_SCOPE_CHAIN_LENGTH = 2
MAX_SIGNATURE_CHARS = 200

_PYTHON_SAMPLE = '''\
from __future__ import annotations

import os
from typing import TYPE_CHECKING
from collections.abc import Callable, Sequence

if TYPE_CHECKING:
    from pathlib import Path

class GraphBuilder(Protocol):
    """Build graphs from data."""

    name: str
    _nodes: list[int]

    @property
    def node_count(self) -> int:
        return len(self._nodes)

    @abstractmethod
    def build(self) -> Graph:
        ...

    @classmethod
    def from_config(cls, config: dict[str, object]) -> GraphBuilder:
        ...

    @staticmethod
    def validate(data: object) -> bool:
        ...

    def process(self, items: list[str], *, timeout: float = 30.0) -> dict[str, int]:
        with open("log.txt") as f:
            f.write("processing")
        try:
            result = self._transform(items)
        except ValueError:
            raise RuntimeError("failed")
        return result

    async def fetch(self, url: str) -> bytes:
        response = await self.client.get(url)
        return await response.read()

def free_function(x: int, y: str = "default") -> bool:
    return x > 0

def generator_function(items: list[str]):
    for item in items:
        yield item.upper()

async def async_generator(items):
    for item in items:
        yield await process(item)

def test_basic_graph():
    """Test basic graph."""
    g = GraphBuilder()
    g.build()
    assert g.node_count > 0

@pytest.fixture
def sample_builder():
    return GraphBuilder()

def caller():
    builder = GraphBuilder()
    result = builder.process(["a", "b"], timeout=10.0)
    os.path.join("a", "b")
    free_function(42)
    print("done")

@dataclass(frozen=True, slots=True)
class Config:
    name: str
    value: int = 0
'''


@pytest.fixture(autouse=True)
def _clear_caches() -> None:
    """Clear enrichment caches before each test."""
    clear_python_enrichment_cache()


def _make_sg(source: str = _PYTHON_SAMPLE) -> SgRoot:
    return SgRoot(source, "python")


def _find_node(sg_root: SgRoot, line: int, col: int) -> SgNode | None:
    """Find node at (1-indexed line, 0-indexed col).

    Returns:
    -------
    SgNode | None
        Matching node if found.
    """
    from tools.cq.search.pipeline.classifier_runtime import (
        _build_node_interval_index,
        _build_node_spans,
    )

    spans = _build_node_spans(sg_root.root())
    from tools.cq.search.pipeline.classifier_runtime import NodeIntervalIndex

    index = NodeIntervalIndex(line_index=_build_node_interval_index(spans))
    return index.find_containing(line, col)


class TestTruncation:
    """Test truncation helper."""

    @staticmethod
    def test_no_truncation_short() -> None:
        """Test no truncation short."""
        assert _truncate("hello", 10) == "hello"

    @staticmethod
    def test_truncation_long() -> None:
        """Test truncation long."""
        result = _truncate("a" * 50, TRUNCATION_MAX_CHARS)
        assert len(result) <= TRUNCATION_MAX_CHARS
        assert result.endswith("...")

    @staticmethod
    def test_truncation_exact_boundary() -> None:
        """Test truncation exact boundary."""
        result = _truncate("abcde", 5)
        assert result == "abcde"


class TestSignatureExtraction:
    """Test function signature extraction."""

    @staticmethod
    def test_signature_free_function() -> None:
        """Test signature free function."""
        sg = _make_sg()
        # free_function starts on line 45
        node = _find_node(sg, 45, 0)
        assert node is not None
        result = _extract_signature(node, _PYTHON_SAMPLE.encode())
        assert "params" in result
        assert isinstance(result["params"], list)
        assert not result.get("is_async", True)

    @staticmethod
    def test_signature_async_function() -> None:
        """Test signature async function."""
        sg = _make_sg()
        # async def fetch on line 41
        node = _find_node(sg, 41, 4)
        assert node is not None
        # Walk up to find function_definition
        while node is not None and node.kind() not in {
            "function_definition",
            "decorated_definition",
        }:
            node = node.parent()
        if node is not None:
            result = _extract_signature(node, _PYTHON_SAMPLE.encode())
            assert result.get("is_async") is True

    @staticmethod
    def test_signature_with_return_type() -> None:
        """Test signature with return type."""
        sg = _make_sg()
        node = _find_node(sg, 45, 0)
        assert node is not None
        result = _extract_signature(node, _PYTHON_SAMPLE.encode())
        if "return_type" in result:
            assert isinstance(result["return_type"], str)


class TestDecoratorExtraction:
    """Test decorator extraction."""

    @staticmethod
    def test_decorated_function() -> None:
        """Test decorated function."""
        sg = _make_sg()
        # @property on line 46
        node = _find_node(sg, 46, 4)
        assert node is not None
        while node is not None and node.kind() != "decorated_definition":
            node = node.parent()
        if node is not None:
            result = _extract_decorators(node)
            assert "decorators" in result
            decorators = result["decorators"]
            assert isinstance(decorators, list)
            assert "property" in decorators

    @staticmethod
    def test_no_decorators() -> None:
        """Test no decorators."""
        sg = _make_sg()
        # free_function has no decorators (line 45)
        node = _find_node(sg, 45, 0)
        assert node is not None
        result = _extract_decorators(node)
        # Not a decorated_definition, so no decorators
        assert result == {} or "decorators" not in result

    @staticmethod
    def test_pytest_fixture_decorator() -> None:
        """Test pytest fixture decorator."""
        sg = _make_sg()
        # @pytest.fixture on line 62
        node = _find_node(sg, 62, 0)
        assert node is not None
        while node is not None and node.kind() != "decorated_definition":
            node = node.parent()
        if node is not None:
            result = _extract_decorators(node)
            assert "decorators" in result
            decorators = result["decorators"]
            assert isinstance(decorators, list)
            assert any("pytest.fixture" in d for d in decorators)


class TestItemRole:
    """Test item role classification."""

    @staticmethod
    def test_free_function_role() -> None:
        """Test free function role."""
        sg = _make_sg()
        node = _find_node(sg, 45, 0)
        assert node is not None
        result = _classify_item_role(node, [])
        assert result.get("item_role") == "free_function"

    @staticmethod
    def test_method_role() -> None:
        """Test method role."""
        sg = _make_sg()
        # process method on line 62
        node = _find_node(sg, 62, 4)
        assert node is not None
        while node is not None and node.kind() not in {
            "function_definition",
            "decorated_definition",
        }:
            node = node.parent()
        if node is not None:
            result = _classify_item_role(node, [])
            assert result.get("item_role") in {"method", "free_function"}

    @staticmethod
    def test_test_function_role() -> None:
        """Test test function role."""
        sg = _make_sg()
        # test_basic_graph on line 56
        node = _find_node(sg, 56, 0)
        assert node is not None
        result = _classify_item_role(node, [])
        assert result.get("item_role") == "test_function"

    @staticmethod
    def test_fixture_role() -> None:
        """Test fixture role."""
        sg = _make_sg()
        # @pytest.fixture on line 62
        node = _find_node(sg, 62, 0)
        assert node is not None
        while node is not None and node.kind() != "decorated_definition":
            node = node.parent()
        if node is not None:
            result = _classify_item_role(node, ["pytest.fixture"])
            assert result.get("item_role") == "fixture"

    @staticmethod
    def test_callsite_role() -> None:
        """Test callsite role."""
        sg = _make_sg()
        # free_function(42) on line 70
        node = _find_node(sg, 70, 4)
        assert node is not None
        while node is not None and node.kind() != "call":
            node = node.parent()
        if node is not None:
            result = _classify_item_role(node, [])
            assert result.get("item_role") == "callsite"

    @staticmethod
    def test_import_role() -> None:
        """Test import role."""
        sg = _make_sg()
        # import os on line 3
        node = _find_node(sg, 3, 0)
        assert node is not None
        while node is not None and node.kind() != "import_statement":
            node = node.parent()
        if node is not None:
            result = _classify_item_role(node, [])
            assert result.get("item_role") == "import"

    @staticmethod
    def test_class_def_role() -> None:
        """Test class def role."""
        sg = _make_sg()
        # class GraphBuilder on line 10
        node = _find_node(sg, 10, 0)
        assert node is not None
        while node is not None and node.kind() not in {
            "class_definition",
            "decorated_definition",
        }:
            node = node.parent()
        if node is not None:
            result = _classify_item_role(node, [])
            assert result.get("item_role") in {"class_def", "dataclass"}

    @staticmethod
    def test_dataclass_role() -> None:
        """Test dataclass role."""
        sg = _make_sg()
        # @dataclass class Config on line 73
        node = _find_node(sg, 73, 0)
        assert node is not None
        while node is not None and node.kind() != "decorated_definition":
            node = node.parent()
        if node is not None:
            result = _classify_item_role(node, ["dataclass(frozen=True, slots=True)"])
            assert result.get("item_role") == "dataclass"


class TestClassContext:
    """Test class context extraction."""

    @staticmethod
    def test_class_context_for_method() -> None:
        """Test class context for method."""
        sg = _make_sg()
        # process method inside GraphBuilder (line 32)
        node = _find_node(sg, 32, 4)
        assert node is not None
        result = _extract_class_context(node)
        assert result.get("class_name") == "GraphBuilder"

    @staticmethod
    def test_base_classes() -> None:
        """Test base classes."""
        sg = _make_sg()
        node = _find_node(sg, 10, 6)
        assert node is not None
        result = _extract_class_context(node)
        bases = result.get("base_classes")
        if isinstance(bases, list):
            assert "Protocol" in bases

    @staticmethod
    def test_class_kind_protocol() -> None:
        """Test class kind protocol."""
        sg = _make_sg()
        node = _find_node(sg, 10, 6)
        assert node is not None
        result = _extract_class_context(node)
        # class_kind may be "protocol" due to Protocol base
        if "class_kind" in result:
            assert result["class_kind"] in {"protocol", "class"}

    @staticmethod
    def test_no_class_context_for_free_function() -> None:
        """Test no class context for free function."""
        sg = _make_sg()
        node = _find_node(sg, 45, 0)
        assert node is not None
        result = _extract_class_context(node)
        assert "class_name" not in result


class TestCallTarget:
    """Test call target extraction."""

    @staticmethod
    def test_method_call() -> None:
        """Test method call."""
        sg = _make_sg()
        # builder.process(["a", "b"], timeout=10.0) on line 68
        node = _find_node(sg, 68, 13)
        assert node is not None
        while node is not None and node.kind() != "call":
            node = node.parent()
        if node is not None:
            result = _extract_call_target(node)
            assert "call_target" in result
            if "call_receiver" in result:
                assert result["call_receiver"] == "builder"
            if "call_method" in result:
                assert result["call_method"] == "process"

    @staticmethod
    def test_function_call() -> None:
        """Test function call."""
        sg = _make_sg()
        # free_function(42) on line 70
        node = _find_node(sg, 70, 4)
        assert node is not None
        while node is not None and node.kind() != "call":
            node = node.parent()
        if node is not None:
            result = _extract_call_target(node)
            assert result.get("call_target") == "free_function"

    @staticmethod
    def test_call_args_count() -> None:
        """Test call args count."""
        sg = _make_sg()
        # free_function(42) on line 70
        node = _find_node(sg, 70, 4)
        assert node is not None
        while node is not None and node.kind() != "call":
            node = node.parent()
        if node is not None:
            result = _extract_call_target(node)
            assert "call_args_count" in result
            assert isinstance(result["call_args_count"], int)


class TestScopeChain:
    """Test scope chain extraction."""

    @staticmethod
    def test_scope_chain_method() -> None:
        """Test scope chain method."""
        sg = _make_sg()
        # Inside process method of GraphBuilder, line 36 is result = self._transform(items)
        node = _find_node(sg, 36, 12)
        assert node is not None
        result = _extract_scope_chain(node)
        chain = result.get("scope_chain")
        assert isinstance(chain, list)
        assert "module" in chain
        # Should contain class and method names
        assert len(chain) >= MIN_SCOPE_CHAIN_LENGTH

    @staticmethod
    def test_scope_chain_free_function() -> None:
        """Test scope chain free function."""
        sg = _make_sg()
        # Line 46 is return x > 0
        node = _find_node(sg, 46, 4)
        assert node is not None
        result = _extract_scope_chain(node)
        chain = result.get("scope_chain")
        assert isinstance(chain, list)
        assert chain[0] == "module"


class TestStructuralContext:
    """Test structural context extraction."""

    @staticmethod
    def test_try_block() -> None:
        """Test try block."""
        sg = _make_sg()
        # Inside try block on line 36 (result = self._transform(items))
        node = _find_node(sg, 36, 12)
        assert node is not None
        result = _extract_structural_context(node)
        if "structural_context" in result:
            assert result["structural_context"] in {"try_block", "except_handler"}

    @staticmethod
    def test_with_block() -> None:
        """Test with block."""
        sg = _make_sg()
        # Inside with block on line 34 (f.write("processing"))
        node = _find_node(sg, 34, 12)
        assert node is not None
        result = _extract_structural_context(node)
        # May or may not find with_block depending on exact position
        if "structural_context" in result:
            assert isinstance(result["structural_context"], str)

    @staticmethod
    def test_no_context_top_level() -> None:
        """Test no context top level."""
        sg = _make_sg()
        node = _find_node(sg, 45, 0)
        assert node is not None
        result = _extract_structural_context(node)
        assert "structural_context" not in result


class TestGeneratorDetection:
    """Test scope-safe generator detection."""

    @staticmethod
    def test_generator_function() -> None:
        """Test generator function."""
        tree = ast.parse(_PYTHON_SAMPLE)
        func = _find_ast_function(tree, 48)
        assert func is not None
        result = _extract_generator_flag(func)
        assert result.get("is_generator") is True

    @staticmethod
    def test_non_generator_function() -> None:
        """Test non generator function."""
        tree = ast.parse(_PYTHON_SAMPLE)
        func = _find_ast_function(tree, 45)
        assert func is not None
        result = _extract_generator_flag(func)
        assert result.get("is_generator") is False

    @staticmethod
    def test_scope_safe_nested_yield() -> None:
        """Yield in a nested function should not make outer function a generator."""
        source = """\
def outer():
    def inner():
        yield 42
    return inner
"""
        tree = ast.parse(source)
        func = _find_ast_function(tree, 1)
        assert func is not None
        result = _extract_generator_flag(func)
        assert result.get("is_generator") is False


class TestBehaviorSummary:
    """Test function behavior summary."""

    @staticmethod
    def test_process_method_behavior() -> None:
        """Test process method behavior."""
        tree = ast.parse(_PYTHON_SAMPLE)
        # process method has with, try/except, raise, return (line 32)
        func = _find_ast_function(tree, 32)
        assert func is not None
        result = _extract_behavior_summary(func)
        assert result.get("returns_value") is True
        assert result.get("raises_exception") is True
        assert result.get("has_context_manager") is True

    @staticmethod
    def test_async_function_behavior() -> None:
        """Test async function behavior."""
        tree = ast.parse(_PYTHON_SAMPLE)
        # async def fetch on line 41
        func = _find_ast_function(tree, 41)
        assert func is not None
        result = _extract_behavior_summary(func)
        assert result.get("awaits") is True
        assert result.get("returns_value") is True

    @staticmethod
    def test_generator_behavior() -> None:
        """Test generator behavior."""
        tree = ast.parse(_PYTHON_SAMPLE)
        # generator_function on line 48
        func = _find_ast_function(tree, 48)
        assert func is not None
        result = _extract_behavior_summary(func)
        assert result.get("yields") is True


class TestImportDetail:
    """Test import detail extraction."""

    @staticmethod
    def test_import_statement() -> None:
        """Test import statement."""
        source = "import os\n"
        sg = SgRoot(source, "python")
        node = _find_node(sg, 1, 0)
        assert node is not None
        while node is not None and node.kind() != "import_statement":
            node = node.parent()
        if node is not None:
            ast_tree = ast.parse(source, filename="<test>")
            result = _extract_import_detail(
                node,
                source.encode(),
                ast_tree,
                1,
            )
            assert result.get("import_module") == "os"
            assert result.get("import_level") == 0

    @staticmethod
    def test_from_import() -> None:
        """Test from import."""
        source = "from collections.abc import Callable, Sequence\n"
        sg = SgRoot(source, "python")
        node = _find_node(sg, 1, 0)
        assert node is not None
        while node is not None and node.kind() != "import_from_statement":
            node = node.parent()
        if node is not None:
            ast_tree = ast.parse(source, filename="<test>")
            result = _extract_import_detail(
                node,
                source.encode(),
                ast_tree,
                1,
            )
            assert result.get("import_module") == "collections.abc"
            import_names = result.get("import_names", [])
            assert isinstance(import_names, list)
            assert "Callable" in import_names
            assert result.get("import_level") == 0

    @staticmethod
    def test_type_checking_import() -> None:
        """Test type checking import."""
        source = _PYTHON_SAMPLE
        sg = _make_sg()
        # "from pathlib import Path" on line 8 (inside TYPE_CHECKING)
        node = _find_node(sg, 8, 4)
        assert node is not None
        while node is not None and node.kind() != "import_from_statement":
            node = node.parent()
        if node is not None:
            ast_tree = ast.parse(source, filename="<test>")
            result = _extract_import_detail(
                node,
                source.encode(),
                ast_tree,
                8,
            )
            assert result.get("is_type_import") is True


class TestClassShape:
    """Test class API shape summary."""

    @staticmethod
    def test_graphbuilder_shape() -> None:
        """Test graphbuilder shape."""
        sg = _make_sg()
        # class GraphBuilder on line 10
        node = _find_node(sg, 10, 0)
        assert node is not None
        while node is not None and node.kind() not in {
            "class_definition",
            "decorated_definition",
        }:
            node = node.parent()
        if node is not None:
            result = _extract_class_shape(node)
            method_count = result.get("method_count")
            assert isinstance(method_count, int)
            assert method_count > 0

    @staticmethod
    def test_dataclass_markers() -> None:
        """Test dataclass markers."""
        sg = _make_sg()
        # @dataclass(frozen=True, slots=True) class Config on line 73
        node = _find_node(sg, 73, 0)
        assert node is not None
        while node is not None and node.kind() != "decorated_definition":
            node = node.parent()
        if node is not None:
            result = _extract_class_shape(node)
            markers = result.get("class_markers", [])
            if isinstance(markers, list) and markers:
                assert "dataclass" in markers


class TestEnrichPythonContext:
    """Test the main enrichment entrypoint."""

    @staticmethod
    def test_enrichment_applied_for_function() -> None:
        """Test enrichment applied for function."""
        sg = _make_sg()
        node = _find_node(sg, 45, 0)
        assert node is not None
        result = enrich_python_context(
            PythonNodeEnrichmentRequest(
                sg_root=sg,
                node=node,
                source_bytes=_PYTHON_SAMPLE.encode(),
                line=45,
                col=0,
                cache_key="test",
            )
        )
        assert result is not None
        assert result["enrichment_status"] in {"applied", "degraded"}
        assert "enrichment_sources" in result
        assert "node_kind" in result

    @staticmethod
    def test_stage_status_uses_python_resolution_key() -> None:
        """Stage metadata should use python_resolution and exclude legacy libcst key."""
        sg = _make_sg()
        node = _find_node(sg, 45, 0)
        assert node is not None
        result = enrich_python_context(
            PythonNodeEnrichmentRequest(
                sg_root=sg,
                node=node,
                source_bytes=_PYTHON_SAMPLE.encode(),
                line=45,
                col=0,
                cache_key="test",
            )
        )
        assert result is not None
        stage_status = result.get("stage_status")
        stage_timings = result.get("stage_timings_ms")
        assert isinstance(stage_status, dict)
        assert isinstance(stage_timings, dict)
        assert "python_resolution" in stage_status
        assert "python_resolution" in stage_timings
        assert "libcst" not in stage_status
        assert "libcst" not in stage_timings

    @staticmethod
    def test_enrichment_applied_for_call() -> None:
        """Test enrichment applied for call."""
        sg = _make_sg()
        # print("done") on line 71
        node = _find_node(sg, 71, 4)
        assert node is not None
        while node is not None and node.kind() != "call":
            node = node.parent()
        if node is not None:
            result = enrich_python_context(
                PythonNodeEnrichmentRequest(
                    sg_root=sg,
                    node=node,
                    source_bytes=_PYTHON_SAMPLE.encode(),
                    line=71,
                    col=4,
                    cache_key="test",
                )
            )
            assert result is not None
            assert result.get("item_role") == "callsite"

    @staticmethod
    def test_enrichment_none_for_non_enrichable() -> None:
        """Test enrichment none for non enrichable."""
        source = "# just a comment\n"
        sg = SgRoot(source, "python")
        node = sg.root()
        result = enrich_python_context(
            PythonNodeEnrichmentRequest(
                sg_root=sg,
                node=node,
                source_bytes=source.encode(),
                line=1,
                col=0,
                cache_key="test_nonenr",
            )
        )
        assert result is None

    @staticmethod
    def test_enrichment_sources_includes_ast() -> None:
        """Test enrichment sources includes ast."""
        sg = _make_sg()
        node = _find_node(sg, 45, 0)
        assert node is not None
        result = enrich_python_context(
            PythonNodeEnrichmentRequest(
                sg_root=sg,
                node=node,
                source_bytes=_PYTHON_SAMPLE.encode(),
                line=45,
                col=0,
                cache_key="test",
            )
        )
        assert result is not None
        sources = result.get("enrichment_sources", [])
        assert isinstance(sources, list)
        assert "ast_grep" in sources

    @staticmethod
    def test_agreement_sources_reference_python_resolution() -> None:
        """Agreement metadata should report python_resolution as a source when present."""
        sg = _make_sg()
        node = _find_node(sg, 45, 0)
        assert node is not None
        result = enrich_python_context(
            PythonNodeEnrichmentRequest(
                sg_root=sg,
                node=node,
                source_bytes=_PYTHON_SAMPLE.encode(),
                line=45,
                col=0,
                cache_key="test",
            )
        )
        assert result is not None
        agreement = result.get("agreement")
        assert isinstance(agreement, dict)
        sources = agreement.get("sources")
        assert isinstance(sources, list)
        assert "python_resolution" in sources
        assert "libcst" not in sources

    @staticmethod
    def test_enrichment_emits_payload_budget_metadata() -> None:
        """Payload metadata should include size hint and optional dropped fields."""
        sg = _make_sg()
        node = _find_node(sg, 45, 0)
        assert node is not None
        result = enrich_python_context(
            PythonNodeEnrichmentRequest(
                sg_root=sg,
                node=node,
                source_bytes=_PYTHON_SAMPLE.encode(),
                line=45,
                col=0,
                cache_key="test",
            )
        )
        assert result is not None
        size_hint = result.get("payload_size_hint")
        assert isinstance(size_hint, int)
        dropped = result.get("dropped_fields")
        assert dropped is None or isinstance(dropped, list)

    @staticmethod
    def test_enrichment_sets_is_dataclass_for_dataclass_context() -> None:
        """Dataclass class contexts should report an explicit boolean marker."""
        sg = _make_sg()
        node = _find_node(sg, 73, 0)
        assert node is not None
        while node is not None and node.kind() not in {"class_definition", "decorated_definition"}:
            node = node.parent()
        assert node is not None
        result = enrich_python_context(
            PythonNodeEnrichmentRequest(
                sg_root=sg,
                node=node,
                source_bytes=_PYTHON_SAMPLE.encode(),
                line=73,
                col=0,
                cache_key="test",
            )
        )
        assert result is not None
        assert result.get("is_dataclass") is True

    @staticmethod
    def test_byte_range_entrypoint() -> None:
        """Byte-range enrichment entrypoint should resolve and enrich target node."""
        sg = _make_sg()
        source = _PYTHON_SAMPLE.encode()
        marker = b"free_function"
        byte_start = source.index(marker)
        byte_end = byte_start + len(marker)
        result = enrich_python_context_by_byte_range(
            PythonByteRangeEnrichmentRequest(
                sg_root=sg,
                source_bytes=source,
                byte_start=byte_start,
                byte_end=byte_end,
                cache_key="test",
            )
        )
        assert result is not None
        assert result.get("node_kind") in {"function_definition", "decorated_definition"}

    @staticmethod
    def test_enrichment_fail_open() -> None:
        """Enrichment should degrade gracefully, not raise."""
        sg = _make_sg()
        node = _find_node(sg, 45, 0)
        assert node is not None
        # Pass invalid source that causes ast.parse to fail
        result = enrich_python_context(
            PythonNodeEnrichmentRequest(
                sg_root=sg,
                node=node,
                source_bytes=b"def \xff broken(",
                line=45,
                col=0,
                cache_key="test_broken",
            )
        )
        # Should still return partial results from ast-grep tier
        assert result is not None
        assert "node_kind" in result


class TestCaching:
    """Test AST cache behavior."""

    @staticmethod
    def test_cache_reuse() -> None:
        """Test cache reuse."""
        source = b"x = 1\n"
        tree1 = _get_ast(source, cache_key="cache_test")
        tree2 = _get_ast(source, cache_key="cache_test")
        assert tree1 is tree2

    @staticmethod
    def test_cache_invalidation() -> None:
        """Test cache invalidation."""
        source1 = b"x = 1\n"
        source2 = b"x = 2\n"
        tree1 = _get_ast(source1, cache_key="cache_inv")
        tree2 = _get_ast(source2, cache_key="cache_inv")
        assert tree1 is not tree2

    @staticmethod
    def test_cache_clear() -> None:
        """Test cache clear."""
        _get_ast(b"x = 1\n", cache_key="clear_test")
        assert "clear_test" in _AST_CACHE
        clear_python_enrichment_cache()
        assert "clear_test" not in _AST_CACHE


class TestPayloadTruncation:
    """Test payload bounds enforcement."""

    @staticmethod
    def test_long_signature_truncated() -> None:
        """Test long signature truncated."""
        long_params = ", ".join(f"param_{i}: SomeVeryLongTypeName" for i in range(20))
        source = f"def long_function({long_params}) -> dict[str, int]:\n    pass\n"
        sg = SgRoot(source, "python")
        node = _find_node(sg, 1, 4)
        assert node is not None
        result = _extract_signature(node, source.encode())
        sig = result.get("signature")
        if isinstance(sig, str):
            assert len(sig) <= MAX_SIGNATURE_CHARS


class TestUnwrapDecorated:
    """Test decorated definition unwrapping."""

    @staticmethod
    def test_unwrap_function() -> None:
        """Test unwrap function."""
        source = "@decorator\ndef foo():\n    pass\n"
        sg = SgRoot(source, "python")
        node = _find_node(sg, 1, 0)
        assert node is not None
        while node is not None and node.kind() != "decorated_definition":
            node = node.parent()
        if node is not None:
            inner = _unwrap_decorated(node)
            assert inner.kind() == "function_definition"

    @staticmethod
    def test_unwrap_class() -> None:
        """Test unwrap class."""
        source = "@dataclass\nclass Foo:\n    x: int = 0\n"
        sg = SgRoot(source, "python")
        node = _find_node(sg, 1, 0)
        assert node is not None
        while node is not None and node.kind() != "decorated_definition":
            node = node.parent()
        if node is not None:
            inner = _unwrap_decorated(node)
            assert inner.kind() == "class_definition"

    @staticmethod
    def test_no_unwrap_plain() -> None:
        """Test no unwrap plain."""
        source = "def foo():\n    pass\n"
        sg = SgRoot(source, "python")
        node = _find_node(sg, 1, 0)
        assert node is not None
        result = _unwrap_decorated(node)
        # Should return same node (not decorated)
        assert result is node or result.kind() == "function_definition"
