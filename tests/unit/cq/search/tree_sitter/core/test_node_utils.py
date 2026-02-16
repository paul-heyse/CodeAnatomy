"""Unit tests for node_utils module."""

from __future__ import annotations

from dataclasses import dataclass

from tools.cq.search.tree_sitter.core.node_utils import (
    NodeLike,
    node_byte_span,
    node_text,
)

TRUNCATED_TEXT_LENGTH = 10


@dataclass
class MockNode:
    """Mock node-like object for testing."""

    start_byte: int
    end_byte: int
    start_point: tuple[int, int] = (0, 0)
    end_point: tuple[int, int] = (0, 0)
    type: str = "identifier"

    def child_by_field_name(self, _name: str, /) -> MockNode | None:
        """Return None for mock node."""
        _ = self
        return None


class TestNodeText:
    """Test node_text function."""

    @staticmethod
    def test_extracts_text_from_node() -> None:
        """Extract text from a mock node."""
        source = b"def foo():\n    pass"
        node = MockNode(0, 10)
        result = node_text(node, source)
        assert result == "def foo():"

    @staticmethod
    def test_returns_empty_string_for_none() -> None:
        """Return empty string when node is None."""
        source = b"def foo():\n    pass"
        result = node_text(None, source)
        assert not result

    @staticmethod
    def test_returns_empty_string_for_empty_span() -> None:
        """Return empty string when node has empty span."""
        source = b"def foo():\n    pass"
        node = MockNode(5, 5)
        result = node_text(node, source)
        assert not result

    @staticmethod
    def test_returns_empty_string_for_negative_span() -> None:
        """Return empty string when node has negative span."""
        source = b"def foo():\n    pass"
        node = MockNode(10, 5)
        result = node_text(node, source)
        assert not result

    @staticmethod
    def test_strips_whitespace_by_default() -> None:
        """Strip leading/trailing whitespace by default."""
        source = b"  def foo():  "
        node = MockNode(0, 13)
        result = node_text(node, source)
        assert result == "def foo():"

    @staticmethod
    def test_preserves_whitespace_when_strip_false() -> None:
        """Preserve whitespace when strip=False."""
        source = b"  def foo():  "
        node = MockNode(0, 14)
        result = node_text(node, source, strip=False)
        assert result == "  def foo():  "

    @staticmethod
    def test_truncates_when_max_len_exceeded() -> None:
        """Truncate text and add ellipsis when max_len exceeded."""
        source = b"def very_long_function_name():"
        node = MockNode(0, 30)
        result = node_text(node, source, max_len=TRUNCATED_TEXT_LENGTH)
        assert result == "def ver..."
        assert len(result) == TRUNCATED_TEXT_LENGTH

    @staticmethod
    def test_no_truncation_when_under_max_len() -> None:
        """Do not truncate when text is under max_len."""
        source = b"def foo():"
        node = MockNode(0, 10)
        result = node_text(node, source, max_len=100)
        assert result == "def foo():"

    @staticmethod
    def test_handles_unicode_decode_errors() -> None:
        """Handle invalid UTF-8 sequences gracefully."""
        source = b"def \xff\xfe foo():"
        node = MockNode(0, 14)
        result = node_text(node, source)
        assert "def" in result
        assert "foo" in result


class TestNodeByteSpan:
    """Test node_byte_span function."""

    @staticmethod
    def test_extracts_byte_span_from_node() -> None:
        """Extract byte span from a mock node."""
        node = MockNode(10, 20)
        result = node_byte_span(node)
        assert result == (10, 20)

    @staticmethod
    def test_returns_zero_span_for_none() -> None:
        """Return (0, 0) when node is None."""
        result = node_byte_span(None)
        assert result == (0, 0)

    @staticmethod
    def test_ensures_end_not_before_start() -> None:
        """Ensure end_byte is not before start_byte."""
        node = MockNode(20, 10)
        result = node_byte_span(node)
        assert result == (20, 20)

    @staticmethod
    def test_handles_missing_attributes() -> None:
        """Handle objects missing start_byte/end_byte attributes."""

        class EmptyObject:
            pass

        obj = EmptyObject()
        result = node_byte_span(obj)
        assert result == (0, 0)


class TestNodeLikeProtocol:
    """Test NodeLike protocol."""

    @staticmethod
    def test_mock_node_satisfies_protocol() -> None:
        """Verify MockNode satisfies NodeLike protocol."""
        node = MockNode(0, 10)
        assert isinstance(node, NodeLike)

    @staticmethod
    def test_protocol_requires_all_properties() -> None:
        """Verify NodeLike protocol checks all required properties."""

        class IncompleteNode:
            def __init__(self) -> None:
                self.start_byte = 0
                self.end_byte = 10

        node = IncompleteNode()
        # Missing start_point and end_point, so it doesn't satisfy the protocol
        assert not isinstance(node, NodeLike)

    @staticmethod
    def test_protocol_with_complete_node() -> None:
        """Verify complete node implementation satisfies protocol."""

        class CompleteNode:
            def __init__(self) -> None:
                self.start_byte = 0
                self.end_byte = 10
                self.start_point = (0, 0)
                self.end_point = (0, 10)
                self.type = "identifier"

            def child_by_field_name(self, _name: str, /) -> NodeLike | None:
                _ = self
                return None

        node = CompleteNode()
        assert isinstance(node, NodeLike)
