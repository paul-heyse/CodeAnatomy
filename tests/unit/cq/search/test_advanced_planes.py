"""Tests for advanced evidence plane structures and stubs."""

from __future__ import annotations

import msgspec
from tools.cq.search.refactor_actions import (
    CodeActionV1,
    DiagnosticItemV1,
    DocumentChangeV1,
    PrepareRenameResultV1,
    WorkspaceEditV1,
    execute_code_action_command,
    pull_document_diagnostics,
    pull_workspace_diagnostics,
    resolve_code_action,
)
from tools.cq.search.rust_extensions import (
    RustMacroExpansionV1,
    RustRunnableV1,
    expand_macro,
    get_runnables,
)
from tools.cq.search.semantic_overlays import (
    InlayHintV1,
    SemanticTokenBundleV1,
    SemanticTokenSpanV1,
    fetch_inlay_hints_range,
    fetch_semantic_tokens_range,
)


class TestSemanticTokenStructs:
    """Test semantic token structures."""

    def test_semantic_token_span_roundtrip(self) -> None:
        """Test SemanticTokenSpanV1 msgspec roundtrip."""
        token = SemanticTokenSpanV1(
            line=10,
            start_char=5,
            length=8,
            token_type="function",
            modifiers=("declaration", "definition"),
        )
        encoded = msgspec.json.encode(token)
        decoded = msgspec.json.decode(encoded, type=SemanticTokenSpanV1)
        assert decoded == token

    def test_semantic_token_span_defaults(self) -> None:
        """Test SemanticTokenSpanV1 default values."""
        token = SemanticTokenSpanV1(
            line=0,
            start_char=0,
            length=0,
            token_type="variable",
        )
        assert token.modifiers == ()

    def test_semantic_token_bundle_roundtrip(self) -> None:
        """Test SemanticTokenBundleV1 msgspec roundtrip."""
        bundle = SemanticTokenBundleV1(
            position_encoding="utf-8",
            legend_token_types=("function", "variable", "class"),
            legend_token_modifiers=("declaration", "readonly"),
            result_id="abc123",
            tokens=(
                SemanticTokenSpanV1(
                    line=0,
                    start_char=0,
                    length=3,
                    token_type="function",
                ),
            ),
            raw_data=(0, 0, 3, 0, 0),
        )
        encoded = msgspec.json.encode(bundle)
        decoded = msgspec.json.decode(encoded, type=SemanticTokenBundleV1)
        assert decoded == bundle

    def test_semantic_token_bundle_defaults(self) -> None:
        """Test SemanticTokenBundleV1 default values."""
        bundle = SemanticTokenBundleV1()
        assert bundle.position_encoding == "utf-16"
        assert bundle.legend_token_types == ()
        assert bundle.legend_token_modifiers == ()
        assert bundle.result_id is None
        assert bundle.previous_result_id is None
        assert bundle.tokens == ()
        assert bundle.raw_data is None

    def test_inlay_hint_roundtrip(self) -> None:
        """Test InlayHintV1 msgspec roundtrip."""
        hint = InlayHintV1(
            line=5,
            character=10,
            label=": String",
            kind="type",
            padding_left=True,
        )
        encoded = msgspec.json.encode(hint)
        decoded = msgspec.json.decode(encoded, type=InlayHintV1)
        assert decoded == hint

    def test_inlay_hint_defaults(self) -> None:
        """Test InlayHintV1 default values."""
        hint = InlayHintV1(line=0, character=0, label="hint")
        assert hint.kind is None
        assert hint.padding_left is False
        assert hint.padding_right is False


class TestRefactorActionStructs:
    """Test refactor action structures."""

    def test_prepare_rename_result_roundtrip(self) -> None:
        """Test PrepareRenameResultV1 msgspec roundtrip."""
        result = PrepareRenameResultV1(
            range_start_line=10,
            range_start_col=5,
            range_end_line=10,
            range_end_col=15,
            placeholder="new_name",
            can_rename=True,
        )
        encoded = msgspec.json.encode(result)
        decoded = msgspec.json.decode(encoded, type=PrepareRenameResultV1)
        assert decoded == result

    def test_prepare_rename_result_defaults(self) -> None:
        """Test PrepareRenameResultV1 default values."""
        result = PrepareRenameResultV1()
        assert result.range_start_line == 0
        assert result.range_start_col == 0
        assert result.range_end_line == 0
        assert result.range_end_col == 0
        assert result.placeholder == ""
        assert result.can_rename is True

    def test_diagnostic_item_roundtrip(self) -> None:
        """Test DiagnosticItemV1 msgspec roundtrip."""
        diagnostic = DiagnosticItemV1(
            uri="file:///path/to/file.py",
            message="undefined variable",
            severity=1,
            code="E0425",
            code_description_href="https://docs.rs/error/E0425",
            tags=(1, 2),
            version=5,
            related_information=({"message": "defined here", "location": {}},),
            data={"fix_available": True},
        )
        encoded = msgspec.json.encode(diagnostic)
        decoded = msgspec.json.decode(encoded, type=DiagnosticItemV1)
        assert decoded == diagnostic

    def test_diagnostic_item_defaults(self) -> None:
        """Test DiagnosticItemV1 default values."""
        diagnostic = DiagnosticItemV1(uri="file:///test.py", message="test")
        assert diagnostic.severity == 0
        assert diagnostic.code is None
        assert diagnostic.code_description_href is None
        assert diagnostic.tags == ()
        assert diagnostic.version is None
        assert diagnostic.related_information == ()
        assert diagnostic.data is None

    def test_code_action_roundtrip(self) -> None:
        """Test CodeActionV1 msgspec roundtrip."""
        action = CodeActionV1(
            title="Extract function",
            kind="refactor.extract",
            is_preferred=True,
            diagnostics=(),
            disabled_reason=None,
            is_resolvable=True,
            has_edit=True,
            has_command=False,
            command_id=None,
            has_snippet_text_edits=False,
        )
        encoded = msgspec.json.encode(action)
        decoded = msgspec.json.decode(encoded, type=CodeActionV1)
        assert decoded == action

    def test_code_action_defaults(self) -> None:
        """Test CodeActionV1 default values."""
        action = CodeActionV1(title="Fix issue")
        assert action.kind is None
        assert action.is_preferred is False
        assert action.diagnostics == ()
        assert action.disabled_reason is None
        assert action.is_resolvable is False
        assert action.has_edit is False
        assert action.has_command is False
        assert action.command_id is None
        assert action.has_snippet_text_edits is False

    def test_document_change_roundtrip(self) -> None:
        """Test DocumentChangeV1 msgspec roundtrip."""
        change = DocumentChangeV1(
            uri="file:///test.py",
            kind="edit",
            edit_count=3,
        )
        encoded = msgspec.json.encode(change)
        decoded = msgspec.json.decode(encoded, type=DocumentChangeV1)
        assert decoded == change

    def test_document_change_defaults(self) -> None:
        """Test DocumentChangeV1 default values."""
        change = DocumentChangeV1(uri="file:///test.py")
        assert change.kind == "edit"
        assert change.edit_count == 0

    def test_workspace_edit_roundtrip(self) -> None:
        """Test WorkspaceEditV1 msgspec roundtrip."""
        edit = WorkspaceEditV1(
            document_changes=(
                DocumentChangeV1(uri="file:///a.py", edit_count=2),
                DocumentChangeV1(uri="file:///b.py", edit_count=1),
            ),
            change_count=3,
        )
        encoded = msgspec.json.encode(edit)
        decoded = msgspec.json.decode(encoded, type=WorkspaceEditV1)
        assert decoded == edit

    def test_workspace_edit_defaults(self) -> None:
        """Test WorkspaceEditV1 default values."""
        edit = WorkspaceEditV1()
        assert edit.document_changes == ()
        assert edit.change_count == 0


class TestRustExtensionStructs:
    """Test Rust extension structures."""

    def test_rust_macro_expansion_roundtrip(self) -> None:
        """Test RustMacroExpansionV1 msgspec roundtrip."""
        expansion = RustMacroExpansionV1(
            name="vec!",
            expansion="Vec::new()",
            expansion_byte_len=11,
        )
        encoded = msgspec.json.encode(expansion)
        decoded = msgspec.json.decode(encoded, type=RustMacroExpansionV1)
        assert decoded == expansion

    def test_rust_macro_expansion_defaults(self) -> None:
        """Test RustMacroExpansionV1 default values."""
        expansion = RustMacroExpansionV1(name="test!", expansion="expanded")
        assert expansion.expansion_byte_len == 0

    def test_rust_runnable_roundtrip(self) -> None:
        """Test RustRunnableV1 msgspec roundtrip."""
        runnable = RustRunnableV1(
            label="test my_test",
            kind="test",
            args=("test", "--", "my_test"),
            location_uri="file:///src/lib.rs",
            location_line=42,
        )
        encoded = msgspec.json.encode(runnable)
        decoded = msgspec.json.decode(encoded, type=RustRunnableV1)
        assert decoded == runnable

    def test_rust_runnable_defaults(self) -> None:
        """Test RustRunnableV1 default values."""
        runnable = RustRunnableV1(label="cargo build", kind="cargo")
        assert runnable.args == ()
        assert runnable.location_uri is None
        assert runnable.location_line == 0


class TestStubFunctions:
    """Test stub functions return correct types."""

    def test_fetch_semantic_tokens_range_stub(self) -> None:
        """Test fetch_semantic_tokens_range stub returns None."""
        result = fetch_semantic_tokens_range(
            session=object(),
            uri="file:///test.py",
            start_line=0,
            end_line=10,
        )
        assert result is None

    def test_fetch_inlay_hints_range_stub(self) -> None:
        """Test fetch_inlay_hints_range stub returns None."""
        result = fetch_inlay_hints_range(
            session=object(),
            uri="file:///test.py",
            start_line=0,
            end_line=10,
        )
        assert result is None

    def test_pull_document_diagnostics_stub(self) -> None:
        """Test pull_document_diagnostics stub returns None."""
        result = pull_document_diagnostics(
            session=object(),
            uri="file:///test.py",
        )
        assert result is None

    def test_pull_workspace_diagnostics_stub(self) -> None:
        """Test pull_workspace_diagnostics stub returns None."""
        result = pull_workspace_diagnostics(session=object())
        assert result is None

    def test_resolve_code_action_stub(self) -> None:
        """Test resolve_code_action stub returns None."""
        action = CodeActionV1(title="Test action")
        result = resolve_code_action(session=object(), action=action)
        assert result is None

    def test_execute_code_action_command_stub(self) -> None:
        """Test execute_code_action_command stub returns False."""
        action = CodeActionV1(title="Test action")
        result = execute_code_action_command(session=object(), action=action)
        assert result is False

    def test_expand_macro_stub(self) -> None:
        """Test expand_macro stub returns None."""
        result = expand_macro(
            session=object(),
            uri="file:///lib.rs",
            line=10,
            col=5,
        )
        assert result is None

    def test_get_runnables_stub(self) -> None:
        """Test get_runnables stub returns empty tuple."""
        result = get_runnables(
            session=object(),
            uri="file:///lib.rs",
        )
        assert result == ()
