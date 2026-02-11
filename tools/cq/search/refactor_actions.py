"""Edit and refactor action plane for CQ enrichment.

This module provides typed structures and stub functions for LSP code actions,
diagnostics, and workspace edits. All functions are capability-gated and fail-open.
"""

from __future__ import annotations

from tools.cq.core.structs import CqStruct


class PrepareRenameResultV1(CqStruct, frozen=True):
    """Result of textDocument/prepareRename request.

    Parameters
    ----------
    range_start_line
        Start line of the rename range (0-indexed).
    range_start_col
        Start column of the rename range.
    range_end_line
        End line of the rename range (0-indexed).
    range_end_col
        End column of the rename range.
    placeholder
        Placeholder text for rename operation.
    can_rename
        Whether the symbol can be renamed.
    """

    range_start_line: int = 0
    range_start_col: int = 0
    range_end_line: int = 0
    range_end_col: int = 0
    placeholder: str = ""
    can_rename: bool = True


class DiagnosticItemV1(CqStruct, frozen=True):
    """Normalized diagnostic with action-bridge fidelity fields.

    Parameters
    ----------
    uri
        Document URI.
    message
        Diagnostic message.
    severity
        Severity level (1=Error, 2=Warning, 3=Info, 4=Hint).
    code
        Diagnostic code (e.g., "E0425" for Rust).
    code_description_href
        URL to diagnostic code documentation.
    tags
        Diagnostic tags (1=Unnecessary, 2=Deprecated).
    version
        Document version when diagnostic was generated.
    related_information
        Related diagnostic information (locations, messages).
    data
        Server-specific diagnostic data for code action bridging.
    """

    uri: str
    message: str
    severity: int = 0
    code: str | None = None
    code_description_href: str | None = None
    tags: tuple[int, ...] = ()
    version: int | None = None
    related_information: tuple[dict[str, object], ...] = ()
    data: dict[str, object] | None = None


class CodeActionV1(CqStruct, frozen=True):
    """Normalized code action from LSP server.

    Parameters
    ----------
    title
        Code action title (e.g., "Extract function").
    kind
        Code action kind (e.g., "quickfix", "refactor.extract").
    is_preferred
        Whether this is the preferred action.
    diagnostics
        Diagnostics this action addresses.
    disabled_reason
        Reason the action is disabled (if applicable).
    is_resolvable
        Whether the action requires resolution via codeAction/resolve.
    has_edit
        Whether the action includes a workspace edit.
    has_command
        Whether the action includes a command.
    command_id
        Command identifier for workspace/executeCommand.
    has_snippet_text_edits
        Whether the action includes snippet text edits (RA extension).
    """

    title: str
    kind: str | None = None
    is_preferred: bool = False
    diagnostics: tuple[dict[str, object], ...] = ()
    disabled_reason: str | None = None
    is_resolvable: bool = False
    has_edit: bool = False
    has_command: bool = False
    command_id: str | None = None
    has_snippet_text_edits: bool = False


class DocumentChangeV1(CqStruct, frozen=True):
    """Single document change within a workspace edit.

    Parameters
    ----------
    uri
        Document URI.
    kind
        Change kind: "edit", "create", "rename", or "delete".
    edit_count
        Number of edits in this document change.
    """

    uri: str
    kind: str = "edit"
    edit_count: int = 0


class WorkspaceEditV1(CqStruct, frozen=True):
    """Normalized workspace edit from code action or refactor operation.

    Parameters
    ----------
    document_changes
        Document changes in this workspace edit.
    change_count
        Total number of changes across all documents.
    """

    document_changes: tuple[DocumentChangeV1, ...] = ()
    change_count: int = 0


def pull_document_diagnostics(
    session: object,
    uri: str,
) -> tuple[DiagnosticItemV1, ...] | None:
    """Fetch diagnostics via textDocument/diagnostic when supported.

    Parameters
    ----------
    session
        LSP session (_PyreflyLspSession or _RustLspSession).
    uri
        Document URI.

    Returns:
    -------
    tuple[DiagnosticItemV1, ...] | None
        Document diagnostics, or None if unavailable.
    """


def pull_workspace_diagnostics(session: object) -> tuple[DiagnosticItemV1, ...] | None:
    """Fetch diagnostics via workspace/diagnostic when supported.

    Parameters
    ----------
    session
        LSP session (_PyreflyLspSession or _RustLspSession).

    Returns:
    -------
    tuple[DiagnosticItemV1, ...] | None
        Workspace diagnostics, or None if unavailable.
    """


def resolve_code_action(session: object, action: CodeActionV1) -> CodeActionV1 | None:
    """Resolve deferred code action payloads via codeAction/resolve.

    Parameters
    ----------
    session
        LSP session (_PyreflyLspSession or _RustLspSession).
    action
        Code action to resolve.

    Returns:
    -------
    CodeActionV1 | None
        Resolved code action with full payload, or None if unavailable.
    """


def execute_code_action_command(session: object, action: CodeActionV1) -> bool:
    """Execute bound command via workspace/executeCommand.

    Parameters
    ----------
    session
        LSP session (_PyreflyLspSession or _RustLspSession).
    action
        Code action with command to execute.

    Returns:
    -------
    bool
        True if command executed successfully, False otherwise.
    """
    _ = session
    _ = action
    return False


__all__ = [
    "CodeActionV1",
    "DiagnosticItemV1",
    "DocumentChangeV1",
    "PrepareRenameResultV1",
    "WorkspaceEditV1",
    "execute_code_action_command",
    "pull_document_diagnostics",
    "pull_workspace_diagnostics",
    "resolve_code_action",
]
