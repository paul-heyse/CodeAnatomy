# ruff: noqa: ANN202,PLR0914
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
    raw_payload: dict[str, object] | None = None


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
    from tools.cq.search.diagnostics_pull import pull_text_document_diagnostics

    rows = pull_text_document_diagnostics(session, uri=uri)
    if rows is None:
        return None
    return tuple(_diagnostic_from_row(row) for row in rows)


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
    from tools.cq.search.diagnostics_pull import pull_workspace_diagnostics as pull_workspace

    rows = pull_workspace(session)
    if rows is None:
        return None
    return tuple(_diagnostic_from_row(row) for row in rows)


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
    request = _request_fn(session)
    if request is None:
        return None
    if not action.is_resolvable:
        return action
    if not _supports_code_action_resolve(session):
        return action

    payload = action.raw_payload or {"title": action.title}
    try:
        resolved = request("codeAction/resolve", payload)
    except Exception:  # noqa: BLE001 - fail-open by design
        return None
    if not isinstance(resolved, dict):
        return None
    return _normalize_code_action(resolved)


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
    request = _request_fn(session)
    if request is None:
        return False
    if not action.command_id:
        return False

    command_args = None
    if action.raw_payload is not None:
        command = action.raw_payload.get("command")
        if isinstance(command, dict):
            args = command.get("arguments")
            if isinstance(args, list):
                command_args = args
    params: dict[str, object] = {"command": action.command_id}
    if command_args is not None:
        params["arguments"] = command_args

    try:
        request("workspace/executeCommand", params)
    except Exception:  # noqa: BLE001 - fail-open by design
        return False
    return True


def _request_fn(session: object):
    request = getattr(session, "_send_request", None)
    if callable(request):
        return request
    return None


def _supports_code_action_resolve(session: object) -> bool:
    env = getattr(session, "_session_env", None)
    caps = getattr(env, "capabilities", None)
    server_caps = getattr(caps, "server_caps", None)
    if server_caps is None:
        return False

    provider_raw = getattr(server_caps, "code_action_provider_raw", None)
    if isinstance(provider_raw, dict):
        return bool(provider_raw.get("resolveProvider"))
    return bool(getattr(server_caps, "code_action_provider", False))


def _diagnostic_from_row(row: dict[str, object]) -> DiagnosticItemV1:
    uri_raw = row.get("uri")
    uri = uri_raw if isinstance(uri_raw, str) else ""
    message_raw = row.get("message")
    message = message_raw if isinstance(message_raw, str) else ""

    severity_raw = row.get("severity")
    severity = severity_raw if isinstance(severity_raw, int) else 0

    code_raw = row.get("code")
    code = code_raw if isinstance(code_raw, str) else None

    href_raw = row.get("code_description_href")
    code_description_href = href_raw if isinstance(href_raw, str) else None

    version_raw = row.get("version")
    version = version_raw if isinstance(version_raw, int) else None

    tags_raw = row.get("tags")
    tags: tuple[int, ...] = ()
    if isinstance(tags_raw, (tuple, list)):
        tags = tuple(tag for tag in tags_raw if isinstance(tag, int))

    related_raw = row.get("related_information")
    related_information: tuple[dict[str, object], ...] = ()
    if isinstance(related_raw, (tuple, list)):
        related_information = tuple(info for info in related_raw if isinstance(info, dict))

    return DiagnosticItemV1(
        uri=uri,
        message=message,
        severity=severity,
        code=code,
        code_description_href=code_description_href,
        tags=tags,
        version=version,
        related_information=related_information,
        data=dict(data) if isinstance((data := row.get("data")), dict) else None,
    )


def _normalize_code_action(payload: dict[str, object]) -> CodeActionV1:
    title = payload.get("title")
    if not isinstance(title, str):
        title = ""

    command = payload.get("command")
    command_id = None
    if isinstance(command, dict):
        command_value = command.get("command")
        if isinstance(command_value, str):
            command_id = command_value

    diagnostics = payload.get("diagnostics")
    diagnostics_rows: tuple[dict[str, object], ...] = ()
    if isinstance(diagnostics, list):
        diagnostics_rows = tuple(row for row in diagnostics if isinstance(row, dict))

    disabled = payload.get("disabled")
    disabled_reason = None
    if isinstance(disabled, dict):
        reason = disabled.get("reason")
        if isinstance(reason, str):
            disabled_reason = reason

    edit = payload.get("edit")
    has_snippet_text_edits = False
    if isinstance(edit, dict):
        document_changes = edit.get("documentChanges")
        if isinstance(document_changes, list):
            has_snippet_text_edits = any(
                isinstance(change, dict) and bool(change.get("snippetTextEdit"))
                for change in document_changes
            )

    return CodeActionV1(
        title=title,
        kind=kind if isinstance((kind := payload.get("kind")), str) else None,
        is_preferred=bool(payload.get("isPreferred")),
        diagnostics=diagnostics_rows,
        disabled_reason=disabled_reason,
        is_resolvable=bool(payload.get("data") is not None or payload.get("edit") is None),
        has_edit=isinstance(edit, dict),
        has_command=command_id is not None,
        command_id=command_id,
        has_snippet_text_edits=has_snippet_text_edits,
        raw_payload=dict(payload),
    )


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
